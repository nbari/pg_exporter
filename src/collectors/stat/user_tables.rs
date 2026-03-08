use crate::collectors::{Collector, i64_to_f64};
use crate::collectors::util::{
    get_default_database, get_excluded_databases, get_or_create_pool_for_db,
};
use anyhow::{Result, anyhow};
use futures::future::BoxFuture;
use prometheus::{GaugeVec, IntGaugeVec, Opts, Registry};
use sqlx::{PgPool, Row, postgres::PgRow};
use std::time::Duration;
use tokio::task::JoinSet;
use tracing::{debug, error, info_span, instrument};
use tracing_futures::Instrument as _;

/// Mirrors `postgres_exporter`'s `pg_stat_user_tables` collector:
/// Metrics are exported as `pg_stat_user_tables`_* with labels {`datname`, schemaname, relname}.
#[derive(Clone)]
pub struct StatUserTablesCollector {
    // Scan counts (cumulative)
    seq_scan: IntGaugeVec,
    seq_tup_read: IntGaugeVec,
    idx_scan: IntGaugeVec,
    idx_tup_fetch: IntGaugeVec,

    // Tuple change counters (cumulative)
    n_tup_ins: IntGaugeVec,
    n_tup_upd: IntGaugeVec,
    n_tup_del: IntGaugeVec,
    n_tup_hot_upd: IntGaugeVec,

    // Tuple visibility (gauges)
    n_live_tup: IntGaugeVec,
    n_dead_tup: IntGaugeVec,
    n_mod_since_analyze: IntGaugeVec,

    // Last maintenance times as epoch seconds (gauges)
    last_vacuum: IntGaugeVec,
    last_autovacuum: IntGaugeVec,
    last_analyze: IntGaugeVec,
    last_autoanalyze: IntGaugeVec,

    // Maintenance counters (cumulative)
    vacuum_count: IntGaugeVec,
    autovacuum_count: IntGaugeVec,
    analyze_count: IntGaugeVec,
    autoanalyze_count: IntGaugeVec,

    // Sizes
    index_size_bytes: IntGaugeVec,
    table_size_bytes: IntGaugeVec,

    // Bloat metrics (derived from tuple counts and sizes)
    bloat_ratio: GaugeVec,
    dead_tuple_size_bytes: GaugeVec,

    // Autovacuum-specific metrics (Phase 1 enhancement)
    // These metrics enable predictive alerting and prevent wraparound disasters
    
    // Time-based metrics (easier for alerting than epoch timestamps)
    last_autovacuum_seconds_ago: GaugeVec,   // Alert when >86400 (24h) - table not being maintained
    last_autoanalyze_seconds_ago: GaugeVec,  // Track analyze freshness
    never_autovacuumed: IntGaugeVec,         // 1 when the table has never been autovacuumed
    never_autoanalyzed: IntGaugeVec,         // 1 when the table has never been autoanalyzed

    // GOLD METRICS - Predict autovacuum triggers BEFORE they happen
    // Ratio: n_dead_tup / (threshold + scale_factor * n_live_tup)
    // Values: 0.0=clean, 0.8=warning, 1.0=trigger point, >1.0=overdue
    // Use these to prevent transaction ID wraparound emergencies!
    autovacuum_threshold_ratio: GaugeVec,    // THE critical metric for autovacuum monitoring
    autoanalyze_threshold_ratio: GaugeVec,   // Predict when autoanalyze will trigger
}

impl Default for StatUserTablesCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl StatUserTablesCollector {
    /// Creates a new `UserTablesCollector`
    ///
    /// # Panics
    ///
    /// Panics if metric creation fails (should never happen with valid metric names)
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        Self {
            seq_scan: int_metric("pg_stat_user_tables_seq_scan", "Number of sequential scans initiated on this table"),
            seq_tup_read: int_metric("pg_stat_user_tables_seq_tup_read", "Number of live rows fetched by sequential scans"),
            idx_scan: int_metric("pg_stat_user_tables_idx_scan", "Number of index scans initiated on this table"),
            idx_tup_fetch: int_metric("pg_stat_user_tables_idx_tup_fetch", "Number of live rows fetched by index scans"),
            n_tup_ins: int_metric("pg_stat_user_tables_n_tup_ins", "Number of rows inserted"),
            n_tup_upd: int_metric("pg_stat_user_tables_n_tup_upd", "Number of rows updated"),
            n_tup_del: int_metric("pg_stat_user_tables_n_tup_del", "Number of rows deleted"),
            n_tup_hot_upd: int_metric("pg_stat_user_tables_n_tup_hot_upd", "Number of rows HOT updated"),
            n_live_tup: int_metric("pg_stat_user_tables_n_live_tup", "Estimated number of live rows"),
            n_dead_tup: int_metric("pg_stat_user_tables_n_dead_tup", "Estimated number of dead rows"),
            n_mod_since_analyze: int_metric("pg_stat_user_tables_n_mod_since_analyze", "Estimated number of rows changed since last analyze"),
            last_vacuum: int_metric("pg_stat_user_tables_last_vacuum", "Last manual vacuum time (epoch seconds)"),
            last_autovacuum: int_metric("pg_stat_user_tables_last_autovacuum", "Last autovacuum time (epoch seconds)"),
            last_analyze: int_metric("pg_stat_user_tables_last_analyze", "Last manual analyze time (epoch seconds)"),
            last_autoanalyze: int_metric("pg_stat_user_tables_last_autoanalyze", "Last autoanalyze time (epoch seconds)"),
            vacuum_count: int_metric("pg_stat_user_tables_vacuum_count", "Number of times manually vacuumed"),
            autovacuum_count: int_metric("pg_stat_user_tables_autovacuum_count", "Number of times vacuumed by autovacuum"),
            analyze_count: int_metric("pg_stat_user_tables_analyze_count", "Number of times manually analyzed"),
            autoanalyze_count: int_metric("pg_stat_user_tables_autoanalyze_count", "Number of times analyzed by autovacuum"),
            index_size_bytes: int_metric("pg_stat_user_tables_index_size_bytes", "Total disk space used by indexes on this table, in bytes"),
            table_size_bytes: int_metric("pg_stat_user_tables_table_size_bytes", "Total disk space used by this table, in bytes"),
            bloat_ratio: gauge_metric("pg_stat_user_tables_bloat_ratio", "Estimated bloat ratio (dead tuples / total tuples)"),
            dead_tuple_size_bytes: gauge_metric("pg_stat_user_tables_dead_tuple_size_bytes", "Estimated disk space used by dead tuples"),
            last_autovacuum_seconds_ago: gauge_metric("pg_stat_user_tables_last_autovacuum_seconds_ago", "Seconds since last autovacuum (alert when > 86400)"),
            last_autoanalyze_seconds_ago: gauge_metric("pg_stat_user_tables_last_autoanalyze_seconds_ago", "Seconds since last autoanalyze (alert when > 86400)"),
            never_autovacuumed: int_metric("pg_stat_user_tables_never_autovacuumed", "Whether the table has never been autovacuumed (1 = never autovacuumed)"),
            never_autoanalyzed: int_metric("pg_stat_user_tables_never_autoanalyzed", "Whether the table has never been autoanalyzed (1 = never autoanalyzed)"),
            autovacuum_threshold_ratio: gauge_metric("pg_stat_user_tables_autovacuum_threshold_ratio", "Ratio of dead tuples to autovacuum threshold (0.0 clean, 1.0 trigger, >1.0 overdue)"),
            autoanalyze_threshold_ratio: gauge_metric("pg_stat_user_tables_autoanalyze_threshold_ratio", "Ratio of modified tuples to autoanalyze threshold (0.0 clean, 1.0 trigger, >1.0 overdue)"),
        }
    }

    fn reset_metrics(&self) {
        self.seq_scan.reset();
        self.seq_tup_read.reset();
        self.idx_scan.reset();
        self.idx_tup_fetch.reset();
        self.n_tup_ins.reset();
        self.n_tup_upd.reset();
        self.n_tup_del.reset();
        self.n_tup_hot_upd.reset();
        self.n_live_tup.reset();
        self.n_dead_tup.reset();
        self.n_mod_since_analyze.reset();
        self.last_vacuum.reset();
        self.last_autovacuum.reset();
        self.last_analyze.reset();
        self.last_autoanalyze.reset();
        self.vacuum_count.reset();
        self.autovacuum_count.reset();
        self.analyze_count.reset();
        self.autoanalyze_count.reset();
        self.index_size_bytes.reset();
        self.table_size_bytes.reset();
        self.bloat_ratio.reset();
        self.dead_tuple_size_bytes.reset();
        self.last_autovacuum_seconds_ago.reset();
        self.last_autoanalyze_seconds_ago.reset();
        self.never_autovacuumed.reset();
        self.never_autoanalyzed.reset();
        self.autovacuum_threshold_ratio.reset();
        self.autoanalyze_threshold_ratio.reset();
    }
}

const USER_TABLE_LABELS: [&str; 3] = ["datname", "schemaname", "relname"];
const PER_DATABASE_COLLECTION_TIMEOUT: Duration = Duration::from_secs(5);
const TASK_JOIN_WAIT_TIMEOUT: Duration = Duration::from_secs(10);

const STAT_USER_TABLES_QUERY: &str = r"
    SELECT
        current_database() AS datname,
        s.schemaname,
        s.relname,
        s.seq_scan::bigint,
        s.seq_tup_read::bigint,
        s.idx_scan::bigint,
        s.idx_tup_fetch::bigint,
        s.n_tup_ins::bigint,
        s.n_tup_upd::bigint,
        s.n_tup_del::bigint,
        s.n_tup_hot_upd::bigint,
        s.n_live_tup::bigint,
        s.n_dead_tup::bigint,
        s.n_mod_since_analyze::bigint,
        COALESCE(EXTRACT(EPOCH FROM s.last_vacuum)::bigint, 0)       AS last_vacuum_epoch,
        COALESCE(EXTRACT(EPOCH FROM s.last_autovacuum)::bigint, 0)  AS last_autovacuum_epoch,
        COALESCE(EXTRACT(EPOCH FROM s.last_analyze)::bigint, 0)     AS last_analyze_epoch,
        COALESCE(EXTRACT(EPOCH FROM s.last_autoanalyze)::bigint, 0) AS last_autoanalyze_epoch,
        s.vacuum_count::bigint,
        s.autovacuum_count::bigint,
        s.analyze_count::bigint,
        s.autoanalyze_count::bigint,
        pg_indexes_size(s.relid)::bigint AS index_size_bytes,
        pg_table_size(s.relid)::bigint   AS table_size_bytes,
        EXTRACT(EPOCH FROM (now() - s.last_autovacuum)) AS last_autovacuum_seconds_ago,
        EXTRACT(EPOCH FROM (now() - s.last_autoanalyze)) AS last_autoanalyze_seconds_ago,
        CASE WHEN s.last_autovacuum IS NULL THEN 1 ELSE 0 END::bigint AS never_autovacuumed,
        CASE WHEN s.last_autoanalyze IS NULL THEN 1 ELSE 0 END::bigint AS never_autoanalyzed,
        CASE
            WHEN s.n_live_tup > 0 THEN
                s.n_dead_tup::double precision /
                (
                    COALESCE(
                        (
                            SELECT option_value::double precision
                            FROM pg_options_to_table(c.reloptions)
                            WHERE option_name = 'autovacuum_vacuum_threshold'
                        ),
                        current_setting('autovacuum_vacuum_threshold')::double precision
                    ) +
                    COALESCE(
                        (
                            SELECT option_value::double precision
                            FROM pg_options_to_table(c.reloptions)
                            WHERE option_name = 'autovacuum_vacuum_scale_factor'
                        ),
                        current_setting('autovacuum_vacuum_scale_factor')::double precision
                    ) * s.n_live_tup::double precision
                )
            ELSE 0
        END AS autovacuum_threshold_ratio,
        CASE
            WHEN s.n_live_tup > 0 THEN
                s.n_mod_since_analyze::double precision /
                (
                    COALESCE(
                        (
                            SELECT option_value::double precision
                            FROM pg_options_to_table(c.reloptions)
                            WHERE option_name = 'autovacuum_analyze_threshold'
                        ),
                        current_setting('autovacuum_analyze_threshold')::double precision
                    ) +
                    COALESCE(
                        (
                            SELECT option_value::double precision
                            FROM pg_options_to_table(c.reloptions)
                            WHERE option_name = 'autovacuum_analyze_scale_factor'
                        ),
                        current_setting('autovacuum_analyze_scale_factor')::double precision
                    ) * s.n_live_tup::double precision
                )
            ELSE 0
        END AS autoanalyze_threshold_ratio
    FROM pg_stat_user_tables s
    JOIN pg_class c ON c.oid = s.relid
    ";

#[derive(Clone, Debug)]
struct UserTableSample {
    datname: String,
    schemaname: String,
    relname: String,
    seq_scan: i64,
    seq_tup_read: i64,
    idx_scan: i64,
    idx_tup_fetch: i64,
    n_tup_ins: i64,
    n_tup_upd: i64,
    n_tup_del: i64,
    n_tup_hot_upd: i64,
    n_live_tup: i64,
    n_dead_tup: i64,
    n_mod_since_analyze: i64,
    last_vacuum_epoch: i64,
    last_autovacuum_epoch: i64,
    last_analyze_epoch: i64,
    last_autoanalyze_epoch: i64,
    vacuum_count: i64,
    autovacuum_count: i64,
    analyze_count: i64,
    autoanalyze_count: i64,
    index_size_bytes: i64,
    table_size_bytes: i64,
    last_autovacuum_seconds_ago: Option<f64>,
    last_autoanalyze_seconds_ago: Option<f64>,
    never_autovacuumed: i64,
    never_autoanalyzed: i64,
    autovacuum_threshold_ratio: f64,
    autoanalyze_threshold_ratio: f64,
}

#[allow(clippy::expect_used)]
fn int_metric(name: &str, help: &str) -> IntGaugeVec {
    IntGaugeVec::new(Opts::new(name, help), &USER_TABLE_LABELS)
        .expect("pg_stat_user_tables metric")
}

#[allow(clippy::expect_used)]
fn gauge_metric(name: &str, help: &str) -> GaugeVec {
    GaugeVec::new(Opts::new(name, help), &USER_TABLE_LABELS)
        .expect("pg_stat_user_tables metric")
}

impl Collector for StatUserTablesCollector {
    fn name(&self) -> &'static str {
        "stat_user_tables"
    }

    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.seq_scan.clone()))?;
        registry.register(Box::new(self.seq_tup_read.clone()))?;
        registry.register(Box::new(self.idx_scan.clone()))?;
        registry.register(Box::new(self.idx_tup_fetch.clone()))?;
        registry.register(Box::new(self.n_tup_ins.clone()))?;
        registry.register(Box::new(self.n_tup_upd.clone()))?;
        registry.register(Box::new(self.n_tup_del.clone()))?;
        registry.register(Box::new(self.n_tup_hot_upd.clone()))?;
        registry.register(Box::new(self.n_live_tup.clone()))?;
        registry.register(Box::new(self.n_dead_tup.clone()))?;
        registry.register(Box::new(self.n_mod_since_analyze.clone()))?;
        registry.register(Box::new(self.last_vacuum.clone()))?;
        registry.register(Box::new(self.last_autovacuum.clone()))?;
        registry.register(Box::new(self.last_analyze.clone()))?;
        registry.register(Box::new(self.last_autoanalyze.clone()))?;
        registry.register(Box::new(self.vacuum_count.clone()))?;
        registry.register(Box::new(self.autovacuum_count.clone()))?;
        registry.register(Box::new(self.analyze_count.clone()))?;
        registry.register(Box::new(self.autoanalyze_count.clone()))?;
        registry.register(Box::new(self.index_size_bytes.clone()))?;
        registry.register(Box::new(self.table_size_bytes.clone()))?;
        registry.register(Box::new(self.bloat_ratio.clone()))?;
        registry.register(Box::new(self.dead_tuple_size_bytes.clone()))?;
        registry.register(Box::new(self.last_autovacuum_seconds_ago.clone()))?;
        registry.register(Box::new(self.last_autoanalyze_seconds_ago.clone()))?;
        registry.register(Box::new(self.never_autovacuumed.clone()))?;
        registry.register(Box::new(self.never_autoanalyzed.clone()))?;
        registry.register(Box::new(self.autovacuum_threshold_ratio.clone()))?;
        registry.register(Box::new(self.autoanalyze_threshold_ratio.clone()))?;
        Ok(())
    }

    #[instrument(skip(self, pool), level = "info", err, fields(collector="stat_user_tables", otel.kind="internal"))]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            // 1) Discover databases (exclude templates and configured exclusions)
            let excluded = get_excluded_databases().to_vec();
            let db_list_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT datname FROM pg_database WHERE datallowconn ...",
                db.sql.table = "pg_database"
            );
            let dbs: Vec<String> = sqlx::query_scalar(
                r"
                SELECT datname
                FROM pg_database
                WHERE datallowconn
                  AND NOT datistemplate
                  AND NOT (datname = ANY($1))
                ORDER BY datname
                ",
            )
            .bind(&excluded)
            .fetch_all(pool)
            .instrument(db_list_span)
            .await?;

            let shared_pool = pool.clone();
            let default_db = get_default_database().map(std::string::ToString::to_string);

            // 2) Spawn one task per DB (no semaphore), reuse shared pool for default DB, tiny pool for others
            let mut tasks = JoinSet::new();

            for datname in dbs {
                let shared_pool = shared_pool.clone();
                let default_db = default_db.clone();

                tasks.spawn(async move {
                    let datname_for_timeout = datname.clone();
                    let use_shared = default_db.as_deref() == Some(datname.as_str());

                    let query_span = info_span!(
                        "db.query",
                        otel.kind = "client",
                        db.system = "postgresql",
                        db.operation = "SELECT",
                        db.statement = "SELECT ... FROM pg_stat_user_tables",
                        db.sql.table = "pg_stat_user_tables",
                        datname = %datname,
                        reuse_pool = use_shared
                    );

                    tokio::time::timeout(PER_DATABASE_COLLECTION_TIMEOUT, async move {
                        let rows_res: anyhow::Result<Vec<PgRow>> = if use_shared {
                            sqlx::query(STAT_USER_TABLES_QUERY)
                                .fetch_all(&shared_pool)
                                .instrument(query_span)
                                .await
                                .map_err(Into::into)
                        } else {
                            match get_or_create_pool_for_db(&datname).await {
                                Ok(per_db_pool) => {
                                    sqlx::query(STAT_USER_TABLES_QUERY)
                                        .fetch_all(&per_db_pool)
                                        .instrument(query_span)
                                        .await
                                        .map_err(Into::into)
                                }
                                Err(e) => Err(e),
                            }
                        };

                        let rows = rows_res?;
                        let mut samples = Vec::with_capacity(rows.len());

                        for row in rows {
                            samples.push(UserTableSample {
                                datname: row
                                    .try_get::<Option<String>, _>("datname")?
                                    .unwrap_or_else(|| "[unknown]".to_string()),
                                schemaname: row.try_get("schemaname")?,
                                relname: row.try_get("relname")?,
                                seq_scan: row.try_get("seq_scan").unwrap_or(0),
                                seq_tup_read: row.try_get("seq_tup_read").unwrap_or(0),
                                idx_scan: row.try_get("idx_scan").unwrap_or(0),
                                idx_tup_fetch: row.try_get("idx_tup_fetch").unwrap_or(0),
                                n_tup_ins: row.try_get("n_tup_ins").unwrap_or(0),
                                n_tup_upd: row.try_get("n_tup_upd").unwrap_or(0),
                                n_tup_del: row.try_get("n_tup_del").unwrap_or(0),
                                n_tup_hot_upd: row.try_get("n_tup_hot_upd").unwrap_or(0),
                                n_live_tup: row.try_get("n_live_tup").unwrap_or(0),
                                n_dead_tup: row.try_get("n_dead_tup").unwrap_or(0),
                                n_mod_since_analyze: row.try_get("n_mod_since_analyze").unwrap_or(0),
                                last_vacuum_epoch: row.try_get("last_vacuum_epoch").unwrap_or(0),
                                last_autovacuum_epoch: row.try_get("last_autovacuum_epoch").unwrap_or(0),
                                last_analyze_epoch: row.try_get("last_analyze_epoch").unwrap_or(0),
                                last_autoanalyze_epoch: row.try_get("last_autoanalyze_epoch").unwrap_or(0),
                                vacuum_count: row.try_get("vacuum_count").unwrap_or(0),
                                autovacuum_count: row.try_get("autovacuum_count").unwrap_or(0),
                                analyze_count: row.try_get("analyze_count").unwrap_or(0),
                                autoanalyze_count: row.try_get("autoanalyze_count").unwrap_or(0),
                                index_size_bytes: row.try_get("index_size_bytes").unwrap_or(0),
                                table_size_bytes: row.try_get("table_size_bytes").unwrap_or(0),
                                last_autovacuum_seconds_ago: row
                                    .try_get("last_autovacuum_seconds_ago")
                                    .ok(),
                                last_autoanalyze_seconds_ago: row
                                    .try_get("last_autoanalyze_seconds_ago")
                                    .ok(),
                                never_autovacuumed: row.try_get("never_autovacuumed").unwrap_or(0),
                                never_autoanalyzed: row.try_get("never_autoanalyzed").unwrap_or(0),
                                autovacuum_threshold_ratio: row
                                    .try_get("autovacuum_threshold_ratio")
                                    .unwrap_or(0.0),
                                autoanalyze_threshold_ratio: row
                                    .try_get("autoanalyze_threshold_ratio")
                                    .unwrap_or(0.0),
                            });
                        }

                        Ok::<Vec<UserTableSample>, anyhow::Error>(samples)
                    })
                    .await
                    .map_err(|_| {
                        anyhow!(
                            "stat_user_tables timed out collecting metrics for database {datname_for_timeout} after {PER_DATABASE_COLLECTION_TIMEOUT:?}"
                        )
                    })?
                });
            }

            let mut all_samples = Vec::new();
            let mut failures = Vec::new();
            while !tasks.is_empty() {
                match tokio::time::timeout(TASK_JOIN_WAIT_TIMEOUT, tasks.join_next()).await {
                    Ok(Some(Ok(Ok(samples)))) => {
                        all_samples.extend(samples);
                    }
                    Ok(Some(Ok(Err(e)))) => {
                        error!(error=?e, "stat_user_tables: task returned error");
                        failures.push(e.to_string());
                    }
                    Ok(Some(Err(e))) => {
                        error!(error=?e, "stat_user_tables: task join error");
                        failures.push(e.to_string());
                    }
                    Ok(None) => {
                        break;
                    }
                    Err(_) => {
                        let pending_tasks = tasks.len();
                        tasks.abort_all();
                        failures.push(format!(
                            "timed out waiting for {pending_tasks} database collection task(s) after {TASK_JOIN_WAIT_TIMEOUT:?}"
                        ));
                        break;
                    }
                }
            }

            if all_samples.is_empty() && !failures.is_empty() {
                return Err(anyhow!(
                    "stat_user_tables collection failed for {} database task(s): {}",
                    failures.len(),
                    failures.join("; ")
                ));
            }

            if !failures.is_empty() {
                error!(
                    failed_databases = failures.len(),
                    errors = %failures.join("; "),
                    "stat_user_tables: continuing with partial snapshot after per-database failures"
                );
            }

            self.reset_metrics();

            for sample in &all_samples {
                let labels = [&sample.datname, &sample.schemaname, &sample.relname];

                self.seq_scan.with_label_values(&labels).set(sample.seq_scan);
                self.seq_tup_read.with_label_values(&labels).set(sample.seq_tup_read);
                self.idx_scan.with_label_values(&labels).set(sample.idx_scan);
                self.idx_tup_fetch.with_label_values(&labels).set(sample.idx_tup_fetch);

                self.n_tup_ins.with_label_values(&labels).set(sample.n_tup_ins);
                self.n_tup_upd.with_label_values(&labels).set(sample.n_tup_upd);
                self.n_tup_del.with_label_values(&labels).set(sample.n_tup_del);
                self.n_tup_hot_upd.with_label_values(&labels).set(sample.n_tup_hot_upd);

                self.n_live_tup.with_label_values(&labels).set(sample.n_live_tup);
                self.n_dead_tup.with_label_values(&labels).set(sample.n_dead_tup);
                self.n_mod_since_analyze.with_label_values(&labels).set(sample.n_mod_since_analyze);

                self.last_vacuum.with_label_values(&labels).set(sample.last_vacuum_epoch);
                self.last_autovacuum.with_label_values(&labels).set(sample.last_autovacuum_epoch);
                self.last_analyze.with_label_values(&labels).set(sample.last_analyze_epoch);
                self.last_autoanalyze.with_label_values(&labels).set(sample.last_autoanalyze_epoch);

                self.vacuum_count.with_label_values(&labels).set(sample.vacuum_count);
                self.autovacuum_count.with_label_values(&labels).set(sample.autovacuum_count);
                self.analyze_count.with_label_values(&labels).set(sample.analyze_count);
                self.autoanalyze_count.with_label_values(&labels).set(sample.autoanalyze_count);

                self.index_size_bytes.with_label_values(&labels).set(sample.index_size_bytes);
                self.table_size_bytes.with_label_values(&labels).set(sample.table_size_bytes);

                let total_tuples = sample.n_live_tup + sample.n_dead_tup;
                let bloat_ratio = if total_tuples > 0 {
                    i64_to_f64(sample.n_dead_tup) / i64_to_f64(total_tuples)
                } else {
                    0.0
                };
                let dead_size_estimate = if sample.table_size_bytes > 0 {
                    i64_to_f64(sample.table_size_bytes) * bloat_ratio
                } else {
                    0.0
                };

                self.bloat_ratio.with_label_values(&labels).set(bloat_ratio);
                self.dead_tuple_size_bytes.with_label_values(&labels).set(dead_size_estimate);
                self.never_autovacuumed.with_label_values(&labels).set(sample.never_autovacuumed);
                self.never_autoanalyzed.with_label_values(&labels).set(sample.never_autoanalyzed);

                if let Some(seconds) = sample.last_autovacuum_seconds_ago {
                    self.last_autovacuum_seconds_ago.with_label_values(&labels).set(seconds);
                }
                if let Some(seconds) = sample.last_autoanalyze_seconds_ago {
                    self.last_autoanalyze_seconds_ago.with_label_values(&labels).set(seconds);
                }

                self.autovacuum_threshold_ratio
                    .with_label_values(&labels)
                    .set(sample.autovacuum_threshold_ratio);
                self.autoanalyze_threshold_ratio
                    .with_label_values(&labels)
                    .set(sample.autoanalyze_threshold_ratio);

                debug!(
                    datname=%sample.datname,
                    schema=%sample.schemaname,
                    table=%sample.relname,
                    "updated pg_stat_user_tables metrics"
                );
            }

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::STAT_USER_TABLES_QUERY;

    #[test]
    fn test_stat_user_tables_query_honors_reloptions() {
        assert!(
            STAT_USER_TABLES_QUERY.contains("pg_options_to_table(c.reloptions)"),
            "query should resolve per-table reloptions"
        );
        assert!(
            STAT_USER_TABLES_QUERY.contains("autovacuum_vacuum_threshold"),
            "query should include vacuum threshold override handling"
        );
        assert!(
            STAT_USER_TABLES_QUERY.contains("autovacuum_analyze_threshold"),
            "query should include analyze threshold override handling"
        );
    }

    #[test]
    fn test_stat_user_tables_query_marks_never_autovacuumed_tables() {
        assert!(
            STAT_USER_TABLES_QUERY.contains("never_autovacuumed"),
            "query should expose never_autovacuumed flag"
        );
        assert!(
            STAT_USER_TABLES_QUERY.contains("never_autoanalyzed"),
            "query should expose never_autoanalyzed flag"
        );
    }
}
