use crate::collectors::{Collector, i64_to_f64};
use crate::collectors::util::{
    get_default_database, get_excluded_databases, get_or_create_pool_for_db,
};
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{GaugeVec, IntGaugeVec, Opts, Registry};
use sqlx::{PgPool, Row};
use std::sync::Arc;
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
            autovacuum_threshold_ratio: gauge_metric("pg_stat_user_tables_autovacuum_threshold_ratio", "Ratio of dead tuples to autovacuum threshold (0.0 clean, 1.0 trigger, >1.0 overdue)"),
            autoanalyze_threshold_ratio: gauge_metric("pg_stat_user_tables_autoanalyze_threshold_ratio", "Ratio of modified tuples to autoanalyze threshold (0.0 clean, 1.0 trigger, >1.0 overdue)"),
        }
    }
}

const USER_TABLE_LABELS: [&str; 3] = ["datname", "schemaname", "relname"];

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
            let this = Arc::new(self.clone());
            let mut tasks = JoinSet::new();

            for datname in dbs {
                let this = Arc::clone(&this);
                let shared_pool = shared_pool.clone();
                let default_db = default_db.clone();

                tasks.spawn(async move {
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

                    let rows_res = if use_shared {
                        sqlx::query(
                            r"
                            SELECT
                                current_database() AS datname,
                                schemaname,
                                relname,
                                seq_scan::bigint,
                                seq_tup_read::bigint,
                                idx_scan::bigint,
                                idx_tup_fetch::bigint,
                                n_tup_ins::bigint,
                                n_tup_upd::bigint,
                                n_tup_del::bigint,
                                n_tup_hot_upd::bigint,
                                n_live_tup::bigint,
                                n_dead_tup::bigint,
                                n_mod_since_analyze::bigint,
                                COALESCE(EXTRACT(EPOCH FROM last_vacuum)::bigint, 0)       AS last_vacuum_epoch,
                                COALESCE(EXTRACT(EPOCH FROM last_autovacuum)::bigint, 0)  AS last_autovacuum_epoch,
                                COALESCE(EXTRACT(EPOCH FROM last_analyze)::bigint, 0)     AS last_analyze_epoch,
                                COALESCE(EXTRACT(EPOCH FROM last_autoanalyze)::bigint, 0) AS last_autoanalyze_epoch,
                                vacuum_count::bigint,
                                autovacuum_count::bigint,
                                analyze_count::bigint,
                                autoanalyze_count::bigint,
                                pg_indexes_size(relid)::bigint AS index_size_bytes,
                                pg_table_size(relid)::bigint   AS table_size_bytes,
                                COALESCE(EXTRACT(EPOCH FROM (now() - last_autovacuum)), 0) AS last_autovacuum_seconds_ago,
                                COALESCE(EXTRACT(EPOCH FROM (now() - last_autoanalyze)), 0) AS last_autoanalyze_seconds_ago,
                                CASE
                                    WHEN n_live_tup > 0 THEN
                                        n_dead_tup::float /
                                        (current_setting('autovacuum_vacuum_threshold')::float +
                                         current_setting('autovacuum_vacuum_scale_factor')::float * n_live_tup)
                                    ELSE 0
                                END AS autovacuum_threshold_ratio,
                                CASE
                                    WHEN n_live_tup > 0 THEN
                                        n_mod_since_analyze::float /
                                        (current_setting('autovacuum_analyze_threshold')::float +
                                         current_setting('autovacuum_analyze_scale_factor')::float * n_live_tup)
                                    ELSE 0
                                END AS autoanalyze_threshold_ratio
                            FROM pg_stat_user_tables
                            ",
                        )
                        .fetch_all(&shared_pool)
                        .instrument(query_span)
                        .await
                    } else {
                        match get_or_create_pool_for_db(&datname).await {
                            Ok(per_db_pool) => {
                                sqlx::query(
                                    r"
                                    SELECT
                                        current_database() AS datname,
                                        schemaname,
                                        relname,
                                        seq_scan::bigint,
                                        seq_tup_read::bigint,
                                        idx_scan::bigint,
                                        idx_tup_fetch::bigint,
                                        n_tup_ins::bigint,
                                        n_tup_upd::bigint,
                                        n_tup_del::bigint,
                                        n_tup_hot_upd::bigint,
                                        n_live_tup::bigint,
                                        n_dead_tup::bigint,
                                        n_mod_since_analyze::bigint,
                                        COALESCE(EXTRACT(EPOCH FROM last_vacuum)::bigint, 0)       AS last_vacuum_epoch,
                                        COALESCE(EXTRACT(EPOCH FROM last_autovacuum)::bigint, 0)  AS last_autovacuum_epoch,
                                        COALESCE(EXTRACT(EPOCH FROM last_analyze)::bigint, 0)     AS last_analyze_epoch,
                                        COALESCE(EXTRACT(EPOCH FROM last_autoanalyze)::bigint, 0) AS last_autoanalyze_epoch,
                                        vacuum_count::bigint,
                                        autovacuum_count::bigint,
                                        analyze_count::bigint,
                                        autoanalyze_count::bigint,
                                        pg_indexes_size(relid)::bigint AS index_size_bytes,
                                        pg_table_size(relid)::bigint   AS table_size_bytes,
                                        COALESCE(EXTRACT(EPOCH FROM (now() - last_autovacuum)), 0) AS last_autovacuum_seconds_ago,
                                        COALESCE(EXTRACT(EPOCH FROM (now() - last_autoanalyze)), 0) AS last_autoanalyze_seconds_ago,
                                        CASE
                                            WHEN n_live_tup > 0 THEN
                                                n_dead_tup::float /
                                                (current_setting('autovacuum_vacuum_threshold')::float +
                                                 current_setting('autovacuum_vacuum_scale_factor')::float * n_live_tup)
                                            ELSE 0
                                        END AS autovacuum_threshold_ratio,
                                        CASE
                                            WHEN n_live_tup > 0 THEN
                                                n_mod_since_analyze::float /
                                                (current_setting('autovacuum_analyze_threshold')::float +
                                                 current_setting('autovacuum_analyze_scale_factor')::float * n_live_tup)
                                            ELSE 0
                                        END AS autoanalyze_threshold_ratio
                                    FROM pg_stat_user_tables
                                    ",
                                )
                                .fetch_all(&per_db_pool)
                                .instrument(query_span)
                                .await
                            }
                            Err(e) => {
                                error!(%datname, error=?e, "stat_user_tables: pool init failed");
                                return Ok::<(), anyhow::Error>(());
                            }
                        }
                    };

                    let rows = match rows_res {
                        Ok(r) => r,
                        Err(e) => {
                            error!(%datname, error=?e, "stat_user_tables: query failed");
                            return Ok(());
                        }
                    };

                    for row in rows {
                        let dat: String = row.try_get::<Option<String>, _>("datname")?.unwrap_or_else(|| "[unknown]".to_string());
                        let schema: String = row.try_get("schemaname")?;
                        let table: String = row.try_get("relname")?;

                        let labels = [&dat, &schema, &table];

                        this.seq_scan.with_label_values(&labels).set(row.try_get::<i64, _>("seq_scan").unwrap_or(0));
                        this.seq_tup_read.with_label_values(&labels).set(row.try_get::<i64, _>("seq_tup_read").unwrap_or(0));
                        this.idx_scan.with_label_values(&labels).set(row.try_get::<i64, _>("idx_scan").unwrap_or(0));
                        this.idx_tup_fetch.with_label_values(&labels).set(row.try_get::<i64, _>("idx_tup_fetch").unwrap_or(0));

                        this.n_tup_ins.with_label_values(&labels).set(row.try_get::<i64, _>("n_tup_ins").unwrap_or(0));
                        this.n_tup_upd.with_label_values(&labels).set(row.try_get::<i64, _>("n_tup_upd").unwrap_or(0));
                        this.n_tup_del.with_label_values(&labels).set(row.try_get::<i64, _>("n_tup_del").unwrap_or(0));
                        this.n_tup_hot_upd.with_label_values(&labels).set(row.try_get::<i64, _>("n_tup_hot_upd").unwrap_or(0));

                        this.n_live_tup.with_label_values(&labels).set(row.try_get::<i64, _>("n_live_tup").unwrap_or(0));
                        this.n_dead_tup.with_label_values(&labels).set(row.try_get::<i64, _>("n_dead_tup").unwrap_or(0));
                        this.n_mod_since_analyze.with_label_values(&labels).set(row.try_get::<i64, _>("n_mod_since_analyze").unwrap_or(0));

                        this.last_vacuum.with_label_values(&labels).set(row.try_get::<i64, _>("last_vacuum_epoch").unwrap_or(0));
                        this.last_autovacuum.with_label_values(&labels).set(row.try_get::<i64, _>("last_autovacuum_epoch").unwrap_or(0));
                        this.last_analyze.with_label_values(&labels).set(row.try_get::<i64, _>("last_analyze_epoch").unwrap_or(0));
                        this.last_autoanalyze.with_label_values(&labels).set(row.try_get::<i64, _>("last_autoanalyze_epoch").unwrap_or(0));

                        this.vacuum_count.with_label_values(&labels).set(row.try_get::<i64, _>("vacuum_count").unwrap_or(0));
                        this.autovacuum_count.with_label_values(&labels).set(row.try_get::<i64, _>("autovacuum_count").unwrap_or(0));
                        this.analyze_count.with_label_values(&labels).set(row.try_get::<i64, _>("analyze_count").unwrap_or(0));
                        this.autoanalyze_count.with_label_values(&labels).set(row.try_get::<i64, _>("autoanalyze_count").unwrap_or(0));

                        this.index_size_bytes.with_label_values(&labels).set(row.try_get::<i64, _>("index_size_bytes").unwrap_or(0));
                        this.table_size_bytes.with_label_values(&labels).set(row.try_get::<i64, _>("table_size_bytes").unwrap_or(0));

                        // Calculate bloat metrics
                        let n_live = row.try_get::<i64, _>("n_live_tup").unwrap_or(0);
                        let n_dead = row.try_get::<i64, _>("n_dead_tup").unwrap_or(0);
                        let tbl_size = row.try_get::<i64, _>("table_size_bytes").unwrap_or(0);
                        
                        let total_tuples = n_live + n_dead;
                        let bloat_ratio = if total_tuples > 0 {
                            i64_to_f64(n_dead) / i64_to_f64(total_tuples)
                        } else {
                            0.0
                        };

                        let dead_size_estimate = if tbl_size > 0 {
                            i64_to_f64(tbl_size) * bloat_ratio
                        } else {
                            0.0
                        };

                        this.bloat_ratio.with_label_values(&labels).set(bloat_ratio);
                        this.dead_tuple_size_bytes.with_label_values(&labels).set(dead_size_estimate);

                        // Autovacuum-specific metrics (Phase 1 enhancement)
                        // These provide predictive alerting and prevent wraparound disasters
                        
                        // Time-based metrics - easier for alerting than epoch timestamps
                        let last_autovac_seconds_ago: f64 = row.try_get("last_autovacuum_seconds_ago").unwrap_or(0.0);
                        let last_autoanalyze_seconds_ago: f64 = row.try_get("last_autoanalyze_seconds_ago").unwrap_or(0.0);

                        // GOLD METRICS - Predict autovacuum triggers BEFORE they happen
                        // Ratio of dead/modified tuples to autovacuum threshold
                        // Values: 0.0=clean, 0.8=warning, 1.0=trigger, >1.0=overdue
                        // These prevent transaction ID wraparound emergencies!
                        let autovac_threshold_ratio: f64 = row.try_get("autovacuum_threshold_ratio").unwrap_or(0.0);
                        let autoanalyze_threshold_ratio: f64 = row.try_get("autoanalyze_threshold_ratio").unwrap_or(0.0);

                        this.last_autovacuum_seconds_ago.with_label_values(&labels).set(last_autovac_seconds_ago);
                        this.last_autoanalyze_seconds_ago.with_label_values(&labels).set(last_autoanalyze_seconds_ago);
                        this.autovacuum_threshold_ratio.with_label_values(&labels).set(autovac_threshold_ratio);
                        this.autoanalyze_threshold_ratio.with_label_values(&labels).set(autoanalyze_threshold_ratio);

                        debug!(datname=%dat, schema=%schema, table=%table, "updated pg_stat_user_tables metrics");
                    }

                    Ok::<(), anyhow::Error>(())
                });
            }

            while let Some(res) = tasks.join_next().await {
                if let Err(e) = res {
                    error!(error=?e, "stat_user_tables: task join error");
                } else if let Ok(Err(e)) = res {
                    error!(error=?e, "stat_user_tables: task returned error");
                }
            }

            Ok(())
        })
    }
}