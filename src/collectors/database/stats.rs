use crate::collectors::{
    Collector, i64_to_f64,
    util::{MS_TO_SEC, get_excluded_databases},
};
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{GaugeVec, Opts, Registry};
use sqlx::{PgPool, Row};
use tracing::{debug, info_span, instrument};
use tracing_futures::Instrument as _;

/// Exposes `pg_stat_database` metrics with the same names/labels as `postgres_exporter`.
///
/// **Metrics:**
/// - `pg_stat_database_numbackends`               {datid,datname} (current)
/// - `pg_stat_database_xact_commit`               {datid,datname}
/// - `pg_stat_database_xact_rollback`             {datid,datname}
/// - `pg_stat_database_blks_read`                 {datid,datname}
/// - `pg_stat_database_blks_hit`                  {datid,datname}
/// - `pg_stat_database_tup_returned`              {datid,datname}
/// - `pg_stat_database_tup_fetched`               {datid,datname}
/// - `pg_stat_database_tup_inserted`              {datid,datname}
/// - `pg_stat_database_tup_updated`               {datid,datname}
/// - `pg_stat_database_tup_deleted`               {datid,datname}
/// - `pg_stat_database_conflicts`                 {datid,datname}
/// - `pg_stat_database_temp_files`                {datid,datname}
/// - `pg_stat_database_temp_bytes`                {datid,datname}
/// - `pg_stat_database_deadlocks`                 {datid,datname}
/// - `pg_stat_database_blk_read_time`             {datid,datname} (ms)
/// - `pg_stat_database_blk_write_time`            {datid,datname} (ms)
/// - `pg_stat_database_stats_reset`               {datid,datname} (epoch seconds)
/// - `pg_stat_database_active_time_seconds_total` {datid,datname} (only PG >= 14; seconds)
///
/// **NEW - Cache Hit Ratio Metrics (Critical for Performance):**
/// - `pg_stat_database_blks_hit_ratio` {datid,datname} - Buffer cache hit ratio (0.0-1.0)
///
/// **Understanding Cache Hit Ratio:**
///
/// The buffer cache hit ratio measures how often `PostgreSQL` finds data in memory (`shared_buffers`)
/// versus reading from disk. This is one of the most critical performance metrics.
///
/// **Formula:**
/// ```text
/// cache_hit_ratio = blks_hit / (blks_hit + blks_read)
/// ```
///
/// **What the Values Mean:**
/// - **>= 0.99 (99%)** - Excellent! Most data served from memory
/// - **0.95-0.98 (95-98%)** - Good, but room for improvement
/// - **0.90-0.94 (90-94%)** - Warning - Consider increasing `shared_buffers`
/// - **< 0.90 (90%)** - Critical - Severe memory pressure, disk I/O bottleneck
///
/// **Why It Matters:**
/// - Memory access: ~100 nanoseconds
/// - Disk access: ~10 milliseconds (100,000x slower!)
/// - Low hit ratio = queries waiting on slow disk I/O
/// - Causes: insufficient `shared_buffers`, poor query patterns, large sequential scans
///
/// **How to Fix Low Hit Ratio (<90%):**
///
/// 1. **Increase `shared_buffers`:**
///    ```sql
///    -- Check current setting
///    SHOW `shared_buffers`;
///
///    -- Typical recommendations:
///    -- - Small DB (<1GB): 256MB
///    -- - Medium DB (1-10GB): 25% of RAM
///    -- - Large DB (>10GB): 25-40% of RAM
///    -- - Max practical: 8-16GB (diminishing returns)
///
///    -- In postgresql.conf:
///    `shared_buffers` = 4GB
///    ```
///
/// 2. **Identify problematic queries:**
///    ```sql
///    -- Find tables with low hit ratios (if `pg_statio_user_tables` available)
///    `SELECT`
///        schemaname,
///        tablename,
///        heap_blks_read,
///        heap_blks_hit,
///        CASE 
///            WHEN heap_blks_hit + heap_blks_read = 0 THEN NULL
///            ELSE heap_blks_hit::float / (heap_blks_hit + heap_blks_read)
///        END AS hit_ratio
///    `FROM` `pg_statio_user_tables`
///    `WHERE` heap_blks_read > 0
///    `ORDER BY` hit_ratio NULLS LAST
///    LIMIT 20;
///    ```
///
/// 3. **Optimize queries:**
///    - Add missing indexes
///    - Avoid `SELECT *`
///    - Use `LIMIT` when appropriate
///    - Consider materialized views for complex aggregations
///
/// 4. **Consider OS page cache:**
///    - `PostgreSQL` relies on OS cache for data beyond `shared_buffers`
///    - Ensure sufficient free RAM for OS page cache
///    - Rule of thumb: `total_ram = shared_buffers + work_mem×connections + OS cache`
///
/// **Alert Thresholds:**
/// ```promql
/// # Warning: Cache hit ratio below 95%
/// pg_stat_database_blks_hit_ratio < 0.95
///
/// # Critical: Cache hit ratio below 90%
/// pg_stat_database_blks_hit_ratio < 0.90
/// ```
///
/// **Troubleshooting Low Hit Ratio:**
///
/// - **Sudden drop:** Check for new queries, sequential scans, or bulk operations
/// - **Gradual decline:** Database growth exceeding available memory
/// - **Always low:** `shared_buffers` too small, or application design issues
/// - **High `blks_read` rate:** Use `rate(pg_stat_database_blks_read[5m])` to track disk I/O
///
/// **Notes:**
/// - We export absolute values as Gauges; use `rate()/increase()` in `PromQL` for cumulative series.
/// - Database exclusions are applied server-side using the global list set via CLI/env.
/// - Cache hit ratio is calculated per collection cycle (not cumulative)
#[derive(Clone)]
pub struct DatabaseStatCollector {
    numbackends: GaugeVec,

    xact_commit: GaugeVec,
    xact_rollback: GaugeVec,
    blks_read: GaugeVec,
    blks_hit: GaugeVec,
    tup_returned: GaugeVec,
    tup_fetched: GaugeVec,
    tup_inserted: GaugeVec,
    tup_updated: GaugeVec,
    tup_deleted: GaugeVec,
    conflicts: GaugeVec,
    temp_files: GaugeVec,
    temp_bytes: GaugeVec,
    deadlocks: GaugeVec,

    blk_read_time: GaugeVec,
    blk_write_time: GaugeVec,

    stats_reset: GaugeVec,

    active_time_seconds_total: GaugeVec, // PG >= 14

    // Cache hit ratio metric (NEW - critical performance indicator)
    // Measures buffer cache efficiency: blks_hit / (blks_hit + blks_read)
    // Alert when < 0.90 (90% hit ratio indicates memory pressure)
    blks_hit_ratio: GaugeVec,
}

impl Default for DatabaseStatCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl DatabaseStatCollector {
    /// Creates a new `DatabaseStatCollector` with all metrics initialized.
    ///
    /// # Panics
    ///
    /// Panics if metric registration fails (this should not happen in normal operation).
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        let numbackends = db_gauge(
            "pg_stat_database_numbackends",
            "Number of backends currently connected to this database.",
        );
        let xact_commit = db_gauge(
            "pg_stat_database_xact_commit",
            "Number of transactions committed.",
        );
        let xact_rollback = db_gauge(
            "pg_stat_database_xact_rollback",
            "Number of transactions rolled back.",
        );
        let blks_read =
            db_gauge("pg_stat_database_blks_read", "Number of disk blocks read.");
        let blks_hit = db_gauge(
            "pg_stat_database_blks_hit",
            "Number of buffer cache hits (PostgreSQL buffer cache).",
        );
        let tup_returned =
            db_gauge("pg_stat_database_tup_returned", "Rows returned by queries.");
        let tup_fetched =
            db_gauge("pg_stat_database_tup_fetched", "Rows fetched by queries.");
        let tup_inserted =
            db_gauge("pg_stat_database_tup_inserted", "Rows inserted by queries.");
        let tup_updated =
            db_gauge("pg_stat_database_tup_updated", "Rows updated by queries.");
        let tup_deleted =
            db_gauge("pg_stat_database_tup_deleted", "Rows deleted by queries.");
        let conflicts = db_gauge(
            "pg_stat_database_conflicts",
            "Queries canceled due to conflicts with recovery.",
        );
        let temp_files = db_gauge(
            "pg_stat_database_temp_files",
            "Number of temporary files created by queries.",
        );
        let temp_bytes = db_gauge(
            "pg_stat_database_temp_bytes",
            "Total data written to temporary files by queries.",
        );
        let deadlocks = db_gauge(
            "pg_stat_database_deadlocks",
            "Number of deadlocks detected in this database.",
        );
        let blk_read_time = db_gauge(
            "pg_stat_database_blk_read_time",
            "Time spent reading data file blocks (milliseconds).",
        );
        let blk_write_time = db_gauge(
            "pg_stat_database_blk_write_time",
            "Time spent writing data file blocks (milliseconds).",
        );
        let stats_reset = db_gauge(
            "pg_stat_database_stats_reset",
            "Time at which these statistics were last reset (epoch seconds).",
        );
        let active_time_seconds_total = db_gauge(
            "pg_stat_database_active_time_seconds_total",
            "Time spent executing SQL statements (seconds, PG >= 14).",
        );
        let blks_hit_ratio = db_gauge(
            "pg_stat_database_blks_hit_ratio",
            "Buffer cache hit ratio (0.0-1.0). Alert when < 0.90 (90%). \
             Formula: blks_hit / (blks_hit + blks_read). \
             >99% = excellent, 95-98% = good, 90-94% = warning, <90% = critical memory pressure.",
        );

        Self {
            numbackends,
            xact_commit,
            xact_rollback,
            blks_read,
            blks_hit,
            tup_returned,
            tup_fetched,
            tup_inserted,
            tup_updated,
            tup_deleted,
            conflicts,
            temp_files,
            temp_bytes,
            deadlocks,
            blk_read_time,
            blk_write_time,
            stats_reset,
            active_time_seconds_total,
            blks_hit_ratio,
        }
    }
}

const DATABASE_LABELS: [&str; 2] = ["datid", "datname"];

#[allow(clippy::expect_used)]
fn db_gauge(metric: &str, help: &str) -> GaugeVec {
    GaugeVec::new(Opts::new(metric, help), &DATABASE_LABELS)
        .expect("register pg_stat_database metric")
}

impl Collector for DatabaseStatCollector {
    fn name(&self) -> &'static str {
        "database_stats"
    }

    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.numbackends.clone()))?;
        registry.register(Box::new(self.xact_commit.clone()))?;
        registry.register(Box::new(self.xact_rollback.clone()))?;
        registry.register(Box::new(self.blks_read.clone()))?;
        registry.register(Box::new(self.blks_hit.clone()))?;
        registry.register(Box::new(self.tup_returned.clone()))?;
        registry.register(Box::new(self.tup_fetched.clone()))?;
        registry.register(Box::new(self.tup_inserted.clone()))?;
        registry.register(Box::new(self.tup_updated.clone()))?;
        registry.register(Box::new(self.tup_deleted.clone()))?;
        registry.register(Box::new(self.conflicts.clone()))?;
        registry.register(Box::new(self.temp_files.clone()))?;
        registry.register(Box::new(self.temp_bytes.clone()))?;
        registry.register(Box::new(self.deadlocks.clone()))?;
        registry.register(Box::new(self.blk_read_time.clone()))?;
        registry.register(Box::new(self.blk_write_time.clone()))?;
        registry.register(Box::new(self.stats_reset.clone()))?;
        registry.register(Box::new(self.active_time_seconds_total.clone()))?;
        registry.register(Box::new(self.blks_hit_ratio.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector="database_stats", otel.kind="internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            // 0) Reset all metrics to clear stale data (e.g. dropped databases)
            self.numbackends.reset();
            self.xact_commit.reset();
            self.xact_rollback.reset();
            self.blks_read.reset();
            self.blks_hit.reset();
            self.tup_returned.reset();
            self.tup_fetched.reset();
            self.tup_inserted.reset();
            self.tup_updated.reset();
            self.tup_deleted.reset();
            self.conflicts.reset();
            self.temp_files.reset();
            self.temp_bytes.reset();
            self.deadlocks.reset();
            self.blk_read_time.reset();
            self.blk_write_time.reset();
            self.stats_reset.reset();
            self.active_time_seconds_total.reset();
            self.blks_hit_ratio.reset();

            // Version check for active_time (PG >= 14)
            let vrow = sqlx::query(r"SELECT current_setting('server_version_num')::int AS v")
                .fetch_one(pool)
                .await?;
            let version_num: i32 = vrow.try_get("v")?;
            let has_active_time = version_num >= 140_000;

            // Columns per postgres_exporter
            let mut cols: Vec<String> = vec![
                "datid::text AS datid".to_string(),
                "datname".to_string(),
                "numbackends::bigint AS numbackends".to_string(),
                "xact_commit::bigint AS xact_commit".to_string(),
                "xact_rollback::bigint AS xact_rollback".to_string(),
                "blks_read::bigint AS blks_read".to_string(),
                "blks_hit::bigint AS blks_hit".to_string(),
                "tup_returned::bigint AS tup_returned".to_string(),
                "tup_fetched::bigint AS tup_fetched".to_string(),
                "tup_inserted::bigint AS tup_inserted".to_string(),
                "tup_updated::bigint AS tup_updated".to_string(),
                "tup_deleted::bigint AS tup_deleted".to_string(),
                "conflicts::bigint AS conflicts".to_string(),
                "temp_files::bigint AS temp_files".to_string(),
                "temp_bytes::bigint AS temp_bytes".to_string(),
                "deadlocks::bigint AS deadlocks".to_string(),
                "blk_read_time::double precision AS blk_read_time".to_string(),
                "blk_write_time::double precision AS blk_write_time".to_string(),
                "EXTRACT(EPOCH FROM stats_reset)::double precision AS stats_reset_epoch"
                    .to_string(),
            ];
            if has_active_time {
                // Convert ms to seconds to match *_seconds_total naming in Go
                cols.push(format!(
                    "(active_time / {MS_TO_SEC})::double precision AS active_time_seconds"
                ));
            }

            // Apply exclusions server-side. If the list is empty, this is a no-op.
            let excluded_list: Vec<String> = get_excluded_databases().to_vec();

            let sql = format!(
                "SELECT {} FROM pg_stat_database WHERE NOT (datname = ANY($1)) ORDER BY datname",
                cols.join(", ")
            );

            let span_q = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "pg_stat_database",
                db.sql.table = "pg_stat_database"
            );

            let rows = sqlx::query(&sql)
                .bind(&excluded_list)
                .fetch_all(pool)
                .instrument(span_q)
                .await?;

            let apply_span = info_span!("database_stats.apply_metrics", databases = rows.len());
            let _g = apply_span.enter();

            #[allow(clippy::cast_precision_loss)]
            for row in &rows {
                let datid: String = row.try_get::<String, _>("datid")?;
                let datname: String = row
                    .try_get::<Option<String>, _>("datname")?
                    .unwrap_or_else(|| "[unknown]".to_string());

                let labels = [&datid, &datname];

                self.numbackends
                    .with_label_values(&labels)
                    .set(i64_to_f64(row.try_get::<i64, _>("numbackends").unwrap_or(0)));

                self.xact_commit
                    .with_label_values(&labels)
                    .set(i64_to_f64(row.try_get::<i64, _>("xact_commit").unwrap_or(0)));
                self.xact_rollback
                    .with_label_values(&labels)
                    .set(i64_to_f64(row.try_get::<i64, _>("xact_rollback").unwrap_or(0)));
                self.blks_read
                    .with_label_values(&labels)
                    .set(i64_to_f64(row.try_get::<i64, _>("blks_read").unwrap_or(0)));
                self.blks_hit
                    .with_label_values(&labels)
                    .set(i64_to_f64(row.try_get::<i64, _>("blks_hit").unwrap_or(0)));
                self.tup_returned
                    .with_label_values(&labels)
                    .set(i64_to_f64(row.try_get::<i64, _>("tup_returned").unwrap_or(0)));
                self.tup_fetched
                    .with_label_values(&labels)
                    .set(i64_to_f64(row.try_get::<i64, _>("tup_fetched").unwrap_or(0)));
                self.tup_inserted
                    .with_label_values(&labels)
                    .set(i64_to_f64(row.try_get::<i64, _>("tup_inserted").unwrap_or(0)));
                self.tup_updated
                    .with_label_values(&labels)
                    .set(i64_to_f64(row.try_get::<i64, _>("tup_updated").unwrap_or(0)));
                self.tup_deleted
                    .with_label_values(&labels)
                    .set(i64_to_f64(row.try_get::<i64, _>("tup_deleted").unwrap_or(0)));
                self.conflicts
                    .with_label_values(&labels)
                    .set(i64_to_f64(row.try_get::<i64, _>("conflicts").unwrap_or(0)));
                self.temp_files
                    .with_label_values(&labels)
                    .set(i64_to_f64(row.try_get::<i64, _>("temp_files").unwrap_or(0)));
                self.temp_bytes
                    .with_label_values(&labels)
                    .set(i64_to_f64(row.try_get::<i64, _>("temp_bytes").unwrap_or(0)));
                self.deadlocks
                    .with_label_values(&labels)
                    .set(i64_to_f64(row.try_get::<i64, _>("deadlocks").unwrap_or(0)));

                self.blk_read_time
                    .with_label_values(&labels)
                    .set(row.try_get::<f64, _>("blk_read_time").unwrap_or(0.0));
                self.blk_write_time
                    .with_label_values(&labels)
                    .set(row.try_get::<f64, _>("blk_write_time").unwrap_or(0.0));

                self.stats_reset
                    .with_label_values(&labels)
                    .set(row.try_get::<f64, _>("stats_reset_epoch").unwrap_or(0.0));

                // Calculate cache hit ratio
                // Formula: blks_hit / (blks_hit + blks_read)
                // Handles division by zero (if no blocks accessed, ratio = 0.0)
                let blks_read = i64_to_f64(row.try_get::<i64, _>("blks_read").unwrap_or(0));
                let blks_hit = i64_to_f64(row.try_get::<i64, _>("blks_hit").unwrap_or(0));
                let total_blks = blks_hit + blks_read;
                
                let hit_ratio = if total_blks > 0.0 {
                    blks_hit / total_blks
                } else {
                    // No blocks accessed = 0% hit ratio (conservative)
                    // Alternatively could be 1.0 (optimistic) or NaN (unknown)
                    // We choose 0.0 to avoid false positives in alerts
                    0.0
                };

                self.blks_hit_ratio
                    .with_label_values(&labels)
                    .set(hit_ratio);

                debug!(
                    datname = %datname,
                    blks_read,
                    blks_hit,
                    hit_ratio = %format!("{hit_ratio:.4}"),
                    "calculated cache hit ratio"
                );

                if has_active_time {
                    self.active_time_seconds_total
                        .with_label_values(&labels)
                        .set(row.try_get::<f64, _>("active_time_seconds").unwrap_or(0.0));
                }

                debug!(%datid, %datname, "updated pg_stat_database metrics");
            }

            Ok(())
        })
    }
}
