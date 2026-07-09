use crate::collectors::util::{
    acquire_db_query_permit, get_default_database, get_excluded_databases, open_db_connection,
};
use crate::collectors::{Collector, all_databases_failed, i64_to_f64};
use anyhow::{Result, anyhow};
use futures::future::BoxFuture;
use prometheus::{GaugeVec, Opts, Registry};
use sqlx::{PgPool, Row, postgres::PgRow};
use tokio::task::JoinSet;
use tracing::{debug, error, info_span, instrument};
use tracing_futures::Instrument as _;

/// Collector for index usage statistics from `pg_stat_user_indexes`
///
/// **What it measures:**
/// Tracks index usage patterns including scan counts, tuples read/fetched, and size metrics.
/// Helps identify which indexes are being used effectively and which may be candidates for removal.
///
/// **Key metrics (labeled by `datname`):**
/// - `pg_index_scans_total`: Number of index scans initiated on indexes in the database
/// - `pg_index_tuples_read_total`: Number of index entries returned by scans
/// - `pg_index_tuples_fetched_total`: Number of live table rows fetched by index scans
/// - `pg_index_size_bytes`: Total size of user indexes in the database, in bytes
/// - `pg_index_valid`: Count of valid user indexes
///
/// **Multi-database:**
/// `pg_stat_user_indexes` is a per-database catalog, so this collector iterates every
/// connectable, non-excluded database (like `pg_stat_user_tables`) and labels each series
/// by `datname`. Connecting to a single database (e.g. `postgres`) is therefore enough to
/// observe index metrics across the whole cluster.
///
/// **Why it matters:**
/// - Low or zero scans indicate unused indexes that waste disk space and slow writes
/// - Invalid indexes (from failed CREATE INDEX CONCURRENTLY) must be dropped and recreated
/// - Large indexes with low usage suggest schema optimization opportunities
/// - High `tuples_read` vs `tuples_fetched` ratio may indicate inefficient index usage
#[derive(Clone)]
pub struct IndexStatsCollector {
    scans: GaugeVec,
    tuples_read: GaugeVec,
    tuples_fetched: GaugeVec,
    size_bytes: GaugeVec,
    valid: GaugeVec,
    idx_blks_read: GaugeVec,
    idx_blks_hit: GaugeVec,
}

impl Default for IndexStatsCollector {
    fn default() -> Self {
        Self::new()
    }
}

const INDEX_STATS_LABELS: [&str; 1] = ["datname"];

/// Per-database aggregate of index usage statistics.
///
/// `pg_stat_user_indexes` only lists indexes in the current database, so this query is
/// executed once per database and tagged with `current_database()`.
const INDEX_STATS_QUERY: &str = r"
    SELECT
        current_database() AS datname,
        COALESCE(SUM(s.idx_scan), 0)::bigint AS total_scans,
        COALESCE(SUM(s.idx_tup_read), 0)::bigint AS total_tup_read,
        COALESCE(SUM(s.idx_tup_fetch), 0)::bigint AS total_tup_fetch,
        COALESCE(SUM(pg_relation_size(s.indexrelid)), 0)::bigint AS total_size_bytes,
        COALESCE(SUM(i.indisvalid::int), 0)::bigint AS valid_count,
        COALESCE(SUM(io.idx_blks_read), 0)::bigint AS total_idx_blks_read,
        COALESCE(SUM(io.idx_blks_hit), 0)::bigint AS total_idx_blks_hit
    FROM pg_stat_user_indexes s
    JOIN pg_index i ON s.indexrelid = i.indexrelid
    LEFT JOIN pg_statio_user_indexes io ON s.indexrelid = io.indexrelid
    WHERE s.schemaname NOT IN ('pg_catalog', 'information_schema')
    ";

#[derive(Clone, Debug)]
struct IndexStatsSample {
    datname: String,
    scans: i64,
    tuples_read: i64,
    tuples_fetched: i64,
    size_bytes: i64,
    valid: i64,
    idx_blks_read: i64,
    idx_blks_hit: i64,
}

impl IndexStatsCollector {
    /// Creates a new `IndexStatsCollector`
    ///
    /// # Panics
    ///
    /// Panics if metric creation fails (should never happen with valid metric names)
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        Self {
            scans: GaugeVec::new(
                Opts::new(
                    "pg_index_scans_total",
                    "Number of index scans initiated on indexes in this database",
                ),
                &INDEX_STATS_LABELS,
            )
            .expect("Failed to create pg_index_scans_total"),
            tuples_read: GaugeVec::new(
                Opts::new(
                    "pg_index_tuples_read_total",
                    "Number of index entries returned by scans on indexes in this database",
                ),
                &INDEX_STATS_LABELS,
            )
            .expect("Failed to create pg_index_tuples_read_total"),
            tuples_fetched: GaugeVec::new(
                Opts::new(
                    "pg_index_tuples_fetched_total",
                    "Number of live table rows fetched by simple index scans in this database",
                ),
                &INDEX_STATS_LABELS,
            )
            .expect("Failed to create pg_index_tuples_fetched_total"),
            size_bytes: GaugeVec::new(
                Opts::new(
                    "pg_index_size_bytes",
                    "Total size of user indexes in this database, in bytes",
                ),
                &INDEX_STATS_LABELS,
            )
            .expect("Failed to create pg_index_size_bytes"),
            valid: GaugeVec::new(
                Opts::new(
                    "pg_index_valid",
                    "Count of valid user indexes in this database",
                ),
                &INDEX_STATS_LABELS,
            )
            .expect("Failed to create pg_index_valid"),
            idx_blks_read: GaugeVec::new(
                Opts::new(
                    "pg_index_idx_blks_read_total",
                    "Number of disk blocks read from all indexes in this database",
                ),
                &INDEX_STATS_LABELS,
            )
            .expect("Failed to create pg_index_idx_blks_read_total"),
            idx_blks_hit: GaugeVec::new(
                Opts::new(
                    "pg_index_idx_blks_hit_total",
                    "Number of buffer hits in all indexes in this database",
                ),
                &INDEX_STATS_LABELS,
            )
            .expect("Failed to create pg_index_idx_blks_hit_total"),
        }
    }

    fn reset_metrics(&self) {
        self.scans.reset();
        self.tuples_read.reset();
        self.tuples_fetched.reset();
        self.size_bytes.reset();
        self.valid.reset();
        self.idx_blks_read.reset();
        self.idx_blks_hit.reset();
    }

    fn sample_from_row(row: &PgRow) -> Result<IndexStatsSample> {
        Ok(IndexStatsSample {
            datname: row
                .try_get::<Option<String>, _>("datname")?
                .unwrap_or_else(|| "[unknown]".to_string()),
            scans: row.try_get("total_scans").unwrap_or(0),
            tuples_read: row.try_get("total_tup_read").unwrap_or(0),
            tuples_fetched: row.try_get("total_tup_fetch").unwrap_or(0),
            size_bytes: row.try_get("total_size_bytes").unwrap_or(0),
            valid: row.try_get("valid_count").unwrap_or(0),
            idx_blks_read: row.try_get("total_idx_blks_read").unwrap_or(0),
            idx_blks_hit: row.try_get("total_idx_blks_hit").unwrap_or(0),
        })
    }
}

impl Collector for IndexStatsCollector {
    fn name(&self) -> &'static str {
        "index_stats"
    }

    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.scans.clone()))?;
        registry.register(Box::new(self.tuples_read.clone()))?;
        registry.register(Box::new(self.tuples_fetched.clone()))?;
        registry.register(Box::new(self.size_bytes.clone()))?;
        registry.register(Box::new(self.valid.clone()))?;
        registry.register(Box::new(self.idx_blks_read.clone()))?;
        registry.register(Box::new(self.idx_blks_hit.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector = "index_stats", otel.kind = "internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            // 1) Discover connectable, non-excluded databases via the shared pool.
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

            // 2) One task per DB. The default DB reuses the shared pool; every other database
            // must pass through the global per-database connection limiter.
            let mut tasks: JoinSet<Result<Option<IndexStatsSample>>> = JoinSet::new();

            let num_dbs = dbs.len();
            for datname in dbs {
                let shared_pool = shared_pool.clone();
                let default_db = default_db.clone();

                tasks.spawn(async move {
                    let use_shared = default_db.as_deref() == Some(datname.as_str());

                    let query_span = info_span!(
                        "db.query",
                        otel.kind = "client",
                        db.system = "postgresql",
                        db.operation = "SELECT",
                        db.statement = "SELECT ... FROM pg_stat_user_indexes",
                        db.sql.table = "pg_stat_user_indexes",
                        datname = %datname,
                        reuse_pool = use_shared
                    );

                    let db_query_permit = if use_shared {
                        None
                    } else {
                        Some(acquire_db_query_permit().await.map_err(|e| {
                            anyhow!("index_stats: failed to acquire database query permit: {e}")
                        })?)
                    };

                    let row_res: anyhow::Result<Option<PgRow>> = if use_shared {
                        sqlx::query(INDEX_STATS_QUERY)
                            .fetch_optional(&shared_pool)
                            .instrument(query_span)
                            .await
                            .map_err(Into::into)
                    } else {
                        let Some(permit) = db_query_permit.as_ref() else {
                            return Err(anyhow!("index_stats: missing database query permit"));
                        };
                        match open_db_connection(&datname, permit).await {
                            Ok(mut conn) => sqlx::query(INDEX_STATS_QUERY)
                                .fetch_optional(&mut conn)
                                .instrument(query_span)
                                .await
                                .map_err(Into::into),
                            Err(e) => Err(e),
                        }
                    };

                    match row_res? {
                        Some(row) => Ok(Some(Self::sample_from_row(&row)?)),
                        None => Ok(None),
                    }
                });
            }

            let mut all_samples = Vec::new();
            let mut failures = Vec::new();
            let mut failed_db_count = 0;
            while let Some(joined) = tasks.join_next().await {
                match joined {
                    Ok(Ok(Some(sample))) => all_samples.push(sample),
                    Ok(Ok(None)) => {}
                    Ok(Err(e)) => {
                        error!(error=?e, "index_stats: task returned error");
                        failures.push(e.to_string());
                        failed_db_count += 1;
                    }
                    Err(e) => {
                        error!(error=?e, "index_stats: task join error");
                        failures.push(e.to_string());
                        failed_db_count += 1;
                    }
                }
            }

            if all_databases_failed(num_dbs, failed_db_count) {
                return Err(anyhow!(
                    "index_stats collection failed for ALL {failed_db_count} database task(s): {}",
                    failures.join("; ")
                ));
            }

            if !failures.is_empty() {
                error!(
                    failed_databases = failed_db_count,
                    errors = %failures.join("; "),
                    "index_stats: continuing with partial snapshot after per-database failures"
                );
            }

            self.reset_metrics();

            for sample in &all_samples {
                let labels = [sample.datname.as_str()];
                self.scans
                    .with_label_values(&labels)
                    .set(i64_to_f64(sample.scans));
                self.tuples_read
                    .with_label_values(&labels)
                    .set(i64_to_f64(sample.tuples_read));
                self.tuples_fetched
                    .with_label_values(&labels)
                    .set(i64_to_f64(sample.tuples_fetched));
                self.size_bytes
                    .with_label_values(&labels)
                    .set(i64_to_f64(sample.size_bytes));
                self.valid
                    .with_label_values(&labels)
                    .set(i64_to_f64(sample.valid));
                self.idx_blks_read
                    .with_label_values(&labels)
                    .set(i64_to_f64(sample.idx_blks_read));
                self.idx_blks_hit
                    .with_label_values(&labels)
                    .set(i64_to_f64(sample.idx_blks_hit));

                debug!(
                    datname = %sample.datname,
                    scans = sample.scans,
                    size_bytes = sample.size_bytes,
                    "updated pg_index stats metrics"
                );
            }

            Ok(())
        })
    }

    fn enabled_by_default(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_stats_collector_name() {
        let collector = IndexStatsCollector::new();
        assert_eq!(collector.name(), "index_stats");
    }

    #[test]
    fn test_index_stats_collector_registers() {
        let registry = Registry::new();
        let collector = IndexStatsCollector::new();
        assert!(collector.register_metrics(&registry).is_ok());
    }

    #[test]
    fn test_index_stats_query_is_per_database() {
        assert!(INDEX_STATS_QUERY.contains("current_database() AS datname"));
        assert!(INDEX_STATS_QUERY.contains("pg_stat_user_indexes"));
        assert!(INDEX_STATS_QUERY.contains("::bigint"));
    }

    #[test]
    fn test_index_stats_query_includes_block_io() {
        assert!(
            INDEX_STATS_QUERY.contains("LEFT JOIN pg_statio_user_indexes"),
            "query should left-join pg_statio_user_indexes so missing rows do not drop indexes"
        );
        assert!(
            INDEX_STATS_QUERY.contains("io.idx_blks_read"),
            "query should aggregate idx_blks_read from pg_statio_user_indexes"
        );
        assert!(
            INDEX_STATS_QUERY.contains("io.idx_blks_hit"),
            "query should aggregate idx_blks_hit from pg_statio_user_indexes"
        );
        assert!(
            INDEX_STATS_QUERY.contains("AS total_idx_blks_read")
                && INDEX_STATS_QUERY.contains("AS total_idx_blks_hit"),
            "query should expose aliased block-I/O aggregates"
        );
    }
}
