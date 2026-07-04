use crate::collectors::i64_to_f64;
use crate::collectors::util::{
    get_default_database, get_excluded_databases, get_or_create_pool_for_db,
};
use crate::collectors::Collector;
use anyhow::{Result, anyhow};
use futures::future::BoxFuture;
use prometheus::{GaugeVec, Opts, Registry};
use sqlx::{PgPool, Row, postgres::PgRow};
use std::time::Duration;
use tokio::task::JoinSet;
use tracing::{debug, error, info_span, instrument};
use tracing_futures::Instrument as _;

/// Collector for unused and invalid indexes
///
/// **What it measures:**
/// Identifies indexes that have never been scanned (`idx_scan` = 0) and invalid indexes
/// from failed concurrent index builds. These represent maintenance opportunities and
/// potential performance improvements.
///
/// **Key metrics (labeled by `datname`):**
/// - `pg_index_unused_count`: Count of indexes that have never been used (`idx_scan` = 0)
/// - `pg_index_unused_size_bytes`: Total disk space wasted by unused indexes
/// - `pg_index_invalid_count`: Count of invalid indexes from failed CREATE INDEX CONCURRENTLY
///
/// **Multi-database:**
/// `pg_stat_user_indexes` and `pg_index` are per-database catalogs, so this collector
/// iterates every connectable, non-excluded database (like `pg_stat_user_tables`) and
/// labels each series by `datname`. Connecting to a single database is therefore enough to
/// observe unused/invalid indexes across the whole cluster.
///
/// **Why it matters:**
/// - **Write performance:** Every index slows down INSERT, UPDATE, and DELETE operations.
///   Unused indexes provide no `query` benefit but still incur write costs.
/// - **Disk space:** Indexes can be large. Unused indexes waste valuable storage.
/// - **Invalid indexes:** Cannot be used by queries but still consume resources and must be dropped.
///
/// **Important notes:**
/// - Primary key and unique constraint indexes should NOT be dropped even if unused
/// - Foreign key indexes with `idx_scan` = 0 may still be critical for referential integrity
/// - Check `pg_stat_user_indexes`.`idx_scan` over time; new indexes may start at zero
#[derive(Clone)]
pub struct UnusedIndexCollector {
    unused_count: GaugeVec,
    unused_size_bytes: GaugeVec,
    invalid_count: GaugeVec,
}

impl Default for UnusedIndexCollector {
    fn default() -> Self {
        Self::new()
    }
}

const UNUSED_INDEX_LABELS: [&str; 1] = ["datname"];
const PER_DATABASE_COLLECTION_TIMEOUT: Duration = Duration::from_secs(5);
const TASK_JOIN_WAIT_TIMEOUT: Duration = Duration::from_secs(10);

/// Per-database counts of unused (`idx_scan` = 0, excluding primary/unique constraints) and
/// invalid indexes. Both underlying catalogs only cover the current database, so this query
/// runs once per database and is tagged with `current_database()`.
const UNUSED_INDEX_QUERY: &str = r"
    SELECT
        current_database() AS datname,
        (
            SELECT COUNT(*)::bigint
            FROM pg_stat_user_indexes s
            JOIN pg_index i ON s.indexrelid = i.indexrelid
            WHERE s.idx_scan = 0
              AND NOT i.indisprimary
              AND NOT i.indisunique
              AND s.schemaname NOT IN ('pg_catalog', 'information_schema')
        ) AS unused_count,
        (
            SELECT COALESCE(SUM(pg_relation_size(s.indexrelid)), 0)::bigint
            FROM pg_stat_user_indexes s
            JOIN pg_index i ON s.indexrelid = i.indexrelid
            WHERE s.idx_scan = 0
              AND NOT i.indisprimary
              AND NOT i.indisunique
              AND s.schemaname NOT IN ('pg_catalog', 'information_schema')
        ) AS unused_size_bytes,
        (
            SELECT COUNT(*)::bigint
            FROM pg_index i
            JOIN pg_class c ON i.indexrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE NOT i.indisvalid
              AND n.nspname NOT IN ('pg_catalog', 'information_schema')
        ) AS invalid_count
    ";

#[derive(Clone, Debug)]
struct UnusedIndexSample {
    datname: String,
    unused_count: i64,
    unused_size_bytes: i64,
    invalid_count: i64,
}

impl UnusedIndexCollector {
    /// Creates a new `UnusedIndexCollector`
    ///
    /// # Panics
    ///
    /// Panics if metric creation fails (should never happen with valid metric names)
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        Self {
            unused_count: GaugeVec::new(
                Opts::new(
                    "pg_index_unused_count",
                    "Number of indexes that have never been scanned (idx_scan = 0, excluding primary/unique constraints)",
                ),
                &UNUSED_INDEX_LABELS,
            )
            .expect("Failed to create pg_index_unused_count"),
            unused_size_bytes: GaugeVec::new(
                Opts::new(
                    "pg_index_unused_size_bytes",
                    "Total size in bytes of unused indexes",
                ),
                &UNUSED_INDEX_LABELS,
            )
            .expect("Failed to create pg_index_unused_size_bytes"),
            invalid_count: GaugeVec::new(
                Opts::new(
                    "pg_index_invalid_count",
                    "Number of invalid indexes from failed CREATE INDEX CONCURRENTLY operations",
                ),
                &UNUSED_INDEX_LABELS,
            )
            .expect("Failed to create pg_index_invalid_count"),
        }
    }

    fn reset_metrics(&self) {
        self.unused_count.reset();
        self.unused_size_bytes.reset();
        self.invalid_count.reset();
    }

    fn sample_from_row(row: &PgRow) -> Result<UnusedIndexSample> {
        Ok(UnusedIndexSample {
            datname: row
                .try_get::<Option<String>, _>("datname")?
                .unwrap_or_else(|| "[unknown]".to_string()),
            unused_count: row.try_get("unused_count").unwrap_or(0),
            unused_size_bytes: row.try_get("unused_size_bytes").unwrap_or(0),
            invalid_count: row.try_get("invalid_count").unwrap_or(0),
        })
    }
}

impl Collector for UnusedIndexCollector {
    fn name(&self) -> &'static str {
        "index_unused"
    }

    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.unused_count.clone()))?;
        registry.register(Box::new(self.unused_size_bytes.clone()))?;
        registry.register(Box::new(self.invalid_count.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector = "index_unused", otel.kind = "internal")
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

            // 2) One task per DB: reuse shared pool for the default DB, tiny pool for others.
            let mut tasks: JoinSet<Result<Option<UnusedIndexSample>>> = JoinSet::new();

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
                        db.statement = "SELECT ... unused/invalid indexes",
                        db.sql.table = "pg_stat_user_indexes",
                        datname = %datname,
                        reuse_pool = use_shared
                    );

                    tokio::time::timeout(PER_DATABASE_COLLECTION_TIMEOUT, async move {
                        let row_res: anyhow::Result<Option<PgRow>> = if use_shared {
                            sqlx::query(UNUSED_INDEX_QUERY)
                                .fetch_optional(&shared_pool)
                                .instrument(query_span)
                                .await
                                .map_err(Into::into)
                        } else {
                            match get_or_create_pool_for_db(&datname).await {
                                Ok(per_db_pool) => sqlx::query(UNUSED_INDEX_QUERY)
                                    .fetch_optional(&per_db_pool)
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
                    })
                    .await
                    .map_err(|_| {
                        anyhow!(
                            "index_unused timed out collecting metrics for database {datname_for_timeout} after {PER_DATABASE_COLLECTION_TIMEOUT:?}"
                        )
                    })?
                });
            }

            let mut all_samples = Vec::new();
            let mut failures = Vec::new();
            while !tasks.is_empty() {
                match tokio::time::timeout(TASK_JOIN_WAIT_TIMEOUT, tasks.join_next()).await {
                    Ok(Some(Ok(Ok(Some(sample))))) => all_samples.push(sample),
                    Ok(Some(Ok(Ok(None)))) => {}
                    Ok(Some(Ok(Err(e)))) => {
                        error!(error=?e, "index_unused: task returned error");
                        failures.push(e.to_string());
                    }
                    Ok(Some(Err(e))) => {
                        error!(error=?e, "index_unused: task join error");
                        failures.push(e.to_string());
                    }
                    Ok(None) => break,
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
                    "index_unused collection failed for {} database task(s): {}",
                    failures.len(),
                    failures.join("; ")
                ));
            }

            if !failures.is_empty() {
                error!(
                    failed_databases = failures.len(),
                    errors = %failures.join("; "),
                    "index_unused: continuing with partial snapshot after per-database failures"
                );
            }

            self.reset_metrics();

            for sample in &all_samples {
                let labels = [sample.datname.as_str()];
                self.unused_count
                    .with_label_values(&labels)
                    .set(i64_to_f64(sample.unused_count));
                self.unused_size_bytes
                    .with_label_values(&labels)
                    .set(i64_to_f64(sample.unused_size_bytes));
                self.invalid_count
                    .with_label_values(&labels)
                    .set(i64_to_f64(sample.invalid_count));

                debug!(
                    datname = %sample.datname,
                    unused_count = sample.unused_count,
                    invalid_count = sample.invalid_count,
                    "updated pg_index unused metrics"
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
    fn test_unused_index_collector_name() {
        let collector = UnusedIndexCollector::new();
        assert_eq!(collector.name(), "index_unused");
    }

    #[test]
    fn test_unused_index_collector_registers() {
        let registry = Registry::new();
        let collector = UnusedIndexCollector::new();
        assert!(collector.register_metrics(&registry).is_ok());
    }

    #[test]
    fn test_unused_index_query_is_per_database() {
        assert!(UNUSED_INDEX_QUERY.contains("current_database() AS datname"));
        assert!(UNUSED_INDEX_QUERY.contains("pg_stat_user_indexes"));
        assert!(UNUSED_INDEX_QUERY.contains("::bigint"));
    }
}
