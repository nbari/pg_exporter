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

            // 2) One task per DB. The default DB reuses the shared pool; every other database
            // must pass through the global per-database connection limiter.
            let mut tasks: JoinSet<Result<Option<UnusedIndexSample>>> = JoinSet::new();

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
                        db.statement = "SELECT ... unused/invalid indexes",
                        db.sql.table = "pg_stat_user_indexes",
                        datname = %datname,
                        reuse_pool = use_shared
                    );

                    let db_query_permit = if use_shared {
                        None
                    } else {
                        Some(acquire_db_query_permit().await.map_err(|e| {
                            anyhow!("index_unused: failed to acquire database query permit: {e}")
                        })?)
                    };

                    let row_res: anyhow::Result<Option<PgRow>> = if use_shared {
                        sqlx::query(UNUSED_INDEX_QUERY)
                            .fetch_optional(&shared_pool)
                            .instrument(query_span)
                            .await
                            .map_err(Into::into)
                    } else {
                        let Some(permit) = db_query_permit.as_ref() else {
                            return Err(anyhow!("index_unused: missing database query permit"));
                        };
                        match open_db_connection(&datname, permit).await {
                            Ok(mut conn) => sqlx::query(UNUSED_INDEX_QUERY)
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
                        error!(error=?e, "index_unused: task returned error");
                        failures.push(e.to_string());
                        failed_db_count += 1;
                    }
                    Err(e) => {
                        error!(error=?e, "index_unused: task join error");
                        failures.push(e.to_string());
                        failed_db_count += 1;
                    }
                }
            }

            if all_databases_failed(num_dbs, failed_db_count) {
                return Err(anyhow!(
                    "index_unused collection failed for ALL {failed_db_count} database task(s): {}",
                    failures.join("; ")
                ));
            }

            if !failures.is_empty() {
                error!(
                    failed_databases = failed_db_count,
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
