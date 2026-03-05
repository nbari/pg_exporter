use crate::collectors::{Collector, i64_to_f64, util::{PG_CATALOG, INFORMATION_SCHEMA}};
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{Gauge, Opts, Registry};
use sqlx::PgPool;

/// Collector for unused and invalid indexes
///
/// **What it measures:**
/// Identifies indexes that have never been scanned (`idx_scan` = 0) and invalid indexes
/// from failed concurrent index builds. These represent maintenance opportunities and
/// potential performance improvements.
///
/// **Key metrics:**
/// - `pg_index_unused_count`: Count of indexes that have never been used (`idx_scan` = 0)
/// - `pg_index_unused_size_bytes`: Total disk space wasted by unused indexes
/// - `pg_index_invalid_count`: Count of invalid indexes from failed CREATE INDEX CONCURRENTLY
///
/// **Why it matters:**
/// - **Write performance:** Every index slows down INSERT, UPDATE, and DELETE operations.
///   Unused indexes provide no `query` benefit but still incur write costs.
/// - **Disk space:** Indexes can be large. Unused indexes waste valuable storage.
/// - **Maintenance overhead:** VACUUM and other maintenance operations must process unused indexes.
/// - **Invalid indexes:** Cannot be used by queries but still consume resources and must be dropped.
///
/// **Common causes of unused indexes:**
/// - Over-indexing during development without production `query` analysis
/// - Duplicate or redundant indexes (covered by multi-column indexes)
/// - Legacy indexes from refactored queries
/// - Speculative indexes that were never beneficial
///
/// **Diagnostic use cases:**
/// - Identify safe index drops to improve write performance
/// - Calculate total disk space recoverable by dropping unused indexes
/// - Detect failed concurrent index builds that need cleanup
/// - Monitor index usage after application changes
///
/// **Important notes:**
/// - Primary key and unique constraint indexes should NOT be dropped even if unused
/// - Foreign key indexes with `idx_scan` = 0 may still be critical for referential integrity
/// - Check `pg_stat_user_indexes`.`idx_scan` over time; new indexes may start at zero
/// - Always verify with application team before dropping any index
#[derive(Clone)]
pub struct UnusedIndexCollector {
    unused_count: Gauge,
    unused_size_bytes: Gauge,
    invalid_count: Gauge,
}

impl Default for UnusedIndexCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl UnusedIndexCollector {
    /// Creates a new `UnusedIndexesCollector`
    ///
    /// # Panics
    ///
    /// Panics if metric creation fails (should never happen with valid metric names)
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        Self {
            unused_count: Gauge::with_opts(Opts::new(
                "pg_index_unused_count",
                "Number of indexes that have never been scanned (idx_scan = 0, excluding primary/unique constraints)",
            ))
            .expect("Failed to create pg_index_unused_count"),
            unused_size_bytes: Gauge::with_opts(Opts::new(
                "pg_index_unused_size_bytes",
                "Total size in bytes of unused indexes",
            ))
            .expect("Failed to create pg_index_unused_size_bytes"),
            invalid_count: Gauge::with_opts(Opts::new(
                "pg_index_invalid_count",
                "Number of invalid indexes from failed CREATE INDEX CONCURRENTLY operations",
            ))
            .expect("Failed to create pg_index_invalid_count"),
        }
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

    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            // Query for unused indexes (idx_scan = 0)
            // Exclude primary keys and unique constraints as they may not be scanned but are still critical
            let unused_query = format!(
                r"
                SELECT 
                    COUNT(*)::BIGINT as unused_count,
                    COALESCE(SUM(pg_relation_size(s.indexrelid)), 0)::BIGINT as unused_size_bytes
                FROM pg_stat_user_indexes s
                JOIN pg_index i ON s.indexrelid = i.indexrelid
                WHERE s.idx_scan = 0
                    AND NOT i.indisprimary
                    AND NOT i.indisunique
                    AND s.schemaname NOT IN ('{PG_CATALOG}', '{INFORMATION_SCHEMA}')
                "
            );

            let (unused_count, unused_size_bytes): (i64, i64) =
                sqlx::query_as(&unused_query).fetch_one(pool).await?;

            // Query for invalid indexes
            let invalid_query = format!(
                r"
                SELECT COUNT(*)::BIGINT as invalid_count
                FROM pg_index i
                JOIN pg_class c ON i.indexrelid = c.oid
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE NOT i.indisvalid
                    AND n.nspname NOT IN ('{PG_CATALOG}', '{INFORMATION_SCHEMA}')
                "
            );

            let (invalid_count,): (i64,) = sqlx::query_as(&invalid_query).fetch_one(pool).await?;

            // Update metrics
            self.unused_count.set(i64_to_f64(unused_count));
            self.unused_size_bytes.set(i64_to_f64(unused_size_bytes));
            self.invalid_count.set(i64_to_f64(invalid_count));

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

    #[tokio::test]
    #[allow(clippy::expect_used)]
    async fn test_unused_index_collector_collects_from_database() {
        let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| String::new());

        if database_url.is_empty() {
            eprintln!("Skipping test: DATABASE_URL not set");
            return;
        }

        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .expect("Failed to connect to database");

        let collector = UnusedIndexCollector::new();
        let registry = Registry::new();
        let result_reg = collector.register_metrics(&registry);
        assert!(result_reg.is_ok(), "Failed to register metrics");

        let result = collector.collect(&pool).await;
        assert!(
            result.is_ok(),
            "Collection should succeed: {:?}",
            result.err()
        );

        // Verify metrics are present
        let metrics = registry.gather();
        assert!(!metrics.is_empty(), "Should have collected metrics");

        let metric_names: Vec<String> = metrics.iter().map(|m| m.name().to_string()).collect();
        assert!(metric_names.contains(&"pg_index_unused_count".to_string()));
        assert!(metric_names.contains(&"pg_index_unused_size_bytes".to_string()));
        assert!(metric_names.contains(&"pg_index_invalid_count".to_string()));
    }

    #[test]
    fn test_unused_index_collector_name() {
        let collector = UnusedIndexCollector::new();
        assert_eq!(collector.name(), "index_unused");
    }
}
