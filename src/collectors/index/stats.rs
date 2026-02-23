use crate::collectors::{Collector, i64_to_f64, util::{PG_CATALOG, INFORMATION_SCHEMA}};
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{Gauge, Opts, Registry};
use sqlx::PgPool;

/// Collector for index usage statistics from `pg_stat_user_indexes`
///
/// **What it measures:**
/// Tracks index usage patterns including scan counts, tuples read/fetched, and size metrics.
/// Helps identify which indexes are being used effectively and which may be candidates for removal.
///
/// **Key metrics:**
/// - `pg_index_scans_total`: Number of index scans initiated on this index
/// - `pg_index_tuples_read_total`: Number of index entries returned by scans
/// - `pg_index_tuples_fetched_total`: Number of live table rows fetched by index scans
/// - `pg_index_size_bytes`: Size of the index in bytes
/// - `pg_index_valid`: Whether the index is valid (1) or invalid (0)
///
/// **Why it matters:**
/// - Low or zero scans indicate unused indexes that waste disk space and slow writes
/// - Invalid indexes (from failed CREATE INDEX CONCURRENTLY) must be dropped and recreated
/// - Large indexes with low usage suggest schema optimization opportunities
/// - High `tuples_read` vs `tuples_fetched` ratio may indicate inefficient index usage
///
/// **Diagnostic use cases:**
/// - Identify indexes safe to drop (`idx_scan` = 0, not supporting constraints)
/// - Detect failed concurrent index builds (indisvalid = false)
/// - Calculate index bloat by comparing actual size to estimated optimal size
/// - Monitor index usage patterns after `query` optimization changes
#[derive(Clone)]
pub struct IndexStatsCollector {
    scans: Gauge,
    tuples_read: Gauge,
    tuples_fetched: Gauge,
    size_bytes: Gauge,
    valid: Gauge,
}

impl Default for IndexStatsCollector {
    fn default() -> Self {
        Self::new()
    }
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
            scans: Gauge::with_opts(Opts::new(
                "pg_index_scans_total",
                "Number of index scans initiated on this index",
            ))
            .expect("Failed to create pg_index_scans_total"),
            tuples_read: Gauge::with_opts(Opts::new(
                "pg_index_tuples_read_total",
                "Number of index entries returned by scans on this index",
            ))
            .expect("Failed to create pg_index_tuples_read_total"),
            tuples_fetched: Gauge::with_opts(Opts::new(
                "pg_index_tuples_fetched_total",
                "Number of live table rows fetched by simple index scans using this index",
            ))
            .expect("Failed to create pg_index_tuples_fetched_total"),
            size_bytes: Gauge::with_opts(Opts::new(
                "pg_index_size_bytes",
                "Size of the index in bytes",
            ))
            .expect("Failed to create pg_index_size_bytes"),
            valid: Gauge::with_opts(Opts::new(
                "pg_index_valid",
                "Whether the index is valid (1) or invalid (0, from failed CREATE INDEX CONCURRENTLY)",
            ))
            .expect("Failed to create pg_index_valid"),
        }
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
        Ok(())
    }

    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        #[derive(sqlx::FromRow)]
        struct IndexStats {
            total_scans: i64,
            total_tup_read: i64,
            total_tup_fetch: i64,
            total_size_bytes: i64,
            valid_count: i64,
        }

        Box::pin(async move {
            // Query pg_stat_user_indexes joined with pg_class for size and pg_index for validity
            // Excludes system databases and tracks key index health metrics
            let query = format!(
                r"
                SELECT 
                    COALESCE(SUM(s.idx_scan), 0)::BIGINT as total_scans,
                    COALESCE(SUM(s.idx_tup_read), 0)::BIGINT as total_tup_read,
                    COALESCE(SUM(s.idx_tup_fetch), 0)::BIGINT as total_tup_fetch,
                    COALESCE(SUM(pg_relation_size(s.indexrelid)), 0)::BIGINT as total_size_bytes,
                    COALESCE(SUM(i.indisvalid::int), 0)::BIGINT as valid_count
                FROM pg_stat_user_indexes s
                JOIN pg_index i ON s.indexrelid = i.indexrelid
                WHERE s.schemaname NOT IN ('{PG_CATALOG}', '{INFORMATION_SCHEMA}')
                "
            );

            let stats: IndexStats = sqlx::query_as(&query).fetch_one(pool).await?;

            // Update metrics
            self.scans.set(i64_to_f64(stats.total_scans));
            self.tuples_read.set(i64_to_f64(stats.total_tup_read));
            self.tuples_fetched.set(i64_to_f64(stats.total_tup_fetch));
            self.size_bytes.set(i64_to_f64(stats.total_size_bytes));
            self.valid.set(i64_to_f64(stats.valid_count));

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
    async fn test_index_stats_collector_collects_from_database() {
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

        let collector = IndexStatsCollector::new();
        let registry = Registry::new();
        collector
            .register_metrics(&registry)
            .expect("Failed to register metrics");

        let result = collector.collect(&pool).await;
        assert!(
            result.is_ok(),
            "Collection should succeed: {:?}",
            result.err()
        );

        // Verify metrics are present (values will vary by database state)
        let metrics = registry.gather();
        assert!(!metrics.is_empty(), "Should have collected metrics");

        let metric_names: Vec<String> = metrics.iter().map(|m| m.name().to_string()).collect();
        assert!(metric_names.contains(&"pg_index_scans_total".to_string()));
        assert!(metric_names.contains(&"pg_index_tuples_read_total".to_string()));
        assert!(metric_names.contains(&"pg_index_tuples_fetched_total".to_string()));
        assert!(metric_names.contains(&"pg_index_size_bytes".to_string()));
        assert!(metric_names.contains(&"pg_index_valid".to_string()));
    }

    #[test]
    fn test_index_stats_collector_name() {
        let collector = IndexStatsCollector::new();
        assert_eq!(collector.name(), "index_stats");
    }
}
