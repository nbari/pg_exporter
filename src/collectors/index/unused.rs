use crate::collectors::i64_to_f64;
use anyhow::Result;
use prometheus::{GaugeVec, Opts, Registry};
use sqlx::{Row, postgres::PgRow};
use tracing::debug;

/// Metric group for unused and invalid indexes.
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
/// The shared index collector queries these per-database catalogs in every
/// connectable, non-excluded database and
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
pub(super) struct UnusedIndexMetrics {
    unused_count: GaugeVec,
    unused_size_bytes: GaugeVec,
    invalid_count: GaugeVec,
}

impl Default for UnusedIndexMetrics {
    fn default() -> Self {
        Self::new()
    }
}

const UNUSED_INDEX_LABELS: [&str; 1] = ["datname"];

/// Per-database counts of unused (`idx_scan` = 0, excluding primary/unique constraints) and
/// invalid indexes. Original query retained for the readability-only compatibility fallback.
pub(super) const UNUSED_INDEX_QUERY: &str = r"
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
pub(super) struct UnusedIndexSample {
    datname: String,
    unused_count: i64,
    unused_size_bytes: i64,
    invalid_count: i64,
}

impl UnusedIndexMetrics {
    /// Creates a new `UnusedIndexMetrics`
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

    pub(super) fn reset_all(&self) {
        self.unused_count.reset();
        self.unused_size_bytes.reset();
        self.invalid_count.reset();
    }

    pub(super) fn sample_from_row(row: &PgRow) -> Result<UnusedIndexSample> {
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

impl UnusedIndexMetrics {
    pub(super) fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.unused_count.clone()))?;
        registry.register(Box::new(self.unused_size_bytes.clone()))?;
        registry.register(Box::new(self.invalid_count.clone()))?;
        Ok(())
    }

    pub(super) fn publish(&self, all_samples: &[UnusedIndexSample]) {
        self.reset_all();

        for sample in all_samples {
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unused_index_collector_registers() {
        let registry = Registry::new();
        let collector = UnusedIndexMetrics::new();
        assert!(collector.register_metrics(&registry).is_ok());
    }

    #[test]
    fn test_unused_index_query_is_per_database() {
        assert!(UNUSED_INDEX_QUERY.contains("current_database() AS datname"));
        assert!(UNUSED_INDEX_QUERY.contains("pg_stat_user_indexes"));
        assert!(UNUSED_INDEX_QUERY.contains("::bigint"));
    }
}
