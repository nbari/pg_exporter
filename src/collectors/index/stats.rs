use crate::collectors::i64_to_f64;
use anyhow::Result;
use prometheus::{GaugeVec, Opts, Registry};
use sqlx::{Row, postgres::PgRow};
use tracing::debug;

/// Metric group for index usage statistics from `pg_stat_user_indexes`.
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
/// The shared index collector queries `pg_stat_user_indexes` in every
/// connectable, non-excluded database and labels each series
/// by `datname`. Connecting to a single database (e.g. `postgres`) is therefore enough to
/// observe index metrics across the whole cluster.
///
/// **Why it matters:**
/// - Low or zero scans indicate unused indexes that waste disk space and slow writes
/// - Invalid indexes (from failed CREATE INDEX CONCURRENTLY) must be dropped and recreated
/// - Large indexes with low usage suggest schema optimization opportunities
/// - High `tuples_read` vs `tuples_fetched` ratio may indicate inefficient index usage
#[derive(Clone)]
pub(super) struct IndexStatsMetrics {
    scans: GaugeVec,
    tuples_read: GaugeVec,
    tuples_fetched: GaugeVec,
    size_bytes: GaugeVec,
    valid: GaugeVec,
    idx_blks_read: GaugeVec,
    idx_blks_hit: GaugeVec,
}

impl Default for IndexStatsMetrics {
    fn default() -> Self {
        Self::new()
    }
}

const INDEX_STATS_LABELS: [&str; 1] = ["datname"];

/// Per-database aggregate of index usage statistics.
///
/// Original query retained for the readability-only compatibility fallback.
pub(super) const INDEX_STATS_QUERY: &str = r"
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
pub(super) struct IndexStatsSample {
    datname: String,
    scans: i64,
    tuples_read: i64,
    tuples_fetched: i64,
    size_bytes: i64,
    valid: i64,
    idx_blks_read: i64,
    idx_blks_hit: i64,
}

impl IndexStatsMetrics {
    /// Creates a new `IndexStatsMetrics`
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

    pub(super) fn reset_all(&self) {
        self.scans.reset();
        self.tuples_read.reset();
        self.tuples_fetched.reset();
        self.size_bytes.reset();
        self.valid.reset();
        self.idx_blks_read.reset();
        self.idx_blks_hit.reset();
    }

    pub(super) fn sample_from_row(row: &PgRow) -> Result<IndexStatsSample> {
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

impl IndexStatsMetrics {
    pub(super) fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.scans.clone()))?;
        registry.register(Box::new(self.tuples_read.clone()))?;
        registry.register(Box::new(self.tuples_fetched.clone()))?;
        registry.register(Box::new(self.size_bytes.clone()))?;
        registry.register(Box::new(self.valid.clone()))?;
        registry.register(Box::new(self.idx_blks_read.clone()))?;
        registry.register(Box::new(self.idx_blks_hit.clone()))?;
        Ok(())
    }

    pub(super) fn publish(&self, all_samples: &[IndexStatsSample]) {
        self.reset_all();

        for sample in all_samples {
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_stats_collector_registers() {
        let registry = Registry::new();
        let collector = IndexStatsMetrics::new();
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
