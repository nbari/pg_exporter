//! Cluster-wide SLRU cache counters from `pg_stat_slru` (`PostgreSQL` 13+).
//!
//! `pg_stat_slru` exposes counters for `PostgreSQL` simple least-recently-used
//! caches such as commit status (`Xact`/CLOG), subtransactions (`Subtrans`),
//! multixacts, notifications, and serializable transaction state. These counters
//! are useful when diagnosing SLRU pressure: flat, near-zero `blks_read` values
//! are healthy, while sustained `Subtrans` or multixact reads are a strong signal
//! that transaction or multixact state is spilling out of cache.
//!
//! This is a **cluster-wide** view, so the collector reads only the shared pool
//! and never fans out per database. It is disabled by default because the metrics
//! are targeted diagnostics for `PostgreSQL` 13+ clusters.
//!
//! # Version handling
//!
//! `pg_stat_slru` was introduced in `PostgreSQL` 13. On older servers the
//! collector skips cleanly (no error, no populated series) and logs a single
//! warning that `PostgreSQL` 13+ is required.

use crate::collectors::{Collector, util::get_pg_version};
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{IntGaugeVec, Opts, Registry};
use sqlx::{PgPool, Row};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tracing::{debug, info_span, instrument, warn};
use tracing_futures::Instrument as _;

/// `pg_stat_slru` was introduced in `PostgreSQL` 13.
const MIN_PG_STAT_SLRU_VERSION: i32 = 130_000;

/// Labels shared by every `pg_stat_slru` metric.
const SLRU_LABELS: [&str; 1] = ["name"];

/// The static `pg_stat_slru` query used on supported servers.
const PG_STAT_SLRU_QUERY: &str = r"
    SELECT
        name,
        blks_zeroed::bigint AS blks_zeroed,
        blks_hit::bigint AS blks_hit,
        blks_read::bigint AS blks_read,
        blks_written::bigint AS blks_written,
        blks_exists::bigint AS blks_exists,
        flushes::bigint AS flushes,
        truncates::bigint AS truncates
    FROM pg_stat_slru
";

/// How the running server exposes `pg_stat_slru`, resolved from `server_version_num`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SlruSupport {
    /// Server predates `PostgreSQL` 13, where `pg_stat_slru` was introduced, so
    /// the view does not exist and the collector must skip.
    Unsupported,
    /// Server exposes `pg_stat_slru`.
    Supported,
}

/// Maps a `server_version_num` (for example `130_000`) to whether
/// `pg_stat_slru` should be read.
///
/// This is kept as a standalone, pure function so the version gate is unit
/// testable without a live server.
const fn slru_support(version_num: i32) -> SlruSupport {
    if version_num < MIN_PG_STAT_SLRU_VERSION {
        SlruSupport::Unsupported
    } else {
        SlruSupport::Supported
    }
}

/// Exposes `pg_stat_slru` cluster-wide SLRU cache counters (`PostgreSQL` 13+).
///
/// All series carry the `name` label. Counter values are cumulative since the
/// last `pg_stat_slru` reset; use `rate()`/`increase()` in `PromQL`.
///
/// **SLRU counters (`IntGauge`):**
/// - `pg_stat_slru_blks_zeroed_total`
/// - `pg_stat_slru_blks_hit_total`
/// - `pg_stat_slru_blks_read_total`
/// - `pg_stat_slru_blks_written_total`
/// - `pg_stat_slru_blks_exists_total`
/// - `pg_stat_slru_flushes_total`
/// - `pg_stat_slru_truncates_total`
#[derive(Clone)]
pub struct PgStatSlruCollector {
    blks_zeroed: IntGaugeVec,
    blks_hit: IntGaugeVec,
    blks_read: IntGaugeVec,
    blks_written: IntGaugeVec,
    blks_exists: IntGaugeVec,
    flushes: IntGaugeVec,
    truncates: IntGaugeVec,
    /// Ensures the "requires `PostgreSQL` 13+" warning is logged at most once per
    /// process instead of on every scrape against an unsupported server.
    unsupported_warned: Arc<AtomicBool>,
}

impl Default for PgStatSlruCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl PgStatSlruCollector {
    /// Creates a new `PgStatSlruCollector` with all metrics initialized.
    ///
    /// # Panics
    ///
    /// Panics if metric creation fails, which only happens with an invalid
    /// metric name or label set and therefore never at runtime.
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        Self {
            blks_zeroed: IntGaugeVec::new(
                Opts::new(
                    "pg_stat_slru_blks_zeroed_total",
                    "Number of SLRU blocks zeroed during initialization, by SLRU name",
                ),
                &SLRU_LABELS,
            )
            .expect("Failed to create pg_stat_slru_blks_zeroed_total"),
            blks_hit: IntGaugeVec::new(
                Opts::new(
                    "pg_stat_slru_blks_hit_total",
                    "Number of SLRU block cache hits, by SLRU name",
                ),
                &SLRU_LABELS,
            )
            .expect("Failed to create pg_stat_slru_blks_hit_total"),
            blks_read: IntGaugeVec::new(
                Opts::new(
                    "pg_stat_slru_blks_read_total",
                    "Number of SLRU blocks read from disk, by SLRU name",
                ),
                &SLRU_LABELS,
            )
            .expect("Failed to create pg_stat_slru_blks_read_total"),
            blks_written: IntGaugeVec::new(
                Opts::new(
                    "pg_stat_slru_blks_written_total",
                    "Number of SLRU blocks written to disk, by SLRU name",
                ),
                &SLRU_LABELS,
            )
            .expect("Failed to create pg_stat_slru_blks_written_total"),
            blks_exists: IntGaugeVec::new(
                Opts::new(
                    "pg_stat_slru_blks_exists_total",
                    "Number of SLRU blocks found to already exist, by SLRU name",
                ),
                &SLRU_LABELS,
            )
            .expect("Failed to create pg_stat_slru_blks_exists_total"),
            flushes: IntGaugeVec::new(
                Opts::new(
                    "pg_stat_slru_flushes_total",
                    "Number of SLRU flushes, by SLRU name",
                ),
                &SLRU_LABELS,
            )
            .expect("Failed to create pg_stat_slru_flushes_total"),
            truncates: IntGaugeVec::new(
                Opts::new(
                    "pg_stat_slru_truncates_total",
                    "Number of SLRU truncates, by SLRU name",
                ),
                &SLRU_LABELS,
            )
            .expect("Failed to create pg_stat_slru_truncates_total"),
            unsupported_warned: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Clears every series so label combinations that disappear between scrapes
    /// do not linger as stale data.
    fn reset_all(&self) {
        self.blks_zeroed.reset();
        self.blks_hit.reset();
        self.blks_read.reset();
        self.blks_written.reset();
        self.blks_exists.reset();
        self.flushes.reset();
        self.truncates.reset();
    }

    fn apply_row(&self, row: &sqlx::postgres::PgRow) {
        let name: String = row.try_get("name").unwrap_or_default();
        let labels = [name.as_str()];

        self.blks_zeroed
            .with_label_values(&labels)
            .set(row.try_get::<i64, _>("blks_zeroed").unwrap_or(0));
        self.blks_hit
            .with_label_values(&labels)
            .set(row.try_get::<i64, _>("blks_hit").unwrap_or(0));
        self.blks_read
            .with_label_values(&labels)
            .set(row.try_get::<i64, _>("blks_read").unwrap_or(0));
        self.blks_written
            .with_label_values(&labels)
            .set(row.try_get::<i64, _>("blks_written").unwrap_or(0));
        self.blks_exists
            .with_label_values(&labels)
            .set(row.try_get::<i64, _>("blks_exists").unwrap_or(0));
        self.flushes
            .with_label_values(&labels)
            .set(row.try_get::<i64, _>("flushes").unwrap_or(0));
        self.truncates
            .with_label_values(&labels)
            .set(row.try_get::<i64, _>("truncates").unwrap_or(0));
    }
}

/// Resolves the server version, preferring the cached value set at startup and
/// falling back to a direct query.
async fn resolve_server_version(pool: &PgPool) -> Result<i32> {
    let cached = get_pg_version();
    if cached > 0 {
        return Ok(cached);
    }

    let row = sqlx::query("SELECT current_setting('server_version_num')::int AS v")
        .fetch_one(pool)
        .await?;
    Ok(row.try_get::<i32, _>("v")?)
}

impl Collector for PgStatSlruCollector {
    fn name(&self) -> &'static str {
        "pg_stat_slru"
    }

    #[instrument(skip(self, registry), level = "info", err, fields(collector = "pg_stat_slru"))]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.blks_zeroed.clone()))?;
        registry.register(Box::new(self.blks_hit.clone()))?;
        registry.register(Box::new(self.blks_read.clone()))?;
        registry.register(Box::new(self.blks_written.clone()))?;
        registry.register(Box::new(self.blks_exists.clone()))?;
        registry.register(Box::new(self.flushes.clone()))?;
        registry.register(Box::new(self.truncates.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector = "pg_stat_slru", otel.kind = "internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let version_num = resolve_server_version(pool).await?;

            if slru_support(version_num) == SlruSupport::Unsupported {
                if !self.unsupported_warned.swap(true, Ordering::Relaxed) {
                    warn!(
                        server_version_num = version_num,
                        "collector.slru is enabled but pg_stat_slru requires PostgreSQL 13+; \
                         skipping (no metrics will be exported until the server is upgraded)"
                    );
                }
                debug!("Skipping pg_stat_slru metrics (requires PostgreSQL 13+)");
                return Ok(());
            }

            let query_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT ... FROM pg_stat_slru",
                db.sql.table = "pg_stat_slru"
            );

            let rows = sqlx::query(PG_STAT_SLRU_QUERY)
                .fetch_all(pool)
                .instrument(query_span)
                .await?;

            self.reset_all();

            for row in &rows {
                self.apply_row(row);
            }

            debug!(rows = rows.len(), "updated pg_stat_slru metrics");

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
    fn collector_name_is_pg_stat_slru() {
        assert_eq!(PgStatSlruCollector::new().name(), "pg_stat_slru");
    }

    #[test]
    fn collector_is_disabled_by_default() {
        assert!(!PgStatSlruCollector::new().enabled_by_default());
    }

    #[test]
    fn query_casts_all_counter_columns() {
        for column in [
            "blks_zeroed",
            "blks_hit",
            "blks_read",
            "blks_written",
            "blks_exists",
            "flushes",
            "truncates",
        ] {
            assert!(
                PG_STAT_SLRU_QUERY.contains(&format!("{column}::bigint AS {column}")),
                "`pg_stat_slru` query must cast `{column}` to `bigint`"
            );
        }
    }

    #[test]
    fn versions_before_pg13_are_unsupported() {
        assert_eq!(slru_support(0), SlruSupport::Unsupported);
        assert_eq!(slru_support(110_000), SlruSupport::Unsupported);
        assert_eq!(slru_support(120_000), SlruSupport::Unsupported);
        assert_eq!(slru_support(129_999), SlruSupport::Unsupported);
    }

    #[test]
    fn pg13_and_newer_are_supported() {
        assert_eq!(slru_support(130_000), SlruSupport::Supported);
        assert_eq!(slru_support(140_000), SlruSupport::Supported);
        assert_eq!(slru_support(180_000), SlruSupport::Supported);
    }

    #[test]
    fn register_metrics_succeeds_with_unique_names() {
        let registry = Registry::new();
        assert!(PgStatSlruCollector::new().register_metrics(&registry).is_ok());
    }
}
