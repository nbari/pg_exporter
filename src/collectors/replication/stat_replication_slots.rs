//! Logical replication slot spill and stream statistics from
//! `pg_stat_replication_slots` (`PostgreSQL` 14+).
//!
//! `pg_stat_replication_slots` is a **cluster-wide** view, so this collector
//! reads only the shared pool and never fans out per database. It is wired under
//! the opt-in `--collector.replication` umbrella.
//!
//! On servers older than `PostgreSQL` 14 the view does not exist. The collector
//! skips cleanly, exports no series, and logs a single warning so operators know
//! why the `pg_stat_replication_slots_*` metrics are absent.

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

/// `pg_stat_replication_slots` was introduced in `PostgreSQL` 14.
const MIN_STAT_REPLICATION_SLOTS_VERSION: i32 = 140_000;

/// Labels shared by every `pg_stat_replication_slots` metric.
const STAT_REPLICATION_SLOTS_LABELS: [&str; 1] = ["slot_name"];

const STAT_REPLICATION_SLOTS_QUERY: &str = r"
SELECT
    slot_name,
    spill_txns::bigint AS spill_txns,
    spill_count::bigint AS spill_count,
    spill_bytes::bigint AS spill_bytes,
    stream_txns::bigint AS stream_txns,
    stream_count::bigint AS stream_count,
    stream_bytes::bigint AS stream_bytes,
    total_txns::bigint AS total_txns,
    total_bytes::bigint AS total_bytes
FROM pg_stat_replication_slots
WHERE slot_name IS NOT NULL
";

/// Returns whether `pg_stat_replication_slots` exists for `server_version_num`.
#[must_use]
const fn supports_stat_replication_slots(version_num: i32) -> bool {
    version_num >= MIN_STAT_REPLICATION_SLOTS_VERSION
}

/// Resolves the server version, preferring the startup cache and falling back to
/// a direct `PostgreSQL` query for tests that bypass exporter bootstrap.
async fn resolve_server_version(pool: &PgPool) -> Result<i32> {
    let cached = get_pg_version();
    if cached > 0 {
        return Ok(cached);
    }

    let row = sqlx::query("SELECT current_setting('server_version_num')::int AS v")
        .fetch_one(pool)
        .await?;
    Ok(row.try_get::<i32, _>("v").unwrap_or(0))
}

/// Exposes `pg_stat_replication_slots` logical replication slot statistics
/// (`PostgreSQL` 14+).
///
/// All series carry the `slot_name` label. Values are cumulative since the last
/// `pg_stat_reset_replication_slot()` or `stats_reset`; use `rate()`/`increase()`
/// in `PromQL`.
///
/// **Spill metrics (`IntGauge`):**
/// - `pg_stat_replication_slots_spill_txns_total`
/// - `pg_stat_replication_slots_spill_count_total`
/// - `pg_stat_replication_slots_spill_bytes_total`
///
/// **Streaming metrics (`IntGauge`):**
/// - `pg_stat_replication_slots_stream_txns_total`
/// - `pg_stat_replication_slots_stream_count_total`
/// - `pg_stat_replication_slots_stream_bytes_total`
///
/// **Total logical decoding metrics (`IntGauge`):**
/// - `pg_stat_replication_slots_total_txns_total`
/// - `pg_stat_replication_slots_total_bytes_total`
#[derive(Clone)]
pub struct StatReplicationSlotsCollector {
    spill_txns: IntGaugeVec,
    spill_count: IntGaugeVec,
    spill_bytes: IntGaugeVec,
    stream_txns: IntGaugeVec,
    stream_count: IntGaugeVec,
    stream_bytes: IntGaugeVec,
    total_txns: IntGaugeVec,
    total_bytes: IntGaugeVec,
    unsupported_warned: Arc<AtomicBool>,
}

impl Default for StatReplicationSlotsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl StatReplicationSlotsCollector {
    /// Creates a new `StatReplicationSlotsCollector` with all metrics initialized.
    ///
    /// # Panics
    ///
    /// Panics if metric creation fails, which only happens with an invalid
    /// metric name or label set and therefore never at runtime.
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        let spill_txns = IntGaugeVec::new(
            Opts::new(
                "pg_stat_replication_slots_spill_txns_total",
                "Transactions spilled to disk while decoding logical changes, by slot",
            ),
            &STAT_REPLICATION_SLOTS_LABELS,
        )
        .expect("pg_stat_replication_slots_spill_txns_total metric");

        let spill_count = IntGaugeVec::new(
            Opts::new(
                "pg_stat_replication_slots_spill_count_total",
                "Times logical decoding changes were spilled to disk, by slot",
            ),
            &STAT_REPLICATION_SLOTS_LABELS,
        )
        .expect("pg_stat_replication_slots_spill_count_total metric");

        let spill_bytes = IntGaugeVec::new(
            Opts::new(
                "pg_stat_replication_slots_spill_bytes_total",
                "Bytes spilled to disk while decoding logical changes, by slot",
            ),
            &STAT_REPLICATION_SLOTS_LABELS,
        )
        .expect("pg_stat_replication_slots_spill_bytes_total metric");

        let stream_txns = IntGaugeVec::new(
            Opts::new(
                "pg_stat_replication_slots_stream_txns_total",
                "Transactions streamed to the decoding output plugin, by slot",
            ),
            &STAT_REPLICATION_SLOTS_LABELS,
        )
        .expect("pg_stat_replication_slots_stream_txns_total metric");

        let stream_count = IntGaugeVec::new(
            Opts::new(
                "pg_stat_replication_slots_stream_count_total",
                "Times logical decoding changes were streamed to the output plugin, by slot",
            ),
            &STAT_REPLICATION_SLOTS_LABELS,
        )
        .expect("pg_stat_replication_slots_stream_count_total metric");

        let stream_bytes = IntGaugeVec::new(
            Opts::new(
                "pg_stat_replication_slots_stream_bytes_total",
                "Bytes streamed to the decoding output plugin, by slot",
            ),
            &STAT_REPLICATION_SLOTS_LABELS,
        )
        .expect("pg_stat_replication_slots_stream_bytes_total metric");

        let total_txns = IntGaugeVec::new(
            Opts::new(
                "pg_stat_replication_slots_total_txns_total",
                "Transactions decoded for logical replication, by slot",
            ),
            &STAT_REPLICATION_SLOTS_LABELS,
        )
        .expect("pg_stat_replication_slots_total_txns_total metric");

        let total_bytes = IntGaugeVec::new(
            Opts::new(
                "pg_stat_replication_slots_total_bytes_total",
                "Bytes decoded for logical replication, by slot",
            ),
            &STAT_REPLICATION_SLOTS_LABELS,
        )
        .expect("pg_stat_replication_slots_total_bytes_total metric");

        Self {
            spill_txns,
            spill_count,
            spill_bytes,
            stream_txns,
            stream_count,
            stream_bytes,
            total_txns,
            total_bytes,
            unsupported_warned: Arc::new(AtomicBool::new(false)),
        }
    }

    fn reset_all(&self) {
        self.spill_txns.reset();
        self.spill_count.reset();
        self.spill_bytes.reset();
        self.stream_txns.reset();
        self.stream_count.reset();
        self.stream_bytes.reset();
        self.total_txns.reset();
        self.total_bytes.reset();
    }

    fn apply_row(&self, row: &sqlx::postgres::PgRow) {
        let slot_name: String = row.try_get("slot_name").unwrap_or_default();
        let labels = [slot_name.as_str()];

        let spill_txns = row.try_get::<i64, _>("spill_txns").unwrap_or(0);
        let spill_count = row.try_get::<i64, _>("spill_count").unwrap_or(0);
        let spill_bytes = row.try_get::<i64, _>("spill_bytes").unwrap_or(0);
        let stream_txns = row.try_get::<i64, _>("stream_txns").unwrap_or(0);
        let stream_count = row.try_get::<i64, _>("stream_count").unwrap_or(0);
        let stream_bytes = row.try_get::<i64, _>("stream_bytes").unwrap_or(0);
        let total_txns = row.try_get::<i64, _>("total_txns").unwrap_or(0);
        let total_bytes = row.try_get::<i64, _>("total_bytes").unwrap_or(0);

        self.spill_txns
            .with_label_values(&labels)
            .set(spill_txns);
        self.spill_count
            .with_label_values(&labels)
            .set(spill_count);
        self.spill_bytes
            .with_label_values(&labels)
            .set(spill_bytes);
        self.stream_txns
            .with_label_values(&labels)
            .set(stream_txns);
        self.stream_count
            .with_label_values(&labels)
            .set(stream_count);
        self.stream_bytes
            .with_label_values(&labels)
            .set(stream_bytes);
        self.total_txns
            .with_label_values(&labels)
            .set(total_txns);
        self.total_bytes
            .with_label_values(&labels)
            .set(total_bytes);

        debug!(
            slot_name = %slot_name,
            spill_txns,
            spill_count,
            spill_bytes,
            stream_txns,
            stream_count,
            stream_bytes,
            total_txns,
            total_bytes,
            "collected pg_stat_replication_slots metric"
        );
    }
}

impl Collector for StatReplicationSlotsCollector {
    fn name(&self) -> &'static str {
        "stat_replication_slots"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "stat_replication_slots")
    )]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.spill_txns.clone()))?;
        registry.register(Box::new(self.spill_count.clone()))?;
        registry.register(Box::new(self.spill_bytes.clone()))?;
        registry.register(Box::new(self.stream_txns.clone()))?;
        registry.register(Box::new(self.stream_count.clone()))?;
        registry.register(Box::new(self.stream_bytes.clone()))?;
        registry.register(Box::new(self.total_txns.clone()))?;
        registry.register(Box::new(self.total_bytes.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector = "stat_replication_slots", otel.kind = "internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let version_num = resolve_server_version(pool).await?;
            if !supports_stat_replication_slots(version_num) {
                self.reset_all();
                if !self.unsupported_warned.swap(true, Ordering::Relaxed) {
                    warn!(
                        server_version_num = version_num,
                        "collector.replication stat_replication_slots is enabled but \
                         pg_stat_replication_slots requires PostgreSQL 14+; skipping"
                    );
                }
                debug!("Skipping pg_stat_replication_slots metrics (requires PostgreSQL 14+)");
                return Ok(());
            }

            let query_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT ... FROM pg_stat_replication_slots",
                db.sql.table = "pg_stat_replication_slots"
            );

            let rows = sqlx::query(STAT_REPLICATION_SLOTS_QUERY)
                .fetch_all(pool)
                .instrument(query_span)
                .await?;

            self.reset_all();

            for row in &rows {
                self.apply_row(row);
            }

            debug!(rows = rows.len(), "updated pg_stat_replication_slots metrics");

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collector_name_is_stat_replication_slots() {
        assert_eq!(
            StatReplicationSlotsCollector::new().name(),
            "stat_replication_slots"
        );
    }

    #[test]
    fn collector_is_disabled_by_default() {
        assert!(!StatReplicationSlotsCollector::new().enabled_by_default());
    }

    #[test]
    fn versions_before_pg14_are_unsupported() {
        assert!(!supports_stat_replication_slots(0));
        assert!(!supports_stat_replication_slots(130_000));
        assert!(!supports_stat_replication_slots(139_999));
    }

    #[test]
    fn pg14_and_newer_are_supported() {
        assert!(supports_stat_replication_slots(140_000));
        assert!(supports_stat_replication_slots(150_000));
        assert!(supports_stat_replication_slots(180_000));
    }

    #[test]
    fn query_casts_all_numeric_columns_to_bigint() {
        for column in [
            "spill_txns",
            "spill_count",
            "spill_bytes",
            "stream_txns",
            "stream_count",
            "stream_bytes",
            "total_txns",
            "total_bytes",
        ] {
            assert!(
                STAT_REPLICATION_SLOTS_QUERY.contains(&format!("{column}::bigint AS {column}")),
                "{column} must be explicitly cast to bigint"
            );
        }
    }

    #[test]
    fn register_metrics_succeeds_with_unique_names() {
        let registry = Registry::new();
        assert!(
            StatReplicationSlotsCollector::new()
                .register_metrics(&registry)
                .is_ok()
        );
    }
}
