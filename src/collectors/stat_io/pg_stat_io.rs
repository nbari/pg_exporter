//! Cluster-wide I/O statistics from `pg_stat_io` (`PostgreSQL` 16+).
//!
//! `pg_stat_io` breaks down backend I/O by `backend_type` (client backend,
//! background writer, checkpointer, autovacuum, ...), `object` (relation, temp
//! relation, and WAL on `PostgreSQL` 18+) and `context` (normal, vacuum,
//! bulkread, bulkwrite, and init on `PostgreSQL` 18+). It surfaces
//! shared-buffer pressure (`evictions`) and storage latency (`read_time` /
//! `write_time`) directly from inside `PostgreSQL`, which is especially useful
//! on managed services such as RDS/Aurora where there is no host access.
//!
//! This is a **cluster-wide** view, so the collector reads only the shared
//! pool and never fans out per database. It is disabled by default to keep the
//! extra label cardinality opt-in.
//!
//! # Version handling
//!
//! `pg_stat_io` was introduced in `PostgreSQL` 16. On older servers the
//! collector skips cleanly (no error, no populated series) and logs a single
//! warning that `PostgreSQL` 16+ is required, because the collector is opt-in
//! and a user who enabled `--collector.stat_io` on an unsupported server should
//! be told why no metrics appear. `PostgreSQL` 18 replaced the single
//! `op_bytes` column with native `read_bytes`, `write_bytes` and `extend_bytes`
//! columns; the collector selects the matching query per server version and, on
//! `PostgreSQL` 16/17, derives byte totals as `operations * op_bytes` so the
//! `*_bytes_total` metrics stay consistent across versions.
//!
//! # Timing metrics
//!
//! Timing columns are cumulative milliseconds in `PostgreSQL` and are exported
//! as seconds (`*_time_seconds_total`). They remain zero unless
//! `track_io_timing` is enabled; WAL timing on `PostgreSQL` 18+ additionally
//! depends on `track_wal_io_timing`.

use crate::collectors::{
    Collector,
    util::{MS_TO_SEC, get_pg_version},
};
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{GaugeVec, IntGaugeVec, Opts, Registry};
use sqlx::{PgPool, Row};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tracing::{debug, info_span, instrument, warn};
use tracing_futures::Instrument as _;

/// `pg_stat_io` was introduced in `PostgreSQL` 16.
const MIN_PG_STAT_IO_VERSION: i32 = 160_000;

/// `PostgreSQL` 18 replaced `op_bytes` with native `read_bytes`/`write_bytes`/`extend_bytes`.
const PG_NATIVE_BYTES_VERSION: i32 = 180_000;

/// Labels shared by every `pg_stat_io` metric.
const STAT_IO_LABELS: [&str; 3] = ["backend_type", "object", "context"];

/// How the running server exposes `pg_stat_io`, resolved from `server_version_num`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatIoSupport {
    /// Server predates `PostgreSQL` 16, where `pg_stat_io` was introduced, so
    /// the view does not exist and the collector must skip.
    Unsupported,
    /// Server exposes `pg_stat_io`. `native_bytes` is `true` on `PostgreSQL`
    /// 18+, which has native byte columns instead of `op_bytes`.
    Supported { native_bytes: bool },
}

/// Maps a `server_version_num` (for example `160_004`) to how `pg_stat_io`
/// should be read.
///
/// This is kept as a standalone, pure function so the version gate is unit
/// testable without a live server: `pg_stat_io` did not exist before
/// `PostgreSQL` 16, and `PostgreSQL` 18 replaced `op_bytes` with native byte
/// columns.
const fn stat_io_support(version_num: i32) -> StatIoSupport {
    if version_num < MIN_PG_STAT_IO_VERSION {
        StatIoSupport::Unsupported
    } else {
        StatIoSupport::Supported {
            native_bytes: version_num >= PG_NATIVE_BYTES_VERSION,
        }
    }
}

#[allow(clippy::expect_used)]
fn stat_io_int_gauge(name: &str, help: &str) -> IntGaugeVec {
    IntGaugeVec::new(Opts::new(name, help), &STAT_IO_LABELS).expect("pg_stat_io int metric")
}

#[allow(clippy::expect_used)]
fn stat_io_gauge(name: &str, help: &str) -> GaugeVec {
    GaugeVec::new(Opts::new(name, help), &STAT_IO_LABELS).expect("pg_stat_io gauge metric")
}

/// Exposes `pg_stat_io` cluster-wide I/O statistics (`PostgreSQL` 16+).
///
/// All series carry the `backend_type`, `object` and `context` labels. Counter
/// values are cumulative since the last `pg_stat_reset_shared('io')`; use
/// `rate()`/`increase()` in `PromQL`.
///
/// **Operation counts (`IntGauge`):**
/// - `pg_stat_io_reads_total`
/// - `pg_stat_io_writes_total`
/// - `pg_stat_io_writebacks_total`
/// - `pg_stat_io_extends_total`
/// - `pg_stat_io_hits_total`
/// - `pg_stat_io_evictions_total`
/// - `pg_stat_io_reuses_total`
/// - `pg_stat_io_fsyncs_total`
///
/// **Byte totals (`IntGauge`):**
/// - `pg_stat_io_read_bytes_total`
/// - `pg_stat_io_write_bytes_total`
/// - `pg_stat_io_extend_bytes_total`
///
/// **Timings in seconds (`Gauge`, require `track_io_timing`):**
/// - `pg_stat_io_read_time_seconds_total`
/// - `pg_stat_io_write_time_seconds_total`
/// - `pg_stat_io_writeback_time_seconds_total`
/// - `pg_stat_io_extend_time_seconds_total`
/// - `pg_stat_io_fsync_time_seconds_total`
#[derive(Clone)]
pub struct PgStatIoCollector {
    reads: IntGaugeVec,
    writes: IntGaugeVec,
    writebacks: IntGaugeVec,
    extends: IntGaugeVec,
    hits: IntGaugeVec,
    evictions: IntGaugeVec,
    reuses: IntGaugeVec,
    fsyncs: IntGaugeVec,

    read_bytes: IntGaugeVec,
    write_bytes: IntGaugeVec,
    extend_bytes: IntGaugeVec,

    read_time: GaugeVec,
    write_time: GaugeVec,
    writeback_time: GaugeVec,
    extend_time: GaugeVec,
    fsync_time: GaugeVec,

    /// Ensures the "requires `PostgreSQL` 16+" warning is logged at most once per
    /// process instead of on every scrape against an unsupported server.
    unsupported_warned: Arc<AtomicBool>,
}

impl Default for PgStatIoCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl PgStatIoCollector {
    /// Creates a new `PgStatIoCollector` with all metrics initialized.
    ///
    /// # Panics
    ///
    /// Panics if metric creation fails, which only happens with an invalid
    /// metric name or label set and therefore never at runtime.
    #[must_use]
    pub fn new() -> Self {
        Self {
            reads: stat_io_int_gauge(
                "pg_stat_io_reads_total",
                "Number of read operations, per backend_type/object/context",
            ),
            writes: stat_io_int_gauge(
                "pg_stat_io_writes_total",
                "Number of write operations, per backend_type/object/context",
            ),
            writebacks: stat_io_int_gauge(
                "pg_stat_io_writebacks_total",
                "Number of writeback operations requested to the kernel",
            ),
            extends: stat_io_int_gauge(
                "pg_stat_io_extends_total",
                "Number of relation extend operations",
            ),
            hits: stat_io_int_gauge(
                "pg_stat_io_hits_total",
                "Number of times a desired block was found in shared buffers",
            ),
            evictions: stat_io_int_gauge(
                "pg_stat_io_evictions_total",
                "Number of times a block was evicted from a buffer to place another; \
                 a direct indicator of shared_buffers pressure",
            ),
            reuses: stat_io_int_gauge(
                "pg_stat_io_reuses_total",
                "Number of times an existing buffer in a size-limited ring was reused",
            ),
            fsyncs: stat_io_int_gauge(
                "pg_stat_io_fsyncs_total",
                "Number of fsync calls issued for this backend_type/object/context",
            ),
            read_bytes: stat_io_int_gauge(
                "pg_stat_io_read_bytes_total",
                "Bytes read. Native on PostgreSQL 18+, derived as reads * op_bytes on 16/17",
            ),
            write_bytes: stat_io_int_gauge(
                "pg_stat_io_write_bytes_total",
                "Bytes written. Native on PostgreSQL 18+, derived as writes * op_bytes on 16/17",
            ),
            extend_bytes: stat_io_int_gauge(
                "pg_stat_io_extend_bytes_total",
                "Bytes added by relation extends. Native on PostgreSQL 18+, \
                 derived as extends * op_bytes on 16/17",
            ),
            read_time: stat_io_gauge(
                "pg_stat_io_read_time_seconds_total",
                "Time spent in read operations, in seconds (requires track_io_timing)",
            ),
            write_time: stat_io_gauge(
                "pg_stat_io_write_time_seconds_total",
                "Time spent in write operations, in seconds (requires track_io_timing)",
            ),
            writeback_time: stat_io_gauge(
                "pg_stat_io_writeback_time_seconds_total",
                "Time spent in writeback operations, in seconds (requires track_io_timing)",
            ),
            extend_time: stat_io_gauge(
                "pg_stat_io_extend_time_seconds_total",
                "Time spent in extend operations, in seconds (requires track_io_timing)",
            ),
            fsync_time: stat_io_gauge(
                "pg_stat_io_fsync_time_seconds_total",
                "Time spent in fsync operations, in seconds (requires track_io_timing)",
            ),
            unsupported_warned: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Clears every series so label combinations that disappear between scrapes
    /// (for example after `pg_stat_reset_shared('io')`) do not linger as stale data.
    fn reset_all(&self) {
        self.reads.reset();
        self.writes.reset();
        self.writebacks.reset();
        self.extends.reset();
        self.hits.reset();
        self.evictions.reset();
        self.reuses.reset();
        self.fsyncs.reset();
        self.read_bytes.reset();
        self.write_bytes.reset();
        self.extend_bytes.reset();
        self.read_time.reset();
        self.write_time.reset();
        self.writeback_time.reset();
        self.extend_time.reset();
        self.fsync_time.reset();
    }

    fn apply_row(&self, row: &sqlx::postgres::PgRow) {
        let backend_type: String = row.try_get("backend_type").unwrap_or_default();
        let object: String = row.try_get("object").unwrap_or_default();
        let context: String = row.try_get("context").unwrap_or_default();
        let labels = [backend_type.as_str(), object.as_str(), context.as_str()];

        self.reads
            .with_label_values(&labels)
            .set(row.try_get::<i64, _>("reads").unwrap_or(0));
        self.writes
            .with_label_values(&labels)
            .set(row.try_get::<i64, _>("writes").unwrap_or(0));
        self.writebacks
            .with_label_values(&labels)
            .set(row.try_get::<i64, _>("writebacks").unwrap_or(0));
        self.extends
            .with_label_values(&labels)
            .set(row.try_get::<i64, _>("extends").unwrap_or(0));
        self.hits
            .with_label_values(&labels)
            .set(row.try_get::<i64, _>("hits").unwrap_or(0));
        self.evictions
            .with_label_values(&labels)
            .set(row.try_get::<i64, _>("evictions").unwrap_or(0));
        self.reuses
            .with_label_values(&labels)
            .set(row.try_get::<i64, _>("reuses").unwrap_or(0));
        self.fsyncs
            .with_label_values(&labels)
            .set(row.try_get::<i64, _>("fsyncs").unwrap_or(0));

        self.read_bytes
            .with_label_values(&labels)
            .set(row.try_get::<i64, _>("read_bytes").unwrap_or(0));
        self.write_bytes
            .with_label_values(&labels)
            .set(row.try_get::<i64, _>("write_bytes").unwrap_or(0));
        self.extend_bytes
            .with_label_values(&labels)
            .set(row.try_get::<i64, _>("extend_bytes").unwrap_or(0));

        self.read_time
            .with_label_values(&labels)
            .set(row.try_get::<f64, _>("read_time_seconds").unwrap_or(0.0));
        self.write_time
            .with_label_values(&labels)
            .set(row.try_get::<f64, _>("write_time_seconds").unwrap_or(0.0));
        self.writeback_time
            .with_label_values(&labels)
            .set(row.try_get::<f64, _>("writeback_time_seconds").unwrap_or(0.0));
        self.extend_time
            .with_label_values(&labels)
            .set(row.try_get::<f64, _>("extend_time_seconds").unwrap_or(0.0));
        self.fsync_time
            .with_label_values(&labels)
            .set(row.try_get::<f64, _>("fsync_time_seconds").unwrap_or(0.0));
    }
}

/// Resolves the server version, preferring the cached value set at startup and
/// falling back to a direct query (the cache is not initialized in unit tests
/// that call `collect` without going through the exporter bootstrap).
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

/// Builds the `pg_stat_io` query for a given server version.
///
/// `PostgreSQL` 18+ exposes native byte columns; earlier versions derive byte
/// totals from `op_bytes`. NULL cells are normalized to zero so row extraction
/// cannot fail on unsupported operation/object/context combinations.
fn stat_io_query(native_bytes: bool) -> String {
    let bytes = if native_bytes {
        "COALESCE(read_bytes, 0)::bigint AS read_bytes,
         COALESCE(write_bytes, 0)::bigint AS write_bytes,
         COALESCE(extend_bytes, 0)::bigint AS extend_bytes,"
    } else {
        "(COALESCE(reads, 0) * COALESCE(op_bytes, 0))::bigint AS read_bytes,
         (COALESCE(writes, 0) * COALESCE(op_bytes, 0))::bigint AS write_bytes,
         (COALESCE(extends, 0) * COALESCE(op_bytes, 0))::bigint AS extend_bytes,"
    };

    format!(
        "SELECT
            COALESCE(backend_type, 'unknown') AS backend_type,
            COALESCE(object, 'unknown') AS object,
            COALESCE(context, 'unknown') AS context,
            COALESCE(reads, 0)::bigint AS reads,
            COALESCE(writes, 0)::bigint AS writes,
            COALESCE(writebacks, 0)::bigint AS writebacks,
            COALESCE(extends, 0)::bigint AS extends,
            COALESCE(hits, 0)::bigint AS hits,
            COALESCE(evictions, 0)::bigint AS evictions,
            COALESCE(reuses, 0)::bigint AS reuses,
            COALESCE(fsyncs, 0)::bigint AS fsyncs,
            {bytes}
            (COALESCE(read_time, 0) / {MS_TO_SEC})::double precision AS read_time_seconds,
            (COALESCE(write_time, 0) / {MS_TO_SEC})::double precision AS write_time_seconds,
            (COALESCE(writeback_time, 0) / {MS_TO_SEC})::double precision AS writeback_time_seconds,
            (COALESCE(extend_time, 0) / {MS_TO_SEC})::double precision AS extend_time_seconds,
            (COALESCE(fsync_time, 0) / {MS_TO_SEC})::double precision AS fsync_time_seconds
        FROM pg_stat_io"
    )
}

impl Collector for PgStatIoCollector {
    fn name(&self) -> &'static str {
        "pg_stat_io"
    }

    #[instrument(skip(self, registry), level = "info", err, fields(collector = "pg_stat_io"))]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.reads.clone()))?;
        registry.register(Box::new(self.writes.clone()))?;
        registry.register(Box::new(self.writebacks.clone()))?;
        registry.register(Box::new(self.extends.clone()))?;
        registry.register(Box::new(self.hits.clone()))?;
        registry.register(Box::new(self.evictions.clone()))?;
        registry.register(Box::new(self.reuses.clone()))?;
        registry.register(Box::new(self.fsyncs.clone()))?;
        registry.register(Box::new(self.read_bytes.clone()))?;
        registry.register(Box::new(self.write_bytes.clone()))?;
        registry.register(Box::new(self.extend_bytes.clone()))?;
        registry.register(Box::new(self.read_time.clone()))?;
        registry.register(Box::new(self.write_time.clone()))?;
        registry.register(Box::new(self.writeback_time.clone()))?;
        registry.register(Box::new(self.extend_time.clone()))?;
        registry.register(Box::new(self.fsync_time.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector = "pg_stat_io", otel.kind = "internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let version_num = resolve_server_version(pool).await?;

            let native_bytes = match stat_io_support(version_num) {
                StatIoSupport::Unsupported => {
                    if !self.unsupported_warned.swap(true, Ordering::Relaxed) {
                        warn!(
                            server_version_num = version_num,
                            "collector.stat_io is enabled but pg_stat_io requires PostgreSQL 16+; \
                             skipping (no metrics will be exported until the server is upgraded)"
                        );
                    }
                    debug!("Skipping pg_stat_io metrics (requires PostgreSQL 16+)");
                    return Ok(());
                }
                StatIoSupport::Supported { native_bytes } => native_bytes,
            };

            let sql = stat_io_query(native_bytes);

            let query_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT ... FROM pg_stat_io",
                db.sql.table = "pg_stat_io"
            );

            let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                .fetch_all(pool)
                .instrument(query_span)
                .await?;

            self.reset_all();

            for row in &rows {
                self.apply_row(row);
            }

            debug!(rows = rows.len(), "updated pg_stat_io metrics");

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
    fn collector_name_is_pg_stat_io() {
        assert_eq!(PgStatIoCollector::new().name(), "pg_stat_io");
    }

    #[test]
    fn collector_is_disabled_by_default() {
        assert!(!PgStatIoCollector::new().enabled_by_default());
    }

    #[test]
    fn native_byte_query_uses_native_columns() {
        let sql = stat_io_query(true);
        assert!(sql.contains("COALESCE(read_bytes, 0)::bigint AS read_bytes"));
        assert!(!sql.contains("op_bytes"));
    }

    #[test]
    fn derived_byte_query_uses_op_bytes() {
        let sql = stat_io_query(false);
        assert!(sql.contains("op_bytes"));
        assert!(!sql.contains("COALESCE(read_bytes, 0)"));
    }

    #[test]
    fn versions_before_pg16_are_unsupported() {
        // pg_stat_io does not exist before PostgreSQL 16, so the collector must
        // classify these as Unsupported and skip rather than query a missing view.
        assert_eq!(stat_io_support(0), StatIoSupport::Unsupported);
        assert_eq!(stat_io_support(140_000), StatIoSupport::Unsupported);
        assert_eq!(stat_io_support(150_000), StatIoSupport::Unsupported);
        assert_eq!(stat_io_support(159_999), StatIoSupport::Unsupported);
    }

    #[test]
    fn pg16_and_pg17_use_derived_bytes() {
        assert_eq!(
            stat_io_support(160_000),
            StatIoSupport::Supported { native_bytes: false }
        );
        assert_eq!(
            stat_io_support(170_000),
            StatIoSupport::Supported { native_bytes: false }
        );
        assert_eq!(
            stat_io_support(179_999),
            StatIoSupport::Supported { native_bytes: false }
        );
    }

    #[test]
    fn pg18_and_newer_use_native_bytes() {
        assert_eq!(
            stat_io_support(180_000),
            StatIoSupport::Supported { native_bytes: true }
        );
        assert_eq!(
            stat_io_support(190_000),
            StatIoSupport::Supported { native_bytes: true }
        );
    }

    #[test]
    fn register_metrics_succeeds_with_unique_names() {
        // A duplicated metric name would make prometheus registration return an
        // error, so a clean registration proves all 16 names are distinct.
        let registry = Registry::new();
        assert!(PgStatIoCollector::new().register_metrics(&registry).is_ok());
    }
}
