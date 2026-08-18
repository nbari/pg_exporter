//! Live temporary-file footprint from `pg_ls_tmpdir()` (`PostgreSQL` 12+).
//!
//! `PostgreSQL` writes sorts, hashes, materialized `CTE`s and large `CURSOR`s that
//! exceed `work_mem` into `pgsql_tmp` directories, one per tablespace. The existing
//! temp metrics (`pg_stat_database_temp_bytes`, `pg_stat_statements_temp_blks_*`)
//! are **cumulative** and only updated when a statement *finishes*, so a single
//! still-running query can fill the data volume while every counter stays flat.
//!
//! This collector reads the directory itself and reports what is on disk right now:
//! total bytes, file count, and the age of the oldest file per tablespace.
//!
//! # Cardinality
//!
//! Three gauges labelled by `tablespace` only. Filenames, `PID`s and query text are
//! deliberately not exported: a spilling query creates one file per worker and per
//! spill batch, which would make the series count unbounded.
//!
//! # Permissions
//!
//! `pg_ls_tmpdir()` is restricted to superusers and members of `pg_monitor`. When
//! the exporter role lacks the privilege the collector warns once and skips,
//! leaving the rest of the scrape and `pg_up` untouched.
//!
//! # Version handling
//!
//! `pg_ls_tmpdir()` was introduced in `PostgreSQL` 12. On older servers the
//! collector skips cleanly and logs a single warning.

use crate::collectors::{Collected, Collector, util::resolve_server_version};
use crate::collectors::util::{INSUFFICIENT_PRIVILEGE, UNDEFINED_FUNCTION};
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

/// `pg_ls_tmpdir()` was introduced in `PostgreSQL` 12.
const MIN_PG_LS_TMPDIR_VERSION: i32 = 120_000;

/// Labels shared by every temp-footprint metric.
const TEMP_LABELS: [&str; 1] = ["tablespace"];

/// Per-tablespace aggregate of the temporary-file directory.
///
/// `pg_global` is excluded because `PostgreSQL` rejects `pg_ls_tmpdir()` for it:
/// temporary relations are never placed in the global tablespace. Tablespaces
/// without a `pgsql_tmp` directory yield zero rows from the `LATERAL` call and are
/// reported as zero rather than dropped, so the gauges fall back to `0` instead of
/// going stale once the files are cleaned up.
const PG_LS_TMPDIR_QUERY: &str = r"
    SELECT
        t.spcname::text AS tablespace,
        COALESCE(SUM(f.size), 0)::bigint AS bytes,
        COUNT(f.name)::bigint AS files,
        COALESCE(
            MAX(EXTRACT(EPOCH FROM (now() - f.modification))),
            0
        )::double precision AS oldest_age_seconds
    FROM pg_tablespace t
    LEFT JOIN LATERAL pg_ls_tmpdir(t.oid) AS f ON true
    WHERE t.spcname <> 'pg_global'
    GROUP BY t.spcname
";

/// How the running server exposes `pg_ls_tmpdir()`, resolved from
/// `server_version_num`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TmpdirSupport {
    /// Server predates `PostgreSQL` 12, where `pg_ls_tmpdir()` was introduced.
    Unsupported,
    /// Server exposes `pg_ls_tmpdir()`.
    Supported,
}

/// Maps a `server_version_num` (for example `120_000`) to whether
/// `pg_ls_tmpdir()` should be read.
///
/// This is kept as a standalone, pure function so the version gate is unit
/// testable without a live server.
const fn tmpdir_support(version_num: i32) -> TmpdirSupport {
    if version_num < MIN_PG_LS_TMPDIR_VERSION {
        TmpdirSupport::Unsupported
    } else {
        TmpdirSupport::Supported
    }
}

/// Exposes the live `pgsql_tmp` footprint per tablespace (`PostgreSQL` 12+).
///
/// **Gauges:**
/// - `pg_temp_files_current_bytes{tablespace}` - bytes currently on disk
/// - `pg_temp_files_current_count{tablespace}` - files currently on disk
/// - `pg_temp_files_oldest_age_seconds{tablespace}` - age of the oldest file
///
/// Unlike `pg_stat_database_temp_bytes` these are point-in-time values: they rise
/// while a query spills and return to zero once `PostgreSQL` removes the files.
/// Values one `pg_ls_tmpdir()` row contributes to the gauges.
///
/// Split out of `apply_row` so the decoding rules — bigint sizes far beyond `i32`,
/// NULL or undecodable columns, and a negative age caused by clock skew between the
/// file mtime and `now()` — are testable without a temp file of that size existing.
#[derive(Debug, Clone, Copy, PartialEq)]
struct TempRowValues {
    bytes: i64,
    files: i64,
    oldest_age_seconds: f64,
}

impl TempRowValues {
    fn new(bytes: Option<i64>, files: Option<i64>, oldest_age_seconds: Option<f64>) -> Self {
        Self {
            bytes: bytes.unwrap_or(0),
            files: files.unwrap_or(0),
            oldest_age_seconds: oldest_age_seconds.unwrap_or(0.0).max(0.0),
        }
    }

    fn from_row(row: &sqlx::postgres::PgRow) -> Self {
        Self::new(
            row.try_get("bytes").ok(),
            row.try_get("files").ok(),
            row.try_get("oldest_age_seconds").ok(),
        )
    }
}

#[derive(Clone)]
pub struct PgLsTmpdirCollector {
    current_bytes: IntGaugeVec,
    current_count: IntGaugeVec,
    oldest_age_seconds: GaugeVec,
    /// Ensures the "requires `PostgreSQL` 12+" warning is logged at most once per
    /// process instead of on every scrape against an unsupported server.
    unsupported_warned: Arc<AtomicBool>,
    /// Ensures the missing-privilege warning is logged at most once per process.
    denied_warned: Arc<AtomicBool>,
}

impl Default for PgLsTmpdirCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl PgLsTmpdirCollector {
    /// Creates a new `PgLsTmpdirCollector` with all metrics initialized.
    ///
    /// # Panics
    ///
    /// Panics if metric creation fails, which only happens with an invalid metric
    /// name or label set and therefore never at runtime.
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        Self {
            current_bytes: IntGaugeVec::new(
                Opts::new(
                    "pg_temp_files_current_bytes",
                    "Bytes currently held by temporary files in the tablespace's pgsql_tmp \
                     directory. Point-in-time value, unlike the cumulative \
                     pg_stat_database_temp_bytes",
                ),
                &TEMP_LABELS,
            )
            .expect("Failed to create pg_temp_files_current_bytes"),
            current_count: IntGaugeVec::new(
                Opts::new(
                    "pg_temp_files_current_count",
                    "Number of temporary files currently in the tablespace's pgsql_tmp directory",
                ),
                &TEMP_LABELS,
            )
            .expect("Failed to create pg_temp_files_current_count"),
            oldest_age_seconds: GaugeVec::new(
                Opts::new(
                    "pg_temp_files_oldest_age_seconds",
                    "Age in seconds of the oldest temporary file in the tablespace's pgsql_tmp \
                     directory, 0 when the directory is empty. A steadily growing value points \
                     at a long-running spilling statement",
                ),
                &TEMP_LABELS,
            )
            .expect("Failed to create pg_temp_files_oldest_age_seconds"),
            unsupported_warned: Arc::new(AtomicBool::new(false)),
            denied_warned: Arc::new(AtomicBool::new(false)),
        }
    }

    fn reset_all(&self) {
        self.current_bytes.reset();
        self.current_count.reset();
        self.oldest_age_seconds.reset();
    }

    fn apply_row(&self, row: &sqlx::postgres::PgRow) {
        let tablespace: String = row.try_get("tablespace").unwrap_or_default();
        let values = TempRowValues::from_row(row);

        self.apply_values(&tablespace, values);
    }

    fn apply_values(&self, tablespace: &str, values: TempRowValues) {
        let labels = [tablespace];

        self.current_bytes
            .with_label_values(&labels)
            .set(values.bytes);
        self.current_count
            .with_label_values(&labels)
            .set(values.files);
        self.oldest_age_seconds
            .with_label_values(&labels)
            .set(values.oldest_age_seconds);
    }

    /// Handles a failed `pg_ls_tmpdir()` call.
    ///
    /// A missing function or a missing privilege is a configuration issue, not a
    /// database problem: it is reported once and the scrape continues so `pg_up`
    /// and the other collectors are unaffected. Everything else is propagated.
    ///
    /// The gauges are cleared on the way out. Skipping without clearing would keep
    /// publishing the last observed footprint as if it were current, which is exactly
    /// wrong when the privilege is revoked or a failover moves the exporter to a server
    /// that lacks the function.
    fn handle_query_error(&self, error: sqlx::Error) -> Result<Collected> {
        let code = match &error {
            sqlx::Error::Database(db_error) => db_error.code().map(|code| code.to_string()),
            _ => None,
        };

        match code.as_deref() {
            Some(INSUFFICIENT_PRIVILEGE) => {
                if !self.denied_warned.swap(true, Ordering::Relaxed) {
                    warn!(
                        "collector.temp is enabled but the exporter role may not call \
                         pg_ls_tmpdir(); grant pg_monitor to expose the live temporary-file \
                         footprint (GRANT pg_monitor TO <exporter role>)"
                    );
                }
                debug!("Skipping pg_ls_tmpdir metrics (insufficient privilege)");
                                Ok(Collected::Skipped)
            }
            Some(UNDEFINED_FUNCTION) => {
                if !self.unsupported_warned.swap(true, Ordering::Relaxed) {
                    warn!(
                        "collector.temp is enabled but pg_ls_tmpdir() is not available on this \
                         server; skipping (no metrics will be exported)"
                    );
                }
                debug!("Skipping pg_ls_tmpdir metrics (function not available)");
                                Ok(Collected::Skipped)
            }
            _ => Err(error.into()),
        }
    }
}

impl Collector for PgLsTmpdirCollector {
    fn name(&self) -> &'static str {
        "pg_ls_tmpdir"
    }

    #[instrument(skip(self, registry), level = "info", err, fields(collector = "pg_ls_tmpdir"))]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.current_bytes.clone()))?;
        registry.register(Box::new(self.current_count.clone()))?;
        registry.register(Box::new(self.oldest_age_seconds.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector = "pg_ls_tmpdir", otel.kind = "internal")
    )]
    fn collect_once<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<Collected>> {
        Box::pin(async move {
            let version_num = resolve_server_version(pool).await?;

            if tmpdir_support(version_num) == TmpdirSupport::Unsupported {
                if !self.unsupported_warned.swap(true, Ordering::Relaxed) {
                    warn!(
                        server_version_num = version_num,
                        "collector.temp is enabled but pg_ls_tmpdir() requires PostgreSQL 12+; \
                         skipping (no metrics will be exported until the server is upgraded)"
                    );
                }
                debug!("Skipping pg_ls_tmpdir metrics (requires PostgreSQL 12+)");
                                return Ok(Collected::Skipped);
            }

            let query_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT ... FROM pg_tablespace LEFT JOIN LATERAL pg_ls_tmpdir(oid)",
                db.sql.table = "pg_tablespace"
            );

            let rows = match sqlx::query(PG_LS_TMPDIR_QUERY)
                .fetch_all(pool)
                .instrument(query_span)
                .await
            {
                Ok(rows) => rows,
                Err(error) => return self.handle_query_error(error),
            };

            // Reset first so a tablespace that disappeared does not keep a stale
            // series; every remaining tablespace is re-set below, including the
            // empty ones which report zero.
            self.reset_all();

            for row in &rows {
                self.apply_row(row);
            }

            debug!(tablespaces = rows.len(), "updated pg_ls_tmpdir metrics");

            Ok(Collected::Fresh)
        })
    }

    /// Delegates to the existing full reset.
    fn reset_metrics(&self) {
        self.reset_all();
    }

    fn enabled_by_default(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Temp directories routinely exceed 2 GiB. The value must survive verbatim: an
    /// `i32` truncation or a sign flip would turn a 250 GiB footprint into a small or
    /// negative number, which is worse than no metric at all.
    #[test]
    fn test_temp_row_values_keep_sizes_beyond_i32() {
        const GIB_250: i64 = 250 * 1024 * 1024 * 1024;

        let values = TempRowValues::new(Some(GIB_250), Some(4_000_000_000), Some(12.5));
        let collector = PgLsTmpdirCollector::new();
        collector.apply_values("pg_default", values);

        assert_eq!(values.bytes, 268_435_456_000);
        assert_eq!(values.files, 4_000_000_000);
        assert!(values.bytes > i64::from(i32::MAX));
        assert_eq!(
            collector
                .current_bytes
                .with_label_values(&["pg_default"])
                .get(),
            GIB_250
        );
        assert_eq!(
            collector
                .current_count
                .with_label_values(&["pg_default"])
                .get(),
            4_000_000_000
        );
    }

    /// An empty directory must overwrite a previous non-zero sample with exact zeros.
    #[test]
    fn test_temp_gauges_reset_and_repopulate_with_zero() {
        let collector = PgLsTmpdirCollector::new();
        collector.apply_values(
            "pg_default",
            TempRowValues::new(Some(4096), Some(2), Some(7.5)),
        );

        collector.reset_all();
        collector.apply_values("pg_default", TempRowValues::new(Some(0), Some(0), Some(0.0)));

        assert_eq!(
            collector
                .current_bytes
                .with_label_values(&["pg_default"])
                .get(),
            0
        );
        assert_eq!(
            collector
                .current_count
                .with_label_values(&["pg_default"])
                .get(),
            0
        );
        assert!(
            collector
                .oldest_age_seconds
                .with_label_values(&["pg_default"])
                .get()
                .abs()
                < f64::EPSILON
        );
    }

    /// `i64::MAX` is the largest thing `PostgreSQL` can hand back for a bigint sum.
    #[test]
    fn test_temp_row_values_handle_the_bigint_maximum() {
        let values = TempRowValues::new(Some(i64::MAX), Some(i64::MAX), Some(f64::MAX));

        assert_eq!(values.bytes, i64::MAX);
        assert_eq!(values.files, i64::MAX);
        assert!(values.oldest_age_seconds > 0.0);
    }

    /// A NULL or undecodable column must read as zero, never as a missing series.
    #[test]
    fn test_temp_row_values_default_missing_columns_to_zero() {
        let values = TempRowValues::new(None, None, None);

        assert_eq!(values.bytes, 0);
        assert_eq!(values.files, 0);
        assert!(values.oldest_age_seconds.abs() < f64::EPSILON);
    }

    /// Clock skew between a file's mtime and `now()` can make the age negative; a
    /// negative age would break `> threshold` alerting, so it is clamped to zero.
    #[test]
    fn test_temp_row_values_clamp_negative_age() {
        let values = TempRowValues::new(Some(1), Some(1), Some(-42.0));

        assert!(values.oldest_age_seconds.abs() < f64::EPSILON);
    }

    #[test]
    fn test_collector_name() {
        assert_eq!(PgLsTmpdirCollector::new().name(), "pg_ls_tmpdir");
    }

    #[test]
    fn test_collector_not_enabled_by_default() {
        assert!(!PgLsTmpdirCollector::new().enabled_by_default());
    }

    #[test]
    fn test_tmpdir_unsupported_before_pg12() {
        for version in [90_600, 100_000, 110_000, 119_999] {
            assert_eq!(
                tmpdir_support(version),
                TmpdirSupport::Unsupported,
                "server_version_num {version} should be unsupported"
            );
        }
    }

    #[test]
    fn test_tmpdir_supported_from_pg12() {
        for version in [120_000, 130_000, 160_000, 180_000] {
            assert_eq!(
                tmpdir_support(version),
                TmpdirSupport::Supported,
                "server_version_num {version} should be supported"
            );
        }
    }

    #[test]
    fn test_query_excludes_pg_global() {
        // pg_ls_tmpdir() raises an error for pg_global, which never holds
        // temporary relations.
        assert!(PG_LS_TMPDIR_QUERY.contains("t.spcname <> 'pg_global'"));
    }

    #[test]
    fn test_query_reports_empty_tablespaces_as_zero() {
        assert!(PG_LS_TMPDIR_QUERY.contains("LEFT JOIN LATERAL"));
        assert!(PG_LS_TMPDIR_QUERY.contains("COALESCE(SUM(f.size), 0)::bigint"));
    }

    #[test]
    fn test_query_casts_numeric_columns() {
        assert!(PG_LS_TMPDIR_QUERY.contains("COUNT(f.name)::bigint AS files"));
        assert!(PG_LS_TMPDIR_QUERY.contains(")::double precision AS oldest_age_seconds"));
    }

    /// A `GaugeVec` with no samples is not gathered at all, so asserting on a freshly
    /// registered collector proves nothing. Apply a row first, then require exactly the
    /// three documented families — that is what a rename or a dropped `register` call
    /// would break.
    #[test]
    fn test_registers_all_metrics() -> Result<()> {
        let registry = Registry::new();
        let collector = PgLsTmpdirCollector::new();
        collector.register_metrics(&registry)?;
        collector.apply_values(
            "pg_default",
            TempRowValues::new(Some(4096), Some(1), Some(1.0)),
        );

        let mut names: Vec<String> = registry
            .gather()
            .iter()
            .map(|family| family.name().to_string())
            .collect();
        names.sort();

        assert_eq!(
            names,
            vec![
                "pg_temp_files_current_bytes",
                "pg_temp_files_current_count",
                "pg_temp_files_oldest_age_seconds",
            ]
        );
        Ok(())
    }
}
