//! Cluster-wide `ANALYZE` progress metrics from `pg_stat_progress_analyze`.
//!
//! `pg_stat_progress_analyze` was introduced in `PostgreSQL` 13. The view is
//! cluster-wide, so this collector uses only the shared pool. Relation names are
//! resolved only when the row belongs to the connected database; rows for other
//! databases fall back to the numeric relation OID label.

use crate::collectors::{
    Collector,
    util::{get_pg_version, is_pg_version_at_least},
};
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{IntGaugeVec, Opts, Registry};
use sqlx::{PgPool, Row, postgres::PgRow};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tracing::{debug, info_span, instrument, warn};
use tracing_futures::Instrument as _;

const MIN_ANALYZE_PROGRESS_VERSION: i32 = 130_000;
const ANALYZE_PROGRESS_LABELS: [&str; 3] = ["database_name", "table_name", "phase"];

const ANALYZE_PROGRESS_QUERY: &str = r"
    SELECT
        p.pid::bigint AS pid,
        p.datid::bigint AS datid,
        COALESCE(d.datname, p.datname, 'unknown') AS database_name,
        p.relid::bigint AS relid,
        COALESCE(p.phase, 'unknown') AS phase,
        p.sample_blks_total::bigint AS sample_blks_total,
        p.sample_blks_scanned::bigint AS sample_blks_scanned,
        CASE WHEN COALESCE(d.datname, p.datname) = current_database()
             THEN n.nspname || '.' || c.relname
             ELSE NULL
        END AS local_table_name
    FROM pg_stat_progress_analyze p
    LEFT JOIN pg_database d ON d.oid = p.datid
    LEFT JOIN pg_class c ON c.oid = p.relid
    LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AnalyzeProgressSupport {
    Unsupported,
    Supported,
}

const fn analyze_progress_support(version_num: i32) -> AnalyzeProgressSupport {
    if version_num < MIN_ANALYZE_PROGRESS_VERSION {
        AnalyzeProgressSupport::Unsupported
    } else {
        AnalyzeProgressSupport::Supported
    }
}

#[derive(Clone, Debug)]
struct AnalyzeProgressSample {
    database_name: String,
    table_name: String,
    phase: String,
    sample_blks_scanned: i64,
    sample_blks_total: i64,
}

/// Exposes `pg_stat_progress_analyze` progress metrics (`PostgreSQL` 13+).
///
/// Every metric is labeled by `database_name`, `table_name`, and `phase`:
/// `pg_stat_progress_analyze_sample_blks_scanned` and
/// `pg_stat_progress_analyze_sample_blks_total`.
#[derive(Clone)]
pub struct AnalyzeProgressCollector {
    sample_blks_scanned: IntGaugeVec,
    sample_blks_total: IntGaugeVec,
    unsupported_warned: Arc<AtomicBool>,
}

impl Default for AnalyzeProgressCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl AnalyzeProgressCollector {
    /// Creates a new `AnalyzeProgressCollector` with all metrics initialized.
    ///
    /// # Panics
    ///
    /// Panics if metric creation fails, which only happens with an invalid
    /// metric name or label set and therefore never at runtime.
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        let sample_blks_scanned = IntGaugeVec::new(
            Opts::new(
                "pg_stat_progress_analyze_sample_blks_scanned",
                "Number of sample blocks scanned by an active ANALYZE operation",
            ),
            &ANALYZE_PROGRESS_LABELS,
        )
        .expect("valid pg_stat_progress_analyze_sample_blks_scanned opts");

        let sample_blks_total = IntGaugeVec::new(
            Opts::new(
                "pg_stat_progress_analyze_sample_blks_total",
                "Total sample blocks to scan for an active ANALYZE operation",
            ),
            &ANALYZE_PROGRESS_LABELS,
        )
        .expect("valid pg_stat_progress_analyze_sample_blks_total opts");

        Self {
            sample_blks_scanned,
            sample_blks_total,
            unsupported_warned: Arc::new(AtomicBool::new(false)),
        }
    }

    fn reset_all(&self) {
        self.sample_blks_scanned.reset();
        self.sample_blks_total.reset();
    }

    fn sample_from_row(row: &PgRow) -> AnalyzeProgressSample {
        let relid = row.try_get::<i64, _>("relid").unwrap_or(0);
        let table_name = row
            .try_get::<Option<String>, _>("local_table_name")
            .ok()
            .flatten()
            .unwrap_or_else(|| relid.to_string());

        AnalyzeProgressSample {
            database_name: row
                .try_get("database_name")
                .unwrap_or_else(|_| "unknown".to_string()),
            table_name,
            phase: row
                .try_get("phase")
                .unwrap_or_else(|_| "unknown".to_string()),
            sample_blks_scanned: row.try_get("sample_blks_scanned").unwrap_or(0),
            sample_blks_total: row.try_get("sample_blks_total").unwrap_or(0),
        }
    }

    fn apply_sample(&self, sample: &AnalyzeProgressSample) {
        let labels = [
            sample.database_name.as_str(),
            sample.table_name.as_str(),
            sample.phase.as_str(),
        ];

        self.sample_blks_scanned
            .with_label_values(&labels)
            .set(sample.sample_blks_scanned);
        self.sample_blks_total
            .with_label_values(&labels)
            .set(sample.sample_blks_total);
    }
}

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

async fn resolve_analyze_progress_support(pool: &PgPool) -> Result<(i32, AnalyzeProgressSupport)> {
    if is_pg_version_at_least(MIN_ANALYZE_PROGRESS_VERSION) {
        return Ok((get_pg_version(), AnalyzeProgressSupport::Supported));
    }

    let version_num = resolve_server_version(pool).await?;
    Ok((version_num, analyze_progress_support(version_num)))
}

impl Collector for AnalyzeProgressCollector {
    fn name(&self) -> &'static str {
        "analyze_progress"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "analyze_progress")
    )]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.sample_blks_scanned.clone()))?;
        registry.register(Box::new(self.sample_blks_total.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector = "analyze_progress", otel.kind = "internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let (version_num, support) = resolve_analyze_progress_support(pool).await?;
            if support == AnalyzeProgressSupport::Unsupported {
                if !self.unsupported_warned.swap(true, Ordering::Relaxed) {
                    warn!(
                        server_version_num = version_num,
                        "collector.analyze_progress is enabled but pg_stat_progress_analyze requires PostgreSQL 13+; skipping"
                    );
                }
                debug!("skipping analyze progress metrics on unsupported PostgreSQL version");
                return Ok(());
            }

            let query_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT ... FROM pg_stat_progress_analyze",
                db.sql.table = "pg_stat_progress_analyze"
            );
            let rows = sqlx::query(ANALYZE_PROGRESS_QUERY)
                .fetch_all(pool)
                .instrument(query_span)
                .await?;

            let samples: Vec<AnalyzeProgressSample> =
                rows.iter().map(Self::sample_from_row).collect();

            self.reset_all();
            for sample in &samples {
                self.apply_sample(sample);
            }

            debug!(rows = samples.len(), "updated analyze progress metrics");
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collector_name_is_analyze_progress() {
        assert_eq!(AnalyzeProgressCollector::new().name(), "analyze_progress");
    }

    #[test]
    fn register_metrics_succeeds_with_unique_names() {
        let registry = Registry::new();
        assert!(AnalyzeProgressCollector::new()
            .register_metrics(&registry)
            .is_ok());
    }

    #[test]
    fn versions_before_pg13_are_unsupported() {
        assert_eq!(analyze_progress_support(0), AnalyzeProgressSupport::Unsupported);
        assert_eq!(
            analyze_progress_support(120_000),
            AnalyzeProgressSupport::Unsupported
        );
        assert_eq!(
            analyze_progress_support(129_999),
            AnalyzeProgressSupport::Unsupported
        );
    }

    #[test]
    fn pg13_and_newer_are_supported() {
        assert_eq!(
            analyze_progress_support(130_000),
            AnalyzeProgressSupport::Supported
        );
        assert_eq!(
            analyze_progress_support(180_000),
            AnalyzeProgressSupport::Supported
        );
    }

    #[test]
    fn query_is_cluster_wide_with_local_name_resolution() {
        assert!(ANALYZE_PROGRESS_QUERY.contains("pg_stat_progress_analyze"));
        assert!(!ANALYZE_PROGRESS_QUERY.contains("WHERE"));
        assert!(ANALYZE_PROGRESS_QUERY.contains("current_database()"));
        assert!(ANALYZE_PROGRESS_QUERY.contains("p.sample_blks_total::bigint AS sample_blks_total"));
        assert!(ANALYZE_PROGRESS_QUERY.contains("p.sample_blks_scanned::bigint AS sample_blks_scanned"));
    }

    #[test]
    fn reset_all_clears_previous_series() -> Result<()> {
        let collector = AnalyzeProgressCollector::new();
        let registry = Registry::new();

        collector.register_metrics(&registry)?;
        collector
            .sample_blks_scanned
            .with_label_values(&["postgres", "public.t", "acquiring sample rows"])
            .set(1);
        collector.reset_all();

        let stale_series = registry
            .gather()
            .iter()
            .find(|family| family.name() == "pg_stat_progress_analyze_sample_blks_scanned")
            .is_some_and(|family| !family.get_metric().is_empty());

        assert!(!stale_series, "analyze progress series should reset cleanly");
        Ok(())
    }
}
