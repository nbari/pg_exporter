//! Cluster-wide `CREATE INDEX` progress metrics from `pg_stat_progress_create_index`.
//!
//! `pg_stat_progress_create_index` was introduced in `PostgreSQL` 12. The view is
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

const MIN_CREATE_INDEX_PROGRESS_VERSION: i32 = 120_000;
const CREATE_INDEX_PROGRESS_LABELS: [&str; 3] = ["database_name", "table_name", "phase"];

const CREATE_INDEX_PROGRESS_QUERY: &str = r"
    SELECT
        p.pid::bigint AS pid,
        p.datid::bigint AS datid,
        COALESCE(d.datname, p.datname, 'unknown') AS database_name,
        p.relid::bigint AS relid,
        p.index_relid::bigint AS index_relid,
        COALESCE(p.command, 'unknown') AS command,
        COALESCE(p.phase, 'unknown') AS phase,
        p.lockers_total::bigint AS lockers_total,
        p.lockers_done::bigint AS lockers_done,
        p.blocks_total::bigint AS blocks_total,
        p.blocks_done::bigint AS blocks_done,
        p.tuples_total::bigint AS tuples_total,
        p.tuples_done::bigint AS tuples_done,
        CASE WHEN COALESCE(d.datname, p.datname) = current_database()
             THEN n.nspname || '.' || c.relname
             ELSE NULL
        END AS local_table_name
    FROM pg_stat_progress_create_index p
    LEFT JOIN pg_database d ON d.oid = p.datid
    LEFT JOIN pg_class c ON c.oid = p.relid
    LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CreateIndexProgressSupport {
    Unsupported,
    Supported,
}

const fn create_index_progress_support(version_num: i32) -> CreateIndexProgressSupport {
    if version_num < MIN_CREATE_INDEX_PROGRESS_VERSION {
        CreateIndexProgressSupport::Unsupported
    } else {
        CreateIndexProgressSupport::Supported
    }
}

#[derive(Clone, Debug)]
struct CreateIndexProgressSample {
    database_name: String,
    table_name: String,
    phase: String,
    blocks_done: i64,
    blocks_total: i64,
    tuples_done: i64,
    tuples_total: i64,
    lockers_done: i64,
    lockers_total: i64,
}

/// Exposes `pg_stat_progress_create_index` progress metrics (`PostgreSQL` 12+).
///
/// Every metric is labeled by `database_name`, `table_name`, and `phase`:
/// `pg_stat_progress_create_index_blocks_done`,
/// `pg_stat_progress_create_index_blocks_total`,
/// `pg_stat_progress_create_index_tuples_done`,
/// `pg_stat_progress_create_index_tuples_total`,
/// `pg_stat_progress_create_index_lockers_done`, and
/// `pg_stat_progress_create_index_lockers_total`.
#[derive(Clone)]
pub struct CreateIndexProgressCollector {
    blocks_done: IntGaugeVec,
    blocks_total: IntGaugeVec,
    tuples_done: IntGaugeVec,
    tuples_total: IntGaugeVec,
    lockers_done: IntGaugeVec,
    lockers_total: IntGaugeVec,
    unsupported_warned: Arc<AtomicBool>,
}

impl Default for CreateIndexProgressCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl CreateIndexProgressCollector {
    /// Creates a new `CreateIndexProgressCollector` with all metrics initialized.
    ///
    /// # Panics
    ///
    /// Panics if metric creation fails, which only happens with an invalid
    /// metric name or label set and therefore never at runtime.
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        let blocks_done = IntGaugeVec::new(
            Opts::new(
                "pg_stat_progress_create_index_blocks_done",
                "Number of blocks processed by an active CREATE INDEX operation",
            ),
            &CREATE_INDEX_PROGRESS_LABELS,
        )
        .expect("valid pg_stat_progress_create_index_blocks_done opts");

        let blocks_total = IntGaugeVec::new(
            Opts::new(
                "pg_stat_progress_create_index_blocks_total",
                "Total blocks to process for an active CREATE INDEX operation",
            ),
            &CREATE_INDEX_PROGRESS_LABELS,
        )
        .expect("valid pg_stat_progress_create_index_blocks_total opts");

        let tuples_done = IntGaugeVec::new(
            Opts::new(
                "pg_stat_progress_create_index_tuples_done",
                "Number of tuples processed by an active CREATE INDEX operation",
            ),
            &CREATE_INDEX_PROGRESS_LABELS,
        )
        .expect("valid pg_stat_progress_create_index_tuples_done opts");

        let tuples_total = IntGaugeVec::new(
            Opts::new(
                "pg_stat_progress_create_index_tuples_total",
                "Total tuples to process for an active CREATE INDEX operation",
            ),
            &CREATE_INDEX_PROGRESS_LABELS,
        )
        .expect("valid pg_stat_progress_create_index_tuples_total opts");

        let lockers_done = IntGaugeVec::new(
            Opts::new(
                "pg_stat_progress_create_index_lockers_done",
                "Number of lockers already waited for by an active CREATE INDEX operation",
            ),
            &CREATE_INDEX_PROGRESS_LABELS,
        )
        .expect("valid pg_stat_progress_create_index_lockers_done opts");

        let lockers_total = IntGaugeVec::new(
            Opts::new(
                "pg_stat_progress_create_index_lockers_total",
                "Total lockers to wait for in an active CREATE INDEX operation",
            ),
            &CREATE_INDEX_PROGRESS_LABELS,
        )
        .expect("valid pg_stat_progress_create_index_lockers_total opts");

        Self {
            blocks_done,
            blocks_total,
            tuples_done,
            tuples_total,
            lockers_done,
            lockers_total,
            unsupported_warned: Arc::new(AtomicBool::new(false)),
        }
    }

    fn reset_all(&self) {
        self.blocks_done.reset();
        self.blocks_total.reset();
        self.tuples_done.reset();
        self.tuples_total.reset();
        self.lockers_done.reset();
        self.lockers_total.reset();
    }

    fn sample_from_row(row: &PgRow) -> CreateIndexProgressSample {
        let relid = row.try_get::<i64, _>("relid").unwrap_or(0);
        let table_name = row
            .try_get::<Option<String>, _>("local_table_name")
            .ok()
            .flatten()
            .unwrap_or_else(|| relid.to_string());

        CreateIndexProgressSample {
            database_name: row
                .try_get("database_name")
                .unwrap_or_else(|_| "unknown".to_string()),
            table_name,
            phase: row
                .try_get("phase")
                .unwrap_or_else(|_| "unknown".to_string()),
            blocks_done: row.try_get("blocks_done").unwrap_or(0),
            blocks_total: row.try_get("blocks_total").unwrap_or(0),
            tuples_done: row.try_get("tuples_done").unwrap_or(0),
            tuples_total: row.try_get("tuples_total").unwrap_or(0),
            lockers_done: row.try_get("lockers_done").unwrap_or(0),
            lockers_total: row.try_get("lockers_total").unwrap_or(0),
        }
    }

    fn apply_sample(&self, sample: &CreateIndexProgressSample) {
        let labels = [
            sample.database_name.as_str(),
            sample.table_name.as_str(),
            sample.phase.as_str(),
        ];

        self.blocks_done
            .with_label_values(&labels)
            .set(sample.blocks_done);
        self.blocks_total
            .with_label_values(&labels)
            .set(sample.blocks_total);
        self.tuples_done
            .with_label_values(&labels)
            .set(sample.tuples_done);
        self.tuples_total
            .with_label_values(&labels)
            .set(sample.tuples_total);
        self.lockers_done
            .with_label_values(&labels)
            .set(sample.lockers_done);
        self.lockers_total
            .with_label_values(&labels)
            .set(sample.lockers_total);
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

async fn resolve_create_index_progress_support(
    pool: &PgPool,
) -> Result<(i32, CreateIndexProgressSupport)> {
    if is_pg_version_at_least(MIN_CREATE_INDEX_PROGRESS_VERSION) {
        return Ok((get_pg_version(), CreateIndexProgressSupport::Supported));
    }

    let version_num = resolve_server_version(pool).await?;
    Ok((version_num, create_index_progress_support(version_num)))
}

impl Collector for CreateIndexProgressCollector {
    fn name(&self) -> &'static str {
        "create_index_progress"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "create_index_progress")
    )]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.blocks_done.clone()))?;
        registry.register(Box::new(self.blocks_total.clone()))?;
        registry.register(Box::new(self.tuples_done.clone()))?;
        registry.register(Box::new(self.tuples_total.clone()))?;
        registry.register(Box::new(self.lockers_done.clone()))?;
        registry.register(Box::new(self.lockers_total.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector = "create_index_progress", otel.kind = "internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let (version_num, support) = resolve_create_index_progress_support(pool).await?;
            if support == CreateIndexProgressSupport::Unsupported {
                if !self.unsupported_warned.swap(true, Ordering::Relaxed) {
                    warn!(
                        server_version_num = version_num,
                        "collector.create_index_progress is enabled but pg_stat_progress_create_index requires PostgreSQL 12+; skipping"
                    );
                }
                debug!("skipping create index progress metrics on unsupported PostgreSQL version");
                return Ok(());
            }

            let query_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT ... FROM pg_stat_progress_create_index",
                db.sql.table = "pg_stat_progress_create_index"
            );
            let rows = sqlx::query(CREATE_INDEX_PROGRESS_QUERY)
                .fetch_all(pool)
                .instrument(query_span)
                .await?;

            let samples: Vec<CreateIndexProgressSample> =
                rows.iter().map(Self::sample_from_row).collect();

            self.reset_all();
            for sample in &samples {
                self.apply_sample(sample);
            }

            debug!(rows = samples.len(), "updated create index progress metrics");
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collector_name_is_create_index_progress() {
        assert_eq!(
            CreateIndexProgressCollector::new().name(),
            "create_index_progress"
        );
    }

    #[test]
    fn register_metrics_succeeds_with_unique_names() {
        let registry = Registry::new();
        assert!(CreateIndexProgressCollector::new()
            .register_metrics(&registry)
            .is_ok());
    }

    #[test]
    fn versions_before_pg12_are_unsupported() {
        assert_eq!(
            create_index_progress_support(0),
            CreateIndexProgressSupport::Unsupported
        );
        assert_eq!(
            create_index_progress_support(110_000),
            CreateIndexProgressSupport::Unsupported
        );
        assert_eq!(
            create_index_progress_support(119_999),
            CreateIndexProgressSupport::Unsupported
        );
    }

    #[test]
    fn pg12_and_newer_are_supported() {
        assert_eq!(
            create_index_progress_support(120_000),
            CreateIndexProgressSupport::Supported
        );
        assert_eq!(
            create_index_progress_support(180_000),
            CreateIndexProgressSupport::Supported
        );
    }

    #[test]
    fn query_is_cluster_wide_with_local_name_resolution() {
        assert!(CREATE_INDEX_PROGRESS_QUERY.contains("pg_stat_progress_create_index"));
        assert!(!CREATE_INDEX_PROGRESS_QUERY.contains("WHERE"));
        assert!(CREATE_INDEX_PROGRESS_QUERY.contains("current_database()"));
        assert!(CREATE_INDEX_PROGRESS_QUERY.contains("p.blocks_total::bigint AS blocks_total"));
        assert!(CREATE_INDEX_PROGRESS_QUERY.contains("p.tuples_done::bigint AS tuples_done"));
    }

    #[test]
    fn reset_all_clears_previous_series() -> Result<()> {
        let collector = CreateIndexProgressCollector::new();
        let registry = Registry::new();

        collector.register_metrics(&registry)?;
        collector
            .blocks_done
            .with_label_values(&["postgres", "public.t", "building index"])
            .set(1);
        collector.reset_all();

        let stale_series = registry
            .gather()
            .iter()
            .find(|family| family.name() == "pg_stat_progress_create_index_blocks_done")
            .is_some_and(|family| !family.get_metric().is_empty());

        assert!(!stale_series, "create index progress series should reset cleanly");
        Ok(())
    }
}
