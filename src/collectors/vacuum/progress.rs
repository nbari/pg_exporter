use crate::collectors::{
    i64_to_f64,
    util::{get_default_database, get_excluded_databases, open_db_connection},
    Collector,
};
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{GaugeVec, IntGauge, IntGaugeVec, Opts, Registry};
use sqlx::{postgres::PgRow, PgPool, Row};
use std::time::Duration;
use tracing::{debug, info_span, instrument};
use tracing_futures::Instrument as _;

/// Timeout for a single lazy per-database table-name resolution query.
const NAME_RESOLUTION_TIMEOUT: Duration = Duration::from_secs(5);

/// Cluster-wide snapshot of in-progress vacuums.
///
/// `pg_stat_progress_vacuum` reports vacuums in **every** database from any connection,
/// so this single query on the main pool sees them all (including template/non-connectable
/// databases). Table names, however, live in each database's own `pg_class`, so a name is
/// only resolved in-query when the vacuum runs in the connected database
/// (`d.datname = current_database()`, which also prevents cross-database OID false matches).
/// Vacuums in other databases return `local_table_name = NULL` and are resolved lazily.
const VACUUM_PROGRESS_QUERY: &str = r"
    SELECT
        COALESCE(d.datname, 'unknown') AS database_name,
        p.relid::bigint AS relid,
        CASE WHEN d.datname = current_database()
             THEN n.nspname || '.' || c.relname
             ELSE NULL
        END AS local_table_name,
        p.heap_blks_total,
        p.heap_blks_scanned,
        p.heap_blks_vacuumed,
        p.index_vacuum_count,
        COALESCE(a.backend_type = 'autovacuum worker', false) AS is_autovacuum,
        COALESCE(EXTRACT(EPOCH FROM (now() - a.xact_start))::bigint, 0) AS duration_seconds
    FROM pg_stat_progress_vacuum p
    LEFT JOIN pg_database d ON d.oid = p.datid
    LEFT JOIN pg_class c ON c.oid = p.relid
    LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_stat_activity a ON a.pid = p.pid
    WHERE (d.datname IS NULL OR NOT (d.datname = ANY($1)))
";

/// Resolves a single relation OID to `schema.table` within the connected database.
const RESOLVE_RELID_QUERY: &str = r"
    SELECT n.nspname || '.' || c.relname AS table_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = ($1::bigint)::oid
";

#[derive(Clone, Debug)]
struct VacuumSample {
    database_name: String,
    relid: i64,
    /// Resolved `schema.table`. `None` until resolved (locally in-query or lazily);
    /// falls back to the numeric relid when a name cannot be resolved.
    table_name: Option<String>,
    heap_blks_total: i64,
    heap_blks_scanned: i64,
    heap_blks_vacuumed: i64,
    index_vacuum_count: i64,
    is_autovacuum: bool,
    duration_seconds: i64,
}

/// Tracks ongoing vacuum/analyze progress
#[derive(Clone)]
pub struct VacuumProgressCollector {
    in_progress: IntGaugeVec,
    heap_progress: GaugeVec,  // Changed to GaugeVec for 0.0-1.0 ratio
    heap_vacuumed: IntGaugeVec,
    index_vacuum_count: IntGaugeVec,
    global_active: IntGauge,
    
    // Autovacuum-specific metrics (Phase 1 enhancement)
    // These metrics help DBREs distinguish autovacuum from manual vacuum
    // and detect stuck/long-running autovacuum processes
    is_autovacuum: IntGaugeVec,      // 1=autovacuum, 0=manual vacuum
    duration_seconds: IntGaugeVec,   // How long the vacuum has been running (detect stuck processes)
}

impl Default for VacuumProgressCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl VacuumProgressCollector {
    /// Creates a new `VacuumProgressCollector`
    ///
    /// # Panics
    ///
    /// Panics if metric creation fails (should never happen with valid metric names)
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        let in_progress = IntGaugeVec::new(
            Opts::new(
                "pg_vacuum_in_progress",
                "Is a vacuum currently running (1=yes,0=no)",
            ),
            &["database", "table"],
        )
        .expect("valid pg_vacuum_in_progress opts");

        let heap_progress = GaugeVec::new(
            Opts::new("pg_vacuum_heap_progress", "Progress of heap blocks scanned (0.0-1.0 ratio)"),
            &["database", "table"],
        )
        .expect("valid pg_vacuum_heap_progress opts");

        let heap_vacuumed = IntGaugeVec::new(
            Opts::new("pg_vacuum_heap_vacuumed", "Number of heap blocks vacuumed"),
            &["database", "table"],
        )
        .expect("valid pg_vacuum_heap_vacuumed opts");

        let index_vacuum_count = IntGaugeVec::new(
            Opts::new(
                "pg_vacuum_index_vacuum_count",
                "Number of index vacuum passes",
            ),
            &["database", "table"],
        )
        .expect("valid pg_vacuum_index_vacuum_count opts");

        let global_active = IntGauge::with_opts(Opts::new(
            "pg_vacuum_active",
            "Are there any vacuums in progress (1=yes,0=no)",
        ))
        .expect("valid pg_vacuum_active opts");

        let is_autovacuum = IntGaugeVec::new(
            Opts::new(
                "pg_vacuum_is_autovacuum",
                "Whether the vacuum is an autovacuum (1) or manual (0)",
            ),
            &["database", "table"],
        )
        .expect("valid pg_vacuum_is_autovacuum opts");

        let duration_seconds = IntGaugeVec::new(
            Opts::new(
                "pg_vacuum_duration_seconds",
                "How long the vacuum has been running in seconds",
            ),
            &["database", "table"],
        )
        .expect("valid pg_vacuum_duration_seconds opts");

        Self {
            in_progress,
            heap_progress,
            heap_vacuumed,
            index_vacuum_count,
            global_active,
            is_autovacuum,
            duration_seconds,
        }
    }

    fn reset_progress_metrics(&self) {
        self.in_progress.reset();
        self.heap_progress.reset();
        self.heap_vacuumed.reset();
        self.index_vacuum_count.reset();
        self.is_autovacuum.reset();
        self.duration_seconds.reset();
    }

    fn sample_from_row(row: &PgRow) -> VacuumSample {
        VacuumSample {
            database_name: row
                .try_get("database_name")
                .unwrap_or_else(|_| "unknown".to_string()),
            relid: row.try_get("relid").unwrap_or(0),
            table_name: row
                .try_get::<Option<String>, _>("local_table_name")
                .ok()
                .flatten(),
            heap_blks_total: row.try_get("heap_blks_total").unwrap_or(0),
            heap_blks_scanned: row.try_get("heap_blks_scanned").unwrap_or(0),
            heap_blks_vacuumed: row.try_get("heap_blks_vacuumed").unwrap_or(0),
            index_vacuum_count: row.try_get("index_vacuum_count").unwrap_or(0),
            is_autovacuum: row.try_get("is_autovacuum").unwrap_or(false),
            duration_seconds: row.try_get("duration_seconds").unwrap_or(0),
        }
    }

    /// Lazily resolves `schema.table` for a vacuum running in another database.
    ///
    /// Best-effort: a connection is opened only for the specific database, and any
    /// failure (unconnectable database such as `template0`, timeout, dropped relation)
    /// returns `None` so the caller falls back to the numeric relid. This is what keeps
    /// the common case (no cross-database vacuum) at a single cluster-wide query with
    /// zero extra connections.
    async fn resolve_table_name(datname: &str, relid: i64) -> Option<String> {
        // Names for the connected (default) database are already resolved in-query.
        if get_default_database() == Some(datname) {
            return None;
        }

        let mut conn = match open_db_connection(datname).await {
            Ok(conn) => conn,
            Err(e) => {
                debug!(database = %datname, error = %e, "vacuum_progress: cannot open connection to resolve table name");
                return None;
            }
        };

        let resolve = sqlx::query_scalar::<_, String>(RESOLVE_RELID_QUERY)
            .bind(relid)
            .fetch_optional(&mut conn);

        match tokio::time::timeout(NAME_RESOLUTION_TIMEOUT, resolve).await {
            Ok(Ok(name)) => name,
            Ok(Err(e)) => {
                debug!(database = %datname, relid, error = %e, "vacuum_progress: relid name lookup failed");
                None
            }
            Err(_) => {
                debug!(database = %datname, relid, "vacuum_progress: relid name lookup timed out");
                None
            }
        }
    }
}

impl Collector for VacuumProgressCollector {
    fn name(&self) -> &'static str {
        "vacuum_progress"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "vacuum_progress")
    )]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.in_progress.clone()))?;
        registry.register(Box::new(self.heap_progress.clone()))?;
        registry.register(Box::new(self.heap_vacuumed.clone()))?;
        registry.register(Box::new(self.index_vacuum_count.clone()))?;
        registry.register(Box::new(self.global_active.clone()))?;
        registry.register(Box::new(self.is_autovacuum.clone()))?;
        registry.register(Box::new(self.duration_seconds.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector="vacuum_progress", otel.kind="internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let excluded: Vec<String> = get_excluded_databases().to_vec();

            // 1) One cluster-wide query on the main pool. `pg_stat_progress_vacuum` is
            //    cluster-wide, so this sees vacuums in every database (including template
            //    and non-connectable ones), with names already resolved for the connected
            //    database.
            let query_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT ... FROM pg_stat_progress_vacuum (cluster-wide)",
                db.sql.table = "pg_stat_progress_vacuum"
            );
            let rows = sqlx::query(VACUUM_PROGRESS_QUERY)
                .bind(&excluded)
                .fetch_all(pool)
                .instrument(query_span)
                .await?;

            let mut all_samples: Vec<VacuumSample> =
                rows.iter().map(Self::sample_from_row).collect();

            // 2) Lazily resolve names for vacuums running in *other* databases (rare, since
            //    active vacuums are transient). No extra connections are opened otherwise.
            for sample in &mut all_samples {
                if sample.table_name.is_none() && sample.database_name != "unknown" {
                    sample.table_name =
                        Self::resolve_table_name(&sample.database_name, sample.relid).await;
                }
            }

            let update_span =
                info_span!("vacuum_progress.update_metrics", active_rows = all_samples.len());
            let _g = update_span.enter();

            self.reset_progress_metrics();

            if all_samples.is_empty() {
                self.global_active.set(0);
                debug!("no active vacuum operations");
            } else {
                self.global_active.set(1);

                for sample in &all_samples {
                    let database = &sample.database_name;
                    let table = sample
                        .table_name
                        .clone()
                        .unwrap_or_else(|| sample.relid.to_string());
                    let table = table.as_str();
                    let heap_total = sample.heap_blks_total;
                    let heap_scanned = sample.heap_blks_scanned;
                    let heap_vac = sample.heap_blks_vacuumed;
                    let idx_count = sample.index_vacuum_count;
                    let is_auto = sample.is_autovacuum;
                    let duration = sample.duration_seconds;

                    let progress_ratio = if heap_total > 0 {
                        // Progress as 0.0-1.0 ratio for percentunit display
                        i64_to_f64(heap_scanned) / i64_to_f64(heap_total)
                    } else {
                        0.0
                    };

                    self.in_progress.with_label_values(&[database, table]).set(1);
                    self.heap_progress
                        .with_label_values(&[database, table])
                        .set(progress_ratio);
                    self.heap_vacuumed
                        .with_label_values(&[database, table])
                        .set(heap_vac);
                    self.index_vacuum_count
                        .with_label_values(&[database, table])
                        .set(idx_count);
                    self.is_autovacuum
                        .with_label_values(&[database, table])
                        .set(i64::from(is_auto));
                    self.duration_seconds
                        .with_label_values(&[database, table])
                        .set(duration);

                    debug!(
                        database = %database,
                        table = %table,
                        heap_total,
                        heap_scanned,
                        heap_vacuumed = heap_vac,
                        index_vacuum_count = idx_count,
                        progress_ratio = %format!("{progress_ratio:.2}"),
                        is_autovacuum = is_auto,
                        duration_seconds = duration,
                        "updated vacuum progress metrics"
                    );
                }
            }

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reset_progress_metrics_clears_previous_table_series() -> Result<()> {
        let collector = VacuumProgressCollector::new();
        let registry = Registry::new();

        collector.register_metrics(&registry)?;
        collector
            .in_progress
            .with_label_values(&["postgres", "public.test_table"])
            .set(1);
        collector
            .heap_progress
            .with_label_values(&["postgres", "public.test_table"])
            .set(0.5);
        collector
            .duration_seconds
            .with_label_values(&["postgres", "public.test_table"])
            .set(42);

        collector.reset_progress_metrics();

        for metric_name in [
            "pg_vacuum_in_progress",
            "pg_vacuum_heap_progress",
            "pg_vacuum_duration_seconds",
        ] {
            let metric_family = registry
                .gather()
                .into_iter()
                .find(|family| family.name() == metric_name);
            if let Some(metric_family) = metric_family {
                assert!(
                    metric_family.get_metric().is_empty(),
                    "metric {metric_name} should have no stale series after reset"
                );
            }
        }

        Ok(())
    }

    #[test]
    fn vacuum_progress_query_is_cluster_wide_with_local_name_resolution() {
        // Hybrid design: a single cluster-wide query (must NOT be scoped to
        // current_database in its WHERE, or it would miss vacuums in other databases),
        // with table names resolved locally only for the connected database via a CASE.
        assert!(VACUUM_PROGRESS_QUERY.contains("pg_stat_progress_vacuum"));
        assert!(
            !VACUUM_PROGRESS_QUERY.contains("WHERE d.datname = current_database()"),
            "query must stay cluster-wide, not per-database"
        );
        assert!(
            VACUUM_PROGRESS_QUERY.contains("CASE WHEN d.datname = current_database()"),
            "local name resolution should be gated per row, not filter rows out"
        );
        assert!(
            VACUUM_PROGRESS_QUERY.contains("NOT (d.datname = ANY($1))"),
            "excluded databases should still be filtered"
        );
    }

    #[test]
    fn resolve_relid_query_looks_up_a_single_relation() {
        assert!(RESOLVE_RELID_QUERY.contains("pg_class"));
        assert!(RESOLVE_RELID_QUERY.contains("WHERE c.oid ="));
    }
}
