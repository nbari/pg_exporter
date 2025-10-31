use crate::collectors::{Collector, util::get_excluded_databases};
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{GaugeVec, IntGauge, IntGaugeVec, Opts, Registry};
use sqlx::{PgPool, Row};
use tracing::{debug, info_span, instrument};
use tracing_futures::Instrument as _;

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
            // Exclusions (set globally via CLI/env)
            let excluded: Vec<String> = get_excluded_databases().to_vec();

            // Query span
            let query_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT progress from pg_stat_progress_vacuum joined with pg_database (filtered)",
                db.sql.table = "pg_stat_progress_vacuum"
            );

            // Filter by excluded databases via LEFT JOIN on datid.
            // Note: pg_stat_progress_vacuum shows vacuums in ALL databases, but pg_class
            // only contains tables from the CURRENT database. When viewing from 'postgres' db,
            // we can't resolve table names from other databases. We include the database name
            // and relid to help identify tables across databases.
            let rows = sqlx::query(
                r#"
                SELECT
                    COALESCE(d.datname, 'unknown') AS database_name,
                    COALESCE(n.nspname || '.' || c.relname, p.relid::text) AS table_name,
                    p.heap_blks_total,
                    p.heap_blks_scanned,
                    p.heap_blks_vacuumed,
                    p.index_vacuum_count,
                    COALESCE(a.query LIKE 'autovacuum:%', false) AS is_autovacuum,
                    COALESCE(EXTRACT(EPOCH FROM (now() - a.xact_start))::bigint, 0) AS duration_seconds
                FROM pg_stat_progress_vacuum p
                LEFT JOIN pg_database d ON d.oid = p.datid
                LEFT JOIN pg_class c ON c.oid = p.relid
                LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
                LEFT JOIN pg_stat_activity a ON a.pid = p.pid
                WHERE (d.datname IS NULL OR NOT (d.datname = ANY($1)))
                "#,
            )
            .bind(&excluded)
            .fetch_all(pool)
            .instrument(query_span)
            .await?;

            let update_span =
                info_span!("vacuum_progress.update_metrics", active_rows = rows.len());
            let _g = update_span.enter();

            if rows.is_empty() {
                // Reset "none" placeholder metrics
                self.in_progress.with_label_values(&["none", "none"]).set(0);
                self.heap_progress.with_label_values(&["none", "none"]).set(0.0);
                self.heap_vacuumed.with_label_values(&["none", "none"]).set(0);
                self.index_vacuum_count.with_label_values(&["none", "none"]).set(0);
                self.is_autovacuum.with_label_values(&["none", "none"]).set(0);
                self.duration_seconds.with_label_values(&["none", "none"]).set(0);
                self.global_active.set(0);
                debug!("no active vacuum operations");
            } else {
                self.global_active.set(1);

                for row in rows {
                    let database: String = row.try_get("database_name")?;
                    let table: String = row.try_get("table_name")?;
                    let heap_total: i64 = row.try_get("heap_blks_total").unwrap_or(0);
                    let heap_scanned: i64 = row.try_get("heap_blks_scanned").unwrap_or(0);
                    let heap_vac: i64 = row.try_get("heap_blks_vacuumed").unwrap_or(0);
                    let idx_count: i64 = row.try_get("index_vacuum_count").unwrap_or(0);
                    let is_auto: bool = row.try_get("is_autovacuum").unwrap_or(false);
                    let duration: i64 = row.try_get("duration_seconds").unwrap_or(0);

                    let progress_ratio = if heap_total > 0 {
                        // Progress as 0.0-1.0 ratio for percentunit display
                        heap_scanned as f64 / heap_total as f64
                    } else {
                        0.0
                    };

                    self.in_progress.with_label_values(&[&database, &table]).set(1);
                    self.heap_progress
                        .with_label_values(&[&database, &table])
                        .set(progress_ratio);
                    self.heap_vacuumed
                        .with_label_values(&[&database, &table])
                        .set(heap_vac);
                    self.index_vacuum_count
                        .with_label_values(&[&database, &table])
                        .set(idx_count);
                    self.is_autovacuum
                        .with_label_values(&[&database, &table])
                        .set(if is_auto { 1 } else { 0 });
                    self.duration_seconds
                        .with_label_values(&[&database, &table])
                        .set(duration);

                    debug!(
                        database = %database,
                        table = %table,
                        heap_total,
                        heap_scanned,
                        heap_vacuumed = heap_vac,
                        index_vacuum_count = idx_count,
                        progress_ratio = %format!("{:.2}", progress_ratio),
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
