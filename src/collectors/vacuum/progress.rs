use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{IntGauge, IntGaugeVec, Opts, Registry};
use sqlx::{PgPool, Row};
use tracing::{debug, info_span, instrument};
use tracing_futures::Instrument as _;

/// Tracks ongoing vacuum/analyze progress
#[derive(Clone)]
pub struct VacuumProgressCollector {
    in_progress: IntGaugeVec,
    heap_progress: IntGaugeVec,
    heap_vacuumed: IntGaugeVec,
    index_vacuum_count: IntGaugeVec,
    global_active: IntGauge,
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
            &["table"],
        )
        .expect("valid pg_vacuum_in_progress opts");

        let heap_progress = IntGaugeVec::new(
            Opts::new("pg_vacuum_heap_progress", "Percent of heap blocks scanned"),
            &["table"],
        )
        .expect("valid pg_vacuum_heap_progress opts");

        let heap_vacuumed = IntGaugeVec::new(
            Opts::new("pg_vacuum_heap_vacuumed", "Number of heap blocks vacuumed"),
            &["table"],
        )
        .expect("valid pg_vacuum_heap_vacuumed opts");

        let index_vacuum_count = IntGaugeVec::new(
            Opts::new(
                "pg_vacuum_index_vacuum_count",
                "Number of index vacuum passes",
            ),
            &["table"],
        )
        .expect("valid pg_vacuum_index_vacuum_count opts");

        let global_active = IntGauge::with_opts(Opts::new(
            "pg_vacuum_active",
            "Are there any vacuums in progress (1=yes,0=no)",
        ))
        .expect("valid pg_vacuum_active opts");

        Self {
            in_progress,
            heap_progress,
            heap_vacuumed,
            index_vacuum_count,
            global_active,
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
            // Query span
            let query_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT relid, heap_blks_total, heap_blks_scanned, heap_blks_vacuumed, index_vacuum_count FROM pg_stat_progress_vacuum",
                db.sql.table = "pg_stat_progress_vacuum"
            );

            let rows = sqlx::query(
                r#"
                SELECT
                    relid::regclass::text AS table_name,
                    heap_blks_total,
                    heap_blks_scanned,
                    heap_blks_vacuumed,
                    index_vacuum_count
                FROM pg_stat_progress_vacuum
                "#,
            )
            .fetch_all(pool)
            .instrument(query_span)
            .await?;

            let update_span =
                info_span!("vacuum_progress.update_metrics", active_rows = rows.len());
            let _g = update_span.enter();

            if rows.is_empty() {
                // Reset "none" placeholder metrics
                self.in_progress.with_label_values(&["none"]).set(0);
                self.heap_progress.with_label_values(&["none"]).set(0);
                self.heap_vacuumed.with_label_values(&["none"]).set(0);
                self.index_vacuum_count.with_label_values(&["none"]).set(0);
                self.global_active.set(0);
                debug!("no active vacuum operations");
            } else {
                self.global_active.set(1);

                for row in rows {
                    let table: String = row.try_get("table_name")?;
                    let heap_total: i64 = row.try_get("heap_blks_total").unwrap_or(0);
                    let heap_scanned: i64 = row.try_get("heap_blks_scanned").unwrap_or(0);
                    let heap_vac: i64 = row.try_get("heap_blks_vacuumed").unwrap_or(0);
                    let idx_count: i64 = row.try_get("index_vacuum_count").unwrap_or(0);

                    let progress_pct = if heap_total > 0 {
                        (heap_scanned as f64 / heap_total as f64) * 100.0
                    } else {
                        0.0
                    };

                    self.in_progress.with_label_values(&[&table]).set(1);
                    self.heap_progress
                        .with_label_values(&[&table])
                        .set(progress_pct as i64);
                    self.heap_vacuumed
                        .with_label_values(&[&table])
                        .set(heap_vac);
                    self.index_vacuum_count
                        .with_label_values(&[&table])
                        .set(idx_count);

                    debug!(
                        table = %table,
                        heap_total,
                        heap_scanned,
                        heap_vacuumed = heap_vac,
                        index_vacuum_count = idx_count,
                        progress_pct,
                        "updated vacuum progress metrics"
                    );
                }
            }

            Ok(())
        })
    }

    fn enabled_by_default(&self) -> bool {
        true
    }
}
