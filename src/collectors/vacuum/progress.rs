use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{IntGaugeVec, Opts, Registry};
use sqlx::{PgPool, Row};

/// Tracks ongoing vacuum/analyze progress
#[derive(Clone)]
pub struct VacuumProgressCollector {
    progress: IntGaugeVec,
}

impl Default for VacuumProgressCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl VacuumProgressCollector {
    pub fn new() -> Self {
        let progress = IntGaugeVec::new(
            Opts::new(
                "pg_vacuum_progress",
                "Progress of ongoing vacuum operations",
            ),
            &["table"],
        )
        .unwrap();

        Self { progress }
    }
}

impl Collector for VacuumProgressCollector {
    fn name(&self) -> &'static str {
        "vacuum_progress"
    }

    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.progress.clone()))?;
        Ok(())
    }

    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let rows = sqlx::query(
                r#"
                SELECT relid::regclass::text AS table_name, (phase || '_' || COALESCE(heap_blks_total,0)) AS dummy
                FROM pg_stat_progress_vacuum
                "#
            )
            .fetch_all(pool)
            .await?;

            for row in rows {
                let table_name: &str = row.try_get("table_name")?;
                self.progress.with_label_values(&[table_name]).set(1);
            }
            Ok(())
        })
    }

    fn enabled_by_default(&self) -> bool {
        true
    }
}
