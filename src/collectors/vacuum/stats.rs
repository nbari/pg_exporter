use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{IntGaugeVec, Opts, Registry};
use sqlx::{PgPool, Row};

/// Collects summary statistics from pg_stat_all_tables
#[derive(Clone)]
pub struct VacuumStatsCollector {
    tuples_dead: IntGaugeVec,
    last_vacuum_age: IntGaugeVec,
}

impl Default for VacuumStatsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl VacuumStatsCollector {
    pub fn new() -> Self {
        let tuples_dead = IntGaugeVec::new(
            Opts::new("pg_vacuum_tuples_dead", "Dead tuples per table"),
            &["table"],
        )
        .unwrap();

        let last_vacuum_age = IntGaugeVec::new(
            Opts::new(
                "pg_vacuum_last_age",
                "Age in seconds since last vacuum per table",
            ),
            &["table"],
        )
        .unwrap();

        Self {
            tuples_dead,
            last_vacuum_age,
        }
    }
}

impl Collector for VacuumStatsCollector {
    fn name(&self) -> &'static str {
        "vacuum_stats"
    }

    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.tuples_dead.clone()))?;
        registry.register(Box::new(self.last_vacuum_age.clone()))?;
        Ok(())
    }

    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let rows = sqlx::query(
                r#"
                SELECT relname, n_dead_tup, EXTRACT(EPOCH FROM (now() - last_vacuum))::bigint AS last_age
                FROM pg_stat_all_tables
                "#
            )
            .fetch_all(pool)
            .await?;

            for row in rows {
                let relname: String = row.try_get("relname")?;
                let n_dead_tup: i64 = row.try_get("n_dead_tup").unwrap_or(0);
                let last_age: i64 = row.try_get("last_age").unwrap_or(0);

                self.tuples_dead
                    .with_label_values(&[&relname])
                    .set(n_dead_tup);
                self.last_vacuum_age
                    .with_label_values(&[&relname])
                    .set(last_age);
            }
            Ok(())
        })
    }

    fn enabled_by_default(&self) -> bool {
        true
    }
}
