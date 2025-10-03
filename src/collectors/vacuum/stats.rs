use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{IntGaugeVec, Opts, Registry};
use sqlx::{PgPool, Row};
use tracing::info;

/// Collects summary statistics from pg_stat_all_tables
#[derive(Clone)]
pub struct VacuumStatsCollector {
    tuples_dead: IntGaugeVec,
    tuples_live: IntGaugeVec,
    last_vacuum_age: IntGaugeVec,
    last_autovacuum_age: IntGaugeVec,
    last_analyze_age: IntGaugeVec,
    last_autoanalyze_age: IntGaugeVec,
    dead_ratio: IntGaugeVec,
}

impl Default for VacuumStatsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl VacuumStatsCollector {
    pub fn new() -> Self {
        let tuples_dead = IntGaugeVec::new(
            Opts::new("pg_table_tuples_dead", "Dead tuples per table"),
            &["schema", "table"],
        )
        .expect("failed to create tuples_dead metric");

        let tuples_live = IntGaugeVec::new(
            Opts::new("pg_table_tuples_live", "Live tuples per table"),
            &["schema", "table"],
        )
        .expect("failed to create tuples_live metric");

        let last_vacuum_age = IntGaugeVec::new(
            Opts::new(
                "pg_table_last_vacuum_age_seconds",
                "Age in seconds since last vacuum per table",
            ),
            &["schema", "table"],
        )
        .expect("failed to create last_vacuum_age metric");

        let last_autovacuum_age = IntGaugeVec::new(
            Opts::new(
                "pg_table_last_autovacuum_age_seconds",
                "Age in seconds since last autovacuum per table",
            ),
            &["schema", "table"],
        )
        .expect("failed to create last_autovacuum_age metric");

        let last_analyze_age = IntGaugeVec::new(
            Opts::new(
                "pg_table_last_analyze_age_seconds",
                "Age in seconds since last analyze per table",
            ),
            &["schema", "table"],
        )
        .expect("failed to create last_analyze_age metric");

        let last_autoanalyze_age = IntGaugeVec::new(
            Opts::new(
                "pg_table_last_autoanalyze_age_seconds",
                "Age in seconds since last autoanalyze per table",
            ),
            &["schema", "table"],
        )
        .expect("failed to create last_autoanalyze_age metric");

        let dead_ratio = IntGaugeVec::new(
            Opts::new(
                "pg_table_dead_ratio",
                "Dead tuples ratio: dead / (dead + live)",
            ),
            &["schema", "table"],
        )
        .expect("failed to create dead_ratio metric");

        Self {
            tuples_dead,
            tuples_live,
            last_vacuum_age,
            last_autovacuum_age,
            last_analyze_age,
            last_autoanalyze_age,
            dead_ratio,
        }
    }
}

impl Collector for VacuumStatsCollector {
    fn name(&self) -> &'static str {
        "vacuum_stats"
    }

    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.tuples_dead.clone()))?;
        registry.register(Box::new(self.tuples_live.clone()))?;
        registry.register(Box::new(self.last_vacuum_age.clone()))?;
        registry.register(Box::new(self.last_autovacuum_age.clone()))?;
        registry.register(Box::new(self.last_analyze_age.clone()))?;
        registry.register(Box::new(self.last_autoanalyze_age.clone()))?;
        registry.register(Box::new(self.dead_ratio.clone()))?;
        Ok(())
    }

    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let rows = sqlx::query(
                r#"
                SELECT
                    schemaname,
                    relname,
                    n_live_tup,
                    n_dead_tup,
                    COALESCE(EXTRACT(EPOCH FROM (now() - last_vacuum))::bigint, -1) AS last_vacuum_age,
                    COALESCE(EXTRACT(EPOCH FROM (now() - last_autovacuum))::bigint, -1) AS last_autovacuum_age,
                    COALESCE(EXTRACT(EPOCH FROM (now() - last_analyze))::bigint, -1) AS last_analyze_age,
                    COALESCE(EXTRACT(EPOCH FROM (now() - last_autoanalyze))::bigint, -1) AS last_autoanalyze_age
                FROM pg_stat_all_tables
                "#
            )
            .fetch_all(pool)
            .await?;

            info!("Collecting vacuum stats for {} tables", rows.len());

            for row in rows {
                let schema: String = row.try_get("schemaname")?;
                let table: String = row.try_get("relname")?;
                let live: i64 = row.try_get("n_live_tup").unwrap_or(0);
                let dead: i64 = row.try_get("n_dead_tup").unwrap_or(0);
                let last_vac: i64 = row.try_get("last_vacuum_age").unwrap_or(-1);
                let last_autovac: i64 = row.try_get("last_autovacuum_age").unwrap_or(-1);
                let last_analyze: i64 = row.try_get("last_analyze_age").unwrap_or(-1);
                let last_autoanalyze: i64 = row.try_get("last_autoanalyze_age").unwrap_or(-1);

                self.tuples_live
                    .with_label_values(&[&schema, &table])
                    .set(live);

                self.tuples_dead
                    .with_label_values(&[&schema, &table])
                    .set(dead);

                self.last_vacuum_age
                    .with_label_values(&[&schema, &table])
                    .set(last_vac);

                self.last_autovacuum_age
                    .with_label_values(&[&schema, &table])
                    .set(last_autovac);

                self.last_analyze_age
                    .with_label_values(&[&schema, &table])
                    .set(last_analyze);

                self.last_autoanalyze_age
                    .with_label_values(&[&schema, &table])
                    .set(last_autoanalyze);

                let ratio = if live + dead > 0 {
                    dead as f64 / (live + dead) as f64
                } else {
                    0.0
                };

                self.dead_ratio
                    .with_label_values(&[&schema, &table])
                    .set((ratio * 100.0) as i64); // percentage
            }

            Ok(())
        })
    }

    fn enabled_by_default(&self) -> bool {
        true
    }
}
