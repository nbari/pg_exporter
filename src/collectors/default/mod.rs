use crate::collectors::Collector;
use anyhow::Result;
use prometheus::{IntGauge, Registry};
use sqlx::PgPool;

#[derive(Clone)]
pub struct DefaultCollector {
    // Store metric handles for updating during collection
    connection_count: IntGauge,
}

impl DefaultCollector {
    pub fn new() -> Self {
        Self {
            connection_count: IntGauge::new("pg_connections_total", "Total number of connections")
                .expect("Failed to create connection_count metric"),
        }
    }
}

impl Default for DefaultCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl Collector for DefaultCollector {
    fn name(&self) -> &'static str {
        "default"
    }

    fn enabled_by_default(&self) -> bool {
        true
    }

    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.connection_count.clone()))?;
        Ok(())
    }

    async fn collect(&self, pool: &PgPool) -> Result<()> {
        // Query the database and update metrics
        let row: (i64,) = sqlx::query_as("SELECT count(*) FROM pg_stat_activity")
            .fetch_one(pool)
            .await?;

        self.connection_count.set(row.0);

        Ok(())
    }
}
