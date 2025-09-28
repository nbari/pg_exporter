use super::Collector;
use anyhow::Result;
use sqlx::PgPool;

#[derive(Clone)]
pub struct DefaultCollector;

impl DefaultCollector {
    pub fn new() -> Self {
        Self
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

    async fn collect(&self, pool: &PgPool) -> Result<String> {
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM pg_database")
            .fetch_one(pool)
            .await?;

        Ok(format!("pg_database_count {}\n", row.0))
    }

    fn enabled_by_default(&self) -> bool {
        true // Default collector enabled by default
    }
}
