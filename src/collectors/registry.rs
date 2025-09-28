use crate::collectors::all_factories;
use crate::collectors::config::CollectorConfig;
use crate::collectors::{Collector, CollectorType};

#[derive(Clone)]
pub struct CollectorRegistry {
    collectors: Vec<CollectorType>,
}

impl CollectorRegistry {
    pub fn new(config: CollectorConfig) -> Self {
        let factories = all_factories();
        let collectors = config
            .enabled_collectors
            .iter()
            .filter_map(|name| factories.get(name.as_str()).map(|f| f()))
            .collect();

        Self { collectors }
    }

    pub async fn collect_all(&self, pool: &sqlx::PgPool) -> anyhow::Result<String> {
        let mut output = String::new();

        for collector in &self.collectors {
            match collector.collect(pool).await {
                Ok(metrics) => {
                    tracing::debug!("Collected metrics from '{}'", collector.name());
                    output.push_str(&metrics);
                }
                Err(e) => {
                    tracing::warn!("Collector '{}' failed: {}", collector.name(), e);
                }
            }
        }

        Ok(output)
    }

    pub fn collector_names(&self) -> Vec<&'static str> {
        self.collectors.iter().map(|c| c.name()).collect()
    }

    pub fn is_empty(&self) -> bool {
        self.collectors.is_empty()
    }
}
