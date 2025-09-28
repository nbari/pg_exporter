use crate::collectors::all_factories;
use crate::collectors::config::CollectorConfig;
use crate::collectors::{Collector, CollectorType};
use prometheus::{Encoder, Registry, TextEncoder};
use std::sync::Arc;
use tracing::{debug, warn};

#[derive(Clone)]
pub struct CollectorRegistry {
    collectors: Vec<CollectorType>,
    registry: Arc<Registry>,
}

impl CollectorRegistry {
    pub fn new(config: CollectorConfig) -> Self {
        let factories = all_factories();
        let registry = Arc::new(Registry::new());

        let collectors = config
            .enabled_collectors
            .iter()
            .filter_map(|name| {
                factories.get(name.as_str()).map(|f| {
                    let collector = f();
                    // Register the collector's metrics with the registry
                    if let Err(e) = collector.register_metrics(&registry) {
                        tracing::warn!(
                            "Failed to register metrics for '{}': {}",
                            collector.name(),
                            e
                        );
                    }
                    collector
                })
            })
            .collect();

        Self {
            collectors,
            registry,
        }
    }

    pub async fn collect_all(&self, pool: &sqlx::PgPool) -> anyhow::Result<String> {
        // Collect metrics from all collectors (they update their registered metrics)
        for collector in &self.collectors {
            if let Err(e) = collector.collect(pool).await {
                warn!("Collector '{}' failed: {}", collector.name(), e);
            } else {
                debug!("Collected metrics from '{}'", collector.name());
            }
        }

        // Generate the final output from the registry
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer)?;

        Ok(String::from_utf8(buffer)?)
    }

    pub fn registry(&self) -> Arc<Registry> {
        self.registry.clone()
    }

    pub fn collector_names(&self) -> Vec<&'static str> {
        self.collectors.iter().map(|c| c.name()).collect()
    }

    pub fn is_empty(&self) -> bool {
        self.collectors.is_empty()
    }
}
