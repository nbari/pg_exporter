use crate::collectors::{Collector, CollectorType, all_factories, config::CollectorConfig};
use prometheus::{Encoder, Gauge, Registry, TextEncoder};
use std::sync::Arc;
use tracing::{debug, error, warn};

#[derive(Clone)]
pub struct CollectorRegistry {
    collectors: Vec<CollectorType>,
    registry: Arc<Registry>,
    pg_up_gauge: Gauge,
}

impl CollectorRegistry {
    pub fn new(config: CollectorConfig) -> Self {
        let registry = Arc::new(Registry::new());

        let pg_up_gauge = Gauge::new("pg_up", "Whether PostgreSQL is up (1) or down (0)")
            .expect("Failed to create pg_up gauge");

        registry
            .register(Box::new(pg_up_gauge.clone()))
            .expect("Failed to register pg_up gauge");

        let factories = all_factories();

        let collectors = config
            .enabled_collectors
            .iter()
            .filter_map(|name| {
                factories.get(name.as_str()).map(|f| {
                    let collector = f();
                    // Register the collector's metrics with the registry
                    if let Err(e) = collector.register_metrics(&registry) {
                        warn!("Failed to register metrics for collector '{}': {}", name, e);
                    }
                    collector
                })
            })
            .collect();

        Self {
            collectors,
            registry,
            pg_up_gauge,
        }
    }

    pub async fn collect_all(&self, pool: &sqlx::PgPool) -> anyhow::Result<String> {
        let mut any_success = false;

        // Test basic connectivity first
        match sqlx::query("SELECT 1").fetch_one(pool).await {
            Ok(_) => {
                self.pg_up_gauge.set(1.0);
                any_success = true;
            }
            Err(e) => {
                error!("Failed to connect to PostgreSQL: {}", e);
                self.pg_up_gauge.set(0.0);
                // Still try to collect from individual collectors in case some can work
            }
        }

        // Collect metrics from all collectors (they update their registered metrics)
        for collector in &self.collectors {
            match collector.collect(pool).await {
                Ok(_) => {
                    debug!("Collected metrics from '{}'", collector.name());
                    any_success = true;
                }
                Err(e) => {
                    error!("Collector '{}' failed: {}", collector.name(), e);
                }
            }
        }

        // If we had no successful connection test but some collectors worked,
        // still consider it up
        if !any_success {
            self.pg_up_gauge.set(0.0);
        } else if self.pg_up_gauge.get() != 1.0 {
            self.pg_up_gauge.set(1.0);
        }

        // Encode the registry to prometheus format
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer)?;

        Ok(String::from_utf8(buffer)?)
    }

    pub fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }

    pub fn collector_names(&self) -> Vec<&'static str> {
        self.collectors.iter().map(|c| c.name()).collect()
    }

    pub fn is_empty(&self) -> bool {
        self.collectors.is_empty()
    }
}
