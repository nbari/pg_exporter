use crate::collectors::{Collector, CollectorType, all_factories, config::CollectorConfig};
use prometheus::{Encoder, Gauge, Registry, TextEncoder};
use std::sync::Arc;
use tracing::{debug, debug_span, error, info_span, instrument, warn};
use tracing_futures::Instrument as _;

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

                    // Span around metrics registration to surface any failures
                    let reg_span = debug_span!("collector.register_metrics", collector = %name);
                    let _g = reg_span.enter();
                    if let Err(e) = collector.register_metrics(&registry) {
                        warn!("Failed to register metrics for collector '{}': {}", name, e);
                    }
                    drop(_g);

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

    #[instrument(skip(self, pool), level = "info", err, fields(otel.kind = "internal"))]
    pub async fn collect_all(&self, pool: &sqlx::PgPool) -> anyhow::Result<String> {
        let mut any_success = false;

        // Connectivity test as a client span (captures duration and errors)
        let connect_span = info_span!(
            "db.connectivity_check",
            otel.kind = "client",
            db.system = "postgresql",
            db.operation = "SELECT",
            db.statement = "SELECT 1"
        );
        match sqlx::query("SELECT 1")
            .fetch_one(pool)
            .instrument(connect_span)
            .await
        {
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

        // Collect metrics from all collectors (each within its own span)
        for collector in &self.collectors {
            let name = collector.name();
            let span = info_span!("collector.collect", collector = %name, otel.kind = "internal");
            match collector.collect(pool).instrument(span).await {
                Ok(_) => {
                    debug!("Collected metrics from '{}'", name);
                    any_success = true;
                }
                Err(e) => {
                    error!("Collector '{}' failed: {}", name, e);
                }
            }
        }

        // If we had no successful connection test but some collectors worked, still consider it up
        if !any_success {
            self.pg_up_gauge.set(0.0);
        } else if (self.pg_up_gauge.get() - 1.0).abs() > f64::EPSILON {
            self.pg_up_gauge.set(1.0);
        }

        // Encode the registry to prometheus format within a span
        let encode_span = debug_span!("prometheus.encode");
        let _g = encode_span.enter();
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer)?;
        drop(_g);

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
