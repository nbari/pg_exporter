use crate::{
    collectors::{Collector, CollectorType, all_factories, config::CollectorConfig},
    exporter::GIT_COMMIT_HASH,
};
use futures::stream::{FuturesUnordered, StreamExt};
use prometheus::{Encoder, Gauge, GaugeVec, Opts, Registry, TextEncoder};
use std::{env, sync::Arc};
use tracing::{debug, debug_span, error, info, info_span, instrument, warn};
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

        // Register pg_up gauge
        let pg_up_gauge = Gauge::new("pg_up", "Whether PostgreSQL is up (1) or down (0)")
            .expect("Failed to create pg_up gauge");

        registry
            .register(Box::new(pg_up_gauge.clone()))
            .expect("Failed to register pg_up gauge");

        // Register pg_exporter_build_info gauge
        let pg_exporter_build_info_opts = Opts::new(
            "pg_exporter_build_info",
            "Build information for pg_exporter",
        );
        let pg_exporter_build_info =
            GaugeVec::new(pg_exporter_build_info_opts, &["version", "commit", "arch"])
                .expect("Failed to create pg_exporter_build_info GaugeVec");

        // Add build information as labels
        let version = env!("CARGO_PKG_VERSION");
        let commit_sha = GIT_COMMIT_HASH;
        let arch = env::consts::ARCH;

        pg_exporter_build_info
            .with_label_values(&[version, commit_sha, arch])
            .set(1.0); // Gauge is always set to 1.0

        registry
            .register(Box::new(pg_exporter_build_info.clone()))
            .expect("Failed to register pg_exporter_build_info GaugeVec");

        info!(
            "Registered pg_exporter_build_info: version={} commit={}",
            version, commit_sha
        );

        let factories = all_factories();

        // Build all requested collectors and register their metrics.
        let collectors = config
            .enabled_collectors
            .iter()
            .filter_map(|name| {
                factories.get(name.as_str()).map(|f| {
                    let collector = f();

                    // Register metrics per collector under a span so failures surface in traces.
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

    /// Collect from all enabled collectors.
    #[instrument(skip(self, pool), level = "info", err, fields(otel.kind = "internal"))]
    pub async fn collect_all(&self, pool: &sqlx::PgPool) -> anyhow::Result<String> {
        let mut any_success = false;

        // Quick connectivity check (does not guarantee every collector will succeed).
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
                // We still try individual collectors; some may succeed (e.g., cached metrics).
            }
        }

        // Launch all collectors concurrently.
        let mut tasks = FuturesUnordered::new();

        // Emit a summary log of which collectors are being launched in parallel.
        let names: Vec<&'static str> = self.collectors.iter().map(|c| c.name()).collect();

        info!("Launching collectors concurrently: {:?}", names);

        for collector in &self.collectors {
            let name = collector.name();

            // Create a span per collector execution to visualize overlap in traces.
            let span = info_span!("collector.collect", collector = %name, otel.kind = "internal");

            // Prepare the future now (do not await here).
            let fut = collector.collect(pool);

            // Push an instrumented future that logs start/finish.
            tasks.push(async move {
                debug!("collector '{}' start", name);

                let res = fut.instrument(span).await;

                match &res {
                    Ok(_) => debug!("collector '{}' done: ok", name),
                    Err(e) => error!("collector '{}' done: error: {}", name, e),
                }

                (name, res)
            });
        }

        // Drain completions as they finish (unordered).
        while let Some((name, res)) = tasks.next().await {
            match res {
                Ok(()) => {
                    debug!("Collected metrics from '{}'", name);
                    any_success = true;
                }

                Err(e) => {
                    error!("Collector '{}' failed: {}", name, e);
                }
            }
        }

        // If nothing worked, mark down; otherwise ensure up=1.
        if !any_success {
            self.pg_up_gauge.set(0.0);
        } else if (self.pg_up_gauge.get() - 1.0).abs() > f64::EPSILON {
            self.pg_up_gauge.set(1.0);
        }

        // Encode current registry into Prometheus exposition format.
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
