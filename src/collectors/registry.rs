use crate::{
    collectors::{
        Collector, CollectorType, all_factories,
        config::CollectorConfig,
        exporter::ScraperCollector,
        util::{get_pg_version, set_pg_version},
    },
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
    scraper: Option<Arc<ScraperCollector>>,
}

impl CollectorRegistry {
    /// Creates a new `CollectorRegistry`
    ///
    /// # Panics
    ///
    /// Panics if core metrics fail to register (should never happen)
    #[allow(clippy::expect_used)]
    pub fn new(config: &CollectorConfig) -> Self {
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
        let commit_sha = GIT_COMMIT_HASH.unwrap_or("unknown");
        let arch = env::consts::ARCH;

        pg_exporter_build_info
            .with_label_values(&[version, commit_sha, arch])
            .set(1.0); // Gauge is always set to 1.0

        registry
            .register(Box::new(pg_exporter_build_info))
            .expect("Failed to register pg_exporter_build_info GaugeVec");

        info!(
            "Registered pg_exporter_build_info: version={} commit={}",
            version, commit_sha
        );

        let factories = all_factories();

        // Extract scraper if exporter collector is enabled
        let mut scraper_opt = None;

        // Build all requested collectors and register their metrics.
        let collectors = config
            .enabled_collectors
            .iter()
            .filter_map(|name| {
                factories.get(name.as_str()).map(|f| {
                    let collector = f();

                    // If this collector provides a scraper, extract it
                    if let Some(scraper) = collector.get_scraper() {
                        scraper_opt = Some(scraper);
                    }

                    // Register metrics per collector under a span so failures surface in traces.
                    let reg_span = debug_span!("collector.register_metrics", collector = %name);
                    let guard = reg_span.enter();
                    if let Err(e) = collector.register_metrics(&registry) {
                        warn!("Failed to register metrics for collector '{}': {}", name, e);
                    }
                    drop(guard);

                    collector
                })
            })
            .collect();

        Self {
            collectors,
            registry,
            pg_up_gauge,
            scraper: scraper_opt,
        }
    }

    /// Collect from all enabled collectors.
    ///
    /// # Errors
    ///
    /// Returns an error if metric collection or encoding fails
    #[instrument(skip(self, pool), level = "info", err, fields(otel.kind = "internal"))]
    pub async fn collect_all(&self, pool: &sqlx::PgPool) -> anyhow::Result<String> {
        // Increment scrape counter if scraper is available
        if let Some(ref scraper) = self.scraper {
            scraper.increment_scrapes();
        }

        // Quick connectivity check (does not guarantee every collector will succeed).
        let connect_span = info_span!(
            "db.connectivity_check",
            otel.kind = "client",
            db.system = "postgresql",
            db.operation = "SELECT",
            db.statement = "SELECT 1"
        );

        let is_up = match sqlx::query("SELECT 1")
            .fetch_one(pool)
            .instrument(connect_span)
            .await
        {
            Ok(_) => {
                self.pg_up_gauge.set(1.0);

                // Deferred version initialization: if not set yet, try now.
                if get_pg_version() == 0 {
                    let version_num_res: Result<String, sqlx::Error> =
                        sqlx::query_scalar("SHOW server_version_num")
                            .fetch_one(pool)
                            .await;

                    if let Ok(version_num) = version_num_res
                        && let Ok(version) = version_num.parse::<i32>()
                    {
                        set_pg_version(version);
                        info!(version, "Deferred PostgreSQL version detection successful");
                    }
                }

                true
            }

            Err(e) => {
                error!("Failed to connect to PostgreSQL: {}", e);
                self.pg_up_gauge.set(0.0);
                false
            }
        };

        // Launch all collectors concurrently.
        let mut tasks = FuturesUnordered::new();

        // Emit a summary log of which collectors are being launched in parallel.
        let names: Vec<&'static str> = self.collectors.iter().map(super::Collector::name).collect();

        info!("Launching collectors concurrently: {:?}", names);

        for collector in &self.collectors {
            let name = collector.name();

            // Skip DB-dependent collectors if DB is down.
            // "exporter" collector should always run as it tracks scrape stats.
            if !is_up && name != "exporter" {
                debug!(
                    "Skipping DB-dependent collector '{}' because DB is down",
                    name
                );
                continue;
            }

            // Create a span per collector execution to visualize overlap in traces.
            let span = info_span!("collector.collect", collector = %name, otel.kind = "internal");

            // Start timing this collector if scraper is available
            let timer = self.scraper.as_ref().map(|s| s.start_scrape(name));

            // Prepare the future now (do not await here).
            let fut = collector.collect(pool);

            // Push an instrumented future that logs start/finish.
            tasks.push(async move {
                debug!("collector '{}' start", name);

                let res = fut.instrument(span).await;

                match &res {
                    Ok(()) => {
                        debug!("collector '{}' done: ok", name);
                        if let Some(t) = timer {
                            t.success();
                        }
                    }
                    Err(e) => {
                        error!("collector '{}' done: error: {}", name, e);
                        if let Some(t) = timer {
                            t.error();
                        }
                    }
                }

                (name, res)
            });
        }

        // Drain completions as they finish (unordered).
        while let Some((name, _res)) = tasks.next().await {
            debug!("Collected metrics from '{}'", name);
        }

        // Encode current registry into Prometheus exposition format.
        let encode_span = debug_span!("prometheus.encode");
        let guard = encode_span.enter();

        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();

        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer)?;

        // Update metrics count for next scrape
        // Count actual time series lines (non-comment, non-empty lines)
        // This matches: curl -s 0:9432/metrics | grep -vEc '^(#|\s*$)'
        // Note: This count will be visible in the NEXT scrape (eventual consistency)
        if let Some(ref scraper) = self.scraper {
            // Prefer zero-copy UTF-8, fall back to lossy for robustness
            let output = match std::str::from_utf8(&buffer) {
                Ok(s) => std::borrow::Cow::Borrowed(s),
                Err(_) => std::borrow::Cow::Owned(String::from_utf8_lossy(&buffer).into_owned()),
            };

            let count = output
                .lines()
                // Ignore comment lines (Prometheus-spec: '#' at column 0)
                .filter(|line| !line.starts_with('#'))
                // Ignore whitespace-only lines
                .filter(|line| !line.trim().is_empty())
                .count();

            let sample_count = i64::try_from(count).unwrap_or(0);

            scraper.update_metrics_count(sample_count);
        }

        drop(guard);

        Ok(String::from_utf8(buffer)?)
    }

    #[must_use]
    pub const fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }

    #[must_use]
    pub fn collector_names(&self) -> Vec<&'static str> {
        self.collectors.iter().map(super::Collector::name).collect()
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.collectors.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collectors::config::CollectorConfig;
    use sqlx::postgres::PgPoolOptions;
    use std::time::Duration;

    #[tokio::test]
    #[allow(clippy::expect_used)]
    async fn test_pg_up_indicator_on_failure() {
        let config = CollectorConfig::new().with_enabled(&["default".to_string()]);
        let registry = CollectorRegistry::new(&config);

        // Use a pool that will definitely fail
        let pool = PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(100))
            .connect_lazy("postgresql://localhost:54321/postgres")
            .expect("failed to connect lazy to invalid DB");

        let _ = registry.collect_all(&pool).await;

        assert!((registry.pg_up_gauge.get() - 0.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    #[allow(clippy::expect_used)]
    async fn test_pg_up_not_overwritten_by_collector_success() {
        // "exporter" collector always runs and should "succeed" even if DB is down
        let config = CollectorConfig::new().with_enabled(&["exporter".to_string()]);
        let registry = CollectorRegistry::new(&config);

        // Use a pool that will definitely fail
        let pool = PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(100))
            .connect_lazy("postgresql://localhost:54321/postgres")
            .expect("failed to connect lazy to invalid DB");

        let _ = registry.collect_all(&pool).await;

        // Even though "exporter" collector ran and succeeded (returned Ok(())),
        // pg_up MUST stay at 0.0 because the connectivity check failed.
        assert!((registry.pg_up_gauge.get() - 0.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    #[allow(clippy::expect_used)]
    async fn test_scrape_count_increments() {
        let config = CollectorConfig::new().with_enabled(&["exporter".to_string()]);
        let registry = CollectorRegistry::new(&config);

        let pool = PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(100))
            .connect_lazy("postgresql://localhost:54321/postgres")
            .expect("failed to connect lazy to invalid DB");

        // Initial count should be 0 (gauge is initialized at 0)
        assert_eq!(
            registry
                .scraper
                .as_ref()
                .expect("scraper missing")
                .scrapes_total(),
            0
        );

        let _ = registry.collect_all(&pool).await;
        assert_eq!(
            registry
                .scraper
                .as_ref()
                .expect("scraper missing")
                .scrapes_total(),
            1
        );

        let _ = registry.collect_all(&pool).await;
        assert_eq!(
            registry
                .scraper
                .as_ref()
                .expect("scraper missing")
                .scrapes_total(),
            2
        );
    }

    #[tokio::test]
    #[allow(clippy::expect_used)]
    async fn test_outage_filtering() {
        // Enabled both exporter (DB-independent) and database (DB-dependent)
        let config =
            CollectorConfig::new().with_enabled(&["exporter".to_string(), "database".to_string()]);
        let registry = CollectorRegistry::new(&config);

        // Use a pool that will definitely fail
        let pool = PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(100))
            .connect_lazy("postgresql://localhost:54321/postgres")
            .expect("failed to connect lazy to invalid DB");

        let _ = registry.collect_all(&pool).await;

        // Check registry content
        let metrics = registry.registry.gather();
        let metric_names: Vec<_> = metrics
            .iter()
            .map(prometheus::proto::MetricFamily::name)
            .collect();

        // pg_up should be present
        assert!(metric_names.contains(&"pg_up"));

        // exporter metrics should be present
        assert!(metric_names.contains(&"pg_exporter_scrapes_total"));

        // pg_database_size_bytes (from database collector) is a GaugeVec
        // It is registered, but should have NO samples because collection was skipped
        let db_size = metrics
            .iter()
            .find(|m| m.name() == "pg_database_size_bytes");
        if let Some(m) = db_size {
            assert!(
                m.get_metric().is_empty(),
                "DB-dependent metric should have no samples during outage"
            );
        }
    }
}
