use crate::{
    collectors::{
        Collector, CollectorType, all_factories,
        config::CollectorConfig,
        exporter::ScraperCollector,
        sequences::SequencesCollector,
        statements::StatementsCollector,
        util::{get_pg_version, get_scrape_timeout, set_pg_version},
    },
    exporter::GIT_COMMIT_HASH,
};
use futures::stream::{FuturesUnordered, StreamExt};
use prometheus::{Encoder, Gauge, GaugeVec, Opts, Registry, TextEncoder};
use std::{
    env,
    error::Error,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{sync::Semaphore, time::timeout};
use tracing::{debug, debug_span, error, info, info_span, instrument, warn};
use tracing_futures::Instrument as _;

fn build_collector(
    name: &str,
    config: &CollectorConfig,
    factories: &std::collections::HashMap<&'static str, fn() -> CollectorType>,
) -> Option<CollectorType> {
    match name {
        "statements" => Some(CollectorType::StatementsCollector(
            StatementsCollector::with_top_n(config.statements.top_n),
        )),
        "sequences" => Some(CollectorType::SequencesCollector(
            SequencesCollector::with_min_ratio(config.sequences.min_ratio),
        )),
        _ => factories.get(name).map(|factory| factory()),
    }
}

#[derive(Debug)]
pub enum ScrapeError {
    Busy,
    Timeout(Duration),
    CollectorFailed(Vec<String>),
    Encode(prometheus::Error),
    Utf8(std::string::FromUtf8Error),
}

impl fmt::Display for ScrapeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Busy => f.write_str("another /metrics scrape is already running"),
            Self::Timeout(duration) => write!(f, "scrape exceeded timeout of {duration:?}"),
            Self::CollectorFailed(errors) => {
                write!(f, "one or more collectors failed: {}", errors.join("; "))
            }
            Self::Encode(error) => write!(f, "failed to encode metrics: {error}"),
            Self::Utf8(error) => write!(f, "failed to convert metrics to UTF-8: {error}"),
        }
    }
}

impl Error for ScrapeError {}

impl From<prometheus::Error> for ScrapeError {
    fn from(error: prometheus::Error) -> Self {
        Self::Encode(error)
    }
}

impl From<std::string::FromUtf8Error> for ScrapeError {
    fn from(error: std::string::FromUtf8Error) -> Self {
        Self::Utf8(error)
    }
}

enum ActivePool {
    Available(sqlx::PgPool),
    Unavailable,
}

#[derive(Clone)]
pub struct CollectorRegistry {
    collectors: Vec<CollectorType>,
    registry: Arc<Registry>,
    pg_up_gauge: Gauge,
    scraper: Option<Arc<ScraperCollector>>,
    scrape_gate: Arc<Semaphore>,
    encode_buffer_capacity: Arc<AtomicUsize>,
}

impl CollectorRegistry {
    /// Creates a new `CollectorRegistry`
    ///
    /// # Panics
    ///
    /// Panics if core metrics fail to register (should never happen)
    #[allow(clippy::expect_used)]
    #[must_use]
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
            .enabled_collectors_in_order()
            .into_iter()
            .filter_map(|name| {
                let collector = build_collector(&name, config, &factories)?;

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

                Some(collector)
            })
            .collect();

        Self {
            collectors,
            registry,
            pg_up_gauge,
            scraper: scraper_opt,
            scrape_gate: Arc::new(Semaphore::new(1)),
            encode_buffer_capacity: Arc::new(AtomicUsize::new(0)),
        }
    }

    async fn connectivity_check(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
        let connect_span = info_span!(
            "db.connectivity_check",
            otel.kind = "client",
            db.system = "postgresql",
            db.operation = "SELECT",
            db.statement = "SELECT 1"
        );

        sqlx::query("SELECT 1")
            .fetch_one(pool)
            .instrument(connect_span)
            .await
            .map(|_| ())
    }

    async fn ensure_version_initialized(&self, pool: &sqlx::PgPool) {
        if get_pg_version() != 0 {
            return;
        }

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

    async fn select_active_pool(&self, shared_pool: &sqlx::PgPool) -> ActivePool {
        match Self::connectivity_check(shared_pool).await {
            Ok(()) => {
                self.pg_up_gauge.set(1.0);
                self.ensure_version_initialized(shared_pool).await;
                ActivePool::Available(shared_pool.clone())
            }
            Err(error) => {
                error!("Failed to connect to PostgreSQL: {}", error);
                self.pg_up_gauge.set(0.0);
                ActivePool::Unavailable
            }
        }
    }

    /// Collect from all enabled collectors.
    ///
    /// # Errors
    ///
    /// Returns an error if metric collection or encoding fails
    #[instrument(skip(self, pool), level = "info", err, fields(otel.kind = "internal"))]
    pub(crate) async fn collect_all_bytes(
        &self,
        pool: &sqlx::PgPool,
    ) -> Result<Vec<u8>, ScrapeError> {
        let permit = self
            .scrape_gate
            .clone()
            .try_acquire_owned()
            .map_err(|_| ScrapeError::Busy)?;

        let scrape_timeout = get_scrape_timeout();
        let registry = self.clone();
        let pool = pool.clone();
        let scrape_task = tokio::spawn(async move {
            let _permit = permit;
            registry.collect_all_bytes_inner(&pool).await
        });

        // On timeout, dropping the JoinHandle detaches the task instead of aborting it.
        // That intentionally keeps the scrape gate permit held until collector futures
        // unwind, so the next scrape cannot start another wave of DB work while the
        // previous scrape's PostgreSQL backends are still cancelling server-side.
        match timeout(scrape_timeout, scrape_task).await {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => Err(ScrapeError::CollectorFailed(vec![format!(
                "scrape task failed: {error}"
            )])),
            Err(_) => Err(ScrapeError::Timeout(scrape_timeout)),
        }
    }

    async fn collect_all_bytes_inner(&self, pool: &sqlx::PgPool) -> Result<Vec<u8>, ScrapeError> {
        // Increment scrape counter if scraper is available
        if let Some(ref scraper) = self.scraper {
            scraper.increment_scrapes();
        }

        let active_pool = match self.select_active_pool(pool).await {
            ActivePool::Available(active_pool) => active_pool,
            ActivePool::Unavailable => {
                warn!("PostgreSQL unavailable; returning pg_up=0 without stale collector metrics");
                return self.encode_outage_metrics();
            }
        };

        // Launch all collectors concurrently.
        let mut tasks = FuturesUnordered::new();

        // Emit a summary log of which collectors are being launched in parallel.
        let names: Vec<&'static str> = self.collectors.iter().map(super::Collector::name).collect();

        info!("Launching collectors concurrently: {:?}", names);

        for collector in &self.collectors {
            let name = collector.name();

            // Create a span per collector execution to visualize overlap in traces.
            let span = info_span!("collector.collect", collector = %name, otel.kind = "internal");

            // Start timing this collector if scraper is available
            let timer = self.scraper.as_ref().map(|s| s.start_scrape(name));

            // Prepare the future now (do not await here).
            let fut = collector.collect(&active_pool);

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
        let mut failures = Vec::new();
        while let Some((name, res)) = tasks.next().await {
            match res {
                Ok(()) => debug!("Collected metrics from '{}'", name),
                Err(error) => failures.push(format!("{name}: {error}")),
            }
        }

        if !failures.is_empty() {
            return Err(ScrapeError::CollectorFailed(failures));
        }

        // Encode current registry into Prometheus exposition format.
        let metric_families = self.registry.gather();
        self.encode_metric_families(&metric_families)
    }

    fn encode_outage_metrics(&self) -> Result<Vec<u8>, ScrapeError> {
        let metric_families = self
            .registry
            .gather()
            .into_iter()
            .filter(|family| matches!(family.name(), "pg_up" | "pg_exporter_build_info"))
            .collect::<Vec<_>>();

        self.encode_metric_families(&metric_families)
    }

    fn encode_metric_families(
        &self,
        metric_families: &[prometheus::proto::MetricFamily],
    ) -> Result<Vec<u8>, ScrapeError> {
        let encode_span = debug_span!("prometheus.encode");
        let guard = encode_span.enter();

        let encoder = TextEncoder::new();
        let mut buffer = Vec::with_capacity(self.encode_buffer_capacity.load(Ordering::Relaxed));
        encoder.encode(metric_families, &mut buffer)?;
        self.encode_buffer_capacity
            .store(buffer.capacity(), Ordering::Relaxed);

        // Update metrics count for next scrape
        // Count actual time series lines (non-comment, non-empty lines)
        // This matches: curl -s 0:9432/metrics | grep -vEc '^(#|\s*$)'
        // Note: This count will be visible in the NEXT scrape (eventual consistency)
        if let Some(ref scraper) = self.scraper {
            let sample_count = i64::try_from(count_exposed_metric_lines(&buffer)).unwrap_or(0);
            scraper.update_metrics_count(sample_count);
        }

        drop(guard);

        Ok(buffer)
    }

    /// Collect from all enabled collectors.
    ///
    /// # Errors
    ///
    /// Returns an error if metric collection or encoding fails
    pub async fn collect_all(&self, pool: &sqlx::PgPool) -> Result<String, ScrapeError> {
        Ok(String::from_utf8(self.collect_all_bytes(pool).await?)?)
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

fn count_exposed_metric_lines(buffer: &[u8]) -> usize {
    let output = match std::str::from_utf8(buffer) {
        Ok(text) => std::borrow::Cow::Borrowed(text),
        Err(_) => std::borrow::Cow::Owned(String::from_utf8_lossy(buffer).into_owned()),
    };

    output
        .lines()
        .filter(|line| !line.starts_with('#'))
        .filter(|line| !line.trim().is_empty())
        .count()
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
        let config = CollectorConfig::new(25).with_enabled(&["default".to_string()]);
        let registry = CollectorRegistry::new(&config);

        // Use a pool that will definitely fail
        let pool = PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(100))
            .connect_lazy("postgresql://localhost:54321/postgres")
            .expect("failed to connect lazy to invalid DB");

        let output = registry
            .collect_all(&pool)
            .await
            .expect("DB-down scrape should still return exporter status metrics");

        assert!((registry.pg_up_gauge.get() - 0.0).abs() < f64::EPSILON);
        assert!(output.contains("pg_up 0"));
        assert!(output.contains("pg_exporter_build_info"));
        assert!(!output.contains("Error collecting metrics"));
    }

    #[tokio::test]
    #[allow(clippy::expect_used)]
    async fn test_pg_up_not_overwritten_by_collector_success() {
        // DB outages return a status-only payload. They must not let DB-independent
        // collector registration or scrape accounting overwrite pg_up=0.
        let config = CollectorConfig::new(25).with_enabled(&["exporter".to_string()]);
        let registry = CollectorRegistry::new(&config);

        // Use a pool that will definitely fail
        let pool = PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(100))
            .connect_lazy("postgresql://localhost:54321/postgres")
            .expect("failed to connect lazy to invalid DB");

        let output = registry
            .collect_all(&pool)
            .await
            .expect("DB-down scrape should still return exporter status metrics");

        // pg_up MUST stay at 0.0 because the connectivity check failed.
        assert!((registry.pg_up_gauge.get() - 0.0).abs() < f64::EPSILON);
        assert!(output.contains("pg_up 0"));
        assert!(
            !output.contains("pg_exporter_scrapes_total"),
            "outage payload must not expose stale exporter collector samples: {output}"
        );
    }

    #[tokio::test]
    #[allow(clippy::expect_used)]
    async fn test_pg_up_recovery() {
        let dsn = std::env::var("PG_EXPORTER_DSN").unwrap_or_else(|_| {
            "postgresql://postgres:postgres@localhost:5432/postgres".to_string()
        });
        let config = CollectorConfig::new(25).with_enabled(&["exporter".to_string()]);

        // 1. Start with a broken pool
        let registry = CollectorRegistry::new(&config);
        let broken_pool = PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(100))
            .connect_lazy("postgresql://localhost:54321/postgres")
            .expect("failed to connect lazy to invalid DB");

        let _ = registry.collect_all(&broken_pool).await;
        assert!((registry.pg_up_gauge.get() - 0.0).abs() < f64::EPSILON);

        // 2. Now use the real pool (recovery)
        // Note: This requires a running PostgreSQL on localhost:5432
        // If it's not running, this part of the test will still "pass" its assertions
        // but won't verify recovery to 1.0.
        let real_pool = PgPoolOptions::new()
            .acquire_timeout(Duration::from_secs(1))
            .connect_lazy(&dsn)
            .expect("failed to connect lazy to test DB");

        let _ = registry.collect_all(&real_pool).await;

        // If the real DB is available, pg_up should be 1.0
        // We only assert if we know the DB is actually there to avoid flaky tests
        if sqlx::query("SELECT 1").fetch_one(&real_pool).await.is_ok() {
            assert!((registry.pg_up_gauge.get() - 1.0).abs() < f64::EPSILON);
        }
    }

    #[tokio::test]
    #[allow(clippy::expect_used)]
    async fn test_scrape_count_increments() {
        let config = CollectorConfig::new(25).with_enabled(&["exporter".to_string()]);
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
        let config = CollectorConfig::new(25)
            .with_enabled(&["exporter".to_string(), "database".to_string()]);
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

    #[tokio::test]
    #[allow(clippy::expect_used)]
    async fn test_database_outage_response_filters_stale_collector_metrics() {
        let dsn = std::env::var("PG_EXPORTER_DSN").unwrap_or_else(|_| {
            "postgresql://postgres:postgres@localhost:5432/postgres".to_string()
        });
        let config = CollectorConfig::new(25).with_enabled(&["default".to_string()]);
        let registry = CollectorRegistry::new(&config);

        let real_pool = PgPoolOptions::new()
            .acquire_timeout(Duration::from_secs(1))
            .connect_lazy(&dsn)
            .expect("failed to connect lazy to test DB");

        if sqlx::query("SELECT 1").fetch_one(&real_pool).await.is_err() {
            return;
        }

        let healthy_output = registry
            .collect_all(&real_pool)
            .await
            .expect("healthy scrape should succeed");
        assert!(healthy_output.contains("pg_up 1"));
        assert!(
            healthy_output.contains("pg_settings_server_version_num"),
            "healthy scrape should populate default collector metrics"
        );

        let broken_pool = PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(100))
            .connect_lazy("postgresql://localhost:54321/postgres")
            .expect("failed to connect lazy to invalid DB");

        let outage_output = registry
            .collect_all(&broken_pool)
            .await
            .expect("DB-down scrape should still return exporter status metrics");

        assert!(outage_output.contains("pg_up 0"));
        assert!(outage_output.contains("pg_exporter_build_info"));
        assert!(
            !outage_output.contains("pg_settings_server_version_num"),
            "DB-down scrape must not expose stale default collector metrics: {outage_output}"
        );
        assert!(!outage_output.contains("Error collecting metrics"));
    }

    #[test]
    fn test_metric_line_count_matches_string_logic() {
        let buffer = br#"# HELP pg_up Whether PostgreSQL is up
# TYPE pg_up gauge
pg_up 1

   	
metric_one{label="a"} 1
metric_two 2
"#;

        let string_count = String::from_utf8_lossy(buffer)
            .lines()
            .filter(|line| !line.starts_with('#'))
            .filter(|line| !line.trim().is_empty())
            .count();

        assert_eq!(count_exposed_metric_lines(buffer), string_count);
    }

    #[test]
    fn test_metric_line_count_handles_crlf_and_invalid_utf8() {
        let buffer = b"# HELP test help\r\nmetric_ok 1\r\n \t\r\nmetric_invalid \xff\r\n";

        let string_count = String::from_utf8_lossy(buffer)
            .lines()
            .filter(|line| !line.starts_with('#'))
            .filter(|line| !line.trim().is_empty())
            .count();

        assert_eq!(count_exposed_metric_lines(buffer), string_count);
    }
}
