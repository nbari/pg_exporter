use crate::{
    collectors::{
        Collector, CollectorType, all_factories,
        config::CollectorConfig,
        exporter::ScraperCollector,
        util::{connect_options_for_db, get_default_database, get_pg_version, set_pg_version},
    },
    exporter::GIT_COMMIT_HASH,
};
use futures::stream::{FuturesUnordered, StreamExt};
use prometheus::{Encoder, Gauge, GaugeVec, Opts, Registry, TextEncoder};
use sqlx::postgres::PgConnectOptions;
use std::{
    env,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::RwLock;
use tracing::{debug, debug_span, error, info, info_span, instrument, warn};
use tracing_futures::Instrument as _;

#[derive(Clone)]
pub struct CollectorRegistry {
    collectors: Vec<CollectorType>,
    registry: Arc<Registry>,
    pg_up_gauge: Gauge,
    scraper: Option<Arc<ScraperCollector>>,
    recovery_pool: Arc<RwLock<Option<sqlx::PgPool>>>,
    recovery_connect_options: Option<PgConnectOptions>,
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
        Self::new_with_recovery_options(config, None)
    }

    /// Creates a new `CollectorRegistry` with instance-specific recovery options.
    ///
    /// # Panics
    ///
    /// Panics if core metrics fail to register (should never happen)
    #[allow(clippy::expect_used)]
    #[must_use]
    pub fn new_with_recovery_options(
        config: &CollectorConfig,
        recovery_connect_options: Option<PgConnectOptions>,
    ) -> Self {
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
            recovery_pool: Arc::new(RwLock::new(None)),
            recovery_connect_options,
            encode_buffer_capacity: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn build_fresh_pool(&self) -> anyhow::Result<sqlx::PgPool> {
        let opts = if let Some(opts) = self.recovery_connect_options.clone() {
            opts
        } else {
            let default_db = get_default_database().unwrap_or("postgres");
            connect_options_for_db(default_db)?
        };

        Ok(sqlx::postgres::PgPoolOptions::new()
            .min_connections(0)
            .max_connections(3)
            .acquire_timeout(Duration::from_secs(5))
            .max_lifetime(Duration::from_secs(120))
            .test_before_acquire(false)
            .connect_lazy_with(opts))
    }

    async fn connectivity_check(pool: &sqlx::PgPool, retry: bool) -> Result<(), sqlx::Error> {
        let connect_span = info_span!(
            "db.connectivity_check",
            otel.kind = "client",
            db.system = "postgresql",
            db.operation = "SELECT",
            db.statement = "SELECT 1",
            retry
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

    async fn recover_with_fresh_pool(
        &self,
        active_pool_error: &sqlx::Error,
    ) -> Option<sqlx::PgPool> {
        match self.build_fresh_pool() {
            Ok(fresh_pool) => match Self::connectivity_check(&fresh_pool, true).await {
                Ok(()) => {
                    warn!(
                        "Connectivity recovered with a fresh pool after failure ({}); caching recovered pool for reuse",
                        active_pool_error
                    );
                    *self.recovery_pool.write().await = Some(fresh_pool.clone());
                    self.pg_up_gauge.set(1.0);
                    self.ensure_version_initialized(&fresh_pool).await;
                    Some(fresh_pool)
                }
                Err(retry_err) => {
                    error!(
                        "Failed to connect to PostgreSQL (active pool: {}, fresh pool retry: {})",
                        active_pool_error, retry_err
                    );
                    self.pg_up_gauge.set(0.0);
                    None
                }
            },
            Err(build_err) => {
                error!(
                    "Failed to connect to PostgreSQL (active pool: {}) and could not build fresh pool: {}",
                    active_pool_error, build_err
                );
                self.pg_up_gauge.set(0.0);
                None
            }
        }
    }

    async fn select_active_pool(&self, shared_pool: &sqlx::PgPool) -> (sqlx::PgPool, bool) {
        let recovery_pool = self.recovery_pool.read().await.clone();
        let active_pool = recovery_pool.clone().unwrap_or_else(|| shared_pool.clone());
        let using_recovery_pool = recovery_pool.is_some();

        match Self::connectivity_check(&active_pool, false).await {
            Ok(()) => {
                self.pg_up_gauge.set(1.0);
                self.ensure_version_initialized(&active_pool).await;
                (active_pool, true)
            }
            Err(active_pool_error) => {
                if using_recovery_pool {
                    warn!(
                        "Recovery pool failed connectivity check ({}); clearing cached recovery pool",
                        active_pool_error
                    );
                    *self.recovery_pool.write().await = None;
                }

                match self.recover_with_fresh_pool(&active_pool_error).await {
                    Some(recovered_pool) => (recovered_pool, true),
                    None => (active_pool, false),
                }
            }
        }
    }

    /// Collect from all enabled collectors.
    ///
    /// # Errors
    ///
    /// Returns an error if metric collection or encoding fails
    #[instrument(skip(self, pool), level = "info", err, fields(otel.kind = "internal"))]
    pub(crate) async fn collect_all_bytes(&self, pool: &sqlx::PgPool) -> anyhow::Result<Vec<u8>> {
        // Increment scrape counter if scraper is available
        if let Some(ref scraper) = self.scraper {
            scraper.increment_scrapes();
        }

        let (active_pool, is_up) = self.select_active_pool(pool).await;

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
        while let Some((name, _res)) = tasks.next().await {
            debug!("Collected metrics from '{}'", name);
        }

        // Encode current registry into Prometheus exposition format.
        let encode_span = debug_span!("prometheus.encode");
        let guard = encode_span.enter();

        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();

        let mut buffer = Vec::with_capacity(self.encode_buffer_capacity.load(Ordering::Relaxed));
        encoder.encode(&metric_families, &mut buffer)?;
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
    pub async fn collect_all(&self, pool: &sqlx::PgPool) -> anyhow::Result<String> {
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
    async fn test_pg_up_recovery() {
        let dsn = std::env::var("PG_EXPORTER_DSN").unwrap_or_else(|_| {
            "postgresql://postgres:postgres@localhost:5432/postgres".to_string()
        });
        let config = CollectorConfig::new().with_enabled(&["exporter".to_string()]);

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
