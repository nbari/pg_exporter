use crate::{
    collectors::{
        Collector, CollectorType, all_factories,
        config::CollectorConfig,
        exporter::{ScrapeTimer, ScraperCollector},
        sequences::SequencesCollector,
        statements::StatementsCollector,
        system::SystemCollector,
        util::{get_pg_version, get_scrape_timeout, set_pg_version},
    },
    exporter::GIT_COMMIT_HASH,
};
use futures::{
    FutureExt as _,
    stream::{FuturesUnordered, StreamExt},
};
use prometheus::{Encoder, Gauge, GaugeVec, Opts, Registry, TextEncoder};
use std::{
    env,
    error::Error,
    fmt,
    future::Future,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{sync::Semaphore, time::timeout};
use tracing::{Span, debug, debug_span, error, info, info_span, instrument, warn};
use tracing_futures::Instrument as _;

fn build_collector(
    name: &str,
    config: &CollectorConfig,
    factories: &std::collections::HashMap<&'static str, fn() -> CollectorType>,
) -> Option<CollectorType> {
    match name {
        "statements" => Some(CollectorType::StatementsCollector(
            StatementsCollector::with_config(
                config.statements.top_n,
                config.statements.query_text_refresh,
            ),
        )),
        "sequences" => Some(CollectorType::SequencesCollector(
            SequencesCollector::with_min_ratio(config.sequences.min_ratio),
        )),
        "system" => Some(CollectorType::SystemCollector(
            SystemCollector::with_config(config.system.process_memory),
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

/// A spawned task that is **aborted** when this handle is dropped, rather than detached.
///
/// `tokio::spawn` hands back a `JoinHandle` whose `Drop` detaches: the task keeps running
/// unsupervised, with no deadline of its own and no way to reach it again. Dropping the
/// handle on a scrape timeout is what wedged `/metrics` permanently in issue #34.
struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Future for AbortOnDrop<T> {
    type Output = Result<T, tokio::task::JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().0).poll(cx)
    }
}

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn panic_payload_message(payload: &(dyn std::any::Any + Send)) -> &str {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload")
}

/// Runs one collector future, converting a panic into an ordinary collector error so its
/// timer records an error rather than looking like a timeout/client-disconnect abort.
async fn collect_with_outcome<F>(
    name: &'static str,
    timer: Option<ScrapeTimer>,
    future: F,
    span: Span,
) -> (&'static str, anyhow::Result<()>)
where
    F: Future<Output = anyhow::Result<()>>,
{
    debug!("collector '{}' start", name);

    let result = match AssertUnwindSafe(future.instrument(span))
        .catch_unwind()
        .await
    {
        Ok(result) => result,
        Err(payload) => Err(anyhow::anyhow!(
            "collector panicked: {}",
            panic_payload_message(payload.as_ref())
        )),
    };

    match &result {
        Ok(()) => {
            debug!("collector '{}' done: ok", name);
            if let Some(timer) = timer {
                timer.success();
            }
        }
        Err(error) => {
            error!("collector '{}' done: error: {}", name, error);
            if let Some(timer) = timer {
                timer.error();
            }
        }
    }

    (name, result)
}

/// Runs one scrape behind the single-flight gate, bounded by `scrape_timeout`.
///
/// # Why the permit is held here and not inside the task
///
/// The permit is owned by **this** future, so it is released the moment this future
/// returns *or is dropped* — on success, on timeout, on a collector panic, and when the
/// HTTP client disconnects mid-scrape. Nothing about the gate depends on the spawned task
/// making progress.
///
/// The previous implementation moved the permit *into* the spawned task and dropped the
/// `JoinHandle` on timeout, which detaches rather than aborts. Releasing the gate then
/// depended entirely on the detached task unwinding on its own, and there is nothing in
/// the code that guarantees it ever does: `collect_all_bytes_inner` drives a
/// `FuturesUnordered` of collector futures with no inner deadline, so one future that
/// never resolves holds the only permit of a `Semaphore::new(1)` for the rest of the
/// process lifetime and every later scrape fails with `ScrapeError::Busy` (503).
///
/// Aborting is not sufficient on its own either. A collector future that blocks inside
/// synchronous code contains no await point, and a `tokio` task can only be cancelled at
/// an await point — so an abort would not land until it yields. The gate therefore must
/// not, and now does not, depend on the task's cancellation at all.
///
/// Aborting is still the right thing to do alongside it: dropping the in-flight `sqlx`
/// futures releases the pooled connections that the timed-out scrape had checked out,
/// instead of leaving them parked server-side in `idle`/`Client:ClientRead` as observed in
/// issue #34.
///
/// The trade-off, accepted deliberately: the next scrape may start while the aborted
/// scrape's backends are still cancelling server-side, normally doubling the observed
/// connection footprint. Each new client-side wave is bounded by the shared pool and global
/// per-database semaphore, but lingering server backends from several aborted generations are
/// bounded only by PostgreSQL's role/cluster connection limits; see "Connection budget" in
/// the README.
async fn run_gated_scrape<F>(
    gate: &Arc<Semaphore>,
    scrape_timeout: Duration,
    scrape: F,
) -> Result<Vec<u8>, ScrapeError>
where
    F: Future<Output = Result<Vec<u8>, ScrapeError>> + Send + 'static,
{
    let _permit = Arc::clone(gate)
        .try_acquire_owned()
        .map_err(|_| ScrapeError::Busy)?;

    // Dropped at the end of this statement on timeout, which aborts the scrape task.
    let outcome = timeout(scrape_timeout, AbortOnDrop(tokio::spawn(scrape))).await;

    match outcome {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => Err(ScrapeError::CollectorFailed(vec![format!(
            "scrape task failed: {error}"
        )])),
        Err(_) => {
            warn!(
                timeout = ?scrape_timeout,
                "scrape exceeded --scrape.timeout-ms; aborted it and released the scrape gate so \
                 the next scrape can run"
            );
            Err(ScrapeError::Timeout(scrape_timeout))
        }
    }
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
        let registry = self.clone();
        let pool = pool.clone();

        run_gated_scrape(&self.scrape_gate, get_scrape_timeout(), async move {
            registry.collect_all_bytes_inner(&pool).await
        })
        .await
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

            // Defer even construction of the collector future until it is inside the panic
            // boundary. Trait implementations normally just box an async block, but a panic
            // before returning that box must still be an error, not an unobserved timer drop.
            let collector_pool = active_pool.clone();
            let fut = async move { collector.collect(&collector_pool).await };

            // Push an instrumented future that logs start/finish.
            tasks.push(collect_with_outcome(name, timer, fut, span));
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
    use anyhow::anyhow;
    use sqlx::postgres::PgPoolOptions;
    use std::time::Duration;

    #[tokio::test]
    async fn panicking_collector_is_recorded_as_error_not_abort() -> anyhow::Result<()> {
        let scraper = ScraperCollector::new();
        let prometheus_registry = Registry::new();
        scraper.register(&prometheus_registry)?;
        let timer = scraper.start_scrape("panicking_collector");

        let (_, result) = collect_with_outcome(
            "panicking_collector",
            Some(timer),
            async {
                #[allow(clippy::panic)]
                {
                    panic!("collector exploded");
                }
            },
            Span::none(),
        )
        .await;
        assert!(result.is_err(), "a collector panic must become an error");

        let metrics = prometheus_registry.gather();
        let errors = metrics
            .iter()
            .find(|metric| metric.name() == "pg_exporter_collector_scrape_errors_total")
            .ok_or_else(|| anyhow!("scrape error metric was not published"))?;
        let error_value = errors
            .get_metric()
            .first()
            .ok_or_else(|| anyhow!("scrape error metric has no sample"))?
            .get_counter()
            .value();
        assert!((error_value - 1.0).abs() < f64::EPSILON);

        let aborted = metrics
            .iter()
            .find(|metric| metric.name() == "pg_exporter_collector_scrape_aborted_total");
        assert!(
            aborted.is_none_or(|metric| metric.get_metric().is_empty()),
            "a caught collector panic must not be classified as a scrape abort"
        );
        Ok(())
    }

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

    /// Regression tests for the scrape gate (issue #34).
    ///
    /// The failure mode was a permanently closed gate: a scrape that timed out left its
    /// permit inside a *detached* task, so `/metrics` answered `503 another /metrics
    /// scrape is already running` for the rest of the process lifetime. These drive
    /// [`run_gated_scrape`] — the exact code path `collect_all_bytes` uses — with scrape
    /// futures that model the collectors observed in production.
    mod scrape_gate {
        use super::*;
        use std::sync::atomic::AtomicBool;
        use tokio::sync::oneshot;

        const TEST_TIMEOUT: Duration = Duration::from_millis(100);

        fn gate() -> Arc<Semaphore> {
            Arc::new(Semaphore::new(1))
        }

        /// Signals on drop, so a test can observe whether a future was actually dropped
        /// (cancelled) rather than left running.
        struct SignalOnDrop(Option<oneshot::Sender<()>>);

        impl Drop for SignalOnDrop {
            fn drop(&mut self) {
                if let Some(tx) = self.0.take() {
                    let _ = tx.send(());
                }
            }
        }

        /// A scrape future that never resolves — the hung collector from issue #34.
        ///
        /// On the old code this permanently wedged the gate: the permit lived in the
        /// detached task, which by construction never unwinds, so every later scrape
        /// returned `Busy`.
        #[tokio::test]
        async fn gate_reopens_after_a_scrape_that_never_finishes() {
            let gate = gate();

            let first = run_gated_scrape(&gate, TEST_TIMEOUT, std::future::pending()).await;
            assert!(
                matches!(first, Err(ScrapeError::Timeout(_))),
                "a scrape that never resolves must be reported as a timeout, got {first:?}"
            );

            for attempt in 1..=3 {
                let next =
                    run_gated_scrape(&gate, TEST_TIMEOUT, std::future::ready(Ok(b"ok".to_vec())))
                        .await;
                assert!(
                    !matches!(next, Err(ScrapeError::Busy)),
                    "attempt {attempt}: the gate stayed closed after a timed-out scrape, so \
                     /metrics is wedged at 503 until restart (issue #34)"
                );
                assert!(next.is_ok(), "attempt {attempt}: scrape failed: {next:?}");
            }

            assert_eq!(
                gate.available_permits(),
                1,
                "the scrape gate must be fully released once no scrape is in flight"
            );
        }

        /// A timed-out scrape must actually be cancelled, not detached to keep burning a
        /// worker and holding pooled `PostgreSQL` connections.
        #[tokio::test]
        async fn timed_out_scrape_is_aborted_not_detached() {
            let gate = gate();
            let (dropped_tx, dropped_rx) = oneshot::channel();

            let result = run_gated_scrape(&gate, TEST_TIMEOUT, async move {
                let _guard = SignalOnDrop(Some(dropped_tx));
                std::future::pending::<()>().await;
                Ok(Vec::new())
            })
            .await;

            assert!(matches!(result, Err(ScrapeError::Timeout(_))));
            assert!(
                timeout(Duration::from_secs(5), dropped_rx).await.is_ok(),
                "the timed-out scrape task was detached instead of aborted: its futures were \
                 never dropped, so pooled connections stay checked out (issue #34)"
            );
        }

        /// The gate must survive a scrape task that blocks its worker thread with no await
        /// point — `abort()` cannot land there, so permit release must not depend on it.
        /// This is the shape of the `system` collector before the issue #35 fix.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn gate_reopens_after_a_scrape_that_blocks_its_worker() {
            let gate = gate();
            let release = Arc::new(AtomicBool::new(false));
            let blocker = Arc::clone(&release);

            let first = run_gated_scrape(&gate, TEST_TIMEOUT, async move {
                while !blocker.load(Ordering::Relaxed) {
                    std::thread::sleep(Duration::from_millis(5));
                }
                Ok(Vec::new())
            })
            .await;
            assert!(matches!(first, Err(ScrapeError::Timeout(_))));

            let next =
                run_gated_scrape(&gate, TEST_TIMEOUT, std::future::ready(Ok(Vec::new()))).await;
            release.store(true, Ordering::Relaxed);

            assert!(
                !matches!(next, Err(ScrapeError::Busy)),
                "a scrape blocked in synchronous code cannot be aborted at an await point, so \
                 the gate must never depend on it to release the permit (issues #34, #35)"
            );
            assert!(next.is_ok(), "scrape failed: {next:?}");
        }

        /// Single-flight still holds: a genuinely concurrent second scrape is rejected
        /// while the first is running, which is what the gate exists for.
        #[tokio::test]
        async fn concurrent_scrape_is_still_rejected_while_one_is_in_flight() {
            let gate = gate();
            let (unblock_tx, unblock_rx) = oneshot::channel::<()>();
            let (started_tx, started_rx) = oneshot::channel::<()>();

            let gate_for_task = Arc::clone(&gate);
            let first = tokio::spawn(async move {
                run_gated_scrape(&gate_for_task, Duration::from_secs(30), async move {
                    let _ = started_tx.send(());
                    let _ = unblock_rx.await;
                    Ok(b"first".to_vec())
                })
                .await
            });

            assert!(started_rx.await.is_ok(), "first scrape never started");

            let concurrent =
                run_gated_scrape(&gate, TEST_TIMEOUT, std::future::ready(Ok(Vec::new()))).await;
            assert!(
                matches!(concurrent, Err(ScrapeError::Busy)),
                "an overlapping scrape must be rejected with Busy, got {concurrent:?}"
            );

            let _ = unblock_tx.send(());
            let first = first.await;
            assert!(
                matches!(first, Ok(Ok(ref bytes)) if bytes == b"first"),
                "the in-flight scrape must still succeed, got {first:?}"
            );
            assert_eq!(gate.available_permits(), 1);
        }

        /// A client that disconnects mid-scrape drops the request future. The permit must
        /// go with it, otherwise a hung client would wedge the gate just like a timeout.
        #[tokio::test]
        async fn gate_reopens_when_the_caller_is_cancelled() {
            let gate = gate();
            let gate_for_task = Arc::clone(&gate);
            let (started_tx, started_rx) = oneshot::channel::<()>();

            let request = tokio::spawn(async move {
                run_gated_scrape(&gate_for_task, Duration::from_secs(30), async move {
                    let _ = started_tx.send(());
                    std::future::pending::<Result<Vec<u8>, ScrapeError>>().await
                })
                .await
            });

            assert!(started_rx.await.is_ok(), "scrape never started");
            request.abort();
            let _ = request.await;

            let next =
                run_gated_scrape(&gate, TEST_TIMEOUT, std::future::ready(Ok(Vec::new()))).await;
            assert!(
                !matches!(next, Err(ScrapeError::Busy)),
                "a cancelled request left the scrape gate closed"
            );
            assert!(next.is_ok(), "scrape failed: {next:?}");
        }

        /// A panicking collector must surface as a failed scrape and still reopen the gate.
        #[tokio::test]
        async fn gate_reopens_after_a_panicking_scrape() {
            let gate = gate();

            let result = run_gated_scrape(&gate, Duration::from_secs(30), async {
                #[allow(clippy::panic)]
                {
                    panic!("collector exploded");
                }
            })
            .await;
            assert!(
                matches!(result, Err(ScrapeError::CollectorFailed(_))),
                "a panicking scrape must be reported, got {result:?}"
            );

            let next =
                run_gated_scrape(&gate, TEST_TIMEOUT, std::future::ready(Ok(Vec::new()))).await;
            assert!(
                next.is_ok(),
                "the gate stayed closed after a panic: {next:?}"
            );
        }
    }
}
