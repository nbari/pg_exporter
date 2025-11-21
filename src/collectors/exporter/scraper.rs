use anyhow::Result;
use prometheus::{CounterVec, GaugeVec, HistogramVec, IntGauge, Opts, Registry};
use std::sync::{Arc, RwLock};
use std::time::Instant;

/// Tracks scrape performance and metrics cardinality
///
/// This collector monitors the health and performance of all other collectors,
/// helping operators identify slow collectors, detect failures, and track
/// metric cardinality (critical for Cortex/Mimir with strict limits).
///
/// # Metrics Exported
///
/// ## Per-Collector Performance
///
/// - `pg_exporter_collector_scrape_duration_seconds{collector}` (Histogram)
///   - Time spent scraping each collector
///   - Buckets: 1ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s
///   - Use `histogram_quantile()` for percentiles (p50, p95, p99)
///   - Example: `histogram_quantile(0.99, rate(pg_exporter_collector_scrape_duration_seconds_bucket[5m]))`
///
/// - `pg_exporter_collector_scrape_errors_total{collector}` (Counter)
///   - Total errors per collector since start
///   - Alert if rate > 0: collector is failing
///   - Example: `rate(pg_exporter_collector_scrape_errors_total[5m]) > 0`
///
/// - `pg_exporter_collector_last_scrape_timestamp_seconds{collector}` (Gauge)
///   - Unix timestamp of last scrape attempt
///   - Detect stale collectors (stuck or disabled)
///   - Example: `time() - pg_exporter_collector_last_scrape_timestamp_seconds > 120`
///
/// - `pg_exporter_collector_last_scrape_success{collector}` (Gauge)
///   - 1 = last scrape succeeded, 0 = failed
///   - Simple success/failure indicator per collector
///
/// ## Global Metrics
///
/// - `pg_exporter_metrics_total` (`IntGauge`)
///   - **Total number of metrics currently exported**
///   - Critical for Cortex/Mimir operators with series limits
///   - Alert if approaching your cardinality limit
///   - Example: `pg_exporter_metrics_total > 10000`
///
/// - `pg_exporter_scrapes_total` (`IntGauge`)
///   - Total scrapes performed since start
///   - Used to detect if exporter is active
///
/// # Usage Pattern with `ScrapeTimer`
///
/// The `ScrapeTimer` is an RAII (Resource Acquisition Is Initialization) timer
/// that automatically records scrape duration and status when dropped:
///
/// ```no_run
/// # use pg_exporter::collectors::exporter::ScraperCollector;
/// # use anyhow::Result;
/// # async fn example() -> Result<()> {
/// let scraper = ScraperCollector::new();
///
/// // Start timing a collector scrape
/// let timer = scraper.start_scrape("database");
///
/// // Simulate collector work
/// match collect_database_metrics().await {
///     Ok(_) => timer.success(),  // Records duration, marks success
///     Err(e) => timer.error(),   // Records error, marks failure
/// }
///
/// // If timer is dropped without calling success()/error(),
/// // it defaults to success (optimistic)
/// # Ok(())
/// # }
/// # async fn collect_database_metrics() -> Result<()> { Ok(()) }
/// ```
///
/// # Thread Safety
///
/// Uses `std::sync::RwLock` for the metrics state:
/// - Multiple readers (metric reads) don't block each other
/// - Single writer (updates) blocks readers briefly
/// - Poison errors handled explicitly for resilience
///
/// Why `RwLock` instead of `Mutex`?
/// - Scrape counters are read-heavy (`Prometheus` scrapes every 15-60s)
/// - Writes only happen during collector execution
/// - Better concurrency for high scrape rates
///
/// ## Poison Error Handling
///
/// If a panic occurs while holding the write lock, the `RwLock` becomes poisoned.
/// We detect this and recover gracefully using `into_inner()`:
///
/// ```rust,no_run
/// # use std::sync::RwLock;
/// # let lock = RwLock::new(0);
/// let mut guard = match lock.write() {
///     Ok(guard) => guard,
///     Err(poisoned) => {
///         eprintln!("Lock poisoned, recovering");
///         poisoned.into_inner()
///     }
/// };
/// ```
///
/// This ensures one panic doesn't break all future metric updates.
///
/// # Example `Prometheus` Queries
///
/// ```promql
/// # Slowest collector (p99 latency)
/// topk(5,
///   histogram_quantile(0.99,
///     rate(pg_exporter_collector_scrape_duration_seconds_bucket[5m])
///   )
/// ) by (collector)
///
/// # Failed collectors
/// sum by (collector) (
///   rate(pg_exporter_collector_scrape_errors_total[5m])
/// ) > 0
///
/// # Metric cardinality trend
/// delta(pg_exporter_metrics_total[1h])
/// ```
#[derive(Clone)]
pub struct ScraperCollector {
    // Per-collector metrics
    scrape_duration_seconds: HistogramVec,
    scrape_errors_total: CounterVec,
    last_scrape_timestamp: GaugeVec,
    last_scrape_success: GaugeVec,
    
    // Global metrics
    metrics_total: IntGauge,
    scrapes_total: IntGauge,
    
    /// Internal `state` for tracking total counts
    ///
    /// Protected by `RwLock` for concurrent reads:
    /// - Reads: `Prometheus` scrapes metrics
    /// - Writes: `update_metrics_count()`, `increment_scrapes()`
    ///
    /// We handle `PoisonError` explicitly to recover from panics:
    /// - If a write panics, the lock becomes poisoned
    /// - We detect this and recover via `into_inner()`
    /// - This prevents one failed update from breaking all future scrapes
    state: Arc<RwLock<ScraperState>>,
}

#[derive(Default)]
struct ScraperState {
    total_scrapes: i64,
    total_metrics: i64,
}

impl Default for ScraperCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl ScraperCollector {
    /// Creates a new `ScraperCollector`
    ///
    /// # Panics
    ///
    /// Panics if metric creation fails (should never happen with valid metric names)
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        let scrape_duration_seconds = HistogramVec::new(
            prometheus::HistogramOpts::new(
                "pg_exporter_collector_scrape_duration_seconds",
                "Time spent scraping each collector in seconds",
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]),
            &["collector"],
        )
        .expect("pg_exporter_collector_scrape_duration_seconds");

        let scrape_errors_total = CounterVec::new(
            Opts::new(
                "pg_exporter_collector_scrape_errors_total",
                "Total number of scrape errors per collector",
            ),
            &["collector"],
        )
        .expect("pg_exporter_collector_scrape_errors_total");

        let last_scrape_timestamp = GaugeVec::new(
            Opts::new(
                "pg_exporter_collector_last_scrape_timestamp_seconds",
                "Unix timestamp of the last scrape attempt per collector",
            ),
            &["collector"],
        )
        .expect("pg_exporter_collector_last_scrape_timestamp_seconds");

        let last_scrape_success = GaugeVec::new(
            Opts::new(
                "pg_exporter_collector_last_scrape_success",
                "Whether the last scrape was successful (1=success, 0=failure)",
            ),
            &["collector"],
        )
        .expect("pg_exporter_collector_last_scrape_success");

        let metrics_total = IntGauge::with_opts(Opts::new(
            "pg_exporter_metrics_total",
            "Total number of metrics currently exported (for cardinality monitoring)",
        ))
        .expect("pg_exporter_metrics_total");

        let scrapes_total = IntGauge::with_opts(Opts::new(
            "pg_exporter_scrapes_total",
            "Total number of scrapes performed since start",
        ))
        .expect("pg_exporter_scrapes_total");

        Self {
            scrape_duration_seconds,
            scrape_errors_total,
            last_scrape_timestamp,
            last_scrape_success,
            metrics_total,
            scrapes_total,
            state: Arc::new(RwLock::new(ScraperState::default())),
        }
    }

    /// Record the start of a collector scrape
    #[must_use]
    pub fn start_scrape(&self, collector_name: &str) -> ScrapeTimer {
        ScrapeTimer {
            collector_name: collector_name.to_string(),
            start: Instant::now(),
            scraper: self.clone(),
        }
    }

    /// Update total metrics count
    /// Call this after each scrape to track cardinality
    /// Handles lock poisoning gracefully.
    pub fn update_metrics_count(&self, count: i64) {
        self.metrics_total.set(count);
        let mut state = match self.state.write() {
            Ok(guard) => guard,
            Err(poisoned) => {
                tracing::warn!("ScraperState write lock was poisoned, recovering");
                poisoned.into_inner()
            }
        };
        state.total_metrics = count;
    }

    /// Increment total scrapes counter
    /// Handles lock poisoning gracefully.
    pub fn increment_scrapes(&self) {
        let mut state = match self.state.write() {
            Ok(guard) => guard,
            Err(poisoned) => {
                tracing::warn!("ScraperState write lock was poisoned, recovering");
                poisoned.into_inner()
            }
        };
        state.total_scrapes += 1;
        self.scrapes_total.set(state.total_scrapes);
    }

    /// Record a successful scrape
    fn record_success(&self, collector_name: &str, duration: f64) {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();

        self.scrape_duration_seconds
            .with_label_values(&[collector_name])
            .observe(duration);

        self.last_scrape_timestamp
            .with_label_values(&[collector_name])
            .set(timestamp);

        self.last_scrape_success
            .with_label_values(&[collector_name])
            .set(1.0);
    }

    /// Record a failed scrape
    fn record_error(&self, collector_name: &str) {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();

        self.scrape_errors_total
            .with_label_values(&[collector_name])
            .inc();

        self.last_scrape_timestamp
            .with_label_values(&[collector_name])
            .set(timestamp);

        self.last_scrape_success
            .with_label_values(&[collector_name])
            .set(0.0);
    }

    /// Register all metrics with the registry
    ///
    /// # Errors
    ///
    /// Returns an error if any metric fails to register
    pub fn register(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.scrape_duration_seconds.clone()))?;
        registry.register(Box::new(self.scrape_errors_total.clone()))?;
        registry.register(Box::new(self.last_scrape_timestamp.clone()))?;
        registry.register(Box::new(self.last_scrape_success.clone()))?;
        registry.register(Box::new(self.metrics_total.clone()))?;
        registry.register(Box::new(self.scrapes_total.clone()))?;
        Ok(())
    }
}

impl crate::collectors::Collector for ScraperCollector {
    fn name(&self) -> &'static str {
        "scraper"
    }

    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        self.register(registry)
    }

    fn collect<'a>(&'a self, _pool: &'a sqlx::PgPool) -> futures::future::BoxFuture<'a, Result<()>> {
        // ScraperCollector doesn't scrape from PostgreSQL
        // It's updated by other collectors via start_scrape(), update_metrics_count(), etc.
        Box::pin(async move { Ok(()) })
    }

    fn enabled_by_default(&self) -> bool {
        false
    }
}

/// RAII timer for recording scrape duration
///
/// Automatically records duration and success/failure on drop
pub struct ScrapeTimer {
    collector_name: String,
    start: Instant,
    scraper: ScraperCollector,
}

impl ScrapeTimer {
    /// Mark scrape as successful
    /// Call this before timer drops if scrape succeeded
    pub fn success(self) {
        let duration = self.start.elapsed().as_secs_f64();
        self.scraper.record_success(&self.collector_name, duration);
    }

    /// Mark scrape as failed
    /// Call this before timer drops if scrape failed
    pub fn error(self) {
        self.scraper.record_error(&self.collector_name);
    }
}

impl Drop for ScrapeTimer {
    fn drop(&mut self) {
        // If neither success() nor error() was called explicitly,
        // default to success (optimistic)
        let duration = self.start.elapsed().as_secs_f64();
        self.scraper.record_success(&self.collector_name, duration);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_scraper_collector_new() {
        let scraper = ScraperCollector::new();
        assert_eq!(scraper.metrics_total.get(), 0);
        assert_eq!(scraper.scrapes_total.get(), 0);
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_scraper_collector_registers_without_error() {
        let scraper = ScraperCollector::new();
        let registry = Registry::new();
        assert!(scraper.register(&registry).is_ok());
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    #[allow(clippy::expect_used)]
    fn test_scrape_timer_records_duration() {
        let scraper = ScraperCollector::new();
        let registry = Registry::new();
        scraper.register(&registry).unwrap();

        {
            let timer = scraper.start_scrape("test_collector");
            thread::sleep(Duration::from_millis(10));
            timer.success();
        }

        // Check that metrics were recorded
        let metrics = registry.gather();
        let duration_metric = metrics
            .iter()
            .find(|m| m.name() == "pg_exporter_collector_scrape_duration_seconds")
            .expect("duration metric should exist");

        assert!(!duration_metric.get_metric().is_empty());
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    #[allow(clippy::expect_used)]
    fn test_scrape_timer_records_error() {
        let scraper = ScraperCollector::new();
        let registry = Registry::new();
        scraper.register(&registry).unwrap();

        {
            let timer = scraper.start_scrape("test_collector");
            timer.error();
        }

        // Check that error was recorded
        let metrics = registry.gather();
        let error_metric = metrics
            .iter()
            .find(|m| m.name() == "pg_exporter_collector_scrape_errors_total")
            .expect("error metric should exist");

        assert!(!error_metric.get_metric().is_empty());
    }

    #[test]
    fn test_update_metrics_count() {
        let scraper = ScraperCollector::new();
        scraper.update_metrics_count(42);
        assert_eq!(scraper.metrics_total.get(), 42);
    }

    #[test]
    fn test_increment_scrapes() {
        let scraper = ScraperCollector::new();
        scraper.increment_scrapes();
        assert_eq!(scraper.scrapes_total.get(), 1);
        scraper.increment_scrapes();
        assert_eq!(scraper.scrapes_total.get(), 2);
    }
}
