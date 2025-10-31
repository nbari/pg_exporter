/// Exporter self-monitoring
///
/// This module provides self-monitoring capabilities for pg_exporter itself.
/// Unlike other collectors that monitor PostgreSQL, this monitors the exporter's
/// own health and performance.
///
/// # Why Exporter Metrics Matter
///
/// When running in production, you need visibility into:
/// - **Resource usage**: Is the exporter leaking memory? Using too much CPU?
/// - **Performance**: Which collectors are slow? Are scrapes failing?
/// - **Cardinality**: How many metrics are being exported? (Critical for Cortex/Mimir)
///
/// # Architecture
///
/// The exporter collector consists of two sub-collectors:
///
/// ## ProcessCollector
/// Monitors the exporter's process resource consumption using the `sysinfo` crate:
/// - CPU time (via /proc/$PID/stat on Linux)
/// - Memory usage (RSS and VSZ)
/// - Thread count and file descriptors
/// - Process start time (for uptime calculation)
///
/// ## ScraperCollector
/// Tracks scrape performance and health:
/// - Per-collector scrape duration (histogram with percentiles)
/// - Error counts per collector
/// - Last scrape timestamp and success status
/// - Total metric cardinality (for operators with limits)
///
/// # Threading and Locking
///
/// We use `std::sync::{Mutex, RwLock}` for thread-safe access to shared state.
///
/// ## Poison Error Handling
///
/// Rust's standard library mutexes become "poisoned" if a thread panics while
/// holding the lock. We handle this explicitly to ensure resilience:
///
///    ```rust,no_run
///    # use std::sync::Mutex;
///    # let mutex = Mutex::new(0);
///    // Acquire lock with poison recovery
///    let guard = match mutex.lock() {
///        Ok(guard) => guard,
///        Err(poisoned) => {
///            // Lock was poisoned, but we can recover
///            eprintln!("Mutex poisoned, recovering");
///            poisoned.into_inner()
///        }
///    };
///    ```
///
/// This pattern ensures that one panic during metrics collection doesn't
/// break all future collections.
///
/// ## Lock Usage Pattern
///
/// ```rust,no_run
/// # use std::sync::Mutex;
/// # use sysinfo::System;
/// # struct ProcessCollector { system: std::sync::Arc<Mutex<System>> }
/// # impl ProcessCollector {
/// # fn collect_stats(&self) {
/// // ProcessCollector locks briefly to read /proc
/// let mut system = match self.system.lock() {
///     Ok(guard) => guard,
///     Err(poisoned) => poisoned.into_inner(),
/// };
/// system.refresh_processes(sysinfo::ProcessesToUpdate::All, true);
/// # drop(system); // Lock released
/// # }
/// # }
/// ```
///
/// The critical section is tiny - just reading process stats from the OS.
/// Lock contention is minimal since scrapes happen every 15-60 seconds.
///
/// # Example Usage
///
/// The exporter collector is **disabled by default**. Enable it explicitly:
///
/// ```bash
/// pg_exporter --dsn postgresql://localhost/postgres --collector.exporter
/// # Exports pg_exporter_process_* and pg_exporter_collector_* metrics
/// ```
///
/// Monitor in Prometheus:
///
/// ```promql
/// # CPU usage %
/// rate(pg_exporter_process_cpu_seconds_total[5m]) * 100
///
/// # Memory usage MB
/// pg_exporter_process_resident_memory_bytes / 1024 / 1024
///
/// # Slowest collector (p99)
/// histogram_quantile(0.99,
///   rate(pg_exporter_collector_scrape_duration_seconds_bucket[5m])
/// )
///
/// # Total metrics (for cardinality limits)
/// pg_exporter_metrics_total
/// ```
///
/// # Platform Support
///
/// - **Linux**: Full metrics including FD count and accurate thread count
/// - **macOS/Windows**: Basic metrics (CPU, memory), limited thread/FD info
///
/// Platform-specific code is guarded with `#[cfg(target_os = "linux")]`.
mod process;
mod scraper;

pub use process::ProcessCollector;
pub use scraper::{ScrapeTimer, ScraperCollector};

use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use prometheus::Registry;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{debug, info_span, instrument, warn};
use tracing_futures::Instrument as _;

/// ExporterCollector combines all exporter self-monitoring
#[derive(Clone)]
pub struct ExporterCollector {
    subs: Vec<Arc<dyn Collector + Send + Sync>>,
    scraper: Arc<ScraperCollector>,
}

impl Default for ExporterCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl ExporterCollector {
    pub fn new() -> Self {
        let scraper = Arc::new(ScraperCollector::new());
        Self {
            subs: vec![
                Arc::new(ProcessCollector::new()),
                Arc::clone(&scraper) as Arc<dyn Collector + Send + Sync>,
            ],
            scraper,
        }
    }

    /// Get a reference to the scraper collector.
    ///
    /// The scraper tracks performance metrics for all collectors:
    /// - Scrape duration histograms (for percentile calculations)
    /// - Error counts per collector
    /// - Total metrics exported (cardinality tracking)
    ///
    /// This is called by `CollectorRegistry` during initialization to
    /// extract the scraper for tracking all collector performance.
    pub fn get_scraper(&self) -> &Arc<ScraperCollector> {
        &self.scraper
    }
}

impl Collector for ExporterCollector {
    fn name(&self) -> &'static str {
        "exporter"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "exporter")
    )]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        for sub in &self.subs {
            let span = info_span!("collector.register_metrics", sub_collector = %sub.name());

            let res = sub.register_metrics(registry);

            match res {
                Ok(_) => debug!(collector = sub.name(), "registered exporter metrics"),
                Err(ref e) => {
                    warn!(collector = sub.name(), error = %e, "failed to register exporter metrics")
                }
            }

            res?;

            drop(span);
        }
        Ok(())
    }

    #[instrument(skip(self, pool), level = "info", err, fields(collector = "exporter", otel.kind = "internal"))]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let mut tasks = FuturesUnordered::new();

            for sub in &self.subs {
                let span = info_span!("collector.collect", sub_collector = %sub.name(), otel.kind = "internal");
                tasks.push(sub.collect(pool).instrument(span));
            }

            while let Some(res) = tasks.next().await {
                res?;
            }

            Ok(())
        })
    }

    fn enabled_by_default(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exporter_collector_new() {
        let collector = ExporterCollector::new();
        assert_eq!(collector.subs.len(), 2);
    }

    #[test]
    fn test_exporter_collector_name() {
        let collector = ExporterCollector::new();
        assert_eq!(collector.name(), "exporter");
    }

    #[test]
    fn test_exporter_collector_not_enabled_by_default() {
        let collector = ExporterCollector::new();
        assert!(!collector.enabled_by_default());
    }

    #[test]
    fn test_exporter_collector_registers_without_error() {
        let collector = ExporterCollector::new();
        let registry = Registry::new();
        assert!(collector.register_metrics(&registry).is_ok());
    }

    #[test]
    fn test_exporter_collector_has_scraper() {
        let collector = ExporterCollector::new();
        let scraper = collector.get_scraper();
        
        // Scraper should be accessible
        assert!(Arc::strong_count(scraper) >= 1);
    }

    #[test]
    fn test_exporter_collector_scraper_is_same_instance() {
        let collector = ExporterCollector::new();
        
        // Get scraper twice and verify it's the same Arc
        let scraper1 = collector.get_scraper();
        let scraper2 = collector.get_scraper();
        
        assert!(Arc::ptr_eq(scraper1, scraper2));
    }

    #[tokio::test]
    async fn test_exporter_collector_collect_succeeds() {
        use sqlx::postgres::PgPoolOptions;
        
        // This test requires a database connection
        let dsn = std::env::var("PG_EXPORTER_DSN")
            .unwrap_or_else(|_| "postgresql://postgres:postgres@localhost:5432/postgres".to_string());
        
        let pool = match PgPoolOptions::new()
            .min_connections(1)
            .max_connections(1)
            .connect(&dsn)
            .await
        {
            Ok(pool) => pool,
            Err(_) => {
                eprintln!("Skipping test: database not available");
                return;
            }
        };

        let collector = ExporterCollector::new();
        let registry = Registry::new();
        
        // Register metrics first
        collector.register_metrics(&registry).unwrap();
        
        // Collect should succeed (it's a no-op but shouldn't error)
        let result = collector.collect(&pool).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_exporter_collector_default_trait() {
        let collector = ExporterCollector::default();
        assert_eq!(collector.name(), "exporter");
    }
}
