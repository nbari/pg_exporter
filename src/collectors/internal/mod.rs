/// Internal exporter metrics
///
/// This module provides self-monitoring capabilities for pg_exporter itself.
/// Unlike other collectors that monitor PostgreSQL, this monitors the exporter's
/// own health and performance.
///
/// # Why Internal Metrics Matter
///
/// When running in production, you need visibility into:
/// - **Resource usage**: Is the exporter leaking memory? Using too much CPU?
/// - **Performance**: Which collectors are slow? Are scrapes failing?
/// - **Cardinality**: How many metrics are being exported? (Critical for Cortex/Mimir)
///
/// # Architecture
///
/// The internal collector consists of two sub-collectors:
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
/// We use `parking_lot::Mutex` instead of `std::sync::Mutex` for performance:
///
/// ## Why parking_lot?
///
/// 1. **No lock poisoning** - If a panic occurs while holding the lock, future
///    acquirers won't panic. This is critical for metrics collection - one
///    failed scrape shouldn't break all future scrapes.
///
/// 2. **Simpler API** - No `.unwrap()` needed:
///    ```rust,no_run
///    # use std::sync::Mutex as StdMutex;
///    # use parking_lot::Mutex;
///    # let std_mutex = StdMutex::new(0);
///    # let parking_mutex = Mutex::new(0);
///    // std::sync::Mutex
///    let guard = std_mutex.lock().unwrap(); // Can panic!
///    
///    // parking_lot::Mutex  
///    let guard = parking_mutex.lock(); // Never panics
///    ```
///
/// 3. **Better performance** - Uses an efficient spinlock before parking,
///    reducing overhead for short critical sections (like reading /proc files).
///
/// 4. **Smaller size** - Only 1 word vs multiple words for std::sync::Mutex.
///
/// ## Lock Usage Pattern
///
/// ```rust,no_run
/// # use parking_lot::Mutex;
/// # use sysinfo::System;
/// # struct ProcessCollector { system: std::sync::Arc<Mutex<System>> }
/// # impl ProcessCollector {
/// # fn collect_stats(&self) {
/// // ProcessCollector locks briefly to read /proc
/// let mut system = self.system.lock();  // ~1-5ms hold time
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
/// The internal collector is enabled by default:
///
/// ```bash
/// pg_exporter --dsn postgresql://localhost/postgres
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
pub use scraper::{ScraperCollector, ScrapeTimer};

use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use prometheus::Registry;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{debug, info_span, instrument, warn};
use tracing_futures::Instrument as _;

/// InternalCollector combines all exporter self-monitoring
#[derive(Clone, Default)]
pub struct InternalCollector {
    subs: Vec<Arc<dyn Collector + Send + Sync>>,
}

impl InternalCollector {
    pub fn new() -> Self {
        Self {
            subs: vec![
                Arc::new(ProcessCollector::new()),
                // ScraperCollector is handled specially in metrics handler
            ],
        }
    }
}

impl Collector for InternalCollector {
    fn name(&self) -> &'static str {
        "internal"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "internal")
    )]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        for sub in &self.subs {
            let span = info_span!("collector.register_metrics", sub_collector = %sub.name());
            let res = sub.register_metrics(registry);
            match res {
                Ok(_) => debug!(collector = sub.name(), "registered internal metrics"),
                Err(ref e) => {
                    warn!(collector = sub.name(), error = %e, "failed to register internal metrics")
                }
            }
            res?;
            drop(span);
        }
        Ok(())
    }

    #[instrument(skip(self, pool), level = "info", err, fields(collector = "internal", otel.kind = "internal"))]
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
        true // Always enable internal monitoring
    }
}

