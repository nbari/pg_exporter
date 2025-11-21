use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{Gauge, IntGauge, Opts, Registry};
use sqlx::PgPool;
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use sysinfo::{Pid, System};
use tracing::{debug, instrument, warn};

/// Monitors the `pg_exporter` process itself
///
/// Tracks CPU and memory usage to help detect resource issues.
/// Matches output from scripts/monitor-exporter.sh for consistency.
///
/// # Metrics Exported
///
/// - `pg_exporter_process_cpu_percent` (Gauge)
///   - Current CPU usage percentage (matches `ps %cpu`)
///   - Range: 0% to (`num_cores` × 100%) - can exceed 100% on multi-core
///   - Example: 150% = using 1.5 cores
///   - Note: First reading after startup will be 0 (needs 2 refreshes for accuracy)
///
/// - `pg_exporter_process_cpu_cores` (`IntGauge`)
///   - Number of CPU cores available on the system
///   - Use to normalize: `cpu_percent / cpu_cores` for per-core % (0-100%)
///   - Example on 24-core: 150% / 24 = 6.25% per-core average
///
/// - `pg_exporter_process_resident_memory_bytes` (`IntGauge`)
///   - RSS (Resident Set Size) - actual RAM used
///
/// - `pg_exporter_process_virtual_memory_bytes` (`IntGauge`)
///   - VSZ (Virtual Size) - total virtual memory
///
/// - `pg_exporter_process_open_fds` (`IntGauge`, Linux only)
///   - Number of open file descriptors
///
/// - `pg_exporter_process_start_time_seconds` (Gauge)
///   - Process start timestamp (Unix epoch)
#[derive(Clone)]
pub struct ProcessCollector {
    cpu_percent: Gauge,
    cpu_cores: IntGauge,
    resident_memory_bytes: IntGauge,
    virtual_memory_bytes: IntGauge,
    open_fds: IntGauge,
    start_time_seconds: Gauge,
    system: Arc<Mutex<SystemState>>,
    pid: Pid,
}

/// Internal `state` for CPU tracking
struct SystemState {
    system: System,
    last_refresh: Option<Instant>,
}

impl Default for ProcessCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcessCollector {
    /// Creates a new `ProcessCollector`
    ///
    /// # Panics
    ///
    /// Panics if metric creation fails (should never happen with valid metric names)
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        let cpu_percent = Gauge::with_opts(Opts::new(
            "pg_exporter_process_cpu_percent",
            "Current CPU usage percentage (matches ps %cpu, can exceed 100%)",
        ))
        .expect("pg_exporter_process_cpu_percent");

        let cpu_cores = IntGauge::with_opts(Opts::new(
            "pg_exporter_process_cpu_cores",
            "Number of CPU cores available on the system",
        ))
        .expect("pg_exporter_process_cpu_cores");

        let resident_memory_bytes = IntGauge::with_opts(Opts::new(
            "pg_exporter_process_resident_memory_bytes",
            "Resident memory size in bytes (RSS)",
        ))
        .expect("pg_exporter_process_resident_memory_bytes");

        let virtual_memory_bytes = IntGauge::with_opts(Opts::new(
            "pg_exporter_process_virtual_memory_bytes",
            "Virtual memory size in bytes (VSZ)",
        ))
        .expect("pg_exporter_process_virtual_memory_bytes");

        let open_fds = IntGauge::with_opts(Opts::new(
            "pg_exporter_process_open_fds",
            "Number of open file descriptors",
        ))
        .expect("pg_exporter_process_open_fds");

        let start_time_seconds = Gauge::with_opts(Opts::new(
            "pg_exporter_process_start_time_seconds",
            "Start time of the process since unix epoch in seconds",
        ))
        .expect("pg_exporter_process_start_time_seconds");

        let system = System::new_all();
        let num_cpus = system.cpus().len().max(1);
        
        let system = Arc::new(Mutex::new(SystemState {
            system,
            last_refresh: None,
        }));
        let pid = Pid::from(std::process::id() as usize);

        // Set static values once
        cpu_cores.set(i64::try_from(num_cpus).unwrap_or(0));
        
        let start_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        start_time_seconds.set(start_time);

        Self {
            cpu_percent,
            cpu_cores,
            resident_memory_bytes,
            virtual_memory_bytes,
            open_fds,
            start_time_seconds,
            system,
            pid,
        }
    }

    fn collect_stats(&self) {
        let now = Instant::now();
        
        let mut state = match self.system.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("System mutex was poisoned, recovering");
                poisoned.into_inner()
            }
        };
        
        // Check if enough time has passed since last refresh
        // sysinfo needs time between refreshes for accurate CPU usage
        let should_wait = if let Some(last) = state.last_refresh {
            now.duration_since(last) < sysinfo::MINIMUM_CPU_UPDATE_INTERVAL
        } else {
            false
        };
        
        if should_wait {
            // Not enough time passed, skip CPU update but collect memory/fds
            if let Some(process) = state.system.process(self.pid) {
                let rss = process.memory();
                let vsz = process.virtual_memory();
                
                self.resident_memory_bytes.set(i64::try_from(rss).unwrap_or(0));
                self.virtual_memory_bytes.set(i64::try_from(vsz).unwrap_or(0));
            }
            return;
        }
        
        // Refresh process data
        state.system.refresh_all();
        state.last_refresh = Some(now);

        if let Some(process) = state.system.process(self.pid) {
            // CPU usage - matches ps %cpu
            // Note: sysinfo's cpu_usage() already returns percentage like ps
            // (can exceed 100% on multi-core if using multiple cores)
            let cpu = f64::from(process.cpu_usage());
            self.cpu_percent.set(cpu);

            // Memory metrics
            let rss = process.memory();
            let vsz = process.virtual_memory();
            
            self.resident_memory_bytes.set(i64::try_from(rss).unwrap_or(0));
            self.virtual_memory_bytes.set(i64::try_from(vsz).unwrap_or(0));

            // File descriptors (Linux-specific)
            #[cfg(target_os = "linux")]
            {
                if let Ok(entries) = std::fs::read_dir(format!("/proc/{}/fd", self.pid)) {
                    let fd_count = i64::try_from(entries.count()).unwrap_or(0);
                    self.open_fds.set(fd_count);
                }
            }
            
            #[cfg(not(target_os = "linux"))]
            {
                self.open_fds.set(0);
            }

            debug!(
                cpu_percent = cpu,
                rss_mb = rss / 1024 / 1024,
                vsz_mb = vsz / 1024 / 1024,
                fds = self.open_fds.get(),
                "collected process metrics"
            );
        }
    }
}

impl Collector for ProcessCollector {
    fn name(&self) -> &'static str {
        "metrics.process"
    }

    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.cpu_percent.clone()))?;
        registry.register(Box::new(self.cpu_cores.clone()))?;
        registry.register(Box::new(self.resident_memory_bytes.clone()))?;
        registry.register(Box::new(self.virtual_memory_bytes.clone()))?;
        registry.register(Box::new(self.open_fds.clone()))?;
        registry.register(Box::new(self.start_time_seconds.clone()))?;
        Ok(())
    }

    #[instrument(skip(self, _pool), level = "debug")]
    fn collect<'a>(&'a self, _pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.collect_stats();
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
    fn test_process_collector_new() {
        let collector = ProcessCollector::new();
        assert!(collector.start_time_seconds.get() > 0.0);
    }

    #[test]
    fn test_process_collector_registers_without_error() {
        let collector = ProcessCollector::new();
        let registry = Registry::new();
        assert!(collector.register_metrics(&registry).is_ok());
    }

    #[test]
    fn test_process_collector_collects_stats() {
        let collector = ProcessCollector::new();
        collector.collect_stats();
        
        assert!(collector.resident_memory_bytes.get() > 0);
        assert!(collector.virtual_memory_bytes.get() > 0);
        assert!(collector.virtual_memory_bytes.get() >= collector.resident_memory_bytes.get());
        assert!(collector.cpu_percent.get() >= 0.0);
    }

    #[test]
    fn test_cpu_percent_reasonable() {
        let collector = ProcessCollector::new();
        collector.collect_stats();
        
        let cpu = collector.cpu_percent.get();
        assert!(cpu >= 0.0);
        assert!(cpu < 10000.0);
    }

    #[test]
    fn test_memory_metrics_reasonable() {
        let collector = ProcessCollector::new();
        collector.collect_stats();
        
        let rss_mb = collector.resident_memory_bytes.get() / 1024 / 1024;
        let vsz_mb = collector.virtual_memory_bytes.get() / 1024 / 1024;
        
        assert!(rss_mb > 1);
        assert!(rss_mb < 10_000);
        assert!(vsz_mb > rss_mb);
        assert!(vsz_mb < 100_000);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn test_file_descriptors_linux() {
        let collector = ProcessCollector::new();
        collector.collect_stats();
        
        let fd_count = collector.open_fds.get();
        assert!(fd_count >= 3);
        assert!(fd_count > 0);
    }

    #[test]
    fn test_multiple_collections_stable() {
        let collector = ProcessCollector::new();
        
        for _ in 0..5 {
            collector.collect_stats();
        }
        
        assert!(collector.resident_memory_bytes.get() > 0);
        assert!(collector.cpu_percent.get() >= 0.0);
    }
}
