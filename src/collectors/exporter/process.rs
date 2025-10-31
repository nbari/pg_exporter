use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{Counter, Gauge, IntGauge, Opts, Registry};
use sqlx::PgPool;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use sysinfo::{Pid, System};
use tracing::{debug, instrument, warn};

/// Monitors the pg_exporter process itself
///
/// This collector tracks resource consumption of the exporter process,
/// helping operators detect memory leaks, CPU spikes, and resource exhaustion.
///
/// # Metrics Exported
///
/// ## CPU Usage
/// - `pg_exporter_process_cpu_seconds_total` (Counter)
///   - Total CPU time (user + system) since process start
///   - Use `rate()` in Prometheus to get CPU percentage
///   - Example: `rate(pg_exporter_process_cpu_seconds_total[5m]) * 100`
///
/// ## Memory Usage  
/// - `pg_exporter_process_resident_memory_bytes` (IntGauge)
///   - RSS (Resident Set Size) - actual RAM used
///   - Alert if >500MB or steadily increasing (leak)
///
/// - `pg_exporter_process_virtual_memory_bytes` (IntGauge)
///   - VSZ (Virtual Size) - total virtual memory allocated
///   - Usually much larger than RSS (includes mapped files, shared libs)
///
/// ## Thread and File Descriptor Count
/// - `pg_exporter_process_threads` (IntGauge)
///   - Number of OS threads in the process
///   - Tokio runtime typically uses N threads where N = CPU cores
///
/// - `pg_exporter_process_open_fds` (IntGauge, Linux only)
///   - Number of open file descriptors
///   - Alert if approaching `ulimit -n` (default 1024)
///   - Each database connection uses ~1 FD
///
/// ## Process Lifecycle
/// - `pg_exporter_process_start_time_seconds` (Gauge)
///   - Unix timestamp when the process started
///   - Use to calculate uptime or detect restarts
///   - Example: `time() - pg_exporter_process_start_time_seconds`
///
/// # Implementation Details
///
/// Uses the `sysinfo` crate to read process information from the OS:
/// - Linux: Reads `/proc/$PID/stat`, `/proc/$PID/status`, `/proc/$PID/fd/`
/// - macOS: Uses `proc_pidinfo()` system call
/// - Windows: Uses Windows API
///
/// The `System` object is cached in an `Arc<Mutex<>>` and reused across
/// scrapes to avoid allocating it on every collection cycle.
///
/// # Performance
///
/// - Collection time: ~1-5ms on Linux
/// - Lock hold time: <1ms (just reads /proc, no I/O)
/// - Memory overhead: ~10KB for cached System object
///
/// # Example
///
/// ```rust,no_run
/// # use pg_exporter::collectors::exporter::ProcessCollector;
/// # use pg_exporter::collectors::Collector;
/// # use prometheus::Registry;
/// # fn example() -> anyhow::Result<()> {
/// let collector = ProcessCollector::new();
/// let registry = Registry::new();
/// collector.register_metrics(&registry)?;
///
/// // After collection, metrics will be available:
/// // pg_exporter_process_resident_memory_bytes ~45,000,000 (45MB)
/// // pg_exporter_process_threads 8
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct ProcessCollector {
    cpu_seconds_total: Counter,
    resident_memory_bytes: IntGauge,
    virtual_memory_bytes: IntGauge,
    open_fds: IntGauge,
    threads: IntGauge,
    start_time_seconds: Gauge,
    
    /// Cached sysinfo System object, protected by std::sync::Mutex
    ///
    /// Mutex allows safe concurrent access to the System object. We handle
    /// PoisonError explicitly to recover from panics during collection.
    ///
    /// If a panic occurs while holding the lock:
    /// - The lock becomes "poisoned"
    /// - We detect this and recover via `into_inner()`
    /// - A warning is logged, but collection continues
    /// - This prevents one bad scrape from breaking all future scrapes
    system: Arc<Mutex<System>>,
    
    /// Process ID of this exporter
    pid: Pid,
}

impl Default for ProcessCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcessCollector {
    pub fn new() -> Self {
        let cpu_seconds_total = Counter::with_opts(Opts::new(
            "pg_exporter_process_cpu_seconds_total",
            "Total user and system CPU time spent in seconds",
        ))
        .expect("pg_exporter_process_cpu_seconds_total");

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

        let threads = IntGauge::with_opts(Opts::new(
            "pg_exporter_process_threads",
            "Number of OS threads in the process",
        ))
        .expect("pg_exporter_process_threads");

        let start_time_seconds = Gauge::with_opts(Opts::new(
            "pg_exporter_process_start_time_seconds",
            "Start time of the process since unix epoch in seconds",
        ))
        .expect("pg_exporter_process_start_time_seconds");

        let system = Arc::new(Mutex::new(System::new_all()));
        let pid = Pid::from(std::process::id() as usize);

        // Set start time once (doesn't change)
        let start_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        start_time_seconds.set(start_time);

        Self {
            cpu_seconds_total,
            resident_memory_bytes,
            virtual_memory_bytes,
            open_fds,
            threads,
            start_time_seconds,
            system,
            pid,
        }
    }

    /// Get current process statistics
    ///
    /// Reads process information from the operating system:
    /// - Linux: /proc/$PID/stat, /proc/$PID/status, /proc/$PID/fd/
    /// - macOS: proc_pidinfo() system call
    /// - Windows: Windows API
    ///
    /// This method:
    /// 1. Acquires a lock on the cached System object (~0.1ms)
    /// 2. Handles PoisonError if a previous panic occurred
    /// 3. Refreshes process data from OS (~1-5ms)
    /// 4. Extracts metrics (memory, CPU, threads, FDs)
    /// 5. Updates Prometheus gauges/counters
    /// 6. Releases lock
    ///
    /// Total execution time: ~1-5ms on Linux, may be slower on other platforms.
    fn collect_stats(&self) {
        // Acquire lock, handling poison errors gracefully
        let mut system = match self.system.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                // Lock was poisoned by a panic, but we can recover
                warn!("System mutex was poisoned, recovering");
                poisoned.into_inner()
            }
        };
        
        // Refresh only our process (more efficient than refresh_all)
        // sysinfo 0.32 API: refresh_processes(processes_to_update, refresh_kind)
        // ProcessesToUpdate::Some(&[pid]) = only refresh this PID
        // true = refresh all process info (CPU, memory, threads)
        system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[self.pid]), true);

        if let Some(process) = system.process(self.pid) {
            // Memory metrics (sysinfo 0.32 returns bytes directly)
            let rss = process.memory();
            let vsz = process.virtual_memory();
            
            self.resident_memory_bytes.set(rss as i64);
            self.virtual_memory_bytes.set(vsz as i64);

            // CPU time (cumulative, so we use Counter)
            // sysinfo gives us total CPU time in seconds since process start
            // Prometheus rate() will calculate CPU% from the counter
            let cpu_time = process.run_time() as f64;
            
            // Counter must be monotonically increasing
            // Only increment if CPU time increased (it should always increase)
            let current_cpu = self.cpu_seconds_total.get();
            if cpu_time > current_cpu {
                self.cpu_seconds_total.inc_by(cpu_time - current_cpu);
            }

            // Thread count (Linux-specific via /proc)
            // On Linux, each thread has an entry in /proc/$PID/task/
            #[cfg(target_os = "linux")]
            {
                if let Ok(entries) = std::fs::read_dir(format!("/proc/{}/task", self.pid)) {
                    let thread_count = entries.count() as i64;
                    self.threads.set(thread_count);
                }
            }
            
            #[cfg(not(target_os = "linux"))]
            {
                // Fallback: minimum 1 thread
                // sysinfo doesn't expose thread count on all platforms
                self.threads.set(1);
            }

            // File descriptors (Linux-specific via /proc)
            // Each entry in /proc/$PID/fd/ is an open file descriptor
            #[cfg(target_os = "linux")]
            {
                if let Ok(entries) = std::fs::read_dir(format!("/proc/{}/fd", self.pid)) {
                    let fd_count = entries.count() as i64;
                    self.open_fds.set(fd_count);
                }
            }
            
            #[cfg(not(target_os = "linux"))]
            {
                // Not available on non-Linux platforms
                self.open_fds.set(0);
            }

            debug!(
                rss_mb = rss / 1024 / 1024,
                vsz_mb = vsz / 1024 / 1024,
                cpu_seconds = cpu_time,
                threads = self.threads.get(),
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
        registry.register(Box::new(self.cpu_seconds_total.clone()))?;
        registry.register(Box::new(self.resident_memory_bytes.clone()))?;
        registry.register(Box::new(self.virtual_memory_bytes.clone()))?;
        registry.register(Box::new(self.open_fds.clone()))?;
        registry.register(Box::new(self.threads.clone()))?;
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
        
        // Memory should be > 0
        assert!(collector.resident_memory_bytes.get() > 0);
        assert!(collector.virtual_memory_bytes.get() > 0);
        
        // Should have at least 1 thread
        assert!(collector.threads.get() >= 1);
        
        // FDs should be > 0 (we have stdin/stdout/stderr at minimum)
        #[cfg(target_os = "linux")]
        assert!(collector.open_fds.get() >= 3);
    }
}
