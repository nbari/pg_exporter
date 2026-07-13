//! Host resource usage for the `PostgreSQL` process group.
//!
//! Aggregates CPU and memory for every OS process whose name starts with
//! `postgres` (the postmaster plus all backends, background workers, autovacuum,
//! checkpointer, WAL writer, ...) into a single low-cardinality series labeled
//! `group="postgres"`. This answers a question the host-wide panels cannot: *is
//! `PostgreSQL` itself eating the box, or is it a co-located neighbour?*
//!
//! - **CPU** is a cumulative counter, `pg_system_process_group_cpu_seconds_total`
//!   (`utime + stime`). It is built by accumulating per-PID deltas so process
//!   churn (backends coming and going) never makes the group counter go
//!   backwards; use `rate()` to get "cores consumed by `PostgreSQL`".
//! - **Memory** is `pg_system_process_group_memory_bytes`. On Linux this is
//!   **PSS** (proportional set size, from `/proc/<pid>/smaps_rollup`), which
//!   divides shared pages such as `shared_buffers` proportionally across the
//!   backends touching them — so summing across backends does **not**
//!   double-count shared memory the way RSS would. PSS requires the exporter to
//!   run as the `postgres` user or root; when a process is not readable it falls
//!   back to that process's RSS. On FreeBSD there is no cheap PSS, so this is the
//!   summed **RSS** and therefore over-counts shared memory (documented caveat).
//! - **Count** is `pg_system_process_group_count`, the number of matched
//!   processes.
//!
//! Like the rest of `--collector.system` this only makes sense when the exporter
//! is co-located with `PostgreSQL` and never touches the database.

use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{CounterVec, IntGaugeVec, Opts, Registry};
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use tracing::{debug, instrument, warn};

#[cfg(target_os = "freebsd")]
use sysinfo::System;

#[cfg(any(target_os = "linux", target_os = "freebsd"))]
use super::cpu::ticks_to_seconds;

/// Process-name prefix that defines the group, and the value of the `group`
/// label. `PostgreSQL` sets every backend's `comm` to `postgres`.
const GROUP: &str = "postgres";

/// Whether per-process sampling is implemented for the current platform.
#[cfg(any(target_os = "linux", target_os = "freebsd"))]
const SUPPORTED: bool = true;
#[cfg(not(any(target_os = "linux", target_os = "freebsd")))]
const SUPPORTED: bool = false;

/// Converts a `u64` byte count into the `i64` a Prometheus `IntGauge` stores,
/// saturating instead of wrapping on the (practically impossible) overflow.
#[inline]
fn to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// One sampled process: its PID, cumulative CPU seconds, and resident bytes.
struct ProcSample {
    pid: u32,
    cpu_seconds: f64,
    mem_bytes: u64,
}

/// Parses the summed `utime + stime` clock ticks from a `/proc/<pid>/stat` line.
///
/// The `comm` field (2) is wrapped in parentheses and may itself contain spaces
/// or parentheses, so fields are read after the **last** `)`: the first token
/// after it is `state` (field 3), making `utime` (field 14) index 11 and `stime`
/// (field 15) index 12.
#[cfg(target_os = "linux")]
fn parse_stat_cpu_ticks(stat: &str) -> Option<u64> {
    let rparen = stat.rfind(')')?;
    let rest = stat.get(rparen + 1..)?;
    let fields: Vec<&str> = rest.split_whitespace().collect();
    let utime = fields.get(11)?.parse::<u64>().ok()?;
    let stime = fields.get(12)?.parse::<u64>().ok()?;
    Some(utime.saturating_add(stime))
}

/// Extracts the `Pss:` value (in kB) from a `/proc/<pid>/smaps_rollup` dump.
#[cfg(target_os = "linux")]
fn parse_pss_kb(smaps_rollup: &str) -> Option<u64> {
    smaps_rollup
        .lines()
        .find_map(|line| line.strip_prefix("Pss:"))
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|value| value.parse::<u64>().ok())
}

/// Extracts resident pages (field 2) from a `/proc/<pid>/statm` line.
#[cfg(target_os = "linux")]
fn parse_statm_resident_pages(statm: &str) -> Option<u64> {
    statm.split_whitespace().nth(1)?.parse::<u64>().ok()
}

/// Returns the clock-tick frequency (`_SC_CLK_TCK`) used to scale `/proc` CPU
/// counters, defaulting to the near-universal 100 Hz.
#[cfg(target_os = "linux")]
fn clk_tck() -> f64 {
    // SAFETY: `sysconf` is a pure, thread-safe query with no side effects.
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    u32::try_from(ticks).map_or(100.0, f64::from)
}

/// Returns the system page size in bytes, defaulting to 4096.
#[cfg(target_os = "linux")]
fn page_size() -> u64 {
    // SAFETY: `sysconf` is a pure, thread-safe query with no side effects.
    let size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    u64::try_from(size).unwrap_or(4096)
}

/// Reads PSS (bytes) for one PID, or `None` when `smaps_rollup` is unavailable
/// (older kernels) or unreadable (insufficient privileges for that process).
#[cfg(target_os = "linux")]
fn read_pss_bytes(pid: u32) -> Option<u64> {
    let content = std::fs::read_to_string(format!("/proc/{pid}/smaps_rollup")).ok()?;
    parse_pss_kb(&content).map(|kb| kb.saturating_mul(1024))
}

/// Reads RSS (bytes) for one PID from the world-readable `statm`, the fallback
/// when PSS is not available.
#[cfg(target_os = "linux")]
fn read_rss_bytes(pid: u32, page_size: u64) -> Option<u64> {
    let content = std::fs::read_to_string(format!("/proc/{pid}/statm")).ok()?;
    parse_statm_resident_pages(&content).map(|pages| pages.saturating_mul(page_size))
}

/// Samples every `postgres*` process on Linux by reading `/proc` directly.
#[cfg(target_os = "linux")]
fn sample_processes(prefix: &str) -> Vec<ProcSample> {
    let hz = clk_tck();
    let bytes_per_page = page_size();
    let mut out = Vec::new();

    let Ok(entries) = std::fs::read_dir("/proc") else {
        return out;
    };

    for entry in entries.flatten() {
        let file_name = entry.file_name();
        let Some(pid) = file_name.to_str().and_then(|name| name.parse::<u32>().ok()) else {
            continue;
        };

        let Ok(comm) = std::fs::read_to_string(format!("/proc/{pid}/comm")) else {
            continue;
        };
        if !comm.trim_end().to_ascii_lowercase().starts_with(prefix) {
            continue;
        }

        let cpu_seconds = std::fs::read_to_string(format!("/proc/{pid}/stat"))
            .ok()
            .and_then(|stat| parse_stat_cpu_ticks(&stat))
            .map_or(0.0, |ticks| ticks_to_seconds(ticks, hz));

        let mem_bytes =
            read_pss_bytes(pid).or_else(|| read_rss_bytes(pid, bytes_per_page)).unwrap_or(0);

        out.push(ProcSample { pid, cpu_seconds, mem_bytes });
    }

    out
}

/// Samples every `postgres*` process on FreeBSD via `sysinfo`. There is no cheap
/// PSS, so memory is RSS (`Process::memory`), which over-counts shared memory.
#[cfg(target_os = "freebsd")]
fn sample_processes(system: &Mutex<System>, prefix: &str) -> Vec<ProcSample> {
    use sysinfo::{ProcessRefreshKind, ProcessesToUpdate};

    let mut system = match system.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            warn!("system process mutex was poisoned, recovering");
            poisoned.into_inner()
        }
    };

    system.refresh_processes_specifics(
        ProcessesToUpdate::All,
        true,
        ProcessRefreshKind::nothing().with_memory().with_cpu(),
    );

    let mut out = Vec::new();
    for (pid, process) in system.processes() {
        let name = process.name().to_string_lossy().to_ascii_lowercase();
        if !name.starts_with(prefix) {
            continue;
        }
        out.push(ProcSample {
            pid: pid.as_u32(),
            // accumulated_cpu_time() is in CPU-milliseconds.
            cpu_seconds: ticks_to_seconds(process.accumulated_cpu_time(), 1000.0),
            mem_bytes: process.memory(),
        });
    }

    out
}

/// Aggregate host CPU and memory for the `postgres` process group.
///
/// **Metrics (labeled `group="postgres"`):**
/// - `pg_system_process_group_cpu_seconds_total` (counter, seconds)
/// - `pg_system_process_group_memory_bytes` (gauge; PSS on Linux, RSS on FreeBSD)
/// - `pg_system_process_group_count` (gauge)
#[derive(Clone)]
pub struct ProcessGroupCollector {
    cpu_seconds: CounterVec,
    memory_bytes: IntGaugeVec,
    proc_count: IntGaugeVec,
    /// Last observed cumulative CPU seconds per live PID, used to accumulate a
    /// monotonic group counter across process churn.
    prev_cpu: Arc<Mutex<HashMap<u32, f64>>>,
    /// Persistent `sysinfo` state for FreeBSD sampling (unused on Linux, which
    /// reads `/proc` directly).
    #[cfg(target_os = "freebsd")]
    system: Arc<Mutex<System>>,
    /// Ensures the "unsupported platform" warning is logged at most once.
    unsupported_warned: Arc<AtomicBool>,
}

impl Default for ProcessGroupCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcessGroupCollector {
    /// Creates a new `ProcessGroupCollector`.
    ///
    /// # Panics
    ///
    /// Panics if metric creation fails, which only happens with an invalid
    /// metric name or label set and therefore never at runtime.
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        let cpu_seconds = CounterVec::new(
            Opts::new(
                "pg_system_process_group_cpu_seconds_total",
                "Cumulative CPU time in seconds (user + system) consumed by host processes in the \
                 group, since the exporter started tracking; use rate() for cores consumed",
            ),
            &["group"],
        )
        .expect("pg_system_process_group_cpu_seconds_total");

        let memory_bytes = IntGaugeVec::new(
            Opts::new(
                "pg_system_process_group_memory_bytes",
                "Resident memory of the host process group in bytes (Linux: PSS, so shared_buffers \
                 is not double-counted; FreeBSD: summed RSS, which over-counts shared memory)",
            ),
            &["group"],
        )
        .expect("pg_system_process_group_memory_bytes");

        let proc_count = IntGaugeVec::new(
            Opts::new(
                "pg_system_process_group_count",
                "Number of host processes matched in the group",
            ),
            &["group"],
        )
        .expect("pg_system_process_group_count");

        Self {
            cpu_seconds,
            memory_bytes,
            proc_count,
            prev_cpu: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(target_os = "freebsd")]
            system: Arc::new(Mutex::new(System::new())),
            unsupported_warned: Arc::new(AtomicBool::new(false)),
        }
    }

    fn collect_stats(&self) {
        if !SUPPORTED {
            if !self.unsupported_warned.swap(true, Ordering::Relaxed) {
                warn!(
                    "collector.system process-group metrics are not supported on this platform \
                     (Linux/FreeBSD only)"
                );
            }
            return;
        }

        #[cfg(target_os = "linux")]
        let samples = sample_processes(GROUP);
        #[cfg(target_os = "freebsd")]
        let samples = sample_processes(&self.system, GROUP);
        #[cfg(not(any(target_os = "linux", target_os = "freebsd")))]
        let samples: Vec<ProcSample> = Vec::new();

        let mut prev = match self.prev_cpu.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("process-group cpu mutex was poisoned, recovering");
                poisoned.into_inner()
            }
        };

        let mut delta_total = 0.0_f64;
        let mut mem_total = 0_u64;
        let mut current = HashMap::with_capacity(samples.len());

        for sample in &samples {
            // Only positive deltas count: a missing PID (exited) simply stops
            // contributing, and a reused PID with a lower total is treated as a
            // reset (new baseline), so the group counter never decreases.
            if let Some(&previous) = prev.get(&sample.pid)
                && sample.cpu_seconds >= previous
            {
                delta_total += sample.cpu_seconds - previous;
            }
            mem_total = mem_total.saturating_add(sample.mem_bytes);
            current.insert(sample.pid, sample.cpu_seconds);
        }

        let count = i64::try_from(samples.len()).unwrap_or(i64::MAX);
        *prev = current;
        drop(prev);

        if delta_total > 0.0 {
            self.cpu_seconds.with_label_values(&[GROUP]).inc_by(delta_total);
        }
        self.memory_bytes.with_label_values(&[GROUP]).set(to_i64(mem_total));
        self.proc_count.with_label_values(&[GROUP]).set(count);

        debug!(count, mem_bytes = mem_total, "updated postgres process-group metrics");
    }
}

impl Collector for ProcessGroupCollector {
    fn name(&self) -> &'static str {
        "system.process"
    }

    #[instrument(skip(self, registry), level = "info", err, fields(collector = "system.process"))]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.cpu_seconds.clone()))?;
        registry.register(Box::new(self.memory_bytes.clone()))?;
        registry.register(Box::new(self.proc_count.clone()))?;
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
    fn collector_name_is_system_process() {
        assert_eq!(ProcessGroupCollector::new().name(), "system.process");
    }

    #[test]
    fn collector_is_disabled_by_default() {
        assert!(!ProcessGroupCollector::new().enabled_by_default());
    }

    #[test]
    fn register_metrics_succeeds() {
        let registry = Registry::new();
        assert!(ProcessGroupCollector::new().register_metrics(&registry).is_ok());
    }

    #[test]
    fn collect_stats_runs_without_panic() {
        let collector = ProcessGroupCollector::new();
        // Two passes: the first establishes per-PID baselines, the second may
        // accumulate a CPU delta. Neither may panic regardless of what is running.
        collector.collect_stats();
        collector.collect_stats();

        #[cfg(any(target_os = "linux", target_os = "freebsd"))]
        {
            let count = collector.proc_count.with_label_values(&[GROUP]).get();
            assert!(count >= 0, "process count must be non-negative");
            let mem = collector.memory_bytes.with_label_values(&[GROUP]).get();
            assert!(mem >= 0, "memory bytes must be non-negative");
        }
    }

    #[test]
    fn cpu_counter_is_monotonic_across_collections() {
        let collector = ProcessGroupCollector::new();
        let mut last = 0.0;
        for _ in 0..3 {
            collector.collect_stats();
            let value = collector.cpu_seconds.with_label_values(&[GROUP]).get();
            assert!(value >= last, "cpu seconds counter must never decrease");
            last = value;
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn parse_stat_cpu_ticks_handles_paren_in_comm() {
        // comm "(pg pool)" contains a space and parens; utime=100 stime=50 -> 150.
        let stat = "1234 (pg pool) S 1 1 1 0 -1 0 0 0 0 0 100 50 0 0 20 0 1 0 0";
        assert_eq!(parse_stat_cpu_ticks(stat), Some(150));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn parse_stat_cpu_ticks_rejects_truncated_line() {
        assert_eq!(parse_stat_cpu_ticks("1234 (postgres) S 1 1"), None);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn parse_pss_kb_reads_pss_line() {
        let rollup = "Rss:               2048 kB\nPss:               1024 kB\nShared_Clean: 512 kB\n";
        assert_eq!(parse_pss_kb(rollup), Some(1024));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn parse_pss_kb_returns_none_without_pss() {
        assert_eq!(parse_pss_kb("Rss: 2048 kB\n"), None);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn parse_statm_resident_pages_reads_second_field() {
        // size resident shared text lib data dt
        assert_eq!(parse_statm_resident_pages("1000 256 128 4 0 512 0"), Some(256));
    }
}
