//! Host CPU counters, core counts, and load average.
//!
//! CPU time is exposed as cumulative **per-core seconds counters**, mirroring
//! `node_exporter`'s `node_cpu_seconds_total{cpu,mode}`. A single metric,
//! `pg_system_cpu_seconds_total{cpu,mode}`, carries one series per logical core
//! and mode; aggregate host utilization is derived in `PromQL`, so there is no
//! separate aggregate metric and no flag to configure. Counters are
//! scrape-interval independent and restart-safe: compute utilization with
//! `rate()`/`irate()` over whatever window you choose, e.g. host-wide busy
//! fraction:
//!
//! ```promql
//! 1 - avg without(cpu)(rate(pg_system_cpu_seconds_total{mode="idle"}[5m]))
//! ```
//!
//! or per-mode busy fraction normalized across all cores:
//!
//! ```promql
//! sum without(cpu)(rate(pg_system_cpu_seconds_total{mode!="idle"}[5m]))
//!   / on() group_left() pg_system_cpu_cores
//! ```
//!
//! The counters are read directly from the OS because `sysinfo` only exposes an
//! instantaneous CPU percentage, not cumulative per-mode counters:
//!
//! - **Linux**: parses the per-core lines of `/proc/stat`. Modes: `user`,
//!   `nice`, `system`, `idle`, `iowait`, `irq`, `softirq`, `steal`.
//! - **FreeBSD**: reads the `kern.cp_times` (per-core) sysctl. Modes: `user`,
//!   `nice`, `system`, `interrupt`, `idle`.
//! - **Other platforms**: CPU counters are skipped (a one-time warning is
//!   logged); memory and load average are still exported by the sibling
//!   collectors.
//!
//! Cardinality is bounded per host (modes × cores) and does not scale with the
//! number of databases. Core counts (`pg_system_cpu_cores`,
//! `pg_system_cpu_cores_physical`) let dashboards normalize load per core (a load
//! of 8 saturates 1 core but is ~25% of 32 cores). Load average
//! (`pg_system_load1/5/15`) comes from `sysinfo`.

use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{Gauge, GaugeVec, IntGauge, Opts, Registry};
use sqlx::PgPool;
use std::num::NonZeroUsize;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use sysinfo::System;
use tracing::{debug, instrument, warn};

/// Cumulative CPU time per mode for one logical core.
struct CoreTimes {
    /// Logical core id used as the `cpu` label value: `"0"`, `"1"`, ...
    cpu: String,
    /// `(mode, seconds)` pairs. `seconds` is cumulative since boot.
    modes: Vec<(&'static str, f64)>,
}

/// Converts raw clock ticks to seconds.
///
/// The cast is unavoidable and safe in practice: tick counts since boot stay far
/// below `2^53`, so `f64` represents them exactly for any realistic uptime.
#[cfg(any(target_os = "linux", target_os = "freebsd"))]
#[allow(clippy::cast_precision_loss)]
#[inline]
pub(super) fn ticks_to_seconds(ticks: u64, hz: f64) -> f64 {
    if hz > 0.0 {
        ticks as f64 / hz
    } else {
        0.0
    }
}

#[cfg(target_os = "linux")]
const LINUX_MODES: [&str; 8] = [
    "user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal",
];

#[cfg(target_os = "freebsd")]
const FREEBSD_MODES: [&str; 5] = ["user", "nice", "system", "interrupt", "idle"];

/// Returns the clock-tick frequency (`_SC_CLK_TCK`, jiffies per second) used to
/// scale `/proc/stat` counters, defaulting to the near-universal 100 Hz.
#[cfg(target_os = "linux")]
#[allow(clippy::cast_precision_loss)]
fn clk_tck() -> f64 {
    // SAFETY: `sysconf` is a pure, thread-safe query with no side effects.
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if ticks > 0 { ticks as f64 } else { 100.0 }
}

/// Parses the per-core CPU lines of `/proc/stat` into cumulative times.
///
/// The bare `cpu` aggregate line is skipped: only per-core series are exported,
/// and the host-wide aggregate is derived in `PromQL`. Kept pure (string in,
/// values out) so it is unit-testable without `/proc`.
#[cfg(target_os = "linux")]
fn parse_proc_stat(content: &str, hz: f64) -> Vec<CoreTimes> {
    let mut out = Vec::new();

    for line in content.lines() {
        let mut fields = line.split_whitespace();
        let Some(label) = fields.next() else {
            continue;
        };
        let Some(suffix) = label.strip_prefix("cpu") else {
            // CPU lines are the first entries in /proc/stat; stop at the first
            // non-cpu line (intr, ctxt, ...).
            break;
        };
        if suffix.is_empty() {
            // The bare `cpu` line is the host-wide aggregate; skip it.
            continue;
        }

        let mut modes = Vec::with_capacity(LINUX_MODES.len());
        for &name in &LINUX_MODES {
            let jiffies = fields
                .next()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(0);
            modes.push((name, ticks_to_seconds(jiffies, hz)));
        }

        out.push(CoreTimes {
            cpu: suffix.to_owned(),
            modes,
        });
    }

    out
}

#[cfg(target_os = "linux")]
fn read_cpu_times() -> Result<Vec<CoreTimes>> {
    let hz = clk_tck();
    let content = std::fs::read_to_string("/proc/stat")?;
    Ok(parse_proc_stat(&content, hz))
}

#[cfg(target_os = "freebsd")]
#[repr(C)]
struct Clockinfo {
    hz: i32,
    tick: i32,
    spare: i32,
    stathz: i32,
    profhz: i32,
}

/// Reads a raw sysctl value by name into a byte buffer.
#[cfg(target_os = "freebsd")]
fn sysctl_raw(name: &str) -> Result<Vec<u8>> {
    use anyhow::anyhow;
    use std::ffi::CString;

    let cname = CString::new(name)?;
    let mut len: libc::size_t = 0;

    // SAFETY: passing a null oldp asks the kernel for the value's size only.
    let rc = unsafe {
        libc::sysctlbyname(
            cname.as_ptr(),
            std::ptr::null_mut(),
            &raw mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if rc != 0 {
        return Err(anyhow!("sysctlbyname({name}) size query failed"));
    }

    let mut buf = vec![0u8; len];
    // SAFETY: buf has capacity `len` bytes, matching the size query above.
    let rc = unsafe {
        libc::sysctlbyname(
            cname.as_ptr(),
            buf.as_mut_ptr().cast(),
            &raw mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if rc != 0 {
        return Err(anyhow!("sysctlbyname({name}) read failed"));
    }

    buf.truncate(len);
    Ok(buf)
}

/// Reads a sysctl whose value is an array of C `long` counters.
#[cfg(target_os = "freebsd")]
fn sysctl_longs(name: &str) -> Result<Vec<i64>> {
    let raw = sysctl_raw(name)?;
    let width = std::mem::size_of::<libc::c_long>();
    let mut out = Vec::with_capacity(raw.len() / width.max(1));

    for chunk in raw.chunks_exact(width) {
        let mut bytes = [0u8; 8];
        for (dst, src) in bytes.iter_mut().zip(chunk.iter()) {
            *dst = *src;
        }
        out.push(i64::from_ne_bytes(bytes));
    }

    Ok(out)
}

/// Returns the frequency (Hz) at which `kern.cp_time*` counters advance,
/// preferring the statistics clock (`stathz`) and falling back to `hz`.
#[cfg(target_os = "freebsd")]
fn freebsd_cpufreq() -> Result<f64> {
    use anyhow::anyhow;

    let raw = sysctl_raw("kern.clockrate")?;
    if raw.len() < std::mem::size_of::<Clockinfo>() {
        return Err(anyhow!(
            "kern.clockrate returned {} bytes, expected at least {}",
            raw.len(),
            std::mem::size_of::<Clockinfo>()
        ));
    }

    // SAFETY: the buffer is at least sizeof(Clockinfo) bytes and Clockinfo is a
    // repr(C) plain-old-data struct; read_unaligned avoids alignment issues.
    let clock: Clockinfo = unsafe { std::ptr::read_unaligned(raw.as_ptr().cast()) };
    let freq = if clock.stathz > 0 {
        clock.stathz
    } else {
        clock.hz
    };

    if freq > 0 {
        Ok(f64::from(freq))
    } else {
        Ok(128.0)
    }
}

#[cfg(target_os = "freebsd")]
fn build_core_times(cpu: String, vals: &[i64], hz: f64) -> CoreTimes {
    let mut modes = Vec::with_capacity(FREEBSD_MODES.len());
    for (idx, &name) in FREEBSD_MODES.iter().enumerate() {
        let raw = vals.get(idx).copied().unwrap_or(0);
        let ticks = u64::try_from(raw).unwrap_or(0);
        modes.push((name, ticks_to_seconds(ticks, hz)));
    }
    CoreTimes { cpu, modes }
}

#[cfg(target_os = "freebsd")]
fn read_cpu_times() -> Result<Vec<CoreTimes>> {
    let hz = freebsd_cpufreq()?;
    let mut out = Vec::new();

    // Only per-core counters (kern.cp_times) are exported; the host-wide
    // aggregate is derived in PromQL.
    let per = sysctl_longs("kern.cp_times")?;
    for (index, chunk) in per.chunks(FREEBSD_MODES.len()).enumerate() {
        out.push(build_core_times(index.to_string(), chunk, hz));
    }

    Ok(out)
}

#[cfg(not(any(target_os = "linux", target_os = "freebsd")))]
fn read_cpu_times() -> Result<Vec<CoreTimes>> {
    // CPU counters are only implemented for Linux and FreeBSD. The caller logs a
    // one-time warning; memory and load average are still exported.
    Ok(Vec::new())
}

/// Exposes host CPU counters, core counts, and load average.
///
/// **Counter (`Gauge`, cumulative seconds):**
/// - `pg_system_cpu_seconds_total{cpu,mode}` — one series per logical core,
///   mirroring `node_exporter`'s `node_cpu_seconds_total`
///
/// **Gauges:**
/// - `pg_system_cpu_cores` (logical cores)
/// - `pg_system_cpu_cores_physical` (physical cores)
/// - `pg_system_load1`, `pg_system_load5`, `pg_system_load15`
#[derive(Clone)]
pub struct CpuCollector {
    cpu_seconds: GaugeVec,
    cpu_cores: IntGauge,
    cpu_cores_physical: IntGauge,
    load1: Gauge,
    load5: Gauge,
    load15: Gauge,
    /// Ensures the "CPU counters unsupported on this platform" warning is logged
    /// at most once per process instead of on every scrape.
    unsupported_warned: Arc<AtomicBool>,
}

impl CpuCollector {
    /// Creates a new `CpuCollector`.
    ///
    /// # Panics
    ///
    /// Panics if metric creation fails, which only happens with an invalid
    /// metric name or label set and therefore never at runtime.
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        Self {
            cpu_seconds: GaugeVec::new(
                Opts::new(
                    "pg_system_cpu_seconds_total",
                    "Cumulative host CPU time in seconds per logical core, by mode",
                ),
                &["cpu", "mode"],
            )
            .expect("pg_system_cpu_seconds_total"),
            cpu_cores: IntGauge::with_opts(Opts::new(
                "pg_system_cpu_cores",
                "Number of logical CPU cores on the host",
            ))
            .expect("pg_system_cpu_cores"),
            cpu_cores_physical: IntGauge::with_opts(Opts::new(
                "pg_system_cpu_cores_physical",
                "Number of physical CPU cores on the host",
            ))
            .expect("pg_system_cpu_cores_physical"),
            load1: Gauge::with_opts(Opts::new(
                "pg_system_load1",
                "Host load average over the last 1 minute",
            ))
            .expect("pg_system_load1"),
            load5: Gauge::with_opts(Opts::new(
                "pg_system_load5",
                "Host load average over the last 5 minutes",
            ))
            .expect("pg_system_load5"),
            load15: Gauge::with_opts(Opts::new(
                "pg_system_load15",
                "Host load average over the last 15 minutes",
            ))
            .expect("pg_system_load15"),
            unsupported_warned: Arc::new(AtomicBool::new(false)),
        }
    }

    fn update_cores(&self) {
        let logical = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
        let physical = System::physical_core_count().unwrap_or(logical);
        self.cpu_cores.set(i64::try_from(logical).unwrap_or(0));
        self.cpu_cores_physical
            .set(i64::try_from(physical).unwrap_or(0));
    }

    fn update_load(&self) {
        let load = System::load_average();
        self.load1.set(load.one);
        self.load5.set(load.five);
        self.load15.set(load.fifteen);
    }

    fn update_cpu_seconds(&self) {
        match read_cpu_times() {
            Ok(times) if !times.is_empty() => {
                self.cpu_seconds.reset();

                for entry in &times {
                    for &(mode, seconds) in &entry.modes {
                        self.cpu_seconds
                            .with_label_values(&[entry.cpu.as_str(), mode])
                            .set(seconds);
                    }
                }

                debug!(cores = times.len(), "updated host CPU counters");
            }
            Ok(_) => {
                if !self.unsupported_warned.swap(true, Ordering::Relaxed) {
                    warn!(
                        "collector.system CPU counters are not supported on this platform; \
                         only memory and load average will be exported"
                    );
                }
            }
            Err(ref error) => {
                warn!(error = %error, "failed to read host CPU counters");
            }
        }
    }

    fn collect_stats(&self) {
        self.update_load();
        self.update_cores();
        self.update_cpu_seconds();
    }
}

impl Default for CpuCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl Collector for CpuCollector {
    fn name(&self) -> &'static str {
        "system.cpu"
    }

    #[instrument(skip(self, registry), level = "info", err, fields(collector = "system.cpu"))]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.cpu_seconds.clone()))?;
        registry.register(Box::new(self.cpu_cores.clone()))?;
        registry.register(Box::new(self.cpu_cores_physical.clone()))?;
        registry.register(Box::new(self.load1.clone()))?;
        registry.register(Box::new(self.load5.clone()))?;
        registry.register(Box::new(self.load15.clone()))?;
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
    fn collector_name_is_system_cpu() {
        assert_eq!(CpuCollector::new().name(), "system.cpu");
    }

    #[test]
    fn collector_is_disabled_by_default() {
        assert!(!CpuCollector::new().enabled_by_default());
    }

    #[test]
    fn register_metrics_succeeds() {
        let registry = Registry::new();
        assert!(CpuCollector::new().register_metrics(&registry).is_ok());
    }

    #[cfg(any(target_os = "linux", target_os = "freebsd"))]
    #[test]
    fn ticks_to_seconds_guards_zero_hz() {
        assert!((ticks_to_seconds(1000, 0.0) - 0.0).abs() < f64::EPSILON);
        assert!((ticks_to_seconds(1000, 100.0) - 10.0).abs() < f64::EPSILON);
    }

    #[test]
    fn collect_stats_populates_load_and_cores() {
        let collector = CpuCollector::new();
        collector.collect_stats();

        assert!(collector.cpu_cores.get() >= 1);
        assert!(collector.cpu_cores_physical.get() >= 1);
        assert!(collector.load1.get() >= 0.0);
        assert!(collector.load5.get() >= 0.0);
        assert!(collector.load15.get() >= 0.0);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn collect_stats_populates_cpu_seconds_on_linux() {
        let collector = CpuCollector::new();
        collector.collect_stats();

        // Core 0 always exists on Linux; idle seconds are cumulative since boot.
        let idle = collector
            .cpu_seconds
            .with_label_values(&["0", "idle"])
            .get();
        assert!(idle > 0.0, "expected non-zero cumulative idle seconds");
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[allow(clippy::expect_used)]
    fn parse_proc_stat_skips_aggregate_and_reads_cores() {
        let sample = "\
cpu  100 200 300 400 500 60 70 80 0 0
cpu0 50 100 150 200 250 30 35 40 0 0
cpu1 50 100 150 200 250 30 35 40 0 0
intr 12345 0 0
ctxt 67890
";
        let times = parse_proc_stat(sample, 100.0);
        assert_eq!(times.len(), 2, "aggregate skipped, two cores kept");

        let core_ids: Vec<&str> = times.iter().map(|entry| entry.cpu.as_str()).collect();
        assert_eq!(core_ids, vec!["0", "1"]);

        let first_core = times.first().expect("core 0 entry");
        assert_eq!(first_core.modes.len(), LINUX_MODES.len());

        // user = 50 jiffies / 100 Hz = 0.5s; idle = 200 / 100 = 2.0s.
        let user = first_core
            .modes
            .iter()
            .find(|(mode, _)| *mode == "user")
            .map_or(-1.0, |(_, seconds)| *seconds);
        let idle = first_core
            .modes
            .iter()
            .find(|(mode, _)| *mode == "idle")
            .map_or(-1.0, |(_, seconds)| *seconds);
        assert!((user - 0.5).abs() < f64::EPSILON);
        assert!((idle - 2.0).abs() < f64::EPSILON);
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[allow(clippy::expect_used)]
    fn parse_proc_stat_tolerates_missing_fields() {
        // Truncated per-core line: missing modes must default to zero, not panic.
        let sample = "cpu0 100 200\n";
        let times = parse_proc_stat(sample, 100.0);
        let core0 = times.first().expect("core 0 entry");
        assert_eq!(core0.modes.len(), LINUX_MODES.len());

        let steal = core0
            .modes
            .iter()
            .find(|(mode, _)| *mode == "steal")
            .map_or(-1.0, |(_, seconds)| *seconds);
        assert!((steal - 0.0).abs() < f64::EPSILON);
    }
}
