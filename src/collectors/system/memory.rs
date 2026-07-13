//! Host memory and swap gauges.
//!
//! Values come from `sysinfo` and are exposed as raw building blocks (total,
//! available, free, used, and the swap equivalents) rather than a single
//! pre-computed "used %". This mirrors `node_exporter`, letting dashboards derive
//! utilization however they prefer, typically:
//!
//! ```promql
//! (pg_system_memory_total_bytes - pg_system_memory_available_bytes)
//!     / pg_system_memory_total_bytes
//! ```
//!
//! # Platform note
//!
//! On Linux, `available` is the kernel's `MemAvailable` (an estimate of memory
//! obtainable without swapping, so it counts reclaimable page cache). On FreeBSD
//! (and Windows) `sysinfo` reports `available == free`, which understates truly
//! reclaimable memory; there, prefer `used`/`free` and treat the `available`
//! series as a conservative floor.

use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{IntGauge, Opts, Registry};
use sqlx::PgPool;
use std::sync::{Arc, Mutex};
use sysinfo::{MemoryRefreshKind, System};
use tracing::{debug, instrument, warn};

/// Converts a `u64` byte count into the `i64` a Prometheus `IntGauge` stores,
/// saturating instead of wrapping on the (practically impossible) overflow.
#[inline]
fn to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// Exposes host memory and swap usage in bytes.
///
/// **Gauges (bytes):**
/// - `pg_system_memory_total_bytes`
/// - `pg_system_memory_available_bytes`
/// - `pg_system_memory_free_bytes`
/// - `pg_system_memory_used_bytes`
/// - `pg_system_swap_total_bytes`
/// - `pg_system_swap_used_bytes`
/// - `pg_system_swap_free_bytes`
#[derive(Clone)]
pub struct MemoryCollector {
    total: IntGauge,
    available: IntGauge,
    free: IntGauge,
    used: IntGauge,
    swap_total: IntGauge,
    swap_used: IntGauge,
    swap_free: IntGauge,
    system: Arc<Mutex<System>>,
}

impl Default for MemoryCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryCollector {
    /// Creates a new `MemoryCollector` with all gauges initialized.
    ///
    /// # Panics
    ///
    /// Panics if metric creation fails, which only happens with an invalid
    /// metric name and therefore never at runtime.
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        let total = IntGauge::with_opts(Opts::new(
            "pg_system_memory_total_bytes",
            "Total physical memory on the host in bytes",
        ))
        .expect("pg_system_memory_total_bytes");

        let available = IntGauge::with_opts(Opts::new(
            "pg_system_memory_available_bytes",
            "Memory available for new allocations without swapping in bytes \
             (equals free memory on FreeBSD/Windows)",
        ))
        .expect("pg_system_memory_available_bytes");

        let free = IntGauge::with_opts(Opts::new(
            "pg_system_memory_free_bytes",
            "Unused physical memory on the host in bytes",
        ))
        .expect("pg_system_memory_free_bytes");

        let used = IntGauge::with_opts(Opts::new(
            "pg_system_memory_used_bytes",
            "Used physical memory on the host in bytes",
        ))
        .expect("pg_system_memory_used_bytes");

        let swap_total = IntGauge::with_opts(Opts::new(
            "pg_system_swap_total_bytes",
            "Total swap space on the host in bytes",
        ))
        .expect("pg_system_swap_total_bytes");

        let swap_used = IntGauge::with_opts(Opts::new(
            "pg_system_swap_used_bytes",
            "Used swap space on the host in bytes",
        ))
        .expect("pg_system_swap_used_bytes");

        let swap_free = IntGauge::with_opts(Opts::new(
            "pg_system_swap_free_bytes",
            "Free swap space on the host in bytes",
        ))
        .expect("pg_system_swap_free_bytes");

        // System::new() starts empty; only memory is ever refreshed, so no
        // process/CPU state is cached (avoiding the /proc FD growth that full
        // process scans can cause on Linux).
        let system = Arc::new(Mutex::new(System::new()));

        Self {
            total,
            available,
            free,
            used,
            swap_total,
            swap_used,
            swap_free,
            system,
        }
    }

    fn collect_stats(&self) {
        let mut system = match self.system.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("system memory mutex was poisoned, recovering");
                poisoned.into_inner()
            }
        };

        system.refresh_memory_specifics(MemoryRefreshKind::nothing().with_ram().with_swap());

        self.total.set(to_i64(system.total_memory()));
        self.available.set(to_i64(system.available_memory()));
        self.free.set(to_i64(system.free_memory()));
        self.used.set(to_i64(system.used_memory()));
        self.swap_total.set(to_i64(system.total_swap()));
        self.swap_used.set(to_i64(system.used_swap()));
        self.swap_free.set(to_i64(system.free_swap()));

        debug!(
            total_mb = system.total_memory() / 1024 / 1024,
            used_mb = system.used_memory() / 1024 / 1024,
            "updated host memory metrics"
        );
    }
}

impl Collector for MemoryCollector {
    fn name(&self) -> &'static str {
        "system.memory"
    }

    #[instrument(skip(self, registry), level = "info", err, fields(collector = "system.memory"))]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.total.clone()))?;
        registry.register(Box::new(self.available.clone()))?;
        registry.register(Box::new(self.free.clone()))?;
        registry.register(Box::new(self.used.clone()))?;
        registry.register(Box::new(self.swap_total.clone()))?;
        registry.register(Box::new(self.swap_used.clone()))?;
        registry.register(Box::new(self.swap_free.clone()))?;
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
    fn collector_name_is_system_memory() {
        assert_eq!(MemoryCollector::new().name(), "system.memory");
    }

    #[test]
    fn collector_is_disabled_by_default() {
        assert!(!MemoryCollector::new().enabled_by_default());
    }

    #[test]
    fn register_metrics_succeeds() {
        let registry = Registry::new();
        assert!(MemoryCollector::new().register_metrics(&registry).is_ok());
    }

    #[test]
    fn collect_stats_reports_total_memory() {
        let collector = MemoryCollector::new();
        collector.collect_stats();

        assert!(
            collector.total.get() > 0,
            "total memory should be greater than zero"
        );
        assert!(collector.available.get() >= 0);
        assert!(collector.used.get() >= 0);
        assert!(
            collector.used.get() <= collector.total.get(),
            "used memory cannot exceed total memory"
        );
    }

    #[test]
    fn to_i64_saturates_instead_of_wrapping() {
        assert_eq!(to_i64(0), 0);
        assert_eq!(to_i64(1024), 1024);
        assert_eq!(to_i64(u64::MAX), i64::MAX);
    }

    #[test]
    fn multiple_collections_are_stable() {
        let collector = MemoryCollector::new();
        for _ in 0..3 {
            collector.collect_stats();
        }
        assert!(collector.total.get() > 0);
    }
}
