//! `system` collector umbrella (host CPU / memory).
//!
//! `mod.rs` is the entry point: it wires up the `cpu` and `memory`
//! sub-collectors and exposes them under the `--collector.system` CLI flag. The
//! actual metric definitions and OS reads live in the sibling [`cpu`] and
//! [`memory`] modules.
//!
//! This collector reports **host-wide** CPU and memory usage for the machine the
//! exporter runs on, plus the resource usage of the **`PostgreSQL` process group**
//! on that host. It never touches `PostgreSQL`: it reads only the operating
//! system (`/proc` on Linux, `kern.cp_time`/`kern.cp_times` sysctls and
//! `sysinfo` on FreeBSD, and `sysinfo` for memory/load), so it adds no query or
//! connection load to the database.
//!
//! # When to enable it
//!
//! It is **disabled by default** and only meaningful when `pg_exporter` runs on
//! the **same host** as `PostgreSQL`. Do **not** enable it for managed services
//! such as AWS RDS/Aurora: there the exporter runs on a separate machine, so the
//! CPU/memory numbers describe the exporter's host, not the database server, and
//! would be misleading.

use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use prometheus::Registry;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{debug, info_span, instrument, warn};
use tracing_futures::Instrument as _;

pub mod cpu;
pub mod memory;
pub mod process;

use cpu::CpuCollector;
use memory::MemoryCollector;
use process::ProcessGroupCollector;

/// Host CPU and memory statistics for the machine running the exporter.
///
/// This is the umbrella collector selected by `--collector.system`. It fans
/// registration and collection out to a [`CpuCollector`], a
/// [`MemoryCollector`], and a [`ProcessGroupCollector`], matching the structure
/// used by the other collectors (`slru`, `stat_io`, `statements`). It is
/// disabled by default and intended only for exporters co-located with
/// `PostgreSQL`.
#[derive(Clone)]
pub struct SystemCollector {
    subs: Vec<Arc<dyn Collector + Send + Sync>>,
}

impl Default for SystemCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl SystemCollector {
    /// Creates a new `SystemCollector`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            subs: vec![
                Arc::new(CpuCollector::new()),
                Arc::new(MemoryCollector::new()),
                Arc::new(ProcessGroupCollector::new()),
            ],
        }
    }
}

impl Collector for SystemCollector {
    fn name(&self) -> &'static str {
        "system"
    }

    #[instrument(skip(self, registry), level = "info", err, fields(collector = "system"))]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        for sub in &self.subs {
            let span = info_span!("collector.register_metrics", sub_collector = %sub.name());
            let res = sub.register_metrics(registry);
            match res {
                Ok(()) => debug!(collector = sub.name(), "registered metrics"),
                Err(ref e) => {
                    warn!(collector = sub.name(), error = %e, "failed to register metrics");
                }
            }
            res?;
            drop(span);
        }
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector = "system", otel.kind = "internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let mut tasks = FuturesUnordered::new();

            for sub in &self.subs {
                let span = info_span!(
                    "collector.collect",
                    sub_collector = %sub.name(),
                    otel.kind = "internal"
                );
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
    fn test_system_collector_name() {
        assert_eq!(SystemCollector::new().name(), "system");
    }

    #[test]
    fn test_system_collector_not_enabled_by_default() {
        assert!(!SystemCollector::new().enabled_by_default());
    }

    #[test]
    fn test_system_collector_registers_without_error() {
        let registry = Registry::new();
        assert!(SystemCollector::new().register_metrics(&registry).is_ok());
    }
}
