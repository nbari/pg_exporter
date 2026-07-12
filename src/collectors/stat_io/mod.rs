//! `stat_io` collector umbrella.
//!
//! `mod.rs` is the entry point: it wires up the `pg_stat_io` sub-collector and
//! exposes it under the `--collector.stat_io` CLI flag. The actual metric
//! definitions, SQL, and version handling live in [`pg_stat_io`].
//!
//! `pg_stat_io` is a **cluster-wide** view (`PostgreSQL` 16+), so the collector
//! reads only the shared pool and never fans out per database. It is disabled
//! by default to keep the extra label cardinality opt-in.

use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use prometheus::Registry;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{debug, info_span, instrument, warn};
use tracing_futures::Instrument as _;

pub mod pg_stat_io;
use pg_stat_io::PgStatIoCollector;

/// Cluster-wide I/O statistics from `pg_stat_io` (`PostgreSQL` 16+).
///
/// This is the umbrella collector selected by `--collector.stat_io`. It holds a
/// single [`PgStatIoCollector`] sub-collector and fans registration and
/// collection out to it, matching the structure used by the other collectors
/// (`stat`, `index`, `statements`).
#[derive(Clone)]
pub struct StatIoCollector {
    subs: Vec<Arc<dyn Collector + Send + Sync>>,
}

impl Default for StatIoCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl StatIoCollector {
    #[must_use]
    pub fn new() -> Self {
        Self {
            subs: vec![Arc::new(PgStatIoCollector::new())],
        }
    }
}

impl Collector for StatIoCollector {
    fn name(&self) -> &'static str {
        "stat_io"
    }

    #[instrument(skip(self, registry), level = "info", err, fields(collector = "stat_io"))]
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
        fields(collector = "stat_io", otel.kind = "internal")
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
    fn test_stat_io_collector_name() {
        assert_eq!(StatIoCollector::new().name(), "stat_io");
    }

    #[test]
    fn test_stat_io_collector_not_enabled_by_default() {
        assert!(!StatIoCollector::new().enabled_by_default());
    }
}
