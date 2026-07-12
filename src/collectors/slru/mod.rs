//! `slru` collector umbrella.
//!
//! `mod.rs` is the entry point: it wires up the `pg_stat_slru` sub-collector
//! and exposes it under the `--collector.slru` CLI flag. The actual metric
//! definitions, SQL, and version handling live in [`pg_stat_slru`].
//!
//! `pg_stat_slru` is a **cluster-wide** view (`PostgreSQL` 13+), so the
//! collector reads only the shared pool and never fans out per database. It is
//! disabled by default because SLRU pressure metrics are opt-in diagnostics.

use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use prometheus::Registry;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{debug, info_span, instrument, warn};
use tracing_futures::Instrument as _;

pub mod pg_stat_slru;
use pg_stat_slru::PgStatSlruCollector;

/// Cluster-wide SLRU cache statistics from `pg_stat_slru` (`PostgreSQL` 13+).
///
/// This is the umbrella collector selected by `--collector.slru`. It holds a
/// single [`PgStatSlruCollector`] sub-collector and fans registration and
/// collection out to it, matching the structure used by the other collectors
/// (`stat`, `index`, `statements`).
#[derive(Clone)]
pub struct SlruCollector {
    subs: Vec<Arc<dyn Collector + Send + Sync>>,
}

impl Default for SlruCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl SlruCollector {
    #[must_use]
    pub fn new() -> Self {
        Self {
            subs: vec![Arc::new(PgStatSlruCollector::new())],
        }
    }
}

impl Collector for SlruCollector {
    fn name(&self) -> &'static str {
        "slru"
    }

    #[instrument(skip(self, registry), level = "info", err, fields(collector = "slru"))]
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
        fields(collector = "slru", otel.kind = "internal")
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
    fn test_slru_collector_name() {
        assert_eq!(SlruCollector::new().name(), "slru");
    }

    #[test]
    fn test_slru_collector_not_enabled_by_default() {
        assert!(!SlruCollector::new().enabled_by_default());
    }
}
