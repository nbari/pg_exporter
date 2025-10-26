mod stats;
mod unused;

pub use stats::IndexStatsCollector;
pub use unused::UnusedIndexCollector;

use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use prometheus::Registry;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{debug, info_span, instrument, warn};
use tracing_futures::Instrument as _;

/// Main index health collector that combines stats and unused index tracking
///
/// This collector provides comprehensive index health monitoring including:
/// - Index usage statistics (scans, tuples read/fetched)
/// - Index size and bloat estimation
/// - Unused index detection (idx_scan = 0)
/// - Invalid index identification
///
/// Helps identify maintenance opportunities and problematic schemas that impact query performance.
/// Unused indexes consume disk space and slow down write operations (INSERT/UPDATE/DELETE).
/// Invalid indexes (from failed CREATE INDEX CONCURRENTLY) need to be dropped and recreated.
#[derive(Clone, Default)]
pub struct IndexCollector {
    subs: Vec<Arc<dyn Collector + Send + Sync>>,
}

impl IndexCollector {
    pub fn new() -> Self {
        Self {
            subs: vec![
                Arc::new(IndexStatsCollector::new()),
                Arc::new(UnusedIndexCollector::new()),
            ],
        }
    }
}

impl Collector for IndexCollector {
    fn name(&self) -> &'static str {
        "index"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "index")
    )]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        for sub in &self.subs {
            let span = info_span!("collector.register_metrics", sub_collector = %sub.name());

            let res = sub.register_metrics(registry);

            match res {
                Ok(_) => debug!(collector = sub.name(), "registered metrics"),

                Err(ref e) => {
                    warn!(collector = sub.name(), error = %e, "failed to register metrics")
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
        fields(collector = "index", otel.kind = "internal")
    )]
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
    fn test_index_collector_name() {
        let collector = IndexCollector::new();
        assert_eq!(collector.name(), "index");
    }

    #[test]
    fn test_index_collector_not_enabled_by_default() {
        let collector = IndexCollector::new();
        assert!(!collector.enabled_by_default());
    }
}

