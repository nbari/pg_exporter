use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use prometheus::Registry;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{debug, info_span, instrument, warn};
use tracing_futures::Instrument as _;

pub mod activity;
pub mod nodes;
pub mod shards;
pub mod stat_counters;
pub mod tables;

use activity::CitusActivityCollector;
use nodes::CitusNodesCollector;
use shards::CitusShardsCollector;
use stat_counters::CitusStatCountersCollector;
use tables::CitusTablesCollector;

/// Citus distributed database collector
///
/// Monitors Citus-specific operational statistics including worker nodes,
/// distributed table sizes, shard placement/sizes, connection stats,
/// and distributed query activity.
///
/// Disabled by default. When enabled, checks for the Citus extension
/// before collecting metrics and gracefully skips if not installed.
#[derive(Clone)]
pub struct CitusCollector {
    subs: Vec<Arc<dyn Collector + Send + Sync>>,
}

impl Default for CitusCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl CitusCollector {
    #[must_use]
    pub fn new() -> Self {
        Self {
            subs: vec![
                Arc::new(CitusNodesCollector::new()),
                Arc::new(CitusTablesCollector::new()),
                Arc::new(CitusShardsCollector::new()),
                Arc::new(CitusStatCountersCollector::new()),
                Arc::new(CitusActivityCollector::new()),
            ],
        }
    }
}

async fn citus_installed(pool: &PgPool) -> Result<bool> {
    Ok(
        sqlx::query("SELECT 1 FROM pg_extension WHERE extname = 'citus'")
            .fetch_optional(pool)
            .await?
            .is_some(),
    )
}

impl Collector for CitusCollector {
    fn name(&self) -> &'static str {
        "citus"
    }

    #[instrument(skip(self, registry), level = "info", err, fields(collector = "citus"))]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        for sub in &self.subs {
            let span = info_span!("collector.register_metrics", sub_collector = %sub.name());
            let _guard = span.enter();
            let res = sub.register_metrics(registry);
            match res {
                Ok(()) => {
                    debug!(collector = sub.name(), "registered metrics");
                }
                Err(ref e) => {
                    warn!(collector = sub.name(), error = %e, "failed to register metrics");
                }
            }
            res?;
        }
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector = "citus", otel.kind = "internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            if !citus_installed(pool).await? {
                debug!(
                    collector = "citus",
                    "citus extension not installed, skipping collection"
                );
                return Ok(());
            }

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
    fn test_citus_collector_name() {
        let collector = CitusCollector::new();
        assert_eq!(collector.name(), "citus");
    }

    #[test]
    fn test_not_enabled_by_default() {
        let collector = CitusCollector::new();
        assert!(!collector.enabled_by_default());
    }

    #[test]
    fn test_citus_collector_register_metrics() {
        let registry = Registry::new();
        let collector = CitusCollector::new();
        collector.register_metrics(&registry).unwrap();
    }
}
