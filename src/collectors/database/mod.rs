use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use prometheus::Registry;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{debug, info_span, instrument, warn};
use tracing_futures::Instrument as _;

// Sub-collectors under the "database" umbrella.
pub mod stats;
use stats::DatabaseStatCollector;

pub mod catalog;
use catalog::DatabaseSubCollector;

/// `DatabaseCollector` aggregates db-level metrics from multiple sources.
/// Collect sub-collectors concurrently to reduce tail latency.
#[derive(Clone, Default)]
pub struct DatabaseCollector {
    subs: Vec<Arc<dyn Collector + Send + Sync>>,
}

impl DatabaseCollector {
    #[must_use]
    pub fn new() -> Self {
        Self {
            subs: vec![
                Arc::new(DatabaseStatCollector::new()),
                Arc::new(DatabaseSubCollector::new()),
            ],
        }
    }
}

impl Collector for DatabaseCollector {
    fn name(&self) -> &'static str {
        "database"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "database")
    )]
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
        fields(collector = "database", otel.kind = "internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            // Collect sub-collectors concurrently (they're independent).
            let mut tasks = FuturesUnordered::new();

            for sub in &self.subs {
                let span = info_span!("collector.collect", sub_collector = %sub.name(), otel.kind="internal");
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
    fn test_database_collector_name() {
        let collector = DatabaseCollector::new();
        assert_eq!(collector.name(), "database");
    }

    #[test]
    fn test_database_collector_not_enabled_by_default() {
        let collector = DatabaseCollector::new();
        assert!(!collector.enabled_by_default());
    }
}
