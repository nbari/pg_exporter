use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use prometheus::Registry;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{debug, info_span, instrument, warn};
use tracing_futures::Instrument as _;

pub mod replica;
use replica::ReplicaCollector;

pub mod stat_replication;
use stat_replication::StatReplicationCollector;

pub mod slots;
use slots::ReplicationSlotsCollector;

#[derive(Clone, Default)]
pub struct ReplicationCollector {
    subs: Vec<Arc<dyn Collector + Send + Sync>>,
}

impl ReplicationCollector {
    #[must_use]
    pub fn new() -> Self {
        Self {
            subs: vec![
                Arc::new(ReplicaCollector::new()),
                Arc::new(StatReplicationCollector::new()),
                Arc::new(ReplicationSlotsCollector::new()),
            ],
        }
    }
}

impl Collector for ReplicationCollector {
    fn name(&self) -> &'static str {
        "replication"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "replication")
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
        fields(collector = "replication", otel.kind = "internal")
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
    fn test_replication_collector_name() {
        let collector = ReplicationCollector::new();
        assert_eq!(collector.name(), "replication");
    }

    #[test]
    fn test_replication_collector_not_enabled_by_default() {
        let collector = ReplicationCollector::new();
        assert!(!collector.enabled_by_default());
    }
}
