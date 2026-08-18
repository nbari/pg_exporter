use crate::collectors::{Collected, Collector};
use anyhow::Result;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use prometheus::Registry;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{debug, info_span, instrument, warn};
use tracing_futures::Instrument as _;

pub mod analyze_progress;
use analyze_progress::AnalyzeProgressCollector;

pub mod blockers;
use blockers::VacuumBlockersCollector;

pub mod create_index_progress;
use create_index_progress::CreateIndexProgressCollector;

pub mod progress;
use progress::VacuumProgressCollector;

pub mod stats;
use stats::VacuumStatsCollector;

#[derive(Clone, Default)]
pub struct VacuumCollector {
    subs: Vec<Arc<dyn Collector + Send + Sync>>,
}

impl VacuumCollector {
    #[must_use]
    pub fn new() -> Self {
        Self {
            subs: vec![
                Arc::new(VacuumStatsCollector::new()),
                Arc::new(VacuumProgressCollector::new()),
                Arc::new(VacuumBlockersCollector::new()),
                Arc::new(CreateIndexProgressCollector::new()),
                Arc::new(AnalyzeProgressCollector::new()),
            ],
        }
    }
}

impl Collector for VacuumCollector {
    fn name(&self) -> &'static str {
        "vacuum"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "vacuum")
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
        fields(collector = "vacuum", otel.kind = "internal")
    )]
    fn collect_once<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<Collected>> {
        Box::pin(async move {
            let mut tasks = FuturesUnordered::new();

            for sub in &self.subs {
                let span = info_span!("collector.collect", sub_collector = %sub.name(), otel.kind = "internal");

                tasks.push(sub.collect(pool).instrument(span));
            }

            while let Some(res) = tasks.next().await {
                res?;
            }

            Ok(Collected::Fresh)
        })
    }

    /// Fans out to the sub-collectors; this umbrella owns no metrics itself.
    /// Each sub already settles via the safe `collect`, so this exists only so a
    /// caller holding the umbrella has something to call.
    fn reset_metrics(&self) {
        for sub in &self.subs {
            sub.reset_metrics();
        }
    }

    fn enabled_by_default(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vacuum_collector_name() {
        let collector = VacuumCollector::new();
        assert_eq!(collector.name(), "vacuum");
    }

    #[test]
    fn test_vacuum_collector_enabled_by_default() {
        let collector = VacuumCollector::new();
        assert!(collector.enabled_by_default());
    }
}
