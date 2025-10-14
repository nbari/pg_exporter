use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use prometheus::Registry;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{debug, info_span, instrument, warn};
use tracing_futures::Instrument as _;

mod progress;
use progress::VacuumProgressCollector;

mod stats;
use stats::VacuumStatsCollector;

/// Main Vacuum Collector (aggregates sub-collectors)
#[derive(Clone, Default)]
pub struct VacuumCollector {
    subs: Vec<Arc<dyn Collector + Send + Sync>>,
}

impl VacuumCollector {
    pub fn new() -> Self {
        Self {
            subs: vec![
                Arc::new(VacuumStatsCollector::new()),
                Arc::new(VacuumProgressCollector::new()),
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
                Ok(_) => {
                    debug!(collector = sub.name(), "registered metrics");
                }
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
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let mut tasks = FuturesUnordered::new();

            for sub in &self.subs {
                let span = info_span!(
                    "collector.collect",
                    sub_collector = %sub.name(),
                    otel.kind = "internal"
                );
                let fut = sub.collect(pool).instrument(span);
                tasks.push(fut);
            }

            while let Some(res) = tasks.next().await {
                // Propagate first error (if any) to caller; other tasks will have completed or be polled.
                res?;
            }

            Ok(())
        })
    }

    fn enabled_by_default(&self) -> bool {
        false
    }
}
