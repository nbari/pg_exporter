use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use prometheus::Registry;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{debug, info_span, instrument, warn};
use tracing_futures::Instrument as _;

mod connections;
use connections::ConnectionsCollector;

mod wait;
use wait::WaitEventsCollector;

#[derive(Clone, Default)]
pub struct ActivityCollector {
    subs: Vec<Arc<dyn Collector + Send + Sync>>,
}

impl ActivityCollector {
    pub fn new() -> Self {
        Self {
            subs: vec![
                Arc::new(ConnectionsCollector::new()),
                Arc::new(WaitEventsCollector::new()),
            ],
        }
    }
}

impl Collector for ActivityCollector {
    fn name(&self) -> &'static str {
        "activity"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "activity")
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
        fields(collector = "activity", otel.kind = "internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            // Collect sub-collectors concurrently (unordered join)
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
        true
    }
}
