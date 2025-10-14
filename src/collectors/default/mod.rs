use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use prometheus::Registry;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{debug, info_span, instrument, warn};
use tracing_futures::Instrument as _;

mod version;
use version::VersionCollector;

mod settings;
use settings::SettingsCollector;

mod postmaster;
use postmaster::PostmasterCollector;

/// DefaultCollector is an umbrella for cheap, always-on signals.
#[derive(Clone, Default)]
pub struct DefaultCollector {
    subs: Vec<Arc<dyn Collector + Send + Sync>>,
}

impl DefaultCollector {
    pub fn new() -> Self {
        Self {
            subs: vec![
                Arc::new(VersionCollector::new()),
                Arc::new(SettingsCollector::new()),
                Arc::new(PostmasterCollector::new()),
            ],
        }
    }
}

impl Collector for DefaultCollector {
    fn name(&self) -> &'static str {
        "default"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "default")
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

    #[instrument(skip(self, pool), level = "info", err, fields(collector = "default", otel.kind = "internal"))]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            // Collect sub-collectors concurrently (they're independent).
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
        true
    }
}
