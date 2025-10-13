use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
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
            info_span!("collector.register_metrics", sub_collector = %sub.name());
            let res = sub.register_metrics(registry);
            match res {
                Ok(_) => {
                    // Attach a small event so you can see success in the span
                    debug!(collector = sub.name(), "registered metrics");
                }
                Err(ref e) => {
                    // Error will also be recorded on the span due to `err` on the #[instrument]
                    warn!(collector = sub.name(), error = %e, "failed to register metrics");
                }
            }
            // No need to .instrument() here as register_metrics is sync
            res?;
        }
        Ok(())
    }

    #[instrument(skip(self, pool), level = "info", err, fields(collector = "default", otel.kind = "internal"))]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            // Collect from each sub-collector within its own span
            for sub in &self.subs {
                let span = info_span!("collector.collect", sub_collector = %sub.name(), otel.kind = "internal");
                sub.collect(pool).instrument(span).await?;
            }
            Ok(())
        })
    }

    fn enabled_by_default(&self) -> bool {
        true
    }
}
