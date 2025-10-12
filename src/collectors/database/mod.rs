use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::Registry;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{debug, info_span, instrument, warn};
use tracing_futures::Instrument as _;

// Sub-collectors under the "database" umbrella.
// pg_stat_database metrics:
mod stats;
use stats::DatabaseStatCollector;

// pg_database metrics (size, connection limit):
mod catalog;
use catalog::DatabaseSubCollector;

/// Main Database Collector (aggregates pg_stat_database + pg_database)
#[derive(Clone, Default)]
pub struct DatabaseCollector {
    subs: Vec<Arc<dyn Collector + Send + Sync>>,
}

impl DatabaseCollector {
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
        fields(collector = "database", otel.kind = "internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            for sub in &self.subs {
                let span = info_span!(
                    "collector.collect",
                    sub_collector = %sub.name(),
                    otel.kind = "internal"
                );
                sub.collect(pool).instrument(span).await?;
            }
            Ok(())
        })
    }

    fn enabled_by_default(&self) -> bool {
        false
    }
}
