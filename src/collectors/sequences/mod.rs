use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use prometheus::Registry;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{debug, info_span, instrument, warn};
use tracing_futures::Instrument as _;

pub mod pg_sequences;
pub use pg_sequences::PgSequencesCollector;

/// Opt-in sequence exhaustion collector.
///
/// The thin umbrella fans out to sub-collectors that read `PostgreSQL` sequence
/// metadata, currently `pg_sequences`, without carrying metric construction or
/// database query details in this module.
#[derive(Clone)]
pub struct SequencesCollector {
    subs: Vec<Arc<dyn Collector + Send + Sync>>,
}

impl SequencesCollector {
    #[must_use]
    pub fn new() -> Self {
        Self::with_min_ratio(0.5)
    }

    #[must_use]
    pub fn with_min_ratio(min_ratio: f64) -> Self {
        Self {
            subs: vec![Arc::new(PgSequencesCollector::with_min_ratio(min_ratio))],
        }
    }
}

impl Default for SequencesCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl Collector for SequencesCollector {
    fn name(&self) -> &'static str {
        "sequences"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "sequences")
    )]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        for sub in &self.subs {
            let span = info_span!("collector.register_metrics", sub_collector = %sub.name());
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
            drop(span);
        }
        Ok(())
    }

    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
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
    fn test_sequences_collector_name() {
        let collector = SequencesCollector::new();
        assert_eq!(collector.name(), "sequences");
    }

    #[test]
    fn test_sequences_collector_not_enabled_by_default() {
        let collector = SequencesCollector::new();
        assert!(!collector.enabled_by_default());
    }
}
