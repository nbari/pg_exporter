use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use prometheus::Registry;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{debug, info_span, instrument, warn};
use tracing_futures::Instrument as _;

pub mod pg_statements;
use pg_statements::PgStatementsCollector;

/// pg_stat_statements collector - THE most critical tool for DBREs
///
/// Tracks query performance metrics from the pg_stat_statements extension.
/// This is the #1 tool Database Reliability Engineers use to:
/// - Find slow queries causing incidents
/// - Detect N+1 query problems
/// - Identify performance regressions
/// - Optimize query patterns
/// - Track resource-intensive queries
///
/// # Prerequisites
///
/// The pg_stat_statements extension must be installed and configured:
///
/// ```text
/// CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
/// ```
///
/// Add to postgresql.conf:
///
/// ```text
/// shared_preload_libraries = 'pg_stat_statements'
/// pg_stat_statements.track = all
/// pg_stat_statements.max = 10000
/// ```
///
/// # Key Metrics for Production DBREs
///
/// 1. **Total execution time** - Which queries consume the most database time?
/// 2. **Mean execution time** - Which queries are individually slow?
/// 3. **Call count** - Which queries run most frequently?
/// 4. **I/O metrics** - Which queries cause disk reads/writes?
/// 5. **WAL generation** - Which queries write the most data?
/// 6. **Temp file usage** - Which queries spill to disk?
///
/// # DBRE Use Cases
///
/// - Incident response: "What query is killing the database right now?"
/// - Performance optimization: "What's our top 10 slowest queries?"
/// - Capacity planning: "Which queries will break first under load?"
/// - Code review: "Did this deploy introduce slow queries?"
#[derive(Clone, Default)]
pub struct StatementsCollector {
    subs: Vec<Arc<dyn Collector + Send + Sync>>,
}

impl StatementsCollector {
    pub fn new() -> Self {
        Self {
            subs: vec![Arc::new(PgStatementsCollector::new())],
        }
    }
}

impl Collector for StatementsCollector {
    fn name(&self) -> &'static str {
        "statements"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "statements")
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
        false // Disabled by default - requires extension
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_statements_collector_name() {
        let collector = StatementsCollector::new();
        assert_eq!(collector.name(), "statements");
    }

    #[test]
    fn test_statements_collector_not_enabled_by_default() {
        let collector = StatementsCollector::new();
        assert!(!collector.enabled_by_default());
    }
}
