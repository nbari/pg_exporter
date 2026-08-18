//! Checkpointer metrics, split into two independent leaves.
//!
//! `pg_control_checkpoint()` works on every supported server; `pg_stat_checkpointer` needs
//! `PostgreSQL` 17. As one collector, `collect` published the former and then returned
//! early on the version gate, so reporting `Collected::Skipped` for the whole thing would
//! have cleared metrics it had just refreshed. Two leaves make each group atomically
//! fresh-or-skipped, and the sub-collectors settle independently through the safe
//! `Collector::collect`.
//!
//! The public path, collector name, metric names and default enablement are unchanged.

use crate::collectors::{Collected, Collector};
use anyhow::Result;
use futures::{StreamExt, future::BoxFuture, stream::FuturesUnordered};
use prometheus::Registry;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{debug, info_span, instrument};
use tracing_futures::Instrument as _;

pub mod control;
pub mod stat;

use control::ControlCheckpointCollector;
use stat::StatCheckpointerCollector;

/// Umbrella over the two checkpointer metric groups.
#[derive(Clone)]
pub struct CheckpointerCollector {
    subs: Vec<Arc<dyn Collector + Send + Sync>>,
}

impl Default for CheckpointerCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl CheckpointerCollector {
    /// Creates the umbrella with both leaves.
    #[must_use]
    pub fn new() -> Self {
        Self {
            subs: vec![
                Arc::new(ControlCheckpointCollector::new()),
                Arc::new(StatCheckpointerCollector::new()),
            ],
        }
    }
}

impl Collector for CheckpointerCollector {
    fn name(&self) -> &'static str {
        "checkpointer"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "checkpointer")
    )]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        for sub in &self.subs {
            sub.register_metrics(registry)?;
        }
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector = "checkpointer", otel.kind = "internal")
    )]
    fn collect_once<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<Collected>> {
        Box::pin(async move {
            let mut tasks = FuturesUnordered::new();

            for sub in &self.subs {
                let span = info_span!(
                    "collector.collect",
                    sub_collector = %sub.name(),
                    otel.kind = "internal"
                );
                // The safe `collect`, so each leaf settles its own skip. Aggregating the
                // outcome upward would be wrong either way: `Skipped` would make the caller
                // clear a sibling that succeeded, `Fresh` would let a skipped sibling keep
                // stale series.
                tasks.push(sub.collect(pool).instrument(span));
            }

            while let Some(res) = tasks.next().await {
                res?;
            }

            debug!("collected checkpointer metrics");

            Ok(Collected::Fresh)
        })
    }

    /// Fans out to the leaves; this umbrella owns no metrics itself.
    fn reset_metrics(&self) {
        for sub in &self.subs {
            sub.reset_metrics();
        }
    }

    fn enabled_by_default(&self) -> bool {
        true
    }
}
