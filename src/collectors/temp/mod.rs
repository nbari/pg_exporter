//! `temp` collector umbrella.
//!
//! `mod.rs` is the entry point: it wires up the `pg_ls_tmpdir` sub-collector and
//! exposes it under the `--collector.temp` CLI flag. The actual metric
//! definitions, SQL, and version handling live in [`pg_ls_tmpdir`].
//!
//! The temporary-file directory is a **cluster-wide** resource, so the collector
//! reads only the shared pool and never fans out per database. It is disabled by
//! default because `pg_ls_tmpdir()` requires superuser or `pg_monitor`.

use crate::collectors::{Collected, Collector};
use anyhow::Result;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use prometheus::Registry;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{debug, info_span, instrument, warn};
use tracing_futures::Instrument as _;

pub mod pg_ls_tmpdir;
use pg_ls_tmpdir::PgLsTmpdirCollector;

/// Live temporary-file footprint from `pg_ls_tmpdir()` (`PostgreSQL` 12+).
///
/// This is the umbrella collector selected by `--collector.temp`. It holds a
/// single [`PgLsTmpdirCollector`] sub-collector and fans registration and
/// collection out to it, matching the structure used by the other collectors
/// (`stat`, `index`, `statements`, `slru`).
///
/// # Why this exists next to `pg_stat_database_temp_bytes`
///
/// `pg_stat_database.temp_bytes` and the `pg_stat_statements` temp counters are
/// **cumulative** and only updated once a statement finishes. A single query that
/// is still running can fill the data volume long before any of those counters
/// move. These gauges report what is on disk *right now*.
#[derive(Clone)]
pub struct TempCollector {
    subs: Vec<Arc<dyn Collector + Send + Sync>>,
}

impl Default for TempCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl TempCollector {
    #[must_use]
    pub fn new() -> Self {
        Self {
            subs: vec![Arc::new(PgLsTmpdirCollector::new())],
        }
    }
}

impl Collector for TempCollector {
    fn name(&self) -> &'static str {
        "temp"
    }

    #[instrument(skip(self, registry), level = "info", err, fields(collector = "temp"))]
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
        fields(collector = "temp", otel.kind = "internal")
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
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_temp_collector_name() {
        assert_eq!(TempCollector::new().name(), "temp");
    }

    #[test]
    fn test_temp_collector_not_enabled_by_default() {
        assert!(!TempCollector::new().enabled_by_default());
    }
}
