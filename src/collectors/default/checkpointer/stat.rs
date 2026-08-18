//! `pg_stat_checkpointer` counters (`PostgreSQL` 17+).
//!
//! Owns only the version-gated metrics, so skipping below `PostgreSQL` 17 cannot disturb
//! the always-available `pg_control_checkpoint()` gauges in the sibling leaf.

use crate::collectors::{Collected, Collector, NO_LABELS, util::resolve_server_version};
use crate::collectors::util::{INSUFFICIENT_PRIVILEGE, UNDEFINED_TABLE};
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{IntCounterVec, Opts, Registry};
use sqlx::{PgPool, Row};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tracing::{debug, info_span, instrument, warn};
use tracing_futures::Instrument as _;

/// `pg_stat_checkpointer` was introduced in `PostgreSQL` 17.
const MIN_PG_STAT_CHECKPOINTER_VERSION: i32 = 170_000;

/// Exposes `pg_stat_checkpointer` (`PostgreSQL` 17+):
/// - `pg_stat_checkpointer_timed_total` (`Counter`)
/// - `pg_stat_checkpointer_requested_total` (`Counter`)
/// - `pg_stat_checkpointer_buffers_written_total` (`Counter`)
/// - `pg_stat_checkpointer_write_time_seconds_total` (`Counter`)
/// - `pg_stat_checkpointer_sync_time_seconds_total` (`Counter`)
///
/// All are zero-label vectors, so below `PostgreSQL` 17 they are **absent** rather than
/// reporting zero timed checkpoints and zero buffers written.
#[derive(Clone)]
pub struct StatCheckpointerCollector {
    timed: IntCounterVec,
    requested: IntCounterVec,
    buffers_written: IntCounterVec,
    write_time: IntCounterVec,
    sync_time: IntCounterVec,
    /// Ensures the missing-privilege warning is logged at most once per process.
    denied_warned: Arc<AtomicBool>,
}

impl Default for StatCheckpointerCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl StatCheckpointerCollector {
    /// Creates the collector with its metrics initialized.
    ///
    /// # Panics
    ///
    /// Panics if metric creation fails, which only happens with an invalid metric name.
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        let counter = |name: &str, help: &str| {
            IntCounterVec::new(Opts::new(name, help), &NO_LABELS)
                .expect("Failed to create pg_stat_checkpointer metric")
        };

        Self {
            timed: counter(
                "pg_stat_checkpointer_timed_total",
                "Number of scheduled checkpoints that have been performed",
            ),
            requested: counter(
                "pg_stat_checkpointer_requested_total",
                "Number of requested checkpoints that have been performed",
            ),
            buffers_written: counter(
                "pg_stat_checkpointer_buffers_written_total",
                "Number of buffers written during checkpoints",
            ),
            write_time: counter(
                "pg_stat_checkpointer_write_time_seconds_total",
                "Total time spent writing buffers to disk during checkpoints, in milliseconds",
            ),
            sync_time: counter(
                "pg_stat_checkpointer_sync_time_seconds_total",
                "Total time spent synchronizing buffers to disk during checkpoints, in milliseconds",
            ),
            denied_warned: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Classifies a failed `pg_stat_checkpointer` read.
    ///
    /// Neither case is an `Err`: the registry treats any collector error as fatal for the
    /// whole scrape, so "I cannot read my source" degrades to a skip.
    fn handle_query_error(&self, error: sqlx::Error) -> Result<Collected> {
        let code = match &error {
            sqlx::Error::Database(db_error) => db_error.code().map(|code| code.to_string()),
            _ => None,
        };

        match code.as_deref() {
            Some(UNDEFINED_TABLE) => {
                debug!("Skipping pg_stat_checkpointer metrics (view not found)");
                Ok(Collected::Skipped)
            }
            Some(INSUFFICIENT_PRIVILEGE) => {
                if !self.denied_warned.swap(true, Ordering::Relaxed) {
                    warn!(
                        "the exporter role may not read pg_stat_checkpointer; grant \
                         pg_monitor to expose checkpoint counters"
                    );
                }
                debug!("Skipping pg_stat_checkpointer metrics (insufficient privilege)");
                Ok(Collected::Skipped)
            }
            _ => Err(error.into()),
        }
    }
}

impl Collector for StatCheckpointerCollector {
    fn name(&self) -> &'static str {
        "checkpointer.stat"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "checkpointer.stat")
    )]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.timed.clone()))?;
        registry.register(Box::new(self.requested.clone()))?;
        registry.register(Box::new(self.buffers_written.clone()))?;
        registry.register(Box::new(self.write_time.clone()))?;
        registry.register(Box::new(self.sync_time.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector = "checkpointer.stat", otel.kind = "internal")
    )]
    fn collect_once<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<Collected>> {
        Box::pin(async move {
            // Resolved with the live fallback rather than `is_pg_version_at_least`, which
            // reads only the process-wide cache and reports 0 when unset — that made this
            // gate fire on every version outside the normal startup path.
            if resolve_server_version(pool).await? < MIN_PG_STAT_CHECKPOINTER_VERSION {
                debug!("Skipping pg_stat_checkpointer metrics (requires PostgreSQL 17+)");
                return Ok(Collected::Skipped);
            }

            let query_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT * FROM pg_stat_checkpointer",
                db.sql.table = "pg_stat_checkpointer"
            );

            let row = match sqlx::query(
                r"
                SELECT
                    num_timed,
                    num_requested,
                    buffers_written,
                    ROUND(GREATEST(write_time, 0))::bigint AS write_time_ms,
                    ROUND(GREATEST(sync_time, 0))::bigint AS sync_time_ms
                FROM pg_stat_checkpointer
                ",
            )
            .fetch_one(pool)
            .instrument(query_span)
            .await
            {
                Ok(row) => row,
                Err(error) => return self.handle_query_error(error),
            };

            let num_timed: i64 = row.try_get("num_timed")?;
            let num_requested: i64 = row.try_get("num_requested")?;
            let buffers_written: i64 = row.try_get("buffers_written")?;
            let write_time_ms: i64 = row.try_get("write_time_ms")?;
            let sync_time_ms: i64 = row.try_get("sync_time_ms")?;

            // These are absolute totals from PostgreSQL, so the child is removed and
            // re-created at the current value rather than incremented.
            self.reset_metrics();
            for (metric, value) in [
                (&self.timed, num_timed),
                (&self.requested, num_requested),
                (&self.buffers_written, buffers_written),
                (&self.write_time, write_time_ms),
                (&self.sync_time, sync_time_ms),
            ] {
                metric
                    .with_label_values(&NO_LABELS)
                    .inc_by(u64::try_from(value).unwrap_or(0));
            }

            debug!(
                num_timed,
                num_requested,
                buffers_written,
                write_time_ms,
                sync_time_ms,
                "updated checkpointer metrics"
            );

            Ok(Collected::Fresh)
        })
    }

    /// Removes the five counters.
    fn reset_metrics(&self) {
        self.timed.reset();
        self.requested.reset();
        self.buffers_written.reset();
        self.write_time.reset();
        self.sync_time.reset();
    }

    fn enabled_by_default(&self) -> bool {
        true
    }
}
