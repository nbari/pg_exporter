//! Checkpoint age and WAL-since-checkpoint from `pg_control_checkpoint()`.
//!
//! Split out from the `pg_stat_checkpointer` leaf on purpose. These two gauges are
//! available on **every** supported server, while `pg_stat_checkpointer` needs
//! `PostgreSQL` 17. When both lived in one collector, `collect` published these first and
//! then returned early on the version gate — so reporting `Collected::Skipped` for the
//! whole collector would have cleared the gauges it had just refreshed. Separate leaves
//! make each one atomically fresh-or-skipped, which is a property of the structure rather
//! than of a comment someone has to remember.

use crate::collectors::{Collected, Collector, NO_LABELS};
use crate::collectors::util::{INSUFFICIENT_PRIVILEGE, UNDEFINED_FUNCTION};
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{GaugeVec, Opts, Registry};
use sqlx::{PgPool, Row};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tracing::{debug, info_span, instrument, warn};
use tracing_futures::Instrument as _;

/// Exposes checkpoint-timing insight from `pg_control_checkpoint()`:
/// - `pg_last_checkpoint_age_seconds` (`Gauge`)
/// - `pg_wal_bytes_since_last_checkpoint` (`Gauge`)
///
/// Both are zero-label vectors so a skip removes them. Publishing zero instead would
/// claim a checkpoint had just completed and that no WAL had been written since.
#[derive(Clone)]
pub struct ControlCheckpointCollector {
    last_checkpoint_age: GaugeVec,
    wal_bytes_since_checkpoint: GaugeVec,
    /// Ensures the missing-privilege warning is logged at most once per process.
    denied_warned: Arc<AtomicBool>,
}

impl Default for ControlCheckpointCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl ControlCheckpointCollector {
    /// Creates the collector with its metrics initialized.
    ///
    /// # Panics
    ///
    /// Panics if metric creation fails, which only happens with an invalid metric name.
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        Self {
            last_checkpoint_age: GaugeVec::new(
                Opts::new(
                    "pg_last_checkpoint_age_seconds",
                    "Seconds since the last completed checkpoint (now() - \
                     pg_control_checkpoint().checkpoint_time). Reflects the achieved \
                     checkpoint interval and checkpointer liveness; grows unbounded if the \
                     checkpointer stalls",
                ),
                &NO_LABELS,
            )
            .expect("Failed to create pg_last_checkpoint_age_seconds"),
            wal_bytes_since_checkpoint: GaugeVec::new(
                Opts::new(
                    "pg_wal_bytes_since_last_checkpoint",
                    "WAL bytes generated since the last checkpoint's redo point (must be \
                     replayed on crash recovery). Proxy for recovery time (RTO) and headroom \
                     against max_wal_size",
                ),
                &NO_LABELS,
            )
            .expect("Failed to create pg_wal_bytes_since_last_checkpoint"),
            denied_warned: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Classifies a failed `pg_control_checkpoint()` read.
    fn handle_query_error(&self, error: sqlx::Error) -> Result<Collected> {
        let code = match &error {
            sqlx::Error::Database(db_error) => db_error.code().map(|code| code.to_string()),
            _ => None,
        };

        match code.as_deref() {
            Some(UNDEFINED_FUNCTION) => {
                debug!("Skipping checkpoint age metrics (pg_control_checkpoint() unavailable)");
                Ok(Collected::Skipped)
            }
            Some(INSUFFICIENT_PRIVILEGE) => {
                if !self.denied_warned.swap(true, Ordering::Relaxed) {
                    warn!(
                        "the exporter role may not call pg_control_checkpoint(); grant \
                         pg_monitor to expose pg_last_checkpoint_age_seconds and \
                         pg_wal_bytes_since_last_checkpoint"
                    );
                }
                debug!("Skipping checkpoint age metrics (insufficient privilege)");
                Ok(Collected::Skipped)
            }
            _ => Err(error.into()),
        }
    }
}

impl Collector for ControlCheckpointCollector {
    fn name(&self) -> &'static str {
        "checkpointer.control"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "checkpointer.control")
    )]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.last_checkpoint_age.clone()))?;
        registry.register(Box::new(self.wal_bytes_since_checkpoint.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector = "checkpointer.control", otel.kind = "internal")
    )]
    fn collect_once<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<Collected>> {
        Box::pin(async move {
            let query_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT ... FROM pg_control_checkpoint()",
                db.sql.table = "pg_control_checkpoint"
            );

            let row = match sqlx::query(
                r"
                SELECT
                    EXTRACT(EPOCH FROM (now() - checkpoint_time))::double precision AS age_seconds,
                    GREATEST(
                        pg_wal_lsn_diff(
                            CASE WHEN pg_is_in_recovery()
                                 THEN pg_last_wal_replay_lsn()
                                 ELSE pg_current_wal_lsn()
                            END,
                            redo_lsn
                        ), 0
                    )::bigint AS wal_bytes_since_checkpoint
                FROM pg_control_checkpoint()
                ",
            )
            .fetch_optional(pool)
            .instrument(query_span)
            .await
            {
                Ok(Some(row)) => row,
                Ok(None) => {
                    debug!("pg_control_checkpoint() returned no rows; skipping");
                    return Ok(Collected::Skipped);
                }
                Err(error) => return self.handle_query_error(error),
            };

            let age_seconds: f64 = row.try_get("age_seconds")?;
            self.last_checkpoint_age
                .with_label_values(&NO_LABELS)
                .set(age_seconds.max(0.0));

            // NULL on a standby that has not replayed any WAL yet. Leaving the previous
            // value would be stale, so remove the series instead of guessing.
            match row.try_get::<Option<i64>, _>("wal_bytes_since_checkpoint")? {
                Some(wal_bytes) => {
                    #[allow(clippy::cast_precision_loss)]
                    self.wal_bytes_since_checkpoint
                        .with_label_values(&NO_LABELS)
                        .set(wal_bytes.max(0) as f64);
                }
                None => self.wal_bytes_since_checkpoint.reset(),
            }

            debug!("updated checkpoint age / wal-since-checkpoint metrics");

            Ok(Collected::Fresh)
        })
    }

    /// Removes both gauges.
    fn reset_metrics(&self) {
        self.last_checkpoint_age.reset();
        self.wal_bytes_since_checkpoint.reset();
    }

    fn enabled_by_default(&self) -> bool {
        true
    }
}
