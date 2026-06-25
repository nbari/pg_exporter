use crate::collectors::Collector;
use crate::collectors::util::is_pg_version_at_least;
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{Gauge, IntCounter, Opts, Registry};
use sqlx::{PgPool, Row};
use tracing::{debug, info_span, instrument, warn};
use tracing_futures::Instrument as _;

/// Exposes `PostgreSQL` checkpointer statistics.
///
/// From `pg_stat_checkpointer` (`PostgreSQL` 17+):
/// - `pg_stat_checkpointer_timed_total` (`Counter`)
/// - `pg_stat_checkpointer_requested_total` (`Counter`)
/// - `pg_stat_checkpointer_buffers_written_total` (`Counter`)
/// - `pg_stat_checkpointer_write_time_seconds_total` (`Counter`)
/// - `pg_stat_checkpointer_sync_time_seconds_total` (`Counter`)
///
/// From `pg_control_checkpoint()` (tuning-insight metrics, independent of the
/// `PostgreSQL` 17 requirement above):
/// - `pg_last_checkpoint_age_seconds` (`Gauge`)
/// - `pg_wal_bytes_since_last_checkpoint` (`Gauge`)
#[derive(Clone)]
pub struct CheckpointerCollector {
    timed: IntCounter,           // pg_stat_checkpointer_timed_total
    requested: IntCounter,        // pg_stat_checkpointer_requested_total
    buffers_written: IntCounter,  // pg_stat_checkpointer_buffers_written_total
    write_time: IntCounter,       // pg_stat_checkpointer_write_time_seconds_total
    sync_time: IntCounter,        // pg_stat_checkpointer_sync_time_seconds_total
    last_checkpoint_age: Gauge,   // pg_last_checkpoint_age_seconds
    wal_bytes_since_checkpoint: Gauge, // pg_wal_bytes_since_last_checkpoint
}

impl Default for CheckpointerCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl CheckpointerCollector {
    /// Creates a new `CheckpointerCollector`
    ///
    /// # Panics
    ///
    /// Panics if metric creation fails (should never happen with valid metric names)
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        let timed = IntCounter::with_opts(Opts::new(
            "pg_stat_checkpointer_timed_total",
            "Number of scheduled checkpoints that have been performed",
        ))
        .expect("Failed to create pg_stat_checkpointer_timed_total");

        let requested = IntCounter::with_opts(Opts::new(
            "pg_stat_checkpointer_requested_total",
            "Number of requested checkpoints that have been performed",
        ))
        .expect("Failed to create pg_stat_checkpointer_requested_total");

        let buffers_written = IntCounter::with_opts(Opts::new(
            "pg_stat_checkpointer_buffers_written_total",
            "Number of buffers written during checkpoints",
        ))
        .expect("Failed to create pg_stat_checkpointer_buffers_written_total");

        let write_time = IntCounter::with_opts(Opts::new(
            "pg_stat_checkpointer_write_time_seconds_total",
            "Total time spent writing buffers to disk during checkpoints, in milliseconds",
        ))
        .expect("Failed to create pg_stat_checkpointer_write_time_seconds_total");

        let sync_time = IntCounter::with_opts(Opts::new(
            "pg_stat_checkpointer_sync_time_seconds_total",
            "Total time spent synchronizing buffers to disk during checkpoints, in milliseconds",
        ))
        .expect("Failed to create pg_stat_checkpointer_sync_time_seconds_total");

        let last_checkpoint_age = Gauge::with_opts(Opts::new(
            "pg_last_checkpoint_age_seconds",
            "Seconds since the last completed checkpoint (now() - pg_control_checkpoint().checkpoint_time). \
             Reflects the achieved checkpoint interval and checkpointer liveness; grows unbounded if the checkpointer stalls",
        ))
        .expect("Failed to create pg_last_checkpoint_age_seconds");

        let wal_bytes_since_checkpoint = Gauge::with_opts(Opts::new(
            "pg_wal_bytes_since_last_checkpoint",
            "WAL bytes generated since the last checkpoint's redo point (must be replayed on crash recovery). \
             Proxy for recovery time (RTO) and headroom against max_wal_size",
        ))
        .expect("Failed to create pg_wal_bytes_since_last_checkpoint");

        Self {
            timed,
            requested,
            buffers_written,
            write_time,
            sync_time,
            last_checkpoint_age,
            wal_bytes_since_checkpoint,
        }
    }

    /// Collects checkpoint age and WAL-since-checkpoint from `pg_control_checkpoint()`.
    ///
    /// These metrics are best-effort: `pg_control_checkpoint()` may require
    /// `pg_monitor`/superuser on older `PostgreSQL` versions, and the WAL position
    /// functions differ on standbys. Any failure is logged and skipped so the rest
    /// of the checkpointer collector keeps working.
    async fn collect_control_checkpoint(&self, pool: &PgPool) {
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
                debug!("pg_control_checkpoint() returned no rows; skipping checkpoint age metrics");
                return;
            }
            Err(e) => {
                warn!(
                    error = %e,
                    "Could not read pg_control_checkpoint() (insufficient privilege or unsupported); \
                     skipping pg_last_checkpoint_age_seconds and pg_wal_bytes_since_last_checkpoint"
                );
                return;
            }
        };

        if let Ok(age_seconds) = row.try_get::<f64, _>("age_seconds") {
            self.last_checkpoint_age.set(age_seconds.max(0.0));
        }

        // NULL on a standby that has not replayed any WAL yet.
        if let Ok(Some(wal_bytes)) = row.try_get::<Option<i64>, _>("wal_bytes_since_checkpoint") {
            #[allow(clippy::cast_precision_loss)]
            self.wal_bytes_since_checkpoint
                .set(wal_bytes.max(0) as f64);
        }

        debug!("updated checkpoint age / wal-since-checkpoint metrics");
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
        registry.register(Box::new(self.timed.clone()))?;
        registry.register(Box::new(self.requested.clone()))?;
        registry.register(Box::new(self.buffers_written.clone()))?;
        registry.register(Box::new(self.write_time.clone()))?;
        registry.register(Box::new(self.sync_time.clone()))?;
        registry.register(Box::new(self.last_checkpoint_age.clone()))?;
        registry.register(Box::new(self.wal_bytes_since_checkpoint.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector="checkpointer", otel.kind="internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            // Tuning-insight metrics from pg_control_checkpoint() are available on
            // all supported PostgreSQL versions and do not depend on
            // pg_stat_checkpointer (PostgreSQL 17+), so collect them first.
            self.collect_control_checkpoint(pool).await;

            // pg_stat_checkpointer was introduced in PostgreSQL 17
            if !is_pg_version_at_least(170_000) {
                debug!("Skipping pg_stat_checkpointer metrics (requires PostgreSQL 17+)");
                return Ok(());
            }

            let query_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT * FROM pg_stat_checkpointer",
                db.sql.table = "pg_stat_checkpointer"
            );

            let row = sqlx::query(
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
            .await?;

            let num_timed: i64 = row.try_get("num_timed")?;
            let num_requested: i64 = row.try_get("num_requested")?;
            let buffers_written: i64 = row.try_get("buffers_written")?;
            let write_time_ms: i64 = row.try_get("write_time_ms")?;
            let sync_time_ms: i64 = row.try_get("sync_time_ms")?;

            // Reset and set the counter values
            self.timed.reset();
            self.requested.reset();
            self.buffers_written.reset();
            self.write_time.reset();
            self.sync_time.reset();

            self.timed.inc_by(u64::try_from(num_timed).unwrap_or(0));
            self.requested.inc_by(u64::try_from(num_requested).unwrap_or(0));
            self.buffers_written.inc_by(u64::try_from(buffers_written).unwrap_or(0));
            self.write_time
                .inc_by(u64::try_from(write_time_ms).unwrap_or(0));
            self.sync_time
                .inc_by(u64::try_from(sync_time_ms).unwrap_or(0));

            debug!(
                num_timed,
                num_requested,
                buffers_written,
                write_time_ms,
                sync_time_ms,
                "updated checkpointer metrics"
            );

            Ok(())
        })
    }

    fn enabled_by_default(&self) -> bool {
        true
    }
}
