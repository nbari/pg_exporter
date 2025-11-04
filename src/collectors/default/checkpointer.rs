use crate::collectors::Collector;
use crate::collectors::util::is_pg_version_at_least;
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{IntCounter, Opts, Registry};
use sqlx::{PgPool, Row};
use tracing::{debug, info_span, instrument};
use tracing_futures::Instrument as _;

/// Exposes PostgreSQL checkpointer statistics from pg_stat_checkpointer:
/// - pg_stat_checkpointer_timed_total (Counter)
/// - pg_stat_checkpointer_requested_total (Counter)
/// - pg_stat_checkpointer_buffers_written_total (Counter)
/// - pg_stat_checkpointer_write_time_seconds_total (Counter)
/// - pg_stat_checkpointer_sync_time_seconds_total (Counter)
#[derive(Clone)]
pub struct CheckpointerCollector {
    timed: IntCounter,           // pg_stat_checkpointer_timed_total
    requested: IntCounter,        // pg_stat_checkpointer_requested_total
    buffers_written: IntCounter,  // pg_stat_checkpointer_buffers_written_total
    write_time: IntCounter,       // pg_stat_checkpointer_write_time_seconds_total
    sync_time: IntCounter,        // pg_stat_checkpointer_sync_time_seconds_total
}

impl Default for CheckpointerCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl CheckpointerCollector {
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

        Self {
            timed,
            requested,
            buffers_written,
            write_time,
            sync_time,
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
        fields(collector="checkpointer", otel.kind="internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            // pg_stat_checkpointer was introduced in PostgreSQL 17
            if !is_pg_version_at_least(170000) {
                debug!("Skipping checkpointer collector (requires PostgreSQL 17+)");
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
                r#"
                SELECT
                    num_timed,
                    num_requested,
                    buffers_written,
                    write_time,
                    sync_time
                FROM pg_stat_checkpointer
                "#,
            )
            .fetch_one(pool)
            .instrument(query_span)
            .await?;

            let num_timed: i64 = row.try_get("num_timed")?;
            let num_requested: i64 = row.try_get("num_requested")?;
            let buffers_written: i64 = row.try_get("buffers_written")?;
            let write_time: f64 = row.try_get("write_time")?;
            let sync_time: f64 = row.try_get("sync_time")?;

            // Reset and set the counter values
            self.timed.reset();
            self.requested.reset();
            self.buffers_written.reset();
            self.write_time.reset();
            self.sync_time.reset();

            self.timed.inc_by(num_timed as u64);
            self.requested.inc_by(num_requested as u64);
            self.buffers_written.inc_by(buffers_written as u64);
            self.write_time.inc_by(write_time as u64);
            self.sync_time.inc_by(sync_time as u64);

            debug!(
                num_timed,
                num_requested,
                buffers_written,
                write_time,
                sync_time,
                "updated checkpointer metrics"
            );

            Ok(())
        })
    }

    fn enabled_by_default(&self) -> bool {
        true
    }
}
