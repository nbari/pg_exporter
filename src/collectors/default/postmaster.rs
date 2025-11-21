#![allow(unused_imports)]
use crate::collectors::{Collector, util::get_excluded_databases};
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{IntGauge, Opts, Registry};
use sqlx::PgPool;
use tracing::{info_span, instrument};
use tracing_futures::Instrument as _;

/// Exposes the `PostgreSQL` postmaster (server) start time as Unix epoch seconds:
/// - `pg_postmaster_start_time_seconds` (`IntGauge`)
#[derive(Clone)]
pub struct PostmasterCollector {
    start_time_epoch_seconds: IntGauge, // pg_postmaster_start_time_seconds
}

impl Default for PostmasterCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl PostmasterCollector {
    /// Creates a new `PostmasterCollector`
    ///
    /// # Panics
    ///
    /// Panics if metric creation fails (should never happen with valid metric names)
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        let start_time_epoch_seconds = IntGauge::with_opts(Opts::new(
            "pg_postmaster_start_time_seconds",
            "PostgreSQL postmaster (server) start time as seconds since Unix epoch",
        ))
        .expect("create pg_postmaster_start_time_seconds");

        Self {
            start_time_epoch_seconds,
        }
    }
}

impl Collector for PostmasterCollector {
    fn name(&self) -> &'static str {
        "postmaster"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "postmaster")
    )]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.start_time_epoch_seconds.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector="postmaster", otel.kind="internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let q_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT EXTRACT(EPOCH FROM pg_postmaster_start_time())::bigint"
            );

            // Returns Unix epoch seconds
            let epoch_seconds: i64 = sqlx::query_scalar(
                r"SELECT EXTRACT(EPOCH FROM pg_postmaster_start_time())::bigint",
            )
            .fetch_one(pool)
            .instrument(q_span)
            .await?;

            self.start_time_epoch_seconds.set(epoch_seconds);
            Ok(())
        })
    }

    fn enabled_by_default(&self) -> bool {
        true
    }
}
