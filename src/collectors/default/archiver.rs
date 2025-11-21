use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{IntCounter, IntGauge, Opts, Registry};
use sqlx::{PgPool, Row};
use tracing::{debug, info_span, instrument};
use tracing_futures::Instrument as _;

/// Exposes `PostgreSQL` archiver statistics from `pg_stat_archiver`:
/// - `pg_stat_archiver_archived_total` (`Counter`)
/// - `pg_stat_archiver_failed_total` (`Counter`)
/// - `pg_stat_archiver_last_archived_age_seconds` (`Gauge`)
/// - `pg_stat_archiver_last_failed_age_seconds` (`Gauge`)
#[derive(Clone)]
pub struct ArchiverCollector {
    archived_count: IntCounter,      // pg_stat_archiver_archived_total
    failed_count: IntCounter,        // pg_stat_archiver_failed_total
    last_archived_age: IntGauge,     // pg_stat_archiver_last_archived_age_seconds
    last_failed_age: IntGauge,       // pg_stat_archiver_last_failed_age_seconds
}

impl Default for ArchiverCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl ArchiverCollector {
    /// Creates a new `ArchiverCollector`
    ///
    /// # Panics
    ///
    /// Panics if metric creation fails (should never happen with valid metric names)
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        let archived_count = IntCounter::with_opts(Opts::new(
            "pg_stat_archiver_archived_total",
            "Number of WAL files that have been successfully archived",
        ))
        .expect("Failed to create pg_stat_archiver_archived_total");

        let failed_count = IntCounter::with_opts(Opts::new(
            "pg_stat_archiver_failed_total",
            "Number of failed attempts for archiving WAL files",
        ))
        .expect("Failed to create pg_stat_archiver_failed_total");

        let last_archived_age = IntGauge::with_opts(Opts::new(
            "pg_stat_archiver_last_archived_age_seconds",
            "Seconds since last successful WAL archive operation",
        ))
        .expect("Failed to create pg_stat_archiver_last_archived_age_seconds");

        let last_failed_age = IntGauge::with_opts(Opts::new(
            "pg_stat_archiver_last_failed_age_seconds",
            "Seconds since last failed WAL archive operation",
        ))
        .expect("Failed to create pg_stat_archiver_last_failed_age_seconds");

        Self {
            archived_count,
            failed_count,
            last_archived_age,
            last_failed_age,
        }
    }
}

impl Collector for ArchiverCollector {
    fn name(&self) -> &'static str {
        "archiver"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "archiver")
    )]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.archived_count.clone()))?;
        registry.register(Box::new(self.failed_count.clone()))?;
        registry.register(Box::new(self.last_archived_age.clone()))?;
        registry.register(Box::new(self.last_failed_age.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector="archiver", otel.kind="internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let query_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT * FROM pg_stat_archiver",
                db.sql.table = "pg_stat_archiver"
            );

            let row_result = sqlx::query(
                r"
                SELECT
                    archived_count,
                    failed_count,
                    EXTRACT(EPOCH FROM (NOW() - last_archived_time))::bigint AS last_archived_age,
                    EXTRACT(EPOCH FROM (NOW() - last_failed_time))::bigint AS last_failed_age
                FROM pg_stat_archiver
                ",
            )
            .fetch_one(pool)
            .instrument(query_span)
            .await;

            let row = match row_result {
                Ok(row) => row,
                Err(e) => {
                    // pg_stat_archiver should be available in all supported versions
                    // but handle gracefully just in case
                    if e.to_string().contains("pg_stat_archiver") {
                        debug!("pg_stat_archiver view not found");
                        return Ok(());
                    }
                    return Err(e.into());
                }
            };

            let archived_count: i64 = row.try_get("archived_count")?;
            let failed_count: i64 = row.try_get("failed_count")?;
            let last_archived_age: Option<i64> = row.try_get("last_archived_age").ok();
            let last_failed_age: Option<i64> = row.try_get("last_failed_age").ok();

            // Reset and set the counter values
            self.archived_count.reset();
            self.failed_count.reset();

            self.archived_count.inc_by(u64::try_from(archived_count).unwrap_or(0));
            self.failed_count.inc_by(u64::try_from(failed_count).unwrap_or(0));

            // Set age gauges (may be NULL if never archived/failed)
            if let Some(age) = last_archived_age {
                self.last_archived_age.set(age);
            }
            if let Some(age) = last_failed_age {
                self.last_failed_age.set(age);
            }

            debug!(
                archived_count,
                failed_count,
                last_archived_age = ?last_archived_age,
                last_failed_age = ?last_failed_age,
                "updated archiver metrics"
            );

            Ok(())
        })
    }

    fn enabled_by_default(&self) -> bool {
        true
    }
}
