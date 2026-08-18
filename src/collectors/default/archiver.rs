use crate::collectors::{NO_LABELS, Collected, Collector};
use crate::collectors::util::{INSUFFICIENT_PRIVILEGE, UNDEFINED_TABLE};
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{IntCounterVec, IntGaugeVec, Opts, Registry};
use sqlx::{PgPool, Row};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tracing::{debug, info_span, instrument, warn};
use tracing_futures::Instrument as _;

/// Exposes `PostgreSQL` archiver statistics from `pg_stat_archiver`:
/// - `pg_stat_archiver_archived_total` (`Counter`)
/// - `pg_stat_archiver_failed_total` (`Counter`)
/// - `pg_stat_archiver_last_archived_age_seconds` (`Gauge`)
/// - `pg_stat_archiver_last_failed_age_seconds` (`Gauge`)
#[derive(Clone)]
pub struct ArchiverCollector {
    archived_count: IntCounterVec,      // pg_stat_archiver_archived_total
    failed_count: IntCounterVec,        // pg_stat_archiver_failed_total
    last_archived_age: IntGaugeVec,     // pg_stat_archiver_last_archived_age_seconds
    last_failed_age: IntGaugeVec,       // pg_stat_archiver_last_failed_age_seconds
    /// Ensures the missing-privilege warning is logged at most once per process.
    denied_warned: Arc<AtomicBool>,
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
        let archived_count = IntCounterVec::new(Opts::new(
            "pg_stat_archiver_archived_total",
            "Number of WAL files that have been successfully archived",
        ), &[])
        .expect("Failed to create pg_stat_archiver_archived_total");

        let failed_count = IntCounterVec::new(Opts::new(
            "pg_stat_archiver_failed_total",
            "Number of failed attempts for archiving WAL files",
        ), &[])
        .expect("Failed to create pg_stat_archiver_failed_total");

        let last_archived_age = IntGaugeVec::new(Opts::new(
            "pg_stat_archiver_last_archived_age_seconds",
            "Seconds since last successful WAL archive operation",
        ), &[])
        .expect("Failed to create pg_stat_archiver_last_archived_age_seconds");

        let last_failed_age = IntGaugeVec::new(Opts::new(
            "pg_stat_archiver_last_failed_age_seconds",
            "Seconds since last failed WAL archive operation",
        ), &[])
        .expect("Failed to create pg_stat_archiver_last_failed_age_seconds");

        Self {
            denied_warned: Arc::new(AtomicBool::new(false)),
            archived_count,
            failed_count,
            last_archived_age,
            last_failed_age,
        }
    }

    /// Classifies a failed `pg_stat_archiver` read.
    ///
    /// This used to match the *error message* for the view name, which reads a permission
    /// error as an absent view: the view name appears in both messages. A missing `GRANT`
    /// was therefore indistinguishable from an old server, and backup-critical archiving
    /// metrics disappeared with no explanation. `SQLSTATE` separates them.
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
                debug!("Skipping pg_stat_archiver metrics (view not found)");
                Ok(Collected::Skipped)
            }
            Some(INSUFFICIENT_PRIVILEGE) => {
                if !self.denied_warned.swap(true, Ordering::Relaxed) {
                    warn!(
                        "collector.default is enabled but the exporter role may not read \
                         pg_stat_archiver; grant pg_monitor to expose WAL archiving metrics \
                         (GRANT pg_monitor TO <exporter role>)"
                    );
                }
                debug!("Skipping pg_stat_archiver metrics (insufficient privilege)");
                Ok(Collected::Skipped)
            }
            _ => Err(error.into()),
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
    fn collect_once<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<Collected>> {
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
                Err(error) => return self.handle_query_error(error),
            };

            let archived_count: i64 = row.try_get("archived_count")?;
            let failed_count: i64 = row.try_get("failed_count")?;
            // Decoded with `?`, not `.ok()`: a real decode failure must surface instead of
            // being flattened into an indistinguishable NULL.
            let last_archived_age: Option<i64> = row.try_get("last_archived_age")?;
            let last_failed_age: Option<i64> = row.try_get("last_failed_age")?;

            // Reset and set the counter values
            self.archived_count.reset();
            self.failed_count.reset();

            self.archived_count.with_label_values(&NO_LABELS).inc_by(u64::try_from(archived_count).unwrap_or(0));
            self.failed_count.with_label_values(&NO_LABELS).inc_by(u64::try_from(failed_count).unwrap_or(0));

            // NULL means "never archived" / "never failed", so there is no age to report.
            //
            // Publishing 0 would assert "archived 0 seconds ago", which is false and can
            // mask an archiver that has never worked: an operator reading 0 concludes
            // archiving is healthy, and an `age > threshold` alert never fires. Removing the
            // series says "unknown" instead, matching how the checkpointer treats a NULL
            // `pg_last_wal_replay_lsn()`. Guard age alerts with `absent()`.
            //
            // Leaving the previous value would be worse still: before this, a
            // `pg_stat_reset_shared('archiver')` pinned the gauge at its last value forever.
            match last_archived_age {
                Some(age) => self.last_archived_age.with_label_values(&NO_LABELS).set(age),
                None => self.last_archived_age.reset(),
            }
            match last_failed_age {
                Some(age) => self.last_failed_age.with_label_values(&NO_LABELS).set(age),
                None => self.last_failed_age.reset(),
            }

            debug!(
                archived_count,
                failed_count,
                last_archived_age = ?last_archived_age,
                last_failed_age = ?last_failed_age,
                "updated archiver metrics"
            );

            Ok(Collected::Fresh)
        })
    }

    /// Removes the archiver series. All four are zero-label vectors, so a skip makes
    /// them absent rather than reporting a fabricated `0` archived count or an age of
    /// zero seconds ("just archived").
    fn reset_metrics(&self) {
        self.archived_count.reset();
        self.failed_count.reset();
        self.last_archived_age.reset();
        self.last_failed_age.reset();
    }

    fn enabled_by_default(&self) -> bool {
        true
    }
}
