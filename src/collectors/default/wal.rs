use crate::collectors::{NO_LABELS, Collected, Collector};
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

/// Exposes `PostgreSQL` WAL statistics from `pg_stat_wal`:
/// - `pg_stat_wal_records_total` (`Counter`)
/// - `pg_stat_wal_fpi_total` (`Counter`)
/// - `pg_stat_wal_bytes_total` (`Counter`)
/// - `pg_stat_wal_buffers_full_total` (`Counter`)
#[derive(Clone)]
pub struct WalCollector {
    records: IntCounterVec,      // pg_stat_wal_records_total
    fpi: IntCounterVec,           // pg_stat_wal_fpi_total
    bytes: IntCounterVec,         // pg_stat_wal_bytes_total
    buffers_full: IntCounterVec,  // pg_stat_wal_buffers_full_total
    /// Ensures the missing-privilege warning is logged at most once per process.
    denied_warned: Arc<AtomicBool>,
}

impl Default for WalCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl WalCollector {
    /// Creates a new `WalCollector`
    ///
    /// # Panics
    ///
    /// Panics if metric creation fails (should never happen with valid metric names)
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        let wal_records = IntCounterVec::new(Opts::new(
            "pg_stat_wal_records_total",
            "Total number of WAL records generated",
        ), &[])
        .expect("Failed to create pg_stat_wal_records_total");

        let wal_fpi = IntCounterVec::new(Opts::new(
            "pg_stat_wal_fpi_total",
            "Total number of WAL full page images generated",
        ), &[])
        .expect("Failed to create pg_stat_wal_fpi_total");

        let wal_bytes = IntCounterVec::new(Opts::new(
            "pg_stat_wal_bytes_total",
            "Total amount of WAL bytes generated",
        ), &[])
        .expect("Failed to create pg_stat_wal_bytes_total");

        let wal_buffers_full = IntCounterVec::new(Opts::new(
            "pg_stat_wal_buffers_full_total",
            "Number of times WAL data was written to disk because WAL buffers became full",
        ), &[])
        .expect("Failed to create pg_stat_wal_buffers_full_total");

        Self {
            denied_warned: Arc::new(AtomicBool::new(false)),
            records: wal_records,
            fpi: wal_fpi,
            bytes: wal_bytes,
            buffers_full: wal_buffers_full,
        }
    }

    /// Classifies a failed `pg_stat_wal` read.
    ///
    /// This used to match the *error message* for the view name, which reads a permission
    /// error as an absent view, since the view name appears in both messages. `SQLSTATE`
    /// separates them. Neither case is an `Err`: the registry treats any collector error as
    /// fatal for the whole scrape, so "I cannot read my source" degrades to a skip.
    fn handle_query_error(&self, error: sqlx::Error) -> Result<Collected> {
        let code = match &error {
            sqlx::Error::Database(db_error) => db_error.code().map(|code| code.to_string()),
            _ => None,
        };

        match code.as_deref() {
            Some(UNDEFINED_TABLE) => {
                debug!("Skipping pg_stat_wal metrics (view not found)");
                Ok(Collected::Skipped)
            }
            Some(INSUFFICIENT_PRIVILEGE) => {
                if !self.denied_warned.swap(true, Ordering::Relaxed) {
                    warn!(
                        "collector.default is enabled but the exporter role may not read \
                         pg_stat_wal; grant pg_monitor to expose WAL generation metrics \
                         (GRANT pg_monitor TO <exporter role>)"
                    );
                }
                debug!("Skipping pg_stat_wal metrics (insufficient privilege)");
                Ok(Collected::Skipped)
            }
            _ => Err(error.into()),
        }
    }
}

impl Collector for WalCollector {
    fn name(&self) -> &'static str {
        "wal"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "wal")
    )]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.records.clone()))?;
        registry.register(Box::new(self.fpi.clone()))?;
        registry.register(Box::new(self.bytes.clone()))?;
        registry.register(Box::new(self.buffers_full.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector="wal", otel.kind="internal")
    )]
    fn collect_once<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<Collected>> {
        Box::pin(async move {
            let query_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT * FROM pg_stat_wal",
                db.sql.table = "pg_stat_wal"
            );

            let row_result = sqlx::query(
                r"
                SELECT
                    wal_records,
                    wal_fpi,
                    wal_bytes::bigint AS wal_bytes,
                    wal_buffers_full
                FROM pg_stat_wal
                ",
            )
            .fetch_one(pool)
            .instrument(query_span)
            .await;

            let row = match row_result {
                Ok(row) => row,
                Err(error) => return self.handle_query_error(error),
            };

            let wal_records: i64 = row.try_get("wal_records")?;
            let wal_fpi: i64 = row.try_get("wal_fpi")?;
            let wal_bytes: i64 = row.try_get("wal_bytes")?;
            let wal_buffers_full: i64 = row.try_get("wal_buffers_full")?;

            // Reset and set the counter values
            self.records.reset();
            self.fpi.reset();
            self.bytes.reset();
            self.buffers_full.reset();

            self.records.with_label_values(&NO_LABELS).inc_by(u64::try_from(wal_records).unwrap_or(0));
            self.fpi.with_label_values(&NO_LABELS).inc_by(u64::try_from(wal_fpi).unwrap_or(0));
            self.bytes.with_label_values(&NO_LABELS).inc_by(u64::try_from(wal_bytes).unwrap_or(0));
            self.buffers_full.with_label_values(&NO_LABELS).inc_by(u64::try_from(wal_buffers_full).unwrap_or(0));

            debug!(
                wal_records,
                wal_fpi,
                wal_bytes,
                wal_buffers_full,
                "updated WAL metrics"
            );

            Ok(Collected::Fresh)
        })
    }

    /// Removes the WAL series. Zero-label vectors, so a skip makes them absent instead
    /// of publishing zero WAL activity.
    fn reset_metrics(&self) {
        self.records.reset();
        self.fpi.reset();
        self.bytes.reset();
        self.buffers_full.reset();
    }

    fn enabled_by_default(&self) -> bool {
        true
    }
}
