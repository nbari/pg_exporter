use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{IntCounter, Opts, Registry};
use sqlx::{PgPool, Row};
use tracing::{debug, info_span, instrument};
use tracing_futures::Instrument as _;

/// Exposes PostgreSQL WAL statistics from pg_stat_wal:
/// - pg_stat_wal_records_total (Counter)
/// - pg_stat_wal_fpi_total (Counter)
/// - pg_stat_wal_bytes_total (Counter)
/// - pg_stat_wal_buffers_full_total (Counter)
#[derive(Clone)]
pub struct WalCollector {
    wal_records: IntCounter,      // pg_stat_wal_records_total
    wal_fpi: IntCounter,           // pg_stat_wal_fpi_total
    wal_bytes: IntCounter,         // pg_stat_wal_bytes_total
    wal_buffers_full: IntCounter,  // pg_stat_wal_buffers_full_total
}

impl Default for WalCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl WalCollector {
    pub fn new() -> Self {
        let wal_records = IntCounter::with_opts(Opts::new(
            "pg_stat_wal_records_total",
            "Total number of WAL records generated",
        ))
        .expect("Failed to create pg_stat_wal_records_total");

        let wal_fpi = IntCounter::with_opts(Opts::new(
            "pg_stat_wal_fpi_total",
            "Total number of WAL full page images generated",
        ))
        .expect("Failed to create pg_stat_wal_fpi_total");

        let wal_bytes = IntCounter::with_opts(Opts::new(
            "pg_stat_wal_bytes_total",
            "Total amount of WAL bytes generated",
        ))
        .expect("Failed to create pg_stat_wal_bytes_total");

        let wal_buffers_full = IntCounter::with_opts(Opts::new(
            "pg_stat_wal_buffers_full_total",
            "Number of times WAL data was written to disk because WAL buffers became full",
        ))
        .expect("Failed to create pg_stat_wal_buffers_full_total");

        Self {
            wal_records,
            wal_fpi,
            wal_bytes,
            wal_buffers_full,
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
        registry.register(Box::new(self.wal_records.clone()))?;
        registry.register(Box::new(self.wal_fpi.clone()))?;
        registry.register(Box::new(self.wal_bytes.clone()))?;
        registry.register(Box::new(self.wal_buffers_full.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector="wal", otel.kind="internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
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
                r#"
                SELECT
                    wal_records,
                    wal_fpi,
                    wal_bytes::bigint AS wal_bytes,
                    wal_buffers_full
                FROM pg_stat_wal
                "#,
            )
            .fetch_one(pool)
            .instrument(query_span)
            .await;

            let row = match row_result {
                Ok(row) => row,
                Err(e) => {
                    // pg_stat_wal was introduced in PostgreSQL 14
                    if e.to_string().contains("pg_stat_wal") {
                        debug!("pg_stat_wal view not found (requires PostgreSQL 14+)");
                        return Ok(());
                    }
                    return Err(e.into());
                }
            };

            let wal_records: i64 = row.try_get("wal_records")?;
            let wal_fpi: i64 = row.try_get("wal_fpi")?;
            let wal_bytes: i64 = row.try_get("wal_bytes")?;
            let wal_buffers_full: i64 = row.try_get("wal_buffers_full")?;

            // Reset and set the counter values
            self.wal_records.reset();
            self.wal_fpi.reset();
            self.wal_bytes.reset();
            self.wal_buffers_full.reset();

            self.wal_records.inc_by(wal_records as u64);
            self.wal_fpi.inc_by(wal_fpi as u64);
            self.wal_bytes.inc_by(wal_bytes as u64);
            self.wal_buffers_full.inc_by(wal_buffers_full as u64);

            debug!(
                wal_records,
                wal_fpi,
                wal_bytes,
                wal_buffers_full,
                "updated WAL metrics"
            );

            Ok(())
        })
    }

    fn enabled_by_default(&self) -> bool {
        true
    }
}
