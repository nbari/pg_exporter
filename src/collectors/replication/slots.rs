use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{GaugeVec, Opts, Registry};
use sqlx::{PgPool, Row};
use tracing::{debug, info_span, instrument};
use tracing_futures::Instrument as _;

/// Tracks `pg_replication_slots` metrics
/// Compatible with `postgres_exporter`'s `pg_replication_slots` namespace
///
/// Metrics (with labels: `slot_name`, `slot_type`, database, active):
/// - `pg_replication_slots_pg_wal_lsn_diff`
/// - `pg_replication_slots_active` (1 if active, 0 if not)
#[derive(Clone)]
pub struct ReplicationSlotsCollector {
    wal_lsn_diff: GaugeVec,
    active: GaugeVec,
}

impl Default for ReplicationSlotsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicationSlotsCollector {
    /// Creates a new `SlotsSubCollector`
    ///
    /// # Panics
    ///
    /// Panics if metric creation fails (should never happen with valid metric names)
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        let labels = &["slot_name", "slot_type", "database"];

        let wal_lsn_diff = GaugeVec::new(
            Opts::new(
                "pg_replication_slots_pg_wal_lsn_diff",
                "Replication slot lag in bytes",
            ),
            labels,
        )
        .expect("Failed to create pg_replication_slots_pg_wal_lsn_diff");

        let active = GaugeVec::new(
            Opts::new(
                "pg_replication_slots_active",
                "Whether the replication slot is active (1) or inactive (0)",
            ),
            labels,
        )
        .expect("Failed to create pg_replication_slots_active");

        Self {
            wal_lsn_diff,
            active,
        }
    }
}

impl Collector for ReplicationSlotsCollector {
    fn name(&self) -> &'static str {
        "replication_slots"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "replication_slots")
    )]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.wal_lsn_diff.clone()))?;
        registry.register(Box::new(self.active.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector="replication_slots", otel.kind="internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let query_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT * FROM pg_replication_slots with WAL metrics",
                db.sql.table = "pg_replication_slots"
            );

            // Compatible with postgres_exporter for PG >= 10
            let rows = sqlx::query(
                r"
                SELECT
                    slot_name,
                    slot_type,
                    COALESCE(database, '') AS database,
                    active,
                    (CASE pg_is_in_recovery() 
                        WHEN 't' THEN pg_wal_lsn_diff(pg_last_wal_receive_lsn(), restart_lsn) 
                        ELSE pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) 
                    END) AS pg_wal_lsn_diff
                FROM pg_replication_slots
                ",
            )
            .fetch_all(pool)
            .instrument(query_span)
            .await?;

            // Reset all metrics
            self.wal_lsn_diff.reset();
            self.active.reset();

            for row in &rows {
                let slot_name: String = row.try_get("slot_name").unwrap_or_default();
                let slot_type: String = row.try_get("slot_type").unwrap_or_default();
                let database: String = row.try_get("database").unwrap_or_default();
                let is_active: bool = row.try_get("active").unwrap_or(false);
                let lsn_diff: f64 = row.try_get("pg_wal_lsn_diff").unwrap_or(0.0);

                self.wal_lsn_diff
                    .with_label_values(&[&slot_name, &slot_type, &database])
                    .set(lsn_diff);

                self.active
                    .with_label_values(&[&slot_name, &slot_type, &database])
                    .set(if is_active { 1.0 } else { 0.0 });

                debug!(
                    slot_name = %slot_name,
                    slot_type = %slot_type,
                    database = %database,
                    active = is_active,
                    wal_lsn_diff = lsn_diff,
                    "collected pg_replication_slots metric"
                );
            }

            debug!(slots_count = rows.len(), "collected replication slots metrics");

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replication_slots_collector_name() {
        let collector = ReplicationSlotsCollector::new();
        assert_eq!(collector.name(), "replication_slots");
    }

    #[test]
    #[allow(clippy::expect_used)]
    fn test_replication_slots_collector_registers_without_error() {
        let collector = ReplicationSlotsCollector::new();
        let registry = Registry::new();
        assert!(collector.register_metrics(&registry).is_ok());
    }

    #[tokio::test]
    #[allow(clippy::expect_used)]
    async fn test_replication_slots_collector_collection() {
        let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| String::new());

        if database_url.is_empty() {
            eprintln!("Skipping test: DATABASE_URL not set");
            return;
        }

        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .expect("Failed to connect to test database");

        let collector = ReplicationSlotsCollector::new();
        let result = collector.collect(&pool).await;

        // Should succeed even if there are no replication slots
        assert!(result.is_ok(), "Collection failed: {:?}", result.err());
    }
}
