use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{GaugeVec, Opts, Registry};
use sqlx::{PgPool, Row};
use tracing::{debug, info_span, instrument};
use tracing_futures::Instrument as _;

/// Tracks pg_stat_replication metrics for primary servers
/// Compatible with postgres_exporter's pg_stat_replication namespace
///
/// Metrics (all with labels: application_name, client_addr, state):
/// - pg_stat_replication_pg_current_wal_lsn_bytes
/// - pg_stat_replication_pg_wal_lsn_diff
/// - pg_stat_replication_reply_time
///
/// Additional metrics:
/// - pg_stat_replication_slots (count of replication slots by application_name and state)
#[derive(Clone)]
pub struct StatReplicationCollector {
    current_wal_lsn_bytes: GaugeVec,
    wal_lsn_diff: GaugeVec,
    reply_time: GaugeVec,
    slots: GaugeVec,
}

impl Default for StatReplicationCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl StatReplicationCollector {
    pub fn new() -> Self {
        let labels = &["application_name", "client_addr", "state"];

        let current_wal_lsn_bytes = GaugeVec::new(
            Opts::new(
                "pg_stat_replication_pg_current_wal_lsn_bytes",
                "Current WAL LSN on primary in bytes",
            ),
            labels,
        )
        .expect("Failed to create pg_stat_replication_pg_current_wal_lsn_bytes");

        let wal_lsn_diff = GaugeVec::new(
            Opts::new(
                "pg_stat_replication_pg_wal_lsn_diff",
                "Lag in bytes between primary WAL LSN and replica replay LSN",
            ),
            labels,
        )
        .expect("Failed to create pg_stat_replication_pg_wal_lsn_diff");

        let reply_time = GaugeVec::new(
            Opts::new(
                "pg_stat_replication_reply_time",
                "Time since last reply from replica in seconds",
            ),
            labels,
        )
        .expect("Failed to create pg_stat_replication_reply_time");

        let slots = GaugeVec::new(
            Opts::new(
                "pg_stat_replication_slots",
                "Number of replication slots by application and state",
            ),
            &["application_name", "state"],
        )
        .expect("Failed to create pg_stat_replication_slots");

        Self {
            current_wal_lsn_bytes,
            wal_lsn_diff,
            reply_time,
            slots,
        }
    }
}

impl Collector for StatReplicationCollector {
    fn name(&self) -> &'static str {
        "stat_replication"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "stat_replication")
    )]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.current_wal_lsn_bytes.clone()))?;
        registry.register(Box::new(self.wal_lsn_diff.clone()))?;
        registry.register(Box::new(self.reply_time.clone()))?;
        registry.register(Box::new(self.slots.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector="stat_replication", otel.kind="internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let query_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT * FROM pg_stat_replication with WAL metrics",
                db.sql.table = "pg_stat_replication"
            );

            // Compatible with postgres_exporter for PG >= 10
            let rows = sqlx::query(
                r#"
                SELECT
                    application_name,
                    COALESCE(client_addr::text, '') AS client_addr,
                    state,
                    (CASE pg_is_in_recovery() 
                        WHEN 't' THEN pg_last_wal_receive_lsn() 
                        ELSE pg_current_wal_lsn() 
                    END) AS pg_current_wal_lsn,
                    (CASE pg_is_in_recovery() 
                        WHEN 't' THEN pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_lsn('0/0'))::float 
                        ELSE pg_wal_lsn_diff(pg_current_wal_lsn(), pg_lsn('0/0'))::float 
                    END) AS pg_current_wal_lsn_bytes,
                    (CASE pg_is_in_recovery() 
                        WHEN 't' THEN pg_wal_lsn_diff(pg_last_wal_receive_lsn(), replay_lsn)::float 
                        ELSE pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)::float 
                    END) AS pg_wal_lsn_diff,
                    EXTRACT(EPOCH FROM (now() - reply_time)) AS reply_time_seconds
                FROM pg_stat_replication
                "#,
            )
            .fetch_all(pool)
            .instrument(query_span)
            .await?;

            // Reset all metrics
            self.current_wal_lsn_bytes.reset();
            self.wal_lsn_diff.reset();
            self.reply_time.reset();
            self.slots.reset();

            // Track seen combinations for slot counting
            let mut slot_counts: std::collections::HashMap<(String, String), i64> =
                std::collections::HashMap::new();

            for row in &rows {
                let app_name: String = row.try_get("application_name").unwrap_or_default();
                let client_addr: String = row.try_get("client_addr").unwrap_or_default();
                let state: String = row.try_get("state").unwrap_or_default();
                let current_wal_bytes: f64 = row.try_get("pg_current_wal_lsn_bytes").unwrap_or(0.0);
                let lsn_diff: f64 = row.try_get("pg_wal_lsn_diff").unwrap_or(0.0);
                let reply_time: f64 = row.try_get("reply_time_seconds").unwrap_or(0.0);

                self.current_wal_lsn_bytes
                    .with_label_values(&[&app_name, &client_addr, &state])
                    .set(current_wal_bytes);

                self.wal_lsn_diff
                    .with_label_values(&[&app_name, &client_addr, &state])
                    .set(lsn_diff);

                self.reply_time
                    .with_label_values(&[&app_name, &client_addr, &state])
                    .set(reply_time);

                // Count slots
                let key = (app_name.clone(), state.clone());
                *slot_counts.entry(key).or_insert(0) += 1;

                debug!(
                    application_name = %app_name,
                    client_addr = %client_addr,
                    state = %state,
                    wal_lsn_diff = lsn_diff,
                    reply_time_seconds = reply_time,
                    "collected pg_stat_replication metric"
                );
            }

            // Set slot counts
            for ((app_name, state), count) in slot_counts {
                self.slots
                    .with_label_values(&[&app_name, &state])
                    .set(count as f64);
            }

            debug!(
                replication_slots = rows.len(),
                "collected stat_replication metrics"
            );

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stat_replication_collector_name() {
        let collector = StatReplicationCollector::new();
        assert_eq!(collector.name(), "stat_replication");
    }

    #[test]
    fn test_stat_replication_collector_registers_without_error() {
        let collector = StatReplicationCollector::new();
        let registry = Registry::new();
        assert!(collector.register_metrics(&registry).is_ok());
    }

    #[tokio::test]
    async fn test_stat_replication_collector_on_primary() {
        let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| "".to_string());

        if database_url.is_empty() {
            eprintln!("Skipping test: DATABASE_URL not set");
            return;
        }

        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .expect("Failed to connect to test database");

        let collector = StatReplicationCollector::new();
        let result = collector.collect(&pool).await;

        // Should succeed even if there are no replicas
        assert!(result.is_ok(), "Collection failed: {:?}", result.err());
    }
}
