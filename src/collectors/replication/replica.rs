use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{Gauge, Opts, Registry};
use sqlx::{PgPool, Row};
use tracing::{debug, info_span, instrument};
use tracing_futures::Instrument as _;

/// Tracks replication metrics for standby/replica servers
/// Compatible with postgres_exporter's pg_replication namespace
///
/// Metrics:
/// - pg_replication_lag_seconds (Gauge)
/// - pg_replication_is_replica (Gauge)
/// - pg_replication_last_replay_seconds (Gauge)
#[derive(Clone)]
pub struct ReplicaCollector {
    lag_seconds: Gauge,
    is_replica: Gauge,
    last_replay_seconds: Gauge,
}

impl Default for ReplicaCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicaCollector {
    pub fn new() -> Self {
        let lag_seconds = Gauge::with_opts(Opts::new(
            "pg_replication_lag_seconds",
            "Replication lag behind primary in seconds",
        ))
        .expect("Failed to create pg_replication_lag_seconds");

        let is_replica = Gauge::with_opts(Opts::new(
            "pg_replication_is_replica",
            "Indicates if the server is a replica (1) or primary (0)",
        ))
        .expect("Failed to create pg_replication_is_replica");

        let last_replay_seconds = Gauge::with_opts(Opts::new(
            "pg_replication_last_replay_seconds",
            "Age of last transaction replay in seconds",
        ))
        .expect("Failed to create pg_replication_last_replay_seconds");

        Self {
            lag_seconds,
            is_replica,
            last_replay_seconds,
        }
    }
}

impl Collector for ReplicaCollector {
    fn name(&self) -> &'static str {
        "replication_replica"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "replication_replica")
    )]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.lag_seconds.clone()))?;
        registry.register(Box::new(self.is_replica.clone()))?;
        registry.register(Box::new(self.last_replay_seconds.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector="replication_replica", otel.kind="internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let query_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT replication lag and replica status",
                db.sql.table = "pg_is_in_recovery, pg_last_wal_receive_lsn, pg_last_wal_replay_lsn, pg_last_xact_replay_timestamp"
            );

            // Query compatible with postgres_exporter
            let row = sqlx::query(
                r#"
                SELECT
                    CASE
                        WHEN NOT pg_is_in_recovery() THEN 0
                        WHEN pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn() THEN 0
                        ELSE GREATEST(0, EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp())))
                    END AS lag,
                    CASE
                        WHEN pg_is_in_recovery() THEN 1
                        ELSE 0
                    END AS is_replica,
                    GREATEST(0, EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))) AS last_replay
                "#,
            )
            .fetch_one(pool)
            .instrument(query_span)
            .await?;

            let lag: f64 = row.try_get("lag").unwrap_or(0.0);
            let replica: i32 = row.try_get("is_replica").unwrap_or(0);
            let last_replay: f64 = row.try_get("last_replay").unwrap_or(0.0);

            self.lag_seconds.set(lag);
            self.is_replica.set(replica as f64);
            self.last_replay_seconds.set(last_replay);

            debug!(
                lag_seconds = lag,
                is_replica = replica,
                last_replay_seconds = last_replay,
                "collected replication replica metrics"
            );

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replica_collector_name() {
        let collector = ReplicaCollector::new();
        assert_eq!(collector.name(), "replication_replica");
    }

    #[test]
    fn test_replica_collector_registers_without_error() {
        let collector = ReplicaCollector::new();
        let registry = Registry::new();
        assert!(collector.register_metrics(&registry).is_ok());
    }

    #[tokio::test]
    async fn test_replica_collector_metrics_on_primary() {
        let database_url =
            std::env::var("DATABASE_URL").unwrap_or_else(|_| "".to_string());

        if database_url.is_empty() {
            eprintln!("Skipping test: DATABASE_URL not set");
            return;
        }

        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .expect("Failed to connect to test database");

        let collector = ReplicaCollector::new();
        let result = collector.collect(&pool).await;

        assert!(result.is_ok(), "Collection failed: {:?}", result.err());

        // On a primary, is_replica should be 0
        let is_replica_val = collector.is_replica.get();
        assert!(
            is_replica_val == 0.0 || is_replica_val == 1.0,
            "is_replica should be 0 or 1"
        );

        // Lag should be non-negative
        let lag_val = collector.lag_seconds.get();
        assert!(lag_val >= 0.0, "lag should be non-negative");
    }
}
