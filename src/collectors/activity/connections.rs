use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{GaugeVec, Opts, Registry};
use sqlx::{PgPool, Row};
use tracing::info;

/// Tracks PostgreSQL active queries and wait events
#[derive(Clone)]
pub struct ConnectionsCollector {
    active_connections: GaugeVec,
    waiting_connections: GaugeVec,
    blocked_connections: GaugeVec,
}

impl Default for ConnectionsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionsCollector {
    pub fn new() -> Self {
        let active_connections = GaugeVec::new(
            Opts::new(
                "pg_activity_active_connections",
                "Number of active connections per database",
            ),
            &["database"],
        )
        .expect("Failed to create active_connections metric");

        let waiting_connections = GaugeVec::new(
            Opts::new(
                "pg_activity_waiting_connections",
                "Number of connections currently waiting for a lock per database",
            ),
            &["database"],
        )
        .expect("Failed to create waiting_connections metric");

        let blocked_connections = GaugeVec::new(
            Opts::new(
                "pg_activity_blocked_connections",
                "Number of blocked connections per database",
            ),
            &["database"],
        )
        .expect("Failed to create blocked_connections metric");

        Self {
            active_connections,
            waiting_connections,
            blocked_connections,
        }
    }
}

impl Collector for ConnectionsCollector {
    fn name(&self) -> &'static str {
        "activity"
    }

    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.active_connections.clone()))?;
        registry.register(Box::new(self.waiting_connections.clone()))?;
        registry.register(Box::new(self.blocked_connections.clone()))?;
        Ok(())
    }

    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            // Count active connections per database
            let rows = sqlx::query(
                r#"
SELECT
    COALESCE(datname, '[background]') AS datname,
    COUNT(*) FILTER (WHERE state = 'active') AS active,
    COUNT(*) FILTER (WHERE wait_event IS NOT NULL) AS waiting,
    COUNT(*) FILTER (WHERE pid IN (
        SELECT blocked_locks.pid
        FROM pg_locks blocked_locks
        JOIN pg_locks blocking_locks
          ON blocked_locks.locktype = blocking_locks.locktype
         AND blocked_locks.database IS NOT DISTINCT FROM blocking_locks.database
         AND blocked_locks.relation IS NOT DISTINCT FROM blocking_locks.relation
         AND blocked_locks.page IS NOT DISTINCT FROM blocking_locks.page
         AND blocked_locks.tuple IS NOT DISTINCT FROM blocking_locks.tuple
         AND blocked_locks.virtualxid IS NOT DISTINCT FROM blocking_locks.virtualxid
         AND blocked_locks.transactionid IS NOT DISTINCT FROM blocking_locks.transactionid
         AND blocked_locks.classid IS NOT DISTINCT FROM blocking_locks.classid
         AND blocked_locks.objid IS NOT DISTINCT FROM blocking_locks.objid
         AND blocked_locks.objsubid IS NOT DISTINCT FROM blocking_locks.objsubid
         AND blocked_locks.pid != blocking_locks.pid
        WHERE NOT blocked_locks.granted AND blocking_locks.granted
    )) AS blocked,
    COUNT(*) AS total
FROM pg_stat_activity
WHERE pid != pg_backend_pid()  -- Exclude the monitoring query itself
GROUP BY datname
ORDER BY datname;
                "#,
            )
            .fetch_all(pool)
            .await?;

            info!("Collected activity metrics for {} databases", rows.len());

            for row in rows {
                let db: String = row.try_get("datname")?;
                let active: i64 = row.try_get("active").unwrap_or(0);
                let waiting: i64 = row.try_get("waiting").unwrap_or(0);
                let blocked: i64 = row.try_get("blocked").unwrap_or(0);

                self.active_connections
                    .with_label_values(&[&db])
                    .set(active as f64);
                self.waiting_connections
                    .with_label_values(&[&db])
                    .set(waiting as f64);
                self.blocked_connections
                    .with_label_values(&[&db])
                    .set(blocked as f64);
            }

            Ok(())
        })
    }

    fn enabled_by_default(&self) -> bool {
        false
    }
}
