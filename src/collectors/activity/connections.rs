use crate::collectors::{Collector, util::get_excluded_databases};
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{IntGaugeVec, Opts, Registry};
use sqlx::{PgPool, Row};
use std::collections::{HashMap, HashSet};
use tracing::{debug, info_span, instrument};
use tracing_futures::Instrument as _;

/// Tracks PostgreSQL connections
/// - pg_stat_activity_count{datname, state}
/// - pg_stat_activity_active_connections{datname}
/// - pg_stat_activity_idle_connections{datname}
/// - pg_stat_activity_waiting_connections{datname}
/// - pg_stat_activity_blocked_connections{datname}
#[derive(Clone)]
pub struct ConnectionsCollector {
    // Compatibility metric: counts by database and state
    count_by_state: IntGaugeVec, // pg_stat_activity_count{datname,state}

    // Convenience per-database gauges
    active_connections: IntGaugeVec, // pg_stat_activity_active_connections{datname}
    idle_connections: IntGaugeVec,   // pg_stat_activity_idle_connections{datname}
    waiting_connections: IntGaugeVec, // pg_stat_activity_waiting_connections{datname}
    blocked_connections: IntGaugeVec, // pg_stat_activity_blocked_connections{datname}
}

impl Default for ConnectionsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionsCollector {
    pub fn new() -> Self {
        let count_by_state = IntGaugeVec::new(
            Opts::new(
                "pg_stat_activity_count",
                "Number of client backends by database and state (from pg_stat_activity)",
            ),
            &["datname", "state"],
        )
        .expect("Failed to create pg_stat_activity_count");

        let active_connections = IntGaugeVec::new(
            Opts::new(
                "pg_stat_activity_active_connections",
                "Number of active client connections per database",
            ),
            &["datname"],
        )
        .expect("Failed to create pg_stat_activity_active_connections");

        let idle_connections = IntGaugeVec::new(
            Opts::new(
                "pg_stat_activity_idle_connections",
                "Number of idle client connections per database",
            ),
            &["datname"],
        )
        .expect("Failed to create pg_stat_activity_idle_connections");

        let waiting_connections = IntGaugeVec::new(
            Opts::new(
                "pg_stat_activity_waiting_connections",
                "Number of client connections currently waiting (wait_event IS NOT NULL) per database",
            ),
            &["datname"],
        )
        .expect("Failed to create pg_stat_activity_waiting_connections");

        let blocked_connections = IntGaugeVec::new(
            Opts::new(
                "pg_stat_activity_blocked_connections",
                "Number of client connections blocked by locks per database",
            ),
            &["datname"],
        )
        .expect("Failed to create pg_stat_activity_blocked_connections");

        Self {
            count_by_state,
            active_connections,
            idle_connections,
            waiting_connections,
            blocked_connections,
        }
    }
}

impl Collector for ConnectionsCollector {
    fn name(&self) -> &'static str {
        "connections"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "connections")
    )]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        // Register all metrics in the provided registry
        registry.register(Box::new(self.count_by_state.clone()))?;
        registry.register(Box::new(self.active_connections.clone()))?;
        registry.register(Box::new(self.idle_connections.clone()))?;
        registry.register(Box::new(self.waiting_connections.clone()))?;
        registry.register(Box::new(self.blocked_connections.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector="connections", otel.kind="internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            // Build exclusion list from global OnceCell (set at startup via Clap/env).
            let excluded: Vec<String> = get_excluded_databases().to_vec();

            // 1) Compatibility metric: count by state
            //    Only count client backends to avoid background processes.
            let q_state = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT datname, state, COUNT(*) FROM pg_stat_activity (filtered)",
                db.sql.table = "pg_stat_activity"
            );

            let state_rows = sqlx::query(
                r#"
                SELECT
                    datname,
                    COALESCE(state, 'unknown') AS state,
                    COUNT(*)::bigint AS cnt
                FROM pg_stat_activity
                WHERE backend_type = 'client backend'
                  AND pid != pg_backend_pid()
                  AND NOT (COALESCE(datname, '') = ANY($1))
                GROUP BY datname, COALESCE(state, 'unknown')
                ORDER BY datname, COALESCE(state, 'unknown')
                "#,
            )
            .bind(&excluded)
            .fetch_all(pool)
            .instrument(q_state)
            .await?;

            let mut dbs_seen: HashSet<String> = HashSet::new();
            let mut active_map: HashMap<String, i64> = HashMap::new();
            let mut idle_map: HashMap<String, i64> = HashMap::new();

            for row in &state_rows {
                let db: String = row
                    .try_get::<Option<String>, _>("datname")?
                    .unwrap_or_else(|| "[unknown]".to_string());
                let state: String = row.try_get::<String, _>("state")?;
                let cnt: i64 = row.try_get::<i64, _>("cnt").unwrap_or(0);

                dbs_seen.insert(db.clone());

                // Emit pg_stat_activity_count
                self.count_by_state
                    .with_label_values(&[&db, &state])
                    .set(cnt);

                // Track active/idle for convenience gauges later
                if state == "active" {
                    active_map.insert(db.clone(), cnt);
                } else if state == "idle" {
                    idle_map.insert(db.clone(), cnt);
                }
            }

            // After processing all states, set per-db active/idle (default 0 if missing)
            for db in &dbs_seen {
                let a = *active_map.get(db).unwrap_or(&0);
                let i = *idle_map.get(db).unwrap_or(&0);
                self.active_connections.with_label_values(&[db]).set(a);
                self.idle_connections.with_label_values(&[db]).set(i);
                debug!(database=%db, active=a, idle=i, "set active/idle gauges");
            }

            // 2) Waiting and blocked connections per database
            // Use pg_blocking_pids(pid) to avoid the heavier pg_locks self-join.
            let q_wait_block = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT wait/blocked per db from pg_stat_activity (filtered + pg_blocking_pids)",
                db.sql.table = "pg_stat_activity"
            );

            let wait_block_rows = sqlx::query(
                r#"
                SELECT
                    a.datname,
                    COUNT(*) FILTER (WHERE a.wait_event IS NOT NULL)::bigint AS waiting,
                    COUNT(*) FILTER (WHERE cardinality(pg_blocking_pids(a.pid)) > 0)::bigint AS blocked
                FROM pg_stat_activity a
                WHERE a.backend_type = 'client backend'
                  AND a.pid != pg_backend_pid()
                  AND NOT (COALESCE(a.datname, '') = ANY($1))
                GROUP BY a.datname
                ORDER BY a.datname
                "#,
            )
            .bind(&excluded)
            .fetch_all(pool)
            .instrument(q_wait_block)
            .await?;

            let mut waiting_map: HashMap<String, i64> = HashMap::new();
            let mut blocked_map: HashMap<String, i64> = HashMap::new();

            for row in &wait_block_rows {
                let db: String = row
                    .try_get::<Option<String>, _>("datname")?
                    .unwrap_or_else(|| "[unknown]".to_string());
                let waiting: i64 = row.try_get::<i64, _>("waiting").unwrap_or(0);
                let blocked: i64 = row.try_get::<i64, _>("blocked").unwrap_or(0);

                dbs_seen.insert(db.clone());
                waiting_map.insert(db.clone(), waiting);
                blocked_map.insert(db.clone(), blocked);

                self.waiting_connections
                    .with_label_values(&[&db])
                    .set(waiting);
                self.blocked_connections
                    .with_label_values(&[&db])
                    .set(blocked);

                debug!(database=%db, waiting, blocked, "set waiting/blocked gauges");
            }

            // Ensure zeroes for databases seen in state query but not in wait/blocked
            for db in &dbs_seen {
                if !waiting_map.contains_key(db) {
                    self.waiting_connections.with_label_values(&[db]).set(0);
                }
                if !blocked_map.contains_key(db) {
                    self.blocked_connections.with_label_values(&[db]).set(0);
                }
            }

            Ok(())
        })
    }

    fn enabled_by_default(&self) -> bool {
        false
    }
}
