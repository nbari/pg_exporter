use crate::collectors::{Collector, util::get_excluded_databases};
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{Gauge, IntGauge, IntGaugeVec, Opts, Registry};
use sqlx::{PgPool, Row};
use std::collections::{HashMap, HashSet};
use tracing::{debug, info_span, instrument};
use tracing_futures::Instrument as _;

/// Tracks PostgreSQL connections and pool saturation
/// 
/// **Existing Metrics (backward compatible):**
/// - pg_stat_activity_count{datname, state}
/// - pg_stat_activity_active_connections{datname}
/// - pg_stat_activity_idle_connections{datname}
/// - pg_stat_activity_waiting_connections{datname}
/// - pg_stat_activity_blocked_connections{datname}
///
/// **New Pool Saturation Metrics (K8s-focused):**
/// - pg_stat_activity_max_connections - Maximum allowed connections
/// - pg_stat_activity_used_connections - Current connections in use
/// - pg_stat_activity_utilization_ratio ⭐ - used/max (0.0-1.0, alert >0.8)
/// - pg_stat_activity_available_connections - Connections remaining
/// - pg_stat_activity_idle_in_transaction{datname} 🔴 - Dangerous state
/// - pg_stat_activity_idle_in_transaction_aborted{datname} - Even worse
/// - pg_stat_activity_connections_by_application{datname, application_name}
/// - pg_stat_activity_idle_age_seconds{datname, bucket} - Idle connection age buckets
#[derive(Clone)]
pub struct ConnectionsCollector {
    // Existing metrics (unchanged for backward compatibility)
    count_by_state: IntGaugeVec, // pg_stat_activity_count{datname,state}
    active_connections: IntGaugeVec, // pg_stat_activity_active_connections{datname}
    idle_connections: IntGaugeVec,   // pg_stat_activity_idle_connections{datname}
    waiting_connections: IntGaugeVec, // pg_stat_activity_waiting_connections{datname}
    blocked_connections: IntGaugeVec, // pg_stat_activity_blocked_connections{datname}

    // Connection pool saturation metrics (new - K8s focused)
    // Help prevent connection exhaustion in containerized environments
    max_connections: IntGauge,          // Total allowed connections (from pg_settings)
    used_connections: IntGauge,         // Current connections in use
    utilization_ratio: Gauge,           // ⭐ GOLD: used/max ratio (alert >0.8)
    available_connections: IntGauge,    // Connections still available

    // Dangerous states that indicate application bugs
    idle_in_transaction: IntGaugeVec,          // 🔴 Holding locks while idle
    idle_in_transaction_aborted: IntGaugeVec,  // 🔴🔴 Even worse - failed tx not cleaned

    // Application breakdown (identify connection hogs in K8s)
    connections_by_application: IntGaugeVec, // {datname, application_name}

    // Idle connection age buckets (detect connection leaks)
    // Buckets: <1m, 1-5m, 5-15m, 15m-1h, >1h
    idle_age_1m: IntGaugeVec,   // Idle <1 minute (normal)
    idle_age_5m: IntGaugeVec,   // Idle 1-5 minutes (acceptable)
    idle_age_15m: IntGaugeVec,  // Idle 5-15 minutes (investigate)
    idle_age_1h: IntGaugeVec,   // Idle 15m-1h (likely leak)
    idle_age_old: IntGaugeVec,  // Idle >1 hour (definite leak!)
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

        // Connection pool saturation metrics (new)
        let max_connections = IntGauge::with_opts(Opts::new(
            "pg_stat_activity_max_connections",
            "Maximum allowed connections (from pg_settings.max_connections)",
        ))
        .expect("Failed to create pg_stat_activity_max_connections");

        let used_connections = IntGauge::with_opts(Opts::new(
            "pg_stat_activity_used_connections",
            "Current number of connections in use (all client backends)",
        ))
        .expect("Failed to create pg_stat_activity_used_connections");

        let utilization_ratio = Gauge::with_opts(Opts::new(
            "pg_stat_activity_utilization_ratio",
            "Connection pool utilization ratio (used/max, 0.0-1.0). Alert when >0.8",
        ))
        .expect("Failed to create pg_stat_activity_utilization_ratio");

        let available_connections = IntGauge::with_opts(Opts::new(
            "pg_stat_activity_available_connections",
            "Number of connections still available (max - used)",
        ))
        .expect("Failed to create pg_stat_activity_available_connections");

        let idle_in_transaction = IntGaugeVec::new(
            Opts::new(
                "pg_stat_activity_idle_in_transaction",
                "Connections idle in transaction (holding locks/snapshots). Should be ~0 in healthy systems.",
            ),
            &["datname"],
        )
        .expect("Failed to create pg_stat_activity_idle_in_transaction");

        let idle_in_transaction_aborted = IntGaugeVec::new(
            Opts::new(
                "pg_stat_activity_idle_in_transaction_aborted",
                "Connections idle in aborted transaction (failed tx not cleaned up). Critical issue.",
            ),
            &["datname"],
        )
        .expect("Failed to create pg_stat_activity_idle_in_transaction_aborted");

        let connections_by_application = IntGaugeVec::new(
            Opts::new(
                "pg_stat_activity_connections_by_application",
                "Number of connections per application (identify connection hogs in K8s)",
            ),
            &["datname", "application_name"],
        )
        .expect("Failed to create pg_stat_activity_connections_by_application");

        let idle_age_1m = IntGaugeVec::new(
            Opts::new(
                "pg_stat_activity_idle_age_1m",
                "Number of idle connections aged <1 minute per database",
            ),
            &["datname"],
        )
        .expect("Failed to create pg_stat_activity_idle_age_1m");

        let idle_age_5m = IntGaugeVec::new(
            Opts::new(
                "pg_stat_activity_idle_age_5m",
                "Number of idle connections aged 1-5 minutes per database",
            ),
            &["datname"],
        )
        .expect("Failed to create pg_stat_activity_idle_age_5m");

        let idle_age_15m = IntGaugeVec::new(
            Opts::new(
                "pg_stat_activity_idle_age_15m",
                "Number of idle connections aged 5-15 minutes per database",
            ),
            &["datname"],
        )
        .expect("Failed to create pg_stat_activity_idle_age_15m");

        let idle_age_1h = IntGaugeVec::new(
            Opts::new(
                "pg_stat_activity_idle_age_1h",
                "Number of idle connections aged 15m-1h per database (investigate)",
            ),
            &["datname"],
        )
        .expect("Failed to create pg_stat_activity_idle_age_1h");

        let idle_age_old = IntGaugeVec::new(
            Opts::new(
                "pg_stat_activity_idle_age_old",
                "Number of idle connections aged >1 hour per database (connection leak!)",
            ),
            &["datname"],
        )
        .expect("Failed to create pg_stat_activity_idle_age_old");

        Self {
            count_by_state,
            active_connections,
            idle_connections,
            waiting_connections,
            blocked_connections,
            max_connections,
            used_connections,
            utilization_ratio,
            available_connections,
            idle_in_transaction,
            idle_in_transaction_aborted,
            connections_by_application,
            idle_age_1m,
            idle_age_5m,
            idle_age_15m,
            idle_age_1h,
            idle_age_old,
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
        // Register existing metrics (backward compatible)
        registry.register(Box::new(self.count_by_state.clone()))?;
        registry.register(Box::new(self.active_connections.clone()))?;
        registry.register(Box::new(self.idle_connections.clone()))?;
        registry.register(Box::new(self.waiting_connections.clone()))?;
        registry.register(Box::new(self.blocked_connections.clone()))?;

        // Register new pool saturation metrics
        registry.register(Box::new(self.max_connections.clone()))?;
        registry.register(Box::new(self.used_connections.clone()))?;
        registry.register(Box::new(self.utilization_ratio.clone()))?;
        registry.register(Box::new(self.available_connections.clone()))?;
        registry.register(Box::new(self.idle_in_transaction.clone()))?;
        registry.register(Box::new(self.idle_in_transaction_aborted.clone()))?;
        registry.register(Box::new(self.connections_by_application.clone()))?;
        registry.register(Box::new(self.idle_age_1m.clone()))?;
        registry.register(Box::new(self.idle_age_5m.clone()))?;
        registry.register(Box::new(self.idle_age_15m.clone()))?;
        registry.register(Box::new(self.idle_age_1h.clone()))?;
        registry.register(Box::new(self.idle_age_old.clone()))?;

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

            // NEW: Get max_connections setting
            let max_conn_query = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SHOW max_connections",
                db.sql.table = "pg_settings"
            );

            let max_conn: i64 = sqlx::query_scalar(
                "SELECT setting::bigint FROM pg_settings WHERE name = 'max_connections'"
            )
            .fetch_one(pool)
            .instrument(max_conn_query)
            .await
            .unwrap_or(100); // Fallback to common default

            self.max_connections.set(max_conn);

            // 1) Compatibility metric: count by state (EXISTING - unchanged)
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

                // Emit pg_stat_activity_count (EXISTING)
                self.count_by_state
                    .with_label_values(&[&db, &state])
                    .set(cnt);

                // Track active/idle for convenience gauges later (EXISTING)
                if state == "active" {
                    active_map.insert(db.clone(), cnt);
                } else if state == "idle" {
                    idle_map.insert(db.clone(), cnt);
                }
            }

            // After processing all states, set per-db active/idle (default 0 if missing) (EXISTING)
            for db in &dbs_seen {
                let a = *active_map.get(db).unwrap_or(&0);
                let i = *idle_map.get(db).unwrap_or(&0);
                self.active_connections.with_label_values(&[db]).set(a);
                self.idle_connections.with_label_values(&[db]).set(i);
                debug!(database=%db, active=a, idle=i, "set active/idle gauges");
            }

            // 2) Waiting and blocked connections per database (EXISTING - unchanged)
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

            // Ensure zeroes for databases seen in state query but not in wait/blocked (EXISTING)
            for db in &dbs_seen {
                if !waiting_map.contains_key(db) {
                    self.waiting_connections.with_label_values(&[db]).set(0);
                }
                if !blocked_map.contains_key(db) {
                    self.blocked_connections.with_label_values(&[db]).set(0);
                }
            }

            // 3) NEW: Pool saturation metrics and detailed connection analysis
            let q_detailed = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT detailed connection info from pg_stat_activity",
                db.sql.table = "pg_stat_activity"
            );

            let detailed_rows = sqlx::query(
                r#"
                SELECT
                    datname,
                    COALESCE(state, 'unknown') AS state,
                    application_name,
                    EXTRACT(EPOCH FROM (now() - state_change))::bigint AS state_duration_seconds,
                    COUNT(*)::bigint AS cnt
                FROM pg_stat_activity
                WHERE backend_type = 'client backend'
                  AND pid != pg_backend_pid()
                  AND NOT (COALESCE(datname, '') = ANY($1))
                GROUP BY datname, COALESCE(state, 'unknown'), application_name, EXTRACT(EPOCH FROM (now() - state_change))::bigint
                "#,
            )
            .bind(&excluded)
            .fetch_all(pool)
            .instrument(q_detailed)
            .await?;

            // Track totals and categorized metrics
            let mut total_connections: i64 = 0;
            let mut idle_in_tx_map: HashMap<String, i64> = HashMap::new();
            let mut idle_in_tx_aborted_map: HashMap<String, i64> = HashMap::new();
            let mut app_conn_map: HashMap<(String, String), i64> = HashMap::new();
            
            // Idle age bucket maps
            let mut idle_1m_map: HashMap<String, i64> = HashMap::new();
            let mut idle_5m_map: HashMap<String, i64> = HashMap::new();
            let mut idle_15m_map: HashMap<String, i64> = HashMap::new();
            let mut idle_1h_map: HashMap<String, i64> = HashMap::new();
            let mut idle_old_map: HashMap<String, i64> = HashMap::new();

            for row in &detailed_rows {
                let db: String = row
                    .try_get::<Option<String>, _>("datname")?
                    .unwrap_or_else(|| "[unknown]".to_string());
                let state: String = row.try_get::<String, _>("state")?;
                let app_name: String = row.try_get::<Option<String>, _>("application_name")?
                    .unwrap_or_else(|| "".to_string());
                let state_duration: i64 = row.try_get::<i64, _>("state_duration_seconds").unwrap_or(0);
                let cnt: i64 = row.try_get::<i64, _>("cnt").unwrap_or(0);

                total_connections += cnt;
                dbs_seen.insert(db.clone());

                // Track idle in transaction states (dangerous!)
                if state == "idle in transaction" {
                    *idle_in_tx_map.entry(db.clone()).or_insert(0) += cnt;
                } else if state == "idle in transaction (aborted)" {
                    *idle_in_tx_aborted_map.entry(db.clone()).or_insert(0) += cnt;
                }

                // Track connections by application (use "[unknown]" for empty app names)
                let app_label = if app_name.is_empty() {
                    "[unknown]".to_string()
                } else {
                    app_name
                };
                let key = (db.clone(), app_label);
                *app_conn_map.entry(key).or_insert(0) += cnt;

                // Categorize idle connections by age
                if state == "idle" {
                    if state_duration < 60 {
                        *idle_1m_map.entry(db.clone()).or_insert(0) += cnt;
                    } else if state_duration < 300 {
                        *idle_5m_map.entry(db.clone()).or_insert(0) += cnt;
                    } else if state_duration < 900 {
                        *idle_15m_map.entry(db.clone()).or_insert(0) += cnt;
                    } else if state_duration < 3600 {
                        *idle_1h_map.entry(db.clone()).or_insert(0) += cnt;
                    } else {
                        *idle_old_map.entry(db.clone()).or_insert(0) += cnt;
                    }
                }
            }

            // Set pool saturation metrics
            self.used_connections.set(total_connections);
            
            let utilization = if max_conn > 0 {
                total_connections as f64 / max_conn as f64
            } else {
                0.0
            };
            self.utilization_ratio.set(utilization);
            
            let available = std::cmp::max(0, max_conn - total_connections);
            self.available_connections.set(available);

            debug!(
                total_connections,
                max_connections = max_conn,
                utilization_ratio = utilization,
                available_connections = available,
                "set pool saturation metrics"
            );

            // Set per-database metrics
            for db in &dbs_seen {
                // Idle in transaction metrics
                let idle_in_tx = *idle_in_tx_map.get(db).unwrap_or(&0);
                let idle_in_tx_aborted = *idle_in_tx_aborted_map.get(db).unwrap_or(&0);
                self.idle_in_transaction.with_label_values(&[db]).set(idle_in_tx);
                self.idle_in_transaction_aborted.with_label_values(&[db]).set(idle_in_tx_aborted);

                // Idle age bucket metrics
                let idle_1m = *idle_1m_map.get(db).unwrap_or(&0);
                let idle_5m = *idle_5m_map.get(db).unwrap_or(&0);
                let idle_15m = *idle_15m_map.get(db).unwrap_or(&0);
                let idle_1h = *idle_1h_map.get(db).unwrap_or(&0);
                let idle_old = *idle_old_map.get(db).unwrap_or(&0);

                self.idle_age_1m.with_label_values(&[db]).set(idle_1m);
                self.idle_age_5m.with_label_values(&[db]).set(idle_5m);
                self.idle_age_15m.with_label_values(&[db]).set(idle_15m);
                self.idle_age_1h.with_label_values(&[db]).set(idle_1h);
                self.idle_age_old.with_label_values(&[db]).set(idle_old);

                debug!(
                    database = %db,
                    idle_in_transaction = idle_in_tx,
                    idle_in_transaction_aborted = idle_in_tx_aborted,
                    idle_1m, idle_5m, idle_15m, idle_1h, idle_old,
                    "set pool detail metrics"
                );
            }

            // Set connections by application
            for ((db, app_name), cnt) in &app_conn_map {
                self.connections_by_application
                    .with_label_values(&[db, app_name])
                    .set(*cnt);
            }

            Ok(())
        })
    }
}
