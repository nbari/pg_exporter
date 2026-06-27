use crate::collectors::{util::get_excluded_databases, Collector};
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{GaugeVec, IntGaugeVec, Opts, Registry};
use sqlx::{PgPool, Row};
use tracing::{debug, info, info_span, instrument};
use tracing_futures::Instrument as _;

/// Tracks `PostgreSQL` lock contention
#[derive(Clone)]
pub struct LocksSubCollector {
    locks_count: IntGaugeVec,
    // Blocking / lock-wait diagnostics (detect "queries that block the DB").
    blocked_sessions: IntGaugeVec, // sessions currently waiting on a lock {datname}
    blocking_sessions: IntGaugeVec, // distinct sessions blocking >=1 other session {datname}
    longest_blocked_seconds: GaugeVec, // longest current lock wait, seconds {datname}
    lock_waits: IntGaugeVec,       // ungranted locks by mode {datname, mode}
}

impl Default for LocksSubCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl LocksSubCollector {
    #[must_use]
    /// Creates a new `LocksSubCollector`
    ///
    /// # Panics
    ///
    /// Panics if metric creation fails (should never happen with valid metric names)
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        let locks_count = IntGaugeVec::new(
            Opts::new("pg_locks_count", "Number of locks per database and mode"),
            &["datname", "mode"],
        )
        .expect("Failed to create pg_locks_count metric");

        let blocked_sessions = IntGaugeVec::new(
            Opts::new(
                "pg_blocked_sessions",
                "Number of sessions currently waiting on a lock held by another session",
            ),
            &["datname"],
        )
        .expect("Failed to create pg_blocked_sessions metric");

        let blocking_sessions = IntGaugeVec::new(
            Opts::new(
                "pg_blocking_sessions",
                "Number of distinct sessions that are blocking at least one other session",
            ),
            &["datname"],
        )
        .expect("Failed to create pg_blocking_sessions metric");

        let longest_blocked_seconds = GaugeVec::new(
            Opts::new(
                "pg_longest_blocked_seconds",
                "Age in seconds of the longest-waiting currently-blocked session (now - query_start)",
            ),
            &["datname"],
        )
        .expect("Failed to create pg_longest_blocked_seconds metric");

        let lock_waits = IntGaugeVec::new(
            Opts::new(
                "pg_lock_waits",
                "Number of ungranted (waiting) locks per database and mode",
            ),
            &["datname", "mode"],
        )
        .expect("Failed to create pg_lock_waits metric");

        Self {
            locks_count,
            blocked_sessions,
            blocking_sessions,
            longest_blocked_seconds,
            lock_waits,
        }
    }
}

impl Collector for LocksSubCollector {
    fn name(&self) -> &'static str {
        "locks"
    }

    #[instrument(skip(self, registry), level = "info", err, fields(collector = "locks"))]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.locks_count.clone()))?;
        registry.register(Box::new(self.blocked_sessions.clone()))?;
        registry.register(Box::new(self.blocking_sessions.clone()))?;
        registry.register(Box::new(self.longest_blocked_seconds.clone()))?;
        registry.register(Box::new(self.lock_waits.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector="locks", otel.kind="internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            // Build exclusion list from global OnceCell (set at startup via Clap/env).
            let excluded: Vec<String> = get_excluded_databases().to_vec();

            // Client span for querying lock statistics
            let query_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT datname, mode, count FROM pg_locks + pg_database join (filtered)",
                db.sql.table = "pg_locks"
            );

            let rows = sqlx::query(
                r"
                SELECT
                    COALESCE(d.datname, '') AS datname,
                    l.mode,
                    COUNT(*)::bigint AS count
                FROM pg_locks l
                LEFT JOIN pg_database d ON l.database = d.oid
                WHERE NOT (COALESCE(d.datname, '') = ANY($1))
                GROUP BY d.datname, l.mode
                ORDER BY datname, mode
                ",
            )
            .bind(&excluded)
            .fetch_all(pool)
            .instrument(query_span)
            .await?;

            // Span for applying metrics
            let apply_span = info_span!("locks.apply_metrics", locks = rows.len());
            let _g = apply_span.enter();

            // Reset all metrics before setting new values
            self.locks_count.reset();

            for row in &rows {
                let datname: String = row.try_get("datname")?;
                let mode: String = row.try_get("mode")?;
                let count: i64 = row.try_get("count").unwrap_or(0);

                self.locks_count
                    .with_label_values(&[&datname, &mode])
                    .set(count);

                debug!(
                    datname = %datname,
                    mode = %mode,
                    count,
                    "updated lock metrics"
                );
            }

            info!("Collected lock metrics for {} database/mode combinations", rows.len());

            // --- Blocking diagnostics (who is blocked / who is blocking, and for how long) ---
            // pg_blocking_pids() is PG9.6+. Visible to all users (only query TEXT is
            // restricted for non-superusers, not pid/datname/state/query_start).
            let block_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT blocked/blocking sessions + longest wait from pg_stat_activity",
                db.sql.table = "pg_stat_activity"
            );

            let block_rows = sqlx::query(
                r"
                WITH act AS (
                    SELECT a.pid,
                           COALESCE(a.datname, '') AS datname,
                           a.query_start,
                           pg_blocking_pids(a.pid) AS blockers
                    FROM pg_stat_activity a
                    WHERE a.backend_type = 'client backend'
                      AND a.pid <> pg_backend_pid()
                      AND NOT (COALESCE(a.datname, '') = ANY($1))
                ),
                blocked AS (
                    SELECT pid, datname,
                           GREATEST(
                               EXTRACT(EPOCH FROM (clock_timestamp() - query_start)),
                               0
                           )::double precision AS wait_seconds
                    FROM act
                    WHERE cardinality(blockers) > 0
                ),
                blocking AS (
                    SELECT DISTINCT a.pid, a.datname
                    FROM (
                        SELECT unnest(blockers) AS pid
                        FROM act
                        WHERE cardinality(blockers) > 0
                    ) b
                    JOIN act a ON a.pid = b.pid
                ),
                dbs AS (
                    SELECT datname FROM blocked
                    UNION
                    SELECT datname FROM blocking
                )
                SELECT
                    d.datname AS datname,
                    COALESCE((SELECT COUNT(*) FROM blocked x WHERE x.datname = d.datname), 0)::bigint
                        AS blocked_sessions,
                    COALESCE((SELECT COUNT(*) FROM blocking y WHERE y.datname = d.datname), 0)::bigint
                        AS blocking_sessions,
                    COALESCE((SELECT MAX(wait_seconds) FROM blocked z WHERE z.datname = d.datname), 0)::double precision
                        AS longest_blocked_seconds
                FROM dbs d
                ORDER BY d.datname
                ",
            )
            .bind(&excluded)
            .fetch_all(pool)
            .instrument(block_span)
            .await?;

            self.blocked_sessions.reset();
            self.blocking_sessions.reset();
            self.longest_blocked_seconds.reset();

            for row in &block_rows {
                let datname: String = row.try_get("datname").unwrap_or_default();
                let blocked: i64 = row.try_get("blocked_sessions").unwrap_or(0);
                let blocking: i64 = row.try_get("blocking_sessions").unwrap_or(0);
                let longest: f64 = row.try_get("longest_blocked_seconds").unwrap_or(0.0);

                self.blocked_sessions
                    .with_label_values(&[&datname])
                    .set(blocked);
                self.blocking_sessions
                    .with_label_values(&[&datname])
                    .set(blocking);
                self.longest_blocked_seconds
                    .with_label_values(&[&datname])
                    .set(longest);

                debug!(
                    datname = %datname,
                    blocked,
                    blocking,
                    longest_blocked_seconds = longest,
                    "updated blocking metrics"
                );
            }

            // --- Ungranted (waiting) locks by mode: shows WHICH lock type is contended ---
            let wait_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT datname, mode, count FROM pg_locks WHERE NOT granted (filtered)",
                db.sql.table = "pg_locks"
            );

            let wait_rows = sqlx::query(
                r"
                SELECT
                    COALESCE(d.datname, '') AS datname,
                    l.mode,
                    COUNT(*)::bigint AS count
                FROM pg_locks l
                LEFT JOIN pg_database d ON l.database = d.oid
                WHERE NOT l.granted
                  AND NOT (COALESCE(d.datname, '') = ANY($1))
                GROUP BY d.datname, l.mode
                ORDER BY datname, mode
                ",
            )
            .bind(&excluded)
            .fetch_all(pool)
            .instrument(wait_span)
            .await?;

            self.lock_waits.reset();

            for row in &wait_rows {
                let datname: String = row.try_get("datname").unwrap_or_default();
                let mode: String = row.try_get("mode").unwrap_or_default();
                let count: i64 = row.try_get("count").unwrap_or(0);

                self.lock_waits
                    .with_label_values(&[&datname, &mode])
                    .set(count);
            }

            Ok(())
        })
    }
}
