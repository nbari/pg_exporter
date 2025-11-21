use crate::collectors::{Collector, i64_to_f64, util::get_excluded_databases};
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{Gauge, GaugeVec, IntGauge, IntGaugeVec, Opts, Registry};
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use tracing::{debug, info_span, instrument};
use tracing_futures::Instrument as _;

/// Tracks long-running queries from `pg_stat_activity`
///
/// **Critical for Production Incident Response:**
/// - Detect stuck migrations immediately
/// - Find queries blocking operations
/// - Identify problematic applications
/// - Debug sudden slowdowns
///
/// **Metrics:**
/// - `pg_stat_activity_long_running_queries`{`datname`, `duration_bucket`} - Count by duration
/// - `pg_stat_activity_max_query_duration_seconds`{`datname`} - Longest running `query`
/// - `pg_stat_activity_long_running_queries_by_state`{`datname`, `state`} - Active vs waiting
/// - `pg_stat_activity_long_running_queries_by_wait_event`{`datname`, `wait_event`} - What's blocking
/// - `pg_stat_activity_oldest_query_age_seconds` - Global oldest `query` (alert > 3600)
/// - `pg_stat_activity_queries_over_5m`{`datname`} - Queries running >5 minutes
/// - `pg_stat_activity_queries_over_15m`{`datname`} - Queries running >15 minutes
/// - `pg_stat_activity_queries_over_1h`{`datname`} - Queries running >1 hour
/// - `pg_stat_activity_queries_over_6h`{`datname`} - Queries running >6 hours (stuck!)
#[derive(Clone)]
pub struct QueriesCollector {
    // Duration bucket counters (primary metrics for alerting)
    queries_over_5m: IntGaugeVec,   // {datname} - queries >5 minutes
    queries_over_15m: IntGaugeVec,  // {datname} - queries >15 minutes
    queries_over_1h: IntGaugeVec,   // {datname} - queries >1 hour
    queries_over_6h: IntGaugeVec,   // {datname} - queries >6 hours (critical!)

    // Per-database aggregates
    max_query_duration: GaugeVec,   // {datname} - longest query in seconds

    // Global metrics (cross-database)
    oldest_query_age: Gauge,        // Oldest query across all databases (seconds)
    total_long_running: IntGauge,   // Total queries running >5 minutes

    // Breakdown by state (what are slow queries doing?)
    long_running_by_state: IntGaugeVec,  // {datname, state} - active, waiting, etc.

    // Breakdown by wait event (what are slow queries waiting on?)
    long_running_by_wait_event: IntGaugeVec,  // {datname, wait_event_type} - Lock, IO, etc.
}

impl Default for QueriesCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl QueriesCollector {
    /// Creates a new `QueriesCollector`
    ///
    /// # Panics
    ///
    /// Panics if metric creation fails (should never happen with valid metric names)
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        let queries_threshold_short = IntGaugeVec::new(
            Opts::new(
                "pg_stat_activity_queries_over_5m",
                "Number of queries running for more than 5 minutes per database",
            ),
            &["datname"],
        )
        .expect("Failed to create pg_stat_activity_queries_over_5m");

        let queries_threshold_medium = IntGaugeVec::new(
            Opts::new(
                "pg_stat_activity_queries_over_15m",
                "Number of queries running for more than 15 minutes per database",
            ),
            &["datname"],
        )
        .expect("Failed to create pg_stat_activity_queries_over_15m");

        let queries_threshold_extended = IntGaugeVec::new(
            Opts::new(
                "pg_stat_activity_queries_over_1h",
                "Number of queries running for more than 1 hour per database",
            ),
            &["datname"],
        )
        .expect("Failed to create pg_stat_activity_queries_over_1h");

        let queries_threshold_prolonged = IntGaugeVec::new(
            Opts::new(
                "pg_stat_activity_queries_over_6h",
                "Number of queries running for more than 6 hours per database (likely stuck!)",
            ),
            &["datname"],
        )
        .expect("Failed to create pg_stat_activity_queries_over_6h");

        let max_query_duration = GaugeVec::new(
            Opts::new(
                "pg_stat_activity_max_query_duration_seconds",
                "Duration in seconds of the longest running query per database",
            ),
            &["datname"],
        )
        .expect("Failed to create pg_stat_activity_max_query_duration_seconds");

        let oldest_query_age = Gauge::with_opts(Opts::new(
            "pg_stat_activity_oldest_query_age_seconds",
            "Age in seconds of the oldest running query across all databases. Alert when >3600 (1 hour)",
        ))
        .expect("Failed to create pg_stat_activity_oldest_query_age_seconds");

        let total_long_running = IntGauge::with_opts(Opts::new(
            "pg_stat_activity_total_long_running",
            "Total number of queries running for more than 5 minutes across all databases",
        ))
        .expect("Failed to create pg_stat_activity_total_long_running");

        let long_running_by_state = IntGaugeVec::new(
            Opts::new(
                "pg_stat_activity_long_running_by_state",
                "Number of long-running queries (>5min) by database and state (active, waiting, etc.)",
            ),
            &["datname", "state"],
        )
        .expect("Failed to create pg_stat_activity_long_running_by_state");

        let long_running_by_wait_event = IntGaugeVec::new(
            Opts::new(
                "pg_stat_activity_long_running_by_wait_event",
                "Number of long-running queries (>5min) by database and wait_event_type (Lock, IO, etc.)",
            ),
            &["datname", "wait_event_type"],
        )
        .expect("Failed to create pg_stat_activity_long_running_by_wait_event");

        Self {
            queries_over_5m: queries_threshold_short,
            queries_over_15m: queries_threshold_medium,
            queries_over_1h: queries_threshold_extended,
            queries_over_6h: queries_threshold_prolonged,
            max_query_duration,
            oldest_query_age,
            total_long_running,
            long_running_by_state,
            long_running_by_wait_event,
        }
    }
}

impl Collector for QueriesCollector {
    fn name(&self) -> &'static str {
        "queries"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "queries")
    )]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.queries_over_5m.clone()))?;
        registry.register(Box::new(self.queries_over_15m.clone()))?;
        registry.register(Box::new(self.queries_over_1h.clone()))?;
        registry.register(Box::new(self.queries_over_6h.clone()))?;
        registry.register(Box::new(self.max_query_duration.clone()))?;
        registry.register(Box::new(self.oldest_query_age.clone()))?;
        registry.register(Box::new(self.total_long_running.clone()))?;
        registry.register(Box::new(self.long_running_by_state.clone()))?;
        registry.register(Box::new(self.long_running_by_wait_event.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector="queries", otel.kind="internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let excluded: Vec<String> = get_excluded_databases().to_vec();

            // Query for long-running queries
            // Only track queries running >5 minutes to avoid noise
            let query_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT long-running queries from pg_stat_activity",
                db.sql.table = "pg_stat_activity"
            );

            let rows = sqlx::query(
                r"
                SELECT
                    datname,
                    state,
                    COALESCE(wait_event_type, 'None') AS wait_event_type,
                    EXTRACT(EPOCH FROM (now() - query_start))::bigint AS duration_seconds
                FROM pg_stat_activity
                WHERE backend_type = 'client backend'
                  AND pid != pg_backend_pid()
                  AND state != 'idle'
                  AND query_start IS NOT NULL
                  AND (now() - query_start) > interval '5 minutes'
                  AND query NOT LIKE 'autovacuum:%'
                  AND NOT (COALESCE(datname, '') = ANY($1))
                ",
            )
            .bind(&excluded)
            .fetch_all(pool)
            .instrument(query_span)
            .await?;

            // Track metrics per database
            let mut db_counts_5m: HashMap<String, i64> = HashMap::new();
            let mut db_counts_15m: HashMap<String, i64> = HashMap::new();
            let mut db_counts_1h: HashMap<String, i64> = HashMap::new();
            let mut db_counts_6h: HashMap<String, i64> = HashMap::new();
            let mut db_max_duration: HashMap<String, f64> = HashMap::new();
            let mut state_counts: HashMap<(String, String), i64> = HashMap::new();
            let mut wait_event_counts: HashMap<(String, String), i64> = HashMap::new();

            let mut total_long = 0i64;
            let mut global_oldest = 0f64;

            for row in &rows {
                let db: String = row
                    .try_get::<Option<String>, _>("datname")?
                    .unwrap_or_else(|| "[unknown]".to_string());
                let state: String = row.try_get("state")?;
                let wait_event_type: String = row.try_get("wait_event_type")?;
                let duration: i64 = row.try_get("duration_seconds").unwrap_or(0);
                let duration_f64 = i64_to_f64(duration);

                total_long += 1;

                // Track oldest globally
                if duration_f64 > global_oldest {
                    global_oldest = duration_f64;
                }

                // Track max per database
                let current_max = db_max_duration.get(&db).copied().unwrap_or(0.0);
                if duration_f64 > current_max {
                    db_max_duration.insert(db.clone(), duration_f64);
                }

                // Categorize by duration buckets
                *db_counts_5m.entry(db.clone()).or_insert(0) += 1;

                if duration >= 900 {
                    // 15 minutes
                    *db_counts_15m.entry(db.clone()).or_insert(0) += 1;
                }
                if duration >= 3600 {
                    // 1 hour
                    *db_counts_1h.entry(db.clone()).or_insert(0) += 1;
                }
                if duration >= 21600 {
                    // 6 hours
                    *db_counts_6h.entry(db.clone()).or_insert(0) += 1;
                }

                // Track by state
                let state_key = (db.clone(), state.clone());
                *state_counts.entry(state_key).or_insert(0) += 1;

                // Track by wait event type (only if waiting)
                if wait_event_type != "None" {
                    let wait_key = (db.clone(), wait_event_type);
                    *wait_event_counts.entry(wait_key).or_insert(0) += 1;
                }
            }

            // Set global metrics
            self.total_long_running.set(total_long);
            self.oldest_query_age.set(global_oldest);

            // Set per-database duration bucket metrics
            for (db, count) in &db_counts_5m {
                self.queries_over_5m.with_label_values(&[db]).set(*count);
            }
            for (db, count) in &db_counts_15m {
                self.queries_over_15m.with_label_values(&[db]).set(*count);
            }
            for (db, count) in &db_counts_1h {
                self.queries_over_1h.with_label_values(&[db]).set(*count);
            }
            for (db, count) in &db_counts_6h {
                self.queries_over_6h.with_label_values(&[db]).set(*count);
            }

            // Set max duration per database
            for (db, max_dur) in &db_max_duration {
                self.max_query_duration.with_label_values(&[db]).set(*max_dur);
            }

            // Set state breakdown
            for ((db, state), count) in &state_counts {
                self.long_running_by_state
                    .with_label_values(&[db, state])
                    .set(*count);
            }

            // Set wait event breakdown
            for ((db, wait_event_type), count) in &wait_event_counts {
                self.long_running_by_wait_event
                    .with_label_values(&[db, wait_event_type])
                    .set(*count);
            }

            debug!(
                total_long_running = total_long,
                oldest_query_age_seconds = global_oldest,
                databases_with_slow_queries = db_counts_5m.len(),
                "updated long-running query metrics"
            );

            Ok(())
        })
    }
}
