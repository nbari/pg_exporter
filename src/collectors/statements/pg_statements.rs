use crate::collectors::{Collected, Collector, i64_to_f64,
    config::DEFAULT_STATEMENTS_QUERY_TEXT_REFRESH,
    util::{MS_TO_SEC, TEMPLATE0, TEMPLATE1},
};
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{GaugeVec, IntGaugeVec, Opts, Registry};
use sqlx::{postgres::PgRow, PgPool, Row};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
    time::{Duration, Instant},
};
use tracing::{debug, info_span, instrument, warn};
use tracing_futures::Instrument as _;

/// `PgStatementsCollector` tracks `pg_stat_statements` metrics
///
/// Collects `query` performance statistics including:
/// - Execution time (total, mean, max, stddev)
/// - Call frequency and row counts
/// - I/O metrics (cache hits/misses, disk reads/writes)
/// - Temp file usage (queries spilling to disk)
/// - WAL generation per `query`
/// - Cache hit ratios
///
/// This collector exposes the top N queries by total execution time plus the top N
/// queries by temporary blocks written, so a statement that spills to disk cannot
/// disappear from the temp metrics just because it is not one of the slowest.
///
/// # Scrape cost
///
/// The per-scrape query reads `pg_stat_statements(false)`, i.e. **without** query
/// texts. That matters: `pg_stat_statements` is a set-returning function that first
/// loads the whole query-text file into backend memory and then materializes every
/// row into a `work_mem`-bounded tuplestore *before* any filter, join or `LIMIT`
/// runs. On a busy instance (~10k entries, tens of MB of text) each scrape therefore
/// wrote a multi-megabyte temporary file. Query texts are instead resolved by a
/// separate, rate-limited background lookup and cached per `queryid`; metric
/// collection never waits for that lookup to finish.
#[derive(Clone)]
pub struct PgStatementsCollector {
    // Execution time metrics (most important for DBREs)
    total_exec_time: GaugeVec,       // {queryid, datname, usename, query_short}
    mean_exec_time: GaugeVec,        // {queryid, datname, usename, query_short}
    max_exec_time: GaugeVec,         // {queryid, datname, usename, query_short}
    stddev_exec_time: GaugeVec,      // {queryid, datname, usename, query_short}
    
    // Call frequency metrics
    calls: IntGaugeVec,              // {queryid, datname, usename, query_short}
    rows: IntGaugeVec,               // {queryid, datname, usename, query_short}
    
    // I/O metrics (critical for performance analysis)
    shared_blks_hit: IntGaugeVec,    // {queryid, datname, usename, query_short} - cache hits
    shared_blks_read: IntGaugeVec,   // {queryid, datname, usename, query_short} - disk reads
    shared_blks_dirtied: IntGaugeVec, // {queryid, datname, usename, query_short}
    shared_blks_written: IntGaugeVec, // {queryid, datname, usename, query_short}
    
    // Local I/O (temp tables)
    local_blks_hit: IntGaugeVec,     // {queryid, datname, usename, query_short}
    local_blks_read: IntGaugeVec,    // {queryid, datname, usename, query_short}
    local_blks_dirtied: IntGaugeVec, // {queryid, datname, usename, query_short}
    local_blks_written: IntGaugeVec, // {queryid, datname, usename, query_short}
    
    // Temp file usage (queries spilling to disk - often indicates memory issues)
    temp_blks_read: IntGaugeVec,     // {queryid, datname, usename, query_short}
    temp_blks_written: IntGaugeVec,  // {queryid, datname, usename, query_short}
    
    // WAL generation (write-heavy queries)
    wal_bytes: IntGaugeVec,          // {queryid, datname, usename, query_short}
    
    // Cache hit ratio (derived metric)
    cache_hit_ratio: GaugeVec,       // {queryid, datname, usename, query_short}
    
    // Top N tracking limit
    top_n: usize,

    /// Minimum delay between two query-text lookups, or `None` when texts are not
    /// fetched at all.
    query_text_refresh: Option<Duration>,

    /// `queryid` -> shortened query text, plus the exporter's own `queryid`s.
    text_cache: Arc<Mutex<QueryTextCache>>,

    /// Ensures overlapping scrapes can schedule at most one query-text lookup.
    text_lookup_in_flight: Arc<AtomicBool>,

    /// Ensures a rejected `SET LOCAL work_mem` warning is emitted only once.
    work_mem_refusal_warned: Arc<AtomicBool>,

    // Cached extension detection to avoid re-querying pg_extension every scrape.
    extension_state: Arc<Mutex<ExtensionState>>,

    /// Cached `pg_stat_activity.query_id` probe, see [`query_id_support`].
    ///
    /// Probed against the catalog rather than read from the global server version so
    /// that self-exclusion does not depend on some other component having initialised
    /// that global first.
    query_id_state: Arc<AtomicU8>,

    /// Cached `pg_stat_statements.toplevel` probe, see [`query_id_support`] for the
    /// shared state encoding and [`PgStatementsCollector::supports_toplevel`] for why
    /// the column is gated.
    toplevel_state: Arc<AtomicU8>,
}

/// Cached query texts and self-identification state.
///
/// A `queryid` maps to one normalized query text for the lifetime of the statistics,
/// so entries never go stale and need no TTL. The cache only exists to keep the
/// expensive `showtext = true` lookup off the scrape path.
#[derive(Debug, Default)]
struct QueryTextCache {
    /// Already shortened (label-ready) text per `queryid`.
    texts: HashMap<i64, String>,
    /// `queryid`s belonging to this collector's own SQL, filtered out of the metrics.
    self_queryids: HashSet<i64>,
    /// When the last text lookup ran, used to rate-limit lookups.
    last_lookup: Option<Instant>,
}

/// Releases the single-flight claim when a background lookup finishes, fails,
/// panics, or is cancelled.
struct TextLookupInFlightGuard {
    in_flight: Arc<AtomicBool>,
}

impl Drop for TextLookupInFlightGuard {
    fn drop(&mut self) {
        self.in_flight.store(false, Ordering::Release);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExtensionState {
    Unknown,
    Installed,
    Missing { last_checked: Instant },
}

const MISSING_EXTENSION_RECHECK_AFTER: Duration = Duration::from_mins(1);

/// Marker embedded in this collector's SQL so its own `pg_stat_statements` entries can
/// be recognised during a text lookup and then excluded from the exported series.
///
/// The marker must sit **after** the first keyword: `PostgreSQL` stores the statement
/// text from its first token onwards, so a leading comment would be dropped.
const SELF_QUERY_MARKER: &str = "pg_exporter:statements";

/// The exact marker-comment forms this collector emits. Self-identification matches
/// these prefixes rather than the bare marker so that a *user* statement merely
/// mentioning [`SELF_QUERY_MARKER`] (in a comment or identifier) is never mistaken for
/// the exporter's own query and silently dropped from the metrics forever.
/// `self_query_prefixes_carry_the_marker` pins these to the builders.
const SELF_QUERY_PREFIXES: [&str; 2] = [
    "WITH /* pg_exporter:statements */",
    "SELECT /* pg_exporter:statements */",
];

/// Maximum length of the `query_short` label value.
const QUERY_SHORT_MAX_LEN: usize = 80;

/// Label value used while a statement's text has not been resolved yet, or when text
/// lookups are disabled.
const UNRESOLVED_QUERY_SHORT: &str = "<unknown>";

/// Placeholder `PostgreSQL` substitutes for the query text of *another* role's statement
/// when the reading role is neither a superuser nor a member of `pg_read_all_stats`.
const PG_INSUFFICIENT_PRIVILEGE_TEXT: &str = "<insufficient privilege>";

/// Smallest number of cached query texts kept before pruning kicks in.
const MIN_TEXT_CACHE_ENTRIES: usize = 1024;

/// `work_mem` applied to the query-text lookup transaction only.
///
/// Sized to hold the query-text file of a default `pg_stat_statements.max = 5000`
/// instance in memory. It is transient (one statement, at most once per
/// `--statements.query-text-refresh`) and on a single connection, so the peak is
/// bounded. A larger corpus simply spills the remainder as it did before.
const QUERY_TEXT_LOOKUP_WORK_MEM: &str = "64MB";

/// Cached outcome of the `pg_stat_activity.query_id` probe.
mod query_id_support {
    pub const UNKNOWN: u8 = 0;
    pub const SUPPORTED: u8 = 1;
    pub const UNSUPPORTED: u8 = 2;
}

impl PgStatementsCollector {
    /// Create a new `pg_statements` collector
    ///
    /// # Arguments
    /// * `top_n` - Number of top queries to track per ranking (default: 25)
    ///   - Too low: Miss important queries
    ///   - Too high: High cardinality, expensive to scrape
    ///   - Recommended: 10-50 for most production deployments
    ///
    /// Query texts are resolved with the default lookup interval; use
    /// [`Self::with_config`] to change it.
    ///
    /// # Panics
    ///
    /// Panics if metric creation fails (should never happen with valid metric names)
    #[must_use]
    pub fn with_top_n(top_n: usize) -> Self {
        Self::with_config(top_n, Some(DEFAULT_STATEMENTS_QUERY_TEXT_REFRESH))
    }

    /// Create a new `pg_statements` collector with an explicit query-text policy.
    ///
    /// `query_text_refresh` is the minimum delay between two query-text lookups;
    /// `None` disables text lookups entirely, leaving `query_short` unresolved.
    ///
    /// # Panics
    ///
    /// Panics if metric creation fails (should never happen with valid metric names)
    #[must_use]
    pub fn with_config(top_n: usize, query_text_refresh: Option<Duration>) -> Self {
        let total_exec_time = statement_gauge(
            "pg_stat_statements_total_exec_time_seconds",
            "Total time spent executing this query (seconds)",
        );
        let mean_exec_time = statement_gauge(
            "pg_stat_statements_mean_exec_time_seconds",
            "Mean time per execution (seconds) - key for finding slow queries",
        );
        let max_exec_time = statement_gauge(
            "pg_stat_statements_max_exec_time_seconds",
            "Maximum execution time observed (seconds)",
        );
        let stddev_exec_time = statement_gauge(
            "pg_stat_statements_stddev_exec_time_seconds",
            "Standard deviation of execution time - high value indicates inconsistent performance",
        );
        let calls = statement_int_gauge(
            "pg_stat_statements_calls_total",
            "Number of times this query has been executed",
        );
        let rows = statement_int_gauge(
            "pg_stat_statements_rows_total",
            "Total number of rows retrieved or affected by this query",
        );
        let shared_blks_hit = statement_int_gauge(
            "pg_stat_statements_shared_blks_hit_total",
            "Shared block cache hits (found in memory)",
        );
        let shared_blks_read = statement_int_gauge(
            "pg_stat_statements_shared_blks_read_total",
            "Shared blocks read from disk (cache miss - expensive!)",
        );
        let shared_blks_dirtied = statement_int_gauge(
            "pg_stat_statements_shared_blks_dirtied_total",
            "Shared blocks dirtied (modified)",
        );
        let shared_blks_written = statement_int_gauge(
            "pg_stat_statements_shared_blks_written_total",
            "Shared blocks written to disk",
        );
        let local_blks_hit = statement_int_gauge(
            "pg_stat_statements_local_blks_hit_total",
            "Local block cache hits (temp tables)",
        );
        let local_blks_read = statement_int_gauge(
            "pg_stat_statements_local_blks_read_total",
            "Local blocks read from disk (temp tables)",
        );
        let local_blks_dirtied = statement_int_gauge(
            "pg_stat_statements_local_blks_dirtied_total",
            "Local blocks dirtied (temp tables)",
        );
        let local_blks_written = statement_int_gauge(
            "pg_stat_statements_local_blks_written_total",
            "Local blocks written to disk (temp tables)",
        );

        let temp_blks_read = statement_int_gauge(
            "pg_stat_statements_temp_blks_read_total",
            "Temp file blocks read - query spilled to disk (work_mem too small!)",
        );
        let temp_blks_written = statement_int_gauge(
            "pg_stat_statements_temp_blks_written_total",
            "Temp file blocks written - query spilled to disk (work_mem too small!)",
        );
        let wal_bytes = statement_int_gauge(
            "pg_stat_statements_wal_bytes_total",
            "WAL bytes generated by this query",
        );
        let cache_hit_ratio = statement_gauge(
            "pg_stat_statements_cache_hit_ratio",
            "Cache hit ratio for this query (0.0-1.0, higher is better)",
        );

        Self {
            total_exec_time,
            mean_exec_time,
            max_exec_time,
            stddev_exec_time,
            calls,
            rows,
            shared_blks_hit,
            shared_blks_read,
            shared_blks_dirtied,
            shared_blks_written,
            local_blks_hit,
            local_blks_read,
            local_blks_dirtied,
            local_blks_written,
            temp_blks_read,
            temp_blks_written,
            wal_bytes,
            cache_hit_ratio,
            top_n,
            query_text_refresh,
            text_cache: Arc::new(Mutex::new(QueryTextCache::default())),
            text_lookup_in_flight: Arc::new(AtomicBool::new(false)),
            work_mem_refusal_warned: Arc::new(AtomicBool::new(false)),
            extension_state: Arc::new(Mutex::new(ExtensionState::Unknown)),
            query_id_state: Arc::new(AtomicU8::new(query_id_support::UNKNOWN)),
            toplevel_state: Arc::new(AtomicU8::new(query_id_support::UNKNOWN)),
        }
    }

    /// Truncate `query` text for labels (avoid high cardinality)
    fn truncate_query(query: &str, max_len: usize) -> String {
        let cleaned = query
            .trim()
            .lines()
            .map(str::trim)
            .collect::<Vec<_>>()
            .join(" ");
        
        if cleaned.len() <= max_len {
            cleaned
        } else {
            let trunc_at = cleaned.floor_char_boundary(max_len);
            format!("{}...", &cleaned[..trunc_at])
        }
    }

    /// Builds the per-scrape statistics query.
    ///
    /// The query reads `pg_stat_statements(false)` so `PostgreSQL` neither loads the
    /// query-text file nor materializes any text: the tuplestore holds only fixed-width
    /// counters, which keeps the scrape off `work_mem` and out of `base/pgsql_tmp`.
    ///
    /// Rows are the deduplicated union of the top N by total execution time and the top
    /// N by temporary blocks written, so at most `2 * top_n` series are exported per
    /// metric. Without the second ranking a statement writing gigabytes of temporary
    /// files stays invisible whenever it is not also one of the slowest statements.
    ///
    /// `$1` carries this collector's own `queryid`s, `$2` the per-ranking limit.
    ///
    /// `supports_query_id` enables a text-free self-exclusion: on `PostgreSQL` 14+ a
    /// backend can read its own `pg_stat_activity.query_id` *while that very query is
    /// executing*, and it equals the `queryid` `pg_stat_statements` records for it. That
    /// makes self-exclusion work on every scrape without reading any query text — in
    /// particular when `--statements.query-text-refresh=0` disables the text lookup that
    /// would otherwise be the only way to learn the collector's own queryid.
    ///
    /// `supports_toplevel` restricts the scan to top-level statements. See
    /// [`Self::supports_toplevel`] for why that matters and what it costs.
    fn build_pg_statements_query(supports_query_id: bool, supports_toplevel: bool) -> String {
        // A NULL query_id (compute_query_id off) makes IS DISTINCT FROM true for every
        // row, so this degrades to "exclude nothing" rather than dropping the scrape.
        let self_exclusion = if supports_query_id {
            "AND s.queryid IS DISTINCT FROM (SELECT a.query_id FROM pg_stat_activity a WHERE a.pid = pg_backend_pid())"
        } else {
            ""
        };

        let toplevel_only = if supports_toplevel {
            "AND s.toplevel"
        } else {
            ""
        };

        // IMPORTANT: keep casts to avoid NUMERIC/i64 mismatches.
        format!(
            r"WITH /* {SELF_QUERY_MARKER} */ stats AS MATERIALIZED (
                SELECT
                    s.queryid AS queryid,
                    s.userid AS userid,
                    d.datname AS datname,
                    s.calls::bigint AS calls,
                    (s.total_exec_time / {MS_TO_SEC})::double precision AS total_exec_time_sec,
                    (s.mean_exec_time / {MS_TO_SEC})::double precision AS mean_exec_time_sec,
                    (s.max_exec_time / {MS_TO_SEC})::double precision AS max_exec_time_sec,
                    (s.stddev_exec_time / {MS_TO_SEC})::double precision AS stddev_exec_time_sec,
                    s.rows::bigint AS rows,
                    s.shared_blks_hit::bigint AS shared_blks_hit,
                    s.shared_blks_read::bigint AS shared_blks_read,
                    s.shared_blks_dirtied::bigint AS shared_blks_dirtied,
                    s.shared_blks_written::bigint AS shared_blks_written,
                    s.local_blks_hit::bigint AS local_blks_hit,
                    s.local_blks_read::bigint AS local_blks_read,
                    s.local_blks_dirtied::bigint AS local_blks_dirtied,
                    s.local_blks_written::bigint AS local_blks_written,
                    s.temp_blks_read::bigint AS temp_blks_read,
                    s.temp_blks_written::bigint AS temp_blks_written,
                    COALESCE(s.wal_bytes, 0)::bigint AS wal_bytes
                FROM pg_stat_statements(false) s
                JOIN pg_database d ON d.oid = s.dbid
                WHERE s.queryid IS NOT NULL
                  AND s.total_exec_time > 0
                  AND d.datname NOT IN ('{TEMPLATE0}', '{TEMPLATE1}')
                  AND s.queryid <> ALL($1::bigint[])
                  {toplevel_only}
                  {self_exclusion}
            ),
            top_by_time AS (
                SELECT * FROM stats ORDER BY total_exec_time_sec DESC LIMIT $2
            ),
            top_by_temp AS (
                SELECT * FROM stats WHERE temp_blks_written > 0
                ORDER BY temp_blks_written DESC LIMIT $2
            ),
            selected AS (
                SELECT * FROM top_by_time
                UNION
                SELECT * FROM top_by_temp
            )
            SELECT
                sel.queryid,
                sel.datname,
                COALESCE(r.rolname, '<unknown>') AS usename,
                sel.calls,
                sel.total_exec_time_sec,
                sel.mean_exec_time_sec,
                sel.max_exec_time_sec,
                sel.stddev_exec_time_sec,
                sel.rows,
                sel.shared_blks_hit,
                sel.shared_blks_read,
                sel.shared_blks_dirtied,
                sel.shared_blks_written,
                sel.local_blks_hit,
                sel.local_blks_read,
                sel.local_blks_dirtied,
                sel.local_blks_written,
                sel.temp_blks_read,
                sel.temp_blks_written,
                sel.wal_bytes
            FROM selected sel
            LEFT JOIN pg_roles r ON r.oid = sel.userid
            "
        )
    }

    /// Builds the query-text lookup.
    ///
    /// This is the only statement that reads `pg_stat_statements` with `showtext = true`
    /// and therefore the only one that can spill: it is kept off the scrape path and
    /// rate-limited. Besides the requested `queryid`s it also returns this collector's
    /// own statements, recognised by [`SELF_QUERY_MARKER`], so they can be filtered out
    /// of the exported series without a text comparison on the scrape path.
    ///
    /// `pg_stat_statements` only records this lookup *after* it finishes, so matching on
    /// the stored text alone would learn the lookup's own `queryid` one lookup late —
    /// up to `--statements.query-text-refresh` of exporting itself. When
    /// `supports_query_id` is set it therefore also reports its own in-flight `query_id`,
    /// which self-identifies it on the very first run.
    fn build_query_text_lookup(supports_query_id: bool) -> String {
        let self_row = if supports_query_id {
            format!(
                r"
            UNION ALL
            SELECT a.query_id, 'SELECT /* {SELF_QUERY_MARKER} */'
            FROM pg_stat_activity a
            WHERE a.pid = pg_backend_pid() AND a.query_id IS NOT NULL"
            )
        } else {
            String::new()
        };

        format!(
            r"SELECT /* {SELF_QUERY_MARKER} */
                s.queryid AS queryid,
                LEFT(s.query, {QUERY_SHORT_MAX_LEN}) AS query_short
            FROM pg_stat_statements(true) s
            WHERE s.query IS NOT NULL
              AND (s.queryid = ANY($1::bigint[])
                   OR s.query LIKE 'WITH /* {SELF_QUERY_MARKER} */%'
                   OR s.query LIKE 'SELECT /* {SELF_QUERY_MARKER} */%'){self_row}
            "
        )
    }

    fn text_cache_lock(&self) -> MutexGuard<'_, QueryTextCache> {
        match self.text_cache.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// `queryid`s of this collector's own statements, excluded from the scrape query.
    fn self_queryids(&self) -> Vec<i64> {
        self.text_cache_lock()
            .self_queryids
            .iter()
            .copied()
            .collect()
    }

    /// Splits `queryids` into cached label values and the ones still missing a text.
    fn resolve_cached_texts(&self, queryids: &[i64]) -> (HashMap<i64, String>, Vec<i64>) {
        let cache = self.text_cache_lock();
        let mut resolved = HashMap::with_capacity(queryids.len());
        let mut missing = Vec::new();

        for queryid in queryids {
            if let Some(text) = cache.texts.get(queryid) {
                resolved.insert(*queryid, text.clone());
            } else {
                missing.push(*queryid);
            }
        }

        (resolved, missing)
    }

    /// Whether a text lookup may run now, honouring the configured rate limit.
    fn text_lookup_due(&self) -> bool {
        let Some(interval) = self.query_text_refresh else {
            return false;
        };

        self.text_cache_lock()
            .last_lookup
            .is_none_or(|last| last.elapsed() >= interval)
    }

    /// Claims the background lookup slot if the refresh interval is due.
    fn begin_text_lookup(&self) -> Option<TextLookupInFlightGuard> {
        if !self.text_lookup_due()
            || self
                .text_lookup_in_flight
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            return None;
        }

        Some(TextLookupInFlightGuard {
            in_flight: Arc::clone(&self.text_lookup_in_flight),
        })
    }

    /// Runs a claimed text lookup without tying its lifetime to the scrape future.
    async fn refresh_query_texts_in_background(
        self,
        pool: PgPool,
        missing: Vec<i64>,
        _in_flight: TextLookupInFlightGuard,
    ) {
        self.refresh_query_texts(&pool, &missing).await;
    }

    /// Starts a rate-limited text lookup and returns immediately.
    fn schedule_query_text_refresh(&self, pool: &PgPool, missing: Vec<i64>) {
        if missing.is_empty() {
            return;
        }

        let Some(in_flight) = self.begin_text_lookup() else {
            return;
        };

        let task = self
            .clone()
            .refresh_query_texts_in_background(pool.clone(), missing, in_flight);
        // Dropping a Tokio JoinHandle detaches the task. The task owns the pool and
        // collector handles it needs, so it can finish after this scrape returns.
        drop(tokio::spawn(task));
    }

    fn should_warn_work_mem_refusal(&self) -> bool {
        !self.work_mem_refusal_warned.swap(true, Ordering::Relaxed)
    }

    /// Stores the result of a text lookup, separating self statements from real ones.
    /// Recognises the collector's own statements by the exact marker-comment prefixes it
    /// emits. A bare `contains` would let any user statement that happens to mention the
    /// marker be classified as ours and excluded permanently, since `self_queryids` is
    /// never pruned.
    fn is_self_query_text(text: &str) -> bool {
        let text = text.trim_start();
        SELF_QUERY_PREFIXES
            .iter()
            .any(|prefix| text.starts_with(prefix))
    }

    /// Probes once whether `pg_stat_activity` exposes `query_id` (`PostgreSQL` 14+).
    ///
    /// The catalog is asked directly instead of comparing the server version, because
    /// the version global is initialised by the exporter/registry and would be unset
    /// when a collector is driven on its own. A failed probe is treated as "unsupported"
    /// and retried on the next scrape rather than failing the collection.
    async fn supports_query_id(&self, pool: &PgPool) -> bool {
        match self.query_id_state.load(Ordering::Relaxed) {
            query_id_support::SUPPORTED => return true,
            query_id_support::UNSUPPORTED => return false,
            _ => {}
        }

        let supported: Option<bool> = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1
                 FROM pg_attribute
                 WHERE attrelid = 'pg_catalog.pg_stat_activity'::regclass
                   AND attname = 'query_id'
                   AND NOT attisdropped
             )",
        )
        .fetch_optional(pool)
        .await
        .unwrap_or_default()
        .flatten();

        let Some(supported) = supported else {
            debug!("could not probe pg_stat_activity.query_id, retrying next scrape");
            return false;
        };

        self.query_id_state.store(
            if supported {
                query_id_support::SUPPORTED
            } else {
                query_id_support::UNSUPPORTED
            },
            Ordering::Relaxed,
        );
        supported
    }

    /// Probes once whether `pg_stat_statements` exposes `toplevel`
    /// (`pg_stat_statements` 1.9, shipped with `PostgreSQL` 14).
    ///
    /// `pg_stat_statements` is keyed by `(userid, dbid, queryid, toplevel)`. With
    /// `pg_stat_statements.track = all` a statement executed both directly and from
    /// inside a function has **two** entries with the same `(userid, dbid, queryid)` and
    /// different counters. Both survive the `UNION`, then collapse onto one
    /// `{queryid, datname, usename, query_short}` label set, so one silently overwrites
    /// the other and each duplicate also consumes a slot of the `2 * top_n` budget.
    /// Restricting the scan to `s.toplevel` makes the key unambiguous.
    ///
    /// The cost is that under `track = all` a statement *only* ever called from inside a
    /// function is no longer exported. Under the default `track = top` nothing changes,
    /// because nested statements are never recorded in the first place.
    ///
    /// The catalog is asked directly instead of comparing the server version, for the
    /// same reason as [`Self::supports_query_id`]: the version global is initialised by
    /// the exporter/registry and would be unset when a collector is driven on its own.
    /// The view is defined as `SELECT * FROM pg_stat_statements(true)`, so its columns
    /// mirror the function the scrape query actually reads. A failed probe is treated as
    /// "unsupported" and retried on the next scrape rather than failing the collection.
    async fn supports_toplevel(&self, pool: &PgPool) -> bool {
        match self.toplevel_state.load(Ordering::Relaxed) {
            query_id_support::SUPPORTED => return true,
            query_id_support::UNSUPPORTED => return false,
            _ => {}
        }

        let supported: Option<bool> = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1
                 FROM pg_attribute
                 WHERE attrelid = 'pg_stat_statements'::regclass
                   AND attname = 'toplevel'
                   AND NOT attisdropped
             )",
        )
        .fetch_optional(pool)
        .await
        .unwrap_or_default()
        .flatten();

        let Some(supported) = supported else {
            debug!("could not probe pg_stat_statements.toplevel, retrying next scrape");
            return false;
        };

        self.toplevel_state.store(
            if supported {
                query_id_support::SUPPORTED
            } else {
                query_id_support::UNSUPPORTED
            },
            Ordering::Relaxed,
        );
        supported
    }

    /// Decides whether a looked-up query text is worth caching.
    ///
    /// `pg_stat_statements` returns NULL when it has no text for an entry, for
    /// example after its query-text file has been garbage collected. Caching the
    /// resulting empty string would pin `query_short=""` on that series forever,
    /// because a cached `queryid` is never looked up again. Leaving it unresolved
    /// labels it `<unknown>` and retries on the next lookup window.
    ///
    /// [`PG_INSUFFICIENT_PRIVILEGE_TEXT`] is rejected for the same reason: it is not a
    /// query text but a statement about the *reader's* privileges, which can change. A
    /// later `GRANT pg_read_all_stats` must be picked up by the next lookup instead of
    /// leaving the placeholder pinned until the cache happens to be pruned.
    fn cacheable_text(text: Option<&str>) -> Option<String> {
        let text = text?;
        let trimmed = text.trim();
        if trimmed.is_empty() || trimmed == PG_INSUFFICIENT_PRIVILEGE_TEXT {
            return None;
        }
        Some(Self::truncate_query(text, QUERY_SHORT_MAX_LEN))
    }

    fn apply_text_lookup(&self, rows: &[PgRow]) {
        let mut cache = self.text_cache_lock();
        cache.last_lookup = Some(Instant::now());

        for row in rows {
            let Ok(queryid) = row.try_get::<i64, _>("queryid") else {
                continue;
            };
            let text: Option<String> = row.try_get("query_short").ok().flatten();

            if text.as_deref().is_some_and(Self::is_self_query_text) {
                cache.texts.remove(&queryid);
                cache.self_queryids.insert(queryid);
                continue;
            }

            if let Some(text) = Self::cacheable_text(text.as_deref()) {
                cache.texts.insert(queryid, text);
            }
        }
    }

    /// Marks the rate limit as consumed without storing anything.
    ///
    /// Used when a lookup fails so a broken or unauthorized lookup cannot be retried on
    /// every scrape.
    fn mark_text_lookup_attempted(&self) {
        self.text_cache_lock().last_lookup = Some(Instant::now());
    }

    /// Keeps the cache bounded: once it grows past the cap, only the currently exported
    /// statements are kept.
    fn prune_text_cache(&self, keep: &HashSet<i64>) {
        let cap = MIN_TEXT_CACHE_ENTRIES.max(self.top_n.saturating_mul(8));
        let mut cache = self.text_cache_lock();

        if cache.texts.len() > cap {
            cache.texts.retain(|queryid, _| keep.contains(queryid));
        }
    }

    /// Runs the query-text lookup with a raised `work_mem`.
    ///
    /// `pg_stat_statements(true)` materializes every entry *and its text* into a
    /// `work_mem`-bounded tuplestore inside the function scan, so on instances with a
    /// large query-text file the lookup itself writes temporary files — the very
    /// behaviour issue #33 is about, just far less often. Raising `work_mem` for this one
    /// statement keeps that tuplestore in memory instead. Measured on a 9.4 MB corpus
    /// with `work_mem = 4MB`: 1391 temp blocks and 30.9 ms, versus 0 blocks and 21.9 ms
    /// at 16MB.
    ///
    /// `SET LOCAL` needs a transaction and reverts on commit, so nothing leaks back into
    /// the pooled connection. A server that refuses the setting is not fatal: the lookup
    /// is retried outside a transaction in the background, spilling as before rather
    /// than losing texts. That fallback is warned about once per exporter process.
    async fn run_query_text_lookup(
        &self,
        pool: &PgPool,
        sql: &str,
        missing: &[i64],
    ) -> Result<Vec<PgRow>, sqlx::Error> {
        let mut tx = pool.begin().await?;

        let set_work_mem = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SET LOCAL work_mem = '{QUERY_TEXT_LOOKUP_WORK_MEM}'"
        )))
        .execute(&mut *tx)
        .await;

        if let Err(error) = set_work_mem {
            if self.should_warn_work_mem_refusal() {
                warn!(
                    collector = "pg_statements",
                    error = %error,
                    "could not raise work_mem for the background query-text lookup; \
                     continuing with the session default, which may spill to temporary files"
                );
            } else {
                debug!(
                    collector = "pg_statements",
                    error = %error,
                    "could not raise work_mem for the background query-text lookup"
                );
            }
            drop(tx);
            return sqlx::query(sqlx::AssertSqlSafe(sql))
                .bind(missing)
                .fetch_all(pool)
                .await;
        }

        let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(missing)
            .fetch_all(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(rows)
    }

    async fn refresh_query_texts(&self, pool: &PgPool, missing: &[i64]) {
        let sql = Self::build_query_text_lookup(self.supports_query_id(pool).await);
        let query_span = info_span!(
            "db.query",
            otel.kind = "client",
            db.system = "postgresql",
            db.operation = "SELECT",
            db.statement = "SELECT queryid, LEFT(query, 80) FROM pg_stat_statements(true)",
            db.sql.table = "pg_stat_statements"
        );

        let result = self
            .run_query_text_lookup(pool, sql.as_str(), missing)
            .instrument(query_span)
            .await;

        match result {
            Ok(rows) => {
                self.apply_text_lookup(&rows);
                debug!(
                    collector = "pg_statements",
                    resolved = rows.len(),
                    requested = missing.len(),
                    "refreshed pg_stat_statements query texts"
                );
            }
            Err(error) => {
                self.mark_text_lookup_attempted();
                warn!(
                    collector = "pg_statements",
                    error = %error,
                    "query text lookup failed - query_short stays unresolved"
                );
            }
        }
    }

    fn extension_state_lock(&self) -> MutexGuard<'_, ExtensionState> {
        match self.extension_state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn cached_extension_availability(&self) -> Option<bool> {
        match *self.extension_state_lock() {
            ExtensionState::Installed => Some(true),
            ExtensionState::Missing { last_checked }
                if last_checked.elapsed() < MISSING_EXTENSION_RECHECK_AFTER =>
            {
                Some(false)
            }
            ExtensionState::Unknown | ExtensionState::Missing { .. } => None,
        }
    }

    fn update_extension_state(&self, installed: bool) {
        let mut state = self.extension_state_lock();
        *state = if installed {
            ExtensionState::Installed
        } else {
            ExtensionState::Missing {
                last_checked: Instant::now(),
            }
        };
    }

    async fn pg_statements_available(&self, pool: &PgPool) -> Result<bool> {
        if let Some(installed) = self.cached_extension_availability() {
            return Ok(installed);
        }

        let installed = pg_statements_installed(pool).await?;
        self.update_extension_state(installed);

        if !installed {
            warn!(
                collector = "pg_statements",
                "pg_stat_statements extension not installed - skipping collection"
            );
        }

        Ok(installed)
    }

    fn record_statement_row(&self, row: &PgRow, queryid: i64, query_short: &str) {
        let queryid = queryid.to_string();
        let datname: String = row
            .try_get("datname")
            .unwrap_or_else(|_| "unknown".to_string());
        let usename: String = row
            .try_get("usename")
            .unwrap_or_else(|_| "unknown".to_string());

        let labels = [
            queryid.as_str(),
            datname.as_str(),
            usename.as_str(),
            query_short,
        ];

        let total_time: f64 = row.try_get("total_exec_time_sec").unwrap_or(0.0);
        let mean_time: f64 = row.try_get("mean_exec_time_sec").unwrap_or(0.0);
        let max_time: f64 = row.try_get("max_exec_time_sec").unwrap_or(0.0);
        let stddev_time: f64 = row.try_get("stddev_exec_time_sec").unwrap_or(0.0);

        self.total_exec_time
            .with_label_values(&labels)
            .set(total_time);
        self.mean_exec_time
            .with_label_values(&labels)
            .set(mean_time);
        self.max_exec_time.with_label_values(&labels).set(max_time);
        self.stddev_exec_time
            .with_label_values(&labels)
            .set(stddev_time);

        let calls: i64 = row.try_get("calls").unwrap_or(0);
        let rows_returned: i64 = row.try_get("rows").unwrap_or(0);
        self.calls.with_label_values(&labels).set(calls);
        self.rows.with_label_values(&labels).set(rows_returned);

        let shared_hit: i64 = row.try_get("shared_blks_hit").unwrap_or(0);
        let shared_read: i64 = row.try_get("shared_blks_read").unwrap_or(0);
        let shared_dirtied: i64 = row.try_get("shared_blks_dirtied").unwrap_or(0);
        let shared_written: i64 = row.try_get("shared_blks_written").unwrap_or(0);

        self.shared_blks_hit
            .with_label_values(&labels)
            .set(shared_hit);
        self.shared_blks_read
            .with_label_values(&labels)
            .set(shared_read);
        self.shared_blks_dirtied
            .with_label_values(&labels)
            .set(shared_dirtied);
        self.shared_blks_written
            .with_label_values(&labels)
            .set(shared_written);

        let local_hit: i64 = row.try_get("local_blks_hit").unwrap_or(0);
        let local_read: i64 = row.try_get("local_blks_read").unwrap_or(0);
        let local_dirtied: i64 = row.try_get("local_blks_dirtied").unwrap_or(0);
        let local_written: i64 = row.try_get("local_blks_written").unwrap_or(0);

        self.local_blks_hit
            .with_label_values(&labels)
            .set(local_hit);
        self.local_blks_read
            .with_label_values(&labels)
            .set(local_read);
        self.local_blks_dirtied
            .with_label_values(&labels)
            .set(local_dirtied);
        self.local_blks_written
            .with_label_values(&labels)
            .set(local_written);

        let temp_read: i64 = row.try_get("temp_blks_read").unwrap_or(0);
        let temp_written: i64 = row.try_get("temp_blks_written").unwrap_or(0);
        self.temp_blks_read
            .with_label_values(&labels)
            .set(temp_read);
        self.temp_blks_written
            .with_label_values(&labels)
            .set(temp_written);

        let wal: i64 = row.try_get("wal_bytes").unwrap_or(0);
        self.wal_bytes.with_label_values(&labels).set(wal);

        let total_blocks = shared_hit + shared_read;
        let hit_ratio = if total_blocks > 0 {
            i64_to_f64(shared_hit) / i64_to_f64(total_blocks)
        } else {
            1.0
        };
        self.cache_hit_ratio
            .with_label_values(&labels)
            .set(hit_ratio);
    }
}

const STATEMENT_LABELS: [&str; 4] = ["queryid", "datname", "usename", "query_short"];

#[allow(clippy::expect_used)]
fn statement_gauge(name: &str, help: &str) -> GaugeVec {
    GaugeVec::new(
        Opts::new(name, help).namespace("postgres"),
        &STATEMENT_LABELS,
    )
    .expect("pg_stat_statements gauge metric")
}

#[allow(clippy::expect_used)]
fn statement_int_gauge(name: &str, help: &str) -> IntGaugeVec {
    IntGaugeVec::new(
        Opts::new(name, help).namespace("postgres"),
        &STATEMENT_LABELS,
    )
    .expect("pg_stat_statements int metric")
}

async fn pg_statements_installed(pool: &PgPool) -> Result<bool> {
    Ok(sqlx::query("SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'")
        .fetch_optional(pool)
        .await?
        .is_some())
}

impl Collector for PgStatementsCollector {
    fn name(&self) -> &'static str {
        "pg_statements"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "pg_statements")
    )]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.total_exec_time.clone()))?;
        registry.register(Box::new(self.mean_exec_time.clone()))?;
        registry.register(Box::new(self.max_exec_time.clone()))?;
        registry.register(Box::new(self.stddev_exec_time.clone()))?;
        registry.register(Box::new(self.calls.clone()))?;
        registry.register(Box::new(self.rows.clone()))?;
        registry.register(Box::new(self.shared_blks_hit.clone()))?;
        registry.register(Box::new(self.shared_blks_read.clone()))?;
        registry.register(Box::new(self.shared_blks_dirtied.clone()))?;
        registry.register(Box::new(self.shared_blks_written.clone()))?;
        registry.register(Box::new(self.local_blks_hit.clone()))?;
        registry.register(Box::new(self.local_blks_read.clone()))?;
        registry.register(Box::new(self.local_blks_dirtied.clone()))?;
        registry.register(Box::new(self.local_blks_written.clone()))?;
        registry.register(Box::new(self.temp_blks_read.clone()))?;
        registry.register(Box::new(self.temp_blks_written.clone()))?;
        registry.register(Box::new(self.wal_bytes.clone()))?;
        registry.register(Box::new(self.cache_hit_ratio.clone()))?;

        debug!(collector = "pg_statements", "registered metrics");
        Ok(())
    }

    fn collect_once<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<Collected>> {
        Box::pin(
            async move {
                if !self.pg_statements_available(pool).await? {
                    // Nothing is published without the extension, so the previous
                    // snapshot must not keep being served.
                    return Ok(Collected::Skipped);
                }

                let query = Self::build_pg_statements_query(
                    self.supports_query_id(pool).await,
                    self.supports_toplevel(pool).await,
                );
                let top_n = i64::try_from(self.top_n).unwrap_or(i64::MAX);
                let rows: Vec<PgRow> = sqlx::query(sqlx::AssertSqlSafe(query.as_str()))
                    .bind(self.self_queryids())
                    .bind(top_n)
                    .fetch_all(pool)
                    .await?;

                // A row without a readable queryid cannot be labelled or cached, so it is
                // dropped rather than collapsed onto a shared placeholder series.
                let statements: Vec<(&PgRow, i64)> = rows
                    .iter()
                    .filter_map(|row| Some((row, row.try_get::<i64, _>("queryid").ok()?)))
                    .collect();
                let queryids: Vec<i64> = statements.iter().map(|&(_, queryid)| queryid).collect();

                let (texts, missing) = self.resolve_cached_texts(&queryids);
                self.schedule_query_text_refresh(pool, missing);

                // The lookup may have just identified some of these rows as the exporter's
                // own statements; skip them now, later scrapes filter them in SQL.
                let self_queryids: HashSet<i64> = self.self_queryids().into_iter().collect();

                // Only clear previous series after we have fresh replacement rows.
                self.total_exec_time.reset();
                self.mean_exec_time.reset();
                self.max_exec_time.reset();
                self.stddev_exec_time.reset();
                self.calls.reset();
                self.rows.reset();
                self.shared_blks_hit.reset();
                self.shared_blks_read.reset();
                self.shared_blks_dirtied.reset();
                self.shared_blks_written.reset();
                self.local_blks_hit.reset();
                self.local_blks_read.reset();
                self.local_blks_dirtied.reset();
                self.local_blks_written.reset();
                self.temp_blks_read.reset();
                self.temp_blks_written.reset();
                self.wal_bytes.reset();
                self.cache_hit_ratio.reset();

                let mut recorded = 0usize;
                for &(row, queryid) in &statements {
                    if self_queryids.contains(&queryid) {
                        continue;
                    }
                    let query_short = texts
                        .get(&queryid)
                        .map_or(UNRESOLVED_QUERY_SHORT, String::as_str);
                    self.record_statement_row(row, queryid, query_short);
                    recorded += 1;
                }

                self.prune_text_cache(&queryids.iter().copied().collect());

                debug!(
                    collector = "pg_statements",
                    queries_tracked = recorded,
                    texts_resolved = texts.len(),
                    "collected pg_stat_statements metrics"
                );

                Ok(Collected::Fresh)
            }
            .instrument(info_span!("pg_statements.collect")),
        )
    }

    /// Removes every labeled series this collector owns.
    fn reset_metrics(&self) {
        self.total_exec_time.reset();
        self.mean_exec_time.reset();
        self.max_exec_time.reset();
        self.stddev_exec_time.reset();
        self.calls.reset();
        self.rows.reset();
        self.shared_blks_hit.reset();
        self.shared_blks_read.reset();
        self.shared_blks_dirtied.reset();
        self.shared_blks_written.reset();
        self.local_blks_hit.reset();
        self.local_blks_read.reset();
        self.local_blks_dirtied.reset();
        self.local_blks_written.reset();
        self.temp_blks_read.reset();
        self.temp_blks_written.reset();
        self.wal_bytes.reset();
        self.cache_hit_ratio.reset();
    }

    fn enabled_by_default(&self) -> bool {
        false // Disabled by default - requires extension
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pg_statements_collector_name() {
        let collector = PgStatementsCollector::with_top_n(25);
        assert_eq!(collector.name(), "pg_statements");
    }

    #[test]
    fn test_pg_statements_collector_not_enabled_by_default() {
        let collector = PgStatementsCollector::with_top_n(25);
        assert!(!collector.enabled_by_default());
    }

    #[test]
    fn test_truncate_query() {
        let short = "SELECT * FROM users";
        assert_eq!(PgStatementsCollector::truncate_query(short, 80), short);

        let long = "SELECT * FROM users WHERE id = 1 AND name = 'test' AND email = 'test@example.com' AND created_at > NOW()";
        let truncated = PgStatementsCollector::truncate_query(long, 80);
        assert_eq!(truncated.len(), 83); // 80 + "..."
        assert!(truncated.ends_with("..."));
    }

    #[test]
    fn test_truncate_query_multiline() {
        let multiline = "SELECT *\n  FROM users\n  WHERE id = 1";
        let result = PgStatementsCollector::truncate_query(multiline, 80);
        assert_eq!(result, "SELECT * FROM users WHERE id = 1");
    }

    #[test]
    fn test_truncate_query_utf8_boundary() {
        let prefix = "a".repeat(79);
        let query = format!("{prefix}ı");
        let result = PgStatementsCollector::truncate_query(&query, 80);
        assert_eq!(result, format!("{prefix}..."));
    }

    #[test]
    fn test_build_pg_statements_query_uses_roles_left_join() {
        let query = PgStatementsCollector::build_pg_statements_query(true, true);

        assert!(query.contains("LEFT JOIN pg_roles r ON r.oid = sel.userid"));
        assert!(query.contains("COALESCE(r.rolname, '<unknown>') AS usename"));
    }

    #[test]
    fn test_scrape_query_never_reads_query_texts() {
        let query = PgStatementsCollector::build_pg_statements_query(true, true);

        // Reading pg_stat_statements with showtext = true makes PostgreSQL load the
        // whole query-text file and materialize every row into a work_mem-bounded
        // tuplestore, which is what spilled to base/pgsql_tmp on every scrape.
        assert!(query.contains("FROM pg_stat_statements(false) s"));
        assert!(!query.contains("FROM pg_stat_statements s"));
        assert!(!query.contains("pg_stat_statements(true)"));
        assert!(!query.contains("LEFT(query"));
        // `s.queryid` is fine; a bare `s.query` reference is not.
        assert!(!query.replace("s.queryid", "").contains("s.query"));
        assert!(!query.contains("REGEXP_REPLACE"));
        assert!(!query.contains("BTRIM"));
    }

    #[test]
    fn test_scrape_query_excludes_self_by_queryid() {
        let query = PgStatementsCollector::build_pg_statements_query(true, true);

        assert!(query.contains("AND s.queryid <> ALL($1::bigint[])"));
        assert!(!query.contains("NOT LIKE"));
    }

    #[test]
    fn test_scrape_query_carries_the_self_marker_after_the_first_keyword() {
        let query = PgStatementsCollector::build_pg_statements_query(true, true);

        // PostgreSQL stores statement text from its first token onwards, so a leading
        // comment would be dropped and the marker lost.
        assert!(query.starts_with("WITH /* pg_exporter:statements */"));
        let marker_at = query.find(SELF_QUERY_MARKER);
        assert!(marker_at.is_some_and(|at| at < QUERY_SHORT_MAX_LEN));
    }

    #[test]
    fn test_scrape_query_unions_time_and_temp_rankings() {
        let query = PgStatementsCollector::build_pg_statements_query(true, true);

        assert!(query.contains("ORDER BY total_exec_time_sec DESC LIMIT $2"));
        assert!(query.contains("WHERE temp_blks_written > 0"));
        assert!(query.contains("ORDER BY temp_blks_written DESC LIMIT $2"));
        // UNION (not UNION ALL) so a statement in both rankings is exported once.
        assert!(query.contains("UNION\n"));
        assert!(!query.contains("UNION ALL"));
    }

    #[test]
    fn test_scrape_query_casts_numeric_columns() {
        let query = PgStatementsCollector::build_pg_statements_query(true, true);

        for column in [
            "calls",
            "rows",
            "shared_blks_hit",
            "temp_blks_read",
            "temp_blks_written",
        ] {
            assert!(
                query.contains(&format!("s.{column}::bigint AS {column}")),
                "`{column}` must be cast to bigint"
            );
        }
        assert!(query.contains("COALESCE(s.wal_bytes, 0)::bigint AS wal_bytes"));
        assert!(query.contains("::double precision AS total_exec_time_sec"));
    }

    #[test]
    fn test_query_text_lookup_is_the_only_showtext_statement() {
        let lookup = PgStatementsCollector::build_query_text_lookup(true);

        assert!(lookup.contains("FROM pg_stat_statements(true) s"));
        assert!(lookup.contains("LEFT(s.query, 80) AS query_short"));
        assert!(lookup.contains("s.queryid = ANY($1::bigint[])"));
        assert!(lookup.contains("s.query LIKE 'WITH /* pg_exporter:statements */%'"));
        assert!(lookup.starts_with("SELECT /* pg_exporter:statements */"));
    }

    #[test]
    fn test_text_lookup_is_disabled_without_a_refresh_interval() {
        let collector = PgStatementsCollector::with_config(25, None);
        assert!(!collector.text_lookup_due());
    }

    #[test]
    fn test_text_lookup_is_due_before_the_first_lookup() {
        let collector = PgStatementsCollector::with_top_n(25);
        assert!(collector.text_lookup_due());
    }

    #[test]
    fn test_text_lookup_is_rate_limited() {
        let collector = PgStatementsCollector::with_config(25, Some(Duration::from_mins(15)));
        collector.mark_text_lookup_attempted();

        assert!(!collector.text_lookup_due());
    }

    #[test]
    fn test_text_lookup_is_due_again_after_the_interval() {
        let collector = PgStatementsCollector::with_config(25, Some(Duration::from_nanos(1)));
        collector.mark_text_lookup_attempted();
        std::thread::sleep(Duration::from_millis(2));

        assert!(collector.text_lookup_due());
    }

    #[test]
    fn test_text_lookup_is_single_flight_and_released_by_guard() {
        let collector = PgStatementsCollector::with_top_n(25);
        let first = collector
            .begin_text_lookup()
            .unwrap_or_else(|| unreachable!("the first lookup must claim the slot"));

        assert!(collector.begin_text_lookup().is_none());

        // Dropping models normal completion as well as cancellation or unwinding.
        drop(first);
        assert!(collector.begin_text_lookup().is_some());
    }

    #[test]
    fn test_work_mem_refusal_warning_is_shared_and_emitted_once() {
        let collector = PgStatementsCollector::with_top_n(25);
        let clone = collector.clone();

        assert!(collector.should_warn_work_mem_refusal());
        assert!(!collector.should_warn_work_mem_refusal());
        assert!(!clone.should_warn_work_mem_refusal());
    }

    #[test]
    fn test_resolve_cached_texts_reports_missing_queryids() {
        let collector = PgStatementsCollector::with_top_n(25);
        {
            let mut cache = collector.text_cache_lock();
            cache.texts.insert(42, "SELECT 1".to_string());
        }

        let (resolved, missing) = collector.resolve_cached_texts(&[42, 7]);

        assert_eq!(resolved.get(&42).map(String::as_str), Some("SELECT 1"));
        assert_eq!(missing, vec![7]);
    }

    /// A NULL or blank query text must not be cached: a cached `queryid` is never
    /// looked up again, so caching it would pin `query_short=""` on that series
    /// for the lifetime of the process.
    #[test]
    fn test_blank_query_text_is_not_cacheable() {
        assert_eq!(PgStatementsCollector::cacheable_text(None), None);
        assert_eq!(PgStatementsCollector::cacheable_text(Some("")), None);
        assert_eq!(PgStatementsCollector::cacheable_text(Some("   \n")), None);
        assert_eq!(
            PgStatementsCollector::cacheable_text(Some("SELECT 1")),
            Some("SELECT 1".to_string())
        );
    }

    /// `<insufficient privilege>` describes the *reader*, not the statement, and the
    /// privilege can be granted later. Caching it would pin the placeholder on that
    /// series until the cache happens to be pruned.
    #[test]
    fn test_insufficient_privilege_text_is_not_cacheable() {
        assert_eq!(
            PgStatementsCollector::cacheable_text(Some(PG_INSUFFICIENT_PRIVILEGE_TEXT)),
            None
        );
        assert_eq!(
            PgStatementsCollector::cacheable_text(Some("  <insufficient privilege>  ")),
            None
        );
        // A real statement that merely mentions the placeholder is still cacheable.
        assert_eq!(
            PgStatementsCollector::cacheable_text(Some("SELECT '<insufficient privilege>'")),
            Some("SELECT '<insufficient privilege>'".to_string())
        );
    }

    /// Long texts are still truncated to the label budget on the way into the cache.
    #[test]
    fn test_cacheable_text_truncates_to_label_budget() {
        let long = "a".repeat(QUERY_SHORT_MAX_LEN * 2);
        let cached = PgStatementsCollector::cacheable_text(Some(&long))
            .unwrap_or_else(|| unreachable!("non-blank text is cacheable"));

        assert_eq!(cached, format!("{}...", "a".repeat(QUERY_SHORT_MAX_LEN)));
    }

    #[test]
    fn test_cacheable_text_truncates_at_a_utf8_boundary() {
        let text = format!("{}сначала выбираем строки", "a".repeat(79));
        assert!(!text.is_char_boundary(QUERY_SHORT_MAX_LEN));

        let cached = PgStatementsCollector::cacheable_text(Some(&text))
            .unwrap_or_else(|| unreachable!("non-blank text is cacheable"));

        assert_eq!(cached, format!("{}...", "a".repeat(79)));
    }

    /// The prefixes used for self-identification must stay in sync with what the two
    /// query builders actually emit, otherwise self-exclusion silently stops working.
    #[test]
    fn self_query_prefixes_carry_the_marker() {
        for prefix in SELF_QUERY_PREFIXES {
            assert!(
                prefix.contains(SELF_QUERY_MARKER),
                "prefix {prefix:?} lost the marker"
            );
        }

        for query in [
            PgStatementsCollector::build_pg_statements_query(true, true),
            PgStatementsCollector::build_pg_statements_query(false, true),
            PgStatementsCollector::build_query_text_lookup(true),
        ] {
            assert!(
                PgStatementsCollector::is_self_query_text(&query),
                "builder output is not self-identifying: {query}"
            );
        }
    }

    /// A user statement that merely mentions the marker must not be mistaken for the
    /// collector's own query: `self_queryids` is never pruned, so a false positive would
    /// drop that statement from the metrics permanently.
    #[test]
    fn test_user_query_mentioning_the_marker_is_not_self() {
        assert!(!PgStatementsCollector::is_self_query_text(
            "SELECT 1 /* pg_exporter:statements */"
        ));
        assert!(!PgStatementsCollector::is_self_query_text(
            "UPDATE jobs SET name = 'pg_exporter:statements' WHERE id = $1"
        ));
        assert!(!PgStatementsCollector::is_self_query_text("SELECT 1"));
    }

    /// PG14+ must exclude the collector's own statement without reading any query text,
    /// so that `--statements.query-text-refresh=0` still keeps it out of the metrics.
    #[test]
    fn test_query_id_self_exclusion_is_text_free_and_version_gated() {
        let modern = PgStatementsCollector::build_pg_statements_query(true, true);
        assert!(modern.contains("pg_backend_pid()"));
        assert!(modern.contains("a.query_id"));
        assert!(
            modern.contains("IS DISTINCT FROM"),
            "a NULL query_id must not drop every row"
        );
        assert!(
            !modern.contains("pg_stat_statements(true)"),
            "self-exclusion must not reintroduce a full text read"
        );

        let legacy = PgStatementsCollector::build_pg_statements_query(false, true);
        assert!(!legacy.contains("pg_backend_pid()"));
        assert!(!legacy.contains("query_id"));
        assert!(legacy.contains("s.queryid <> ALL($1::bigint[])"));
    }

    /// `toplevel` arrived in `pg_stat_statements` 1.9 (`PostgreSQL` 14). Emitting the
    /// filter unconditionally would fail on 12/13 with `column does not exist`, taking
    /// the whole collector down instead of degrading.
    #[test]
    fn test_toplevel_filter_is_version_gated() {
        let modern = PgStatementsCollector::build_pg_statements_query(true, true);
        assert!(
            modern.contains("AND s.toplevel"),
            "track = all must not collapse a nested and a top-level entry onto one series"
        );

        let legacy = PgStatementsCollector::build_pg_statements_query(true, false);
        assert!(!legacy.contains("toplevel"));
        // Dropping the filter must not disturb the rest of the predicate.
        assert!(legacy.contains("AND s.queryid <> ALL($1::bigint[])"));
        assert!(legacy.contains("FROM pg_stat_statements(false) s"));
    }

    /// The text lookup must anchor its self-match instead of scanning for the bare
    /// marker anywhere in a user's query text.
    #[test]
    fn test_text_lookup_anchors_the_self_match() {
        let lookup = PgStatementsCollector::build_query_text_lookup(true);
        assert!(!lookup.contains("LIKE '%pg_exporter:statements%'"));
        assert!(lookup.contains("LIKE 'WITH /* pg_exporter:statements */%'"));
        assert!(lookup.contains("LIKE 'SELECT /* pg_exporter:statements */%'"));
    }

    #[test]
    fn test_prune_keeps_the_cache_bounded() {
        let collector = PgStatementsCollector::with_top_n(25);
        {
            let mut cache = collector.text_cache_lock();
            for queryid in 0..2000 {
                cache.texts.insert(queryid, "SELECT 1".to_string());
            }
        }

        let keep: HashSet<i64> = [1, 2, 3].into_iter().collect();
        collector.prune_text_cache(&keep);

        let cache = collector.text_cache_lock();
        assert_eq!(cache.texts.len(), 3);
        assert!(cache.texts.contains_key(&1));
    }

    #[test]
    fn test_prune_keeps_entries_below_the_cap() {
        let collector = PgStatementsCollector::with_top_n(25);
        {
            let mut cache = collector.text_cache_lock();
            cache.texts.insert(1, "SELECT 1".to_string());
            cache.texts.insert(2, "SELECT 2".to_string());
        }

        collector.prune_text_cache(&HashSet::new());

        assert_eq!(collector.text_cache_lock().texts.len(), 2);
    }

    #[test]
    fn test_self_queryids_start_empty() {
        let collector = PgStatementsCollector::with_top_n(25);
        assert!(collector.self_queryids().is_empty());
    }

    #[test]
    fn test_cached_extension_availability_uses_installed_cache() {
        let collector = PgStatementsCollector::with_top_n(25);
        collector.update_extension_state(true);

        assert_eq!(collector.cached_extension_availability(), Some(true));
    }

    #[test]
    fn test_cached_extension_availability_uses_missing_cache_before_ttl() {
        let collector = PgStatementsCollector::with_top_n(25);
        {
            let mut state = collector.extension_state_lock();
            *state = ExtensionState::Missing {
                last_checked: Instant::now(),
            };
        }

        assert_eq!(collector.cached_extension_availability(), Some(false));
    }

    #[test]
    fn test_cached_extension_availability_rechecks_missing_after_ttl() {
        let collector = PgStatementsCollector::with_top_n(25);
        let expired_check = Instant::now()
            .checked_sub(MISSING_EXTENSION_RECHECK_AFTER)
            .unwrap_or_else(Instant::now);
        {
            let mut state = collector.extension_state_lock();
            *state = ExtensionState::Missing {
                last_checked: expired_check,
            };
        }

        assert_eq!(collector.cached_extension_availability(), None);
    }
}
