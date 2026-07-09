use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::Registry;
use sqlx::PgPool;
use std::collections::HashMap;

#[macro_use]
mod register_macro;

pub trait Collector {
    fn name(&self) -> &'static str;

    /// Register metrics with the prometheus registry
    ///
    /// # Errors
    ///
    /// Returns an error if metric registration fails
    fn register_metrics(&self, registry: &Registry) -> Result<()>;

    // lifetime 'a is needed to tie the future to the lifetime of self and pool
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>>;

    fn enabled_by_default(&self) -> bool {
        false
    }
}

// Make utils available to all collectors (exclusions, etc.)
pub mod util;

/// Convert i64 to f64 for Prometheus metrics.
///
/// This conversion is safe for `PostgreSQL` metric values because:
/// - Values are typically small (row counts, connections, etc.)
/// - f64 has 52-bit mantissa precision, accurate up to 2^53 (9 quadrillion)
/// - `PostgreSQL` metrics will never realistically exceed this threshold
///
/// # Arguments
/// * `value` - The i64 value to convert
///
/// # Returns
/// The f64 representation of the value
#[inline]
#[allow(clippy::cast_precision_loss)]
const fn i64_to_f64(value: i64) -> f64 {
    value as f64
}

/// Returns `true` when every per-database collection task failed, meaning the whole
/// scrape should error instead of publishing an empty or partial snapshot.
///
/// `failed_db_count` must count **all** databases that did not produce a result,
/// including every task aborted by an aggregated join-wait timeout (each aborted
/// pending task counts as one failed database). Counting an aggregated timeout as a
/// single failure would let a total collection stall masquerade as "no data" and
/// silently wipe the previous snapshot.
#[inline]
const fn all_databases_failed(num_dbs: usize, failed_db_count: usize) -> bool {
    num_dbs > 0 && failed_db_count >= num_dbs
}

/// Maximum number of non-default-database scrape queries that may run concurrently across
/// the whole exporter.
///
/// The multi-database collectors (`index_stats`, `index_unused`, `stat_user_tables`)
/// open one connection per database (a `PostgreSQL` connection is bound to a single
/// database). Without a cap, a cluster with N databases would open ~N connections
/// simultaneously on every scrape — linear in the database count — which can exhaust
/// `max_connections` on small or shared instances (for example AWS RDS). This global cap
/// is shared by every collector, so the default exporter footprint is bounded to roughly
/// the shared pool (`3`) plus this value (`5`) instead of multiplying per collector.
pub(crate) const MAX_DB_QUERY_CONCURRENCY: usize = 5;

// A zero-permit semaphore would deadlock every multi-database collector, so enforce a
// non-zero limit at compile time.
const _: () = assert!(
    MAX_DB_QUERY_CONCURRENCY > 0,
    "MAX_DB_QUERY_CONCURRENCY must be non-zero"
);

/// Default client-side timeout (milliseconds) for establishing a new `PostgreSQL` connection.
///
/// Server-side `lock_timeout` and `statement_timeout` only exist after `PostgreSQL` accepts the
/// startup packet, so they do not protect DNS, TCP, TLS, or authentication stalls. The shared
/// pool uses this as its acquisition/connect deadline, and ephemeral per-database connections
/// use it to bound connection establishment.
pub(crate) const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 5000;

/// Default server-side `lock_timeout` (milliseconds) injected into every scrape connection
/// when the DSN does not already set one.
///
/// A scrape query only takes weak `AccessShareLock`s, which are granted instantly unless a
/// concurrent session holds an `AccessExclusiveLock` (routine DDL such as `ALTER TABLE`,
/// `VACUUM FULL`, `REINDEX`, `TRUNCATE`, or an abandoned transaction). Without a
/// `lock_timeout`, a blocked scrape backend waits indefinitely server-side even after the
/// client gives up, holding a connection slot. Over successive scrapes these blocked
/// backends accumulate until `max_connections` is exhausted and the whole cluster stops
/// accepting connections. This safe default makes a blocked scrape fail fast and release
/// its slot instead of queuing.
///
/// It is intentionally conservative and fully overridable via the DSN (e.g.
/// `...?options=-c%20lock_timeout%3D5000`); when the DSN sets `lock_timeout`, that value is
/// used instead and this default is not applied.
pub(crate) const DEFAULT_LOCK_TIMEOUT_MS: u64 = 2000;

/// Default server-side `statement_timeout` (milliseconds) injected into every scrape
/// connection when the DSN does not already set one.
///
/// `lock_timeout` only covers time spent waiting for locks. `statement_timeout` is the
/// backend-side backstop for slow or stuck scrape queries after they start running. It must
/// remain positive so a backend can never run indefinitely after the HTTP scrape has gone
/// away.
pub(crate) const DEFAULT_STATEMENT_TIMEOUT_MS: u64 = 10_000;

/// Default wall-clock timeout for a full `/metrics` scrape.
///
/// This is intentionally longer than `statement_timeout`: `PostgreSQL` aborts individual
/// queries server-side first, then the exporter aborts the HTTP scrape if collector fan-out
/// or encoding still takes too long.
pub(crate) const DEFAULT_SCRAPE_TIMEOUT_MS: u64 = 15_000;

#[cfg(test)]
mod tests {
    use super::all_databases_failed;

    #[test]
    fn no_databases_never_fails() {
        assert!(!all_databases_failed(0, 0));
    }

    #[test]
    fn all_individual_failures_fail_the_scrape() {
        assert!(all_databases_failed(3, 3));
    }

    #[test]
    fn partial_failures_do_not_fail_the_scrape() {
        assert!(!all_databases_failed(3, 2));
        assert!(!all_databases_failed(3, 0));
    }

    #[test]
    fn aggregated_timeout_counting_all_pending_fails_the_scrape() {
        // Regression: an aggregated join-wait timeout aborts every pending task. Those
        // pending tasks must all be counted (num_dbs failed), otherwise a total stall
        // would be misreported as a successful empty scrape.
        let num_dbs = 3;
        let prior_individual_failures = 0;
        let aborted_pending = 3;
        let failed_db_count = prior_individual_failures + aborted_pending;
        assert!(all_databases_failed(num_dbs, failed_db_count));
    }

    #[test]
    fn aggregated_timeout_with_some_successes_is_partial_not_total() {
        // 1 DB already succeeded, then a timeout aborts the remaining 2 pending tasks.
        let num_dbs = 3;
        let aborted_pending = 2;
        assert!(!all_databases_failed(num_dbs, aborted_pending));
    }
}

// THIS IS THE ONLY PLACE YOU NEED TO ADD NEW COLLECTORS
register_collectors! {
    default => DefaultCollector,
    vacuum => VacuumCollector,
    activity => ActivityCollector,
    locks => LocksCollector,
    database => DatabaseCollector,
    stat => StatCollector,
    replication => ReplicationCollector,
    index => IndexCollector,
    statements => StatementsCollector,
    exporter => ExporterCollector,
    tls => TlsCollector,
    // Add more collectors here - just follow the same pattern!
}

// Other modules
pub mod config;
pub mod registry;
