use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::Registry;
use sqlx::PgPool;
use std::collections::HashMap;

#[macro_use]
mod register_macro;

/// Outcome of one collection attempt.
///
/// `Skipped` means the collector published **nothing at all** this scrape — an unsupported
/// server version, a missing view, a revoked privilege. A collector that refreshed part of
/// its surface and skipped the rest must report `Fresh`, or [`Collector::collect`] would
/// clear the values it just published.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Collected {
    /// Everything this collector publishes is current.
    Fresh,
    /// Nothing was published; previously published series must not persist.
    Skipped,
}

pub trait Collector {
    fn name(&self) -> &'static str;

    /// Register metrics with the prometheus registry
    ///
    /// # Errors
    ///
    /// Returns an error if metric registration fails
    fn register_metrics(&self, registry: &Registry) -> Result<()>;

    /// Collect once, reporting whether anything was published.
    ///
    /// This is the implementation hook; callers should use [`Collector::collect`], which
    /// also settles a skip.
    ///
    /// # Errors
    ///
    /// Return an error for a **genuine fault**, and note precisely what that buys:
    /// [`Collector::collect`] does not clear on an error, so registry state is preserved and
    /// the series resumes on the next successful scrape. The errored scrape itself serves
    /// no metric samples — the registry fails it and the handler returns 503 with an
    /// error-only body (`# Error collecting metrics: ...`) — so the choice is not "stale data
    /// versus none", it is "the series resumes versus the series was cleared".
    ///
    /// [`Collected::Skipped`] is *not* the safe default for anything that failed — it
    /// **clears** the collector's metrics, so using it for a transient fault destroys the
    /// previous snapshot instead of preserving it. Reserve it for conditions where the data
    /// is known to be unavailable rather than unread:
    ///
    /// - the server version, extension, view or function does not exist (`42P01`, `42883`,
    ///   `42704`)
    /// - the role lacks the privilege to read it (`42501`)
    /// - the source exists and is readable but genuinely holds nothing to publish
    ///
    /// Classify by `SQLSTATE`; do not treat "the query returned an error" as a skip.
    ///
    /// An error does fail the whole scrape — the registry aggregates into
    /// `ScrapeError::CollectorFailed` and the handler maps that to a 503 carrying only an
    /// error line, with no metric samples — so it is not free.
    /// A server that is already unreachable is handled earlier and more cheaply:
    /// `select_active_pool` runs a connectivity check first and serves `pg_up 0` instead of
    /// running collectors at all. But that check only proves the server was reachable *at
    /// that moment*: a connection dropped mid-scrape, a backend terminated, or a restart
    /// between the check and this query all surface here as an ordinary query error. Those
    /// are faults, and reporting them as such is correct — it keeps the previous snapshot,
    /// where a skip would delete it.
    // lifetime 'a is needed to tie the future to the lifetime of self and pool
    fn collect_once<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<Collected>>;

    /// Stop asserting values from an earlier scrape.
    ///
    /// Required on purpose: every collector must decide what a skip means for it.
    ///
    /// Labeled metrics: `reset()` removes the children, so the series disappear. Scalar
    /// metrics cannot be removed while registered, so zeroing one is a *claim* rather than
    /// an absence. Skip-capable scalars are therefore declared as zero-label metric vectors
    /// (identical on the wire, removable via `reset()`); a collector with no skip path may
    /// implement this as a documented no-op.
    fn reset_metrics(&self);

    fn enabled_by_default(&self) -> bool {
        false
    }

    /// Collect, then settle the result. **This is the entry point callers should use.**
    ///
    /// Clears the collector's metrics after a [`Collected::Skipped`], so a skip cannot keep
    /// serving stale values, and deliberately does **not** clear after an error: a failed
    /// scrape preserves the last good snapshot rather than blanking it.
    ///
    /// # Errors
    ///
    /// Propagates whatever `collect_once` returned.
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>>
    where
        Self: Sync,
    {
        Box::pin(async move {
            if matches!(self.collect_once(pool).await?, Collected::Skipped) {
                self.reset_metrics();
            }
            Ok(())
        })
    }
}

/// Label values for a zero-label metric vector.
///
/// Skip-capable scalar metrics are declared as `*Vec` with an empty label set: the wire
/// format is identical to a plain scalar (no `{}` is emitted for empty label pairs), but
/// `reset()` removes the child so the series genuinely disappears on a skip instead of
/// being zeroed — a zeroed gauge is a claim, an absent one is not. An empty slice literal
/// cannot infer its element type, hence the named constant.
pub(crate) const NO_LABELS: [&str; 0] = [];

/// Runs collectors' blocking OS reads on Tokio's blocking pool (issue #35).
pub(crate) mod blocking;

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
/// the shared pool (`3`) plus this value (`2`) instead of multiplying per collector.
pub(crate) const MAX_DB_QUERY_CONCURRENCY: usize = 2;

/// Largest operator-configurable non-default-database query concurrency.
///
/// This is intentionally conservative: an exporter should not be able to create an
/// arbitrarily large connection wave because of a mistyped CLI or environment value.
pub(crate) const MAX_DB_QUERY_CONCURRENCY_LIMIT: usize = 16;

/// Maximum number of connections retained by the shared default-database pool.
pub(crate) const SHARED_POOL_MAX_CONNECTIONS: u32 = 3;

// A zero-permit semaphore would deadlock every multi-database collector, so enforce a
// non-zero limit at compile time.
const _: () = assert!(
    MAX_DB_QUERY_CONCURRENCY > 0,
    "MAX_DB_QUERY_CONCURRENCY must be non-zero"
);
const _: () = assert!(
    MAX_DB_QUERY_CONCURRENCY <= MAX_DB_QUERY_CONCURRENCY_LIMIT,
    "default database concurrency must not exceed its configurable limit"
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
#[allow(clippy::expect_used)]
mod settlement_contract {
    use super::{Collected, Collector};
    use anyhow::Result;
    use futures::future::BoxFuture;
    use prometheus::{IntGaugeVec, Opts, Registry};
    use sqlx::PgPool;
    use sqlx::postgres::PgPoolOptions;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A collector whose outcome is dictated by the test.
    struct Probe {
        name: &'static str,
        outcome: fn() -> Result<Collected>,
        gauge: IntGaugeVec,
        resets: Arc<AtomicUsize>,
    }

    impl Probe {
        fn new(name: &'static str, outcome: fn() -> Result<Collected>) -> Self {
            let gauge = IntGaugeVec::new(Opts::new(name, "probe"), &["k"]).expect("gauge");
            gauge.with_label_values(&["v"]).set(1);
            Self {
                name,
                outcome,
                gauge,
                resets: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn samples(&self) -> usize {
            // UFCS: prometheus has its own `Collector` trait whose `collect` would clash with
            // the one under test here.
            prometheus::core::Collector::collect(&self.gauge)
                .first()
                .map_or(0, |f| f.get_metric().len())
        }
    }

    impl Collector for Probe {
        fn name(&self) -> &'static str {
            self.name
        }
        fn register_metrics(&self, registry: &Registry) -> Result<()> {
            registry.register(Box::new(self.gauge.clone()))?;
            Ok(())
        }
        fn collect_once<'a>(&'a self, _pool: &'a PgPool) -> BoxFuture<'a, Result<Collected>> {
            Box::pin(async move { (self.outcome)() })
        }
        fn reset_metrics(&self) {
            self.resets.fetch_add(1, Ordering::Relaxed);
            self.gauge.reset();
        }
    }

    /// An umbrella that settles each sub through the safe `collect`, as the real ones do.
    struct Umbrella {
        subs: Vec<Arc<dyn Collector + Send + Sync>>,
    }

    impl Collector for Umbrella {
        fn name(&self) -> &'static str {
            "umbrella"
        }
        fn register_metrics(&self, _registry: &Registry) -> Result<()> {
            Ok(())
        }
        fn collect_once<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<Collected>> {
            Box::pin(async move {
                for sub in &self.subs {
                    sub.collect(pool).await?;
                }
                Ok(Collected::Fresh)
            })
        }
        fn reset_metrics(&self) {
            for sub in &self.subs {
                sub.reset_metrics();
            }
        }
    }

    /// A pool that is never connected: `collect_once` here ignores it entirely.
    fn lazy_pool() -> PgPool {
        PgPoolOptions::new()
            .connect_lazy("postgresql://unused:unused@127.0.0.1:1/unused")
            .expect("lazy pool")
    }

    #[tokio::test]
    async fn fresh_does_not_reset() {
        let probe = Probe::new("probe_fresh", || Ok(Collected::Fresh));
        probe.collect(&lazy_pool()).await.expect("collect");

        assert_eq!(probe.resets.load(Ordering::Relaxed), 0);
        assert_eq!(
            probe.samples(),
            1,
            "a fresh collection must keep its series"
        );
    }

    #[tokio::test]
    async fn skipped_resets() {
        let probe = Probe::new("probe_skipped", || Ok(Collected::Skipped));
        probe.collect(&lazy_pool()).await.expect("collect");

        assert_eq!(probe.resets.load(Ordering::Relaxed), 1);
        assert_eq!(probe.samples(), 0, "a skip must remove the series");
    }

    /// The invariant that keeps a transient fault from destroying the last good snapshot: an
    /// error propagates *and* leaves the metrics alone. `pg_statements` and `vacuum/stats`
    /// both depend on this, and it is why a failure must not be reported as a skip.
    #[tokio::test]
    async fn err_propagates_without_resetting() {
        let probe = Probe::new("probe_err", || Err(anyhow::anyhow!("boom")));
        let result = probe.collect(&lazy_pool()).await;

        assert!(
            result.is_err(),
            "an error must reach the caller, which fails the scrape"
        );
        assert_eq!(probe.resets.load(Ordering::Relaxed), 0);
        assert_eq!(
            probe.samples(),
            1,
            "an error must preserve the previous snapshot, not clear it"
        );
    }

    /// Aggregating an outcome upward would be wrong in both directions. This pins the
    /// per-sub settlement that avoids it.
    #[tokio::test]
    async fn one_skipped_sub_cannot_clear_a_sibling() {
        let fresh = Arc::new(Probe::new("probe_sibling_fresh", || Ok(Collected::Fresh)));
        let skipped = Arc::new(Probe::new("probe_sibling_skipped", || {
            Ok(Collected::Skipped)
        }));
        let umbrella = Umbrella {
            subs: vec![
                Arc::clone(&fresh) as Arc<dyn Collector + Send + Sync>,
                Arc::clone(&skipped) as Arc<dyn Collector + Send + Sync>,
            ],
        };

        umbrella.collect(&lazy_pool()).await.expect("collect");

        assert_eq!(fresh.samples(), 1, "the fresh sibling must be untouched");
        assert_eq!(skipped.samples(), 0, "the skipped sub must be cleared");
        assert_eq!(fresh.resets.load(Ordering::Relaxed), 0);
        assert_eq!(skipped.resets.load(Ordering::Relaxed), 1);
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod zero_label_vector_contract {
    use super::NO_LABELS;
    use prometheus::{Encoder, IntGaugeVec, Opts, Registry, TextEncoder};

    fn render(registry: &Registry) -> String {
        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&registry.gather(), &mut buf)
            .expect("encode");
        String::from_utf8(buf).expect("utf8")
    }

    /// The whole skip-clearing design rests on this: a zero-label vector must expose the
    /// *same* wire format as a plain scalar, so converting a scalar to one cannot break a
    /// dashboard or an alert, and `reset()` must make the sample disappear rather than
    /// zero it. A zeroed state gauge is a false claim; an absent one is honest.
    #[test]
    fn zero_label_vector_is_wire_identical_and_removable() {
        let registry = Registry::new();
        let gauge =
            IntGaugeVec::new(Opts::new("pg_test_zero_label", "help"), &NO_LABELS).expect("create");
        registry
            .register(Box::new(gauge.clone()))
            .expect("register");

        // Before any value is set there is no child, so no sample is exposed.
        assert!(
            !render(&registry).contains("pg_test_zero_label "),
            "an unset zero-label vector must expose no sample"
        );

        gauge.with_label_values(&NO_LABELS).set(7);
        let exposed = render(&registry);
        assert!(
            exposed.contains("\npg_test_zero_label 7\n"),
            "must render exactly like a scalar, got: {exposed}"
        );
        assert!(
            !exposed.contains("pg_test_zero_label{"),
            "no label braces may be emitted for an empty label set, got: {exposed}"
        );

        gauge.reset();
        assert!(
            !render(&registry).contains("pg_test_zero_label "),
            "reset() must remove the series, not zero it"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_DB_QUERY_CONCURRENCY, MAX_DB_QUERY_CONCURRENCY_LIMIT, SHARED_POOL_MAX_CONNECTIONS,
        all_databases_failed,
    };

    #[test]
    fn default_connection_budget_is_five() {
        let shared_connections = usize::try_from(SHARED_POOL_MAX_CONNECTIONS).ok();
        assert_eq!(
            shared_connections.map(|shared| shared + MAX_DB_QUERY_CONCURRENCY),
            Some(5)
        );
        assert_eq!(MAX_DB_QUERY_CONCURRENCY, 2);
        assert_eq!(MAX_DB_QUERY_CONCURRENCY_LIMIT, 16);
    }

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
    stat_io => StatIoCollector,
    slru => SlruCollector,
    temp => TempCollector,
    replication => ReplicationCollector,
    index => IndexCollector,
    sequences => SequencesCollector,
    system => SystemCollector,
    statements => StatementsCollector,
    exporter => ExporterCollector,
    tls => TlsCollector,
    // Add more collectors here - just follow the same pattern!
}

// Other modules
pub mod config;
pub mod registry;
