//! Regression tests for the per-database connection model.
//!
//! The multi-database collectors (`stat`, `index`, `index_unused`) must open per-database
//! connections **ephemerally** — one connection per scrape query, closed on drop — via
//! `util::open_db_connection`. They must NOT cache a pool/connection per database: doing so
//! reintroduces connection-per-database accumulation that can exhaust `max_connections` on
//! large or connection-constrained clusters (e.g. AWS RDS). These tests lock that invariant
//! so a future change cannot silently regress it.

use super::common;
use anyhow::{Result, anyhow};
use pg_exporter::collectors::util::{get_max_db_concurrency, open_db_connection};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

/// Each call must return a **fresh** backend (no cache/reuse), and dropping it must close it.
#[tokio::test]
async fn open_db_connection_is_fresh_and_ephemeral() -> Result<()> {
    // Initialises the global base connect options used by `open_db_connection`.
    let admin = common::create_test_pool().await?;
    let test_db = common::IsolatedTestDatabase::new("ephemeral_conn").await?;
    let dbname = test_db.database_name().to_string();

    // Two per-database connections held at once must be two DISTINCT backends. A per-database
    // pool cache would hand back the same reused connection (identical backend PID).
    let mut c1 = open_db_connection(&dbname).await?;
    let mut c2 = open_db_connection(&dbname).await?;
    let pid1: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut c1)
        .await?;
    let pid2: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut c2)
        .await?;
    assert_ne!(
        pid1, pid2,
        "open_db_connection must open a fresh connection each call (no per-database pool cache)"
    );

    // Dropping the connections must close them (ephemeral): the backends must disappear.
    drop(c1);
    drop(c2);

    let mut remaining = i64::MAX;
    for _ in 0..25 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        remaining = sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE pid = ANY($1)")
            .bind(vec![pid1, pid2])
            .fetch_one(&admin)
            .await?;
        if remaining == 0 {
            break;
        }
    }
    assert_eq!(
        remaining, 0,
        "per-database connections must be closed on drop (ephemeral); backends {pid1}/{pid2} \
         lingered — was a per-database pool cache reintroduced?"
    );

    admin.close().await;
    test_db.cleanup().await?;
    Ok(())
}

/// The number of per-database connections open at any instant must never exceed the
/// configured concurrency limit.
///
/// Every multi-database fan-out collector (`stat`, `index`, `index_unused`) gates its
/// ephemeral `open_db_connection` calls behind `Semaphore::new(get_max_db_concurrency())`.
/// That semaphore is what bounds the exporter's per-database connection footprint to the
/// concurrency limit *regardless of how many databases exist in the cluster* — the whole
/// point of the ephemeral model. This test reproduces that exact primitive (semaphore sized
/// to `get_max_db_concurrency()` + ephemeral `open_db_connection`) with more tasks than
/// permits, and asserts the peak number of simultaneously-open connections never exceeds the
/// limit. If a future change drops or weakens the semaphore, the observed peak jumps to the
/// task count and this test fails.
#[tokio::test]
async fn per_database_connections_never_exceed_concurrency_limit() -> Result<()> {
    let admin = common::create_test_pool().await?;
    let test_db = common::IsolatedTestDatabase::new("conn_concurrency").await?;
    let dbname = test_db.database_name().to_string();

    let limit = get_max_db_concurrency();
    assert!(limit >= 1, "concurrency limit must be at least 1");

    // More tasks than permits so the semaphore — not the task count — is the binding
    // constraint. A removed semaphore would let all `task_count` connections open at once.
    let task_count = limit + 4;

    let semaphore = Arc::new(Semaphore::new(limit));
    let in_flight = Arc::new(AtomicUsize::new(0));
    let observed_peak = Arc::new(AtomicUsize::new(0));

    let mut tasks = JoinSet::new();
    for _ in 0..task_count {
        let semaphore = Arc::clone(&semaphore);
        let in_flight = Arc::clone(&in_flight);
        let observed_peak = Arc::clone(&observed_peak);
        let dbname = dbname.clone();

        tasks.spawn(async move {
            let _permit = semaphore
                .acquire_owned()
                .await
                .map_err(|_| anyhow!("concurrency semaphore closed before acquiring permit"))?;

            // Open the real ephemeral per-database connection, then record how many are open
            // right now. Counting between open and the matching decrement (below) measures the
            // true simultaneous connection count while the permit is held.
            let mut conn = open_db_connection(&dbname).await?;
            let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            observed_peak.fetch_max(now, Ordering::SeqCst);

            // Hold the connection open long enough that peers running concurrently overlap.
            sqlx::query("SELECT pg_sleep(0.15)")
                .execute(&mut conn)
                .await?;

            in_flight.fetch_sub(1, Ordering::SeqCst);
            drop(conn);
            Ok::<(), anyhow::Error>(())
        });
    }

    while let Some(joined) = tasks.join_next().await {
        joined.map_err(|e| anyhow!("per-database task panicked: {e}"))??;
    }

    let peak = observed_peak.load(Ordering::SeqCst);
    assert!(
        peak <= limit,
        "peak simultaneous per-database connections ({peak}) exceeded the concurrency limit \
         ({limit}); was the fan-out concurrency semaphore removed or weakened?"
    );
    if limit >= 2 {
        assert!(
            peak >= 2,
            "expected the {task_count} tasks to actually run concurrently (peak {peak}) under a \
             limit of {limit}; the workload serialized unexpectedly, so the bound above is not \
             being exercised"
        );
    }

    admin.close().await;
    test_db.cleanup().await?;
    Ok(())
}
