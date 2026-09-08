//! Keeps collectors' synchronous OS reads off the async runtime.
//!
//! Several collectors sample the operating system rather than `PostgreSQL`, and every one
//! of those APIs is **blocking**: `std::fs` on `/proc` (Linux), `sysctlbyname` (FreeBSD),
//! `sysinfo` refreshes, and reading and parsing a certificate off disk. None of that is
//! async, and none of it yields.
//!
//! Running it directly inside an `async` block — which is what every one of those
//! `collect_once` implementations used to do — occupies a Tokio worker thread for the full
//! duration of the read. The registry launches every collector concurrently on that same
//! runtime, so a slow read stops the `sqlx` pool's futures from being polled and
//! *unrelated* collectors fail with `pool timed out while waiting for an open connection`.
//! That is what made issue #35 so hard to attribute: a `/proc` problem presented as a
//! `PostgreSQL` connectivity problem.
//!
//! `offload` moves the work to Tokio's blocking pool, where a long sample degrades into
//! a merely slow collector instead of a runtime-wide outage. It also gives the scrape task
//! real await points, so the scrape timeout can fire and stop waiting on it.
//!
//! Note what it does **not** buy: a `spawn_blocking` task that has already started cannot
//! be cancelled, so an aborted scrape's sample keeps running to completion on the blocking
//! pool. Collectors that carry state between scrapes must therefore tolerate a sample from
//! a previous, already-abandoned scrape overlapping the current one — see
//! `system::process::ProcessGroupCollector` for how that is handled.
//!
//! `tests/collector_safety.rs` enforces that every collector doing OS I/O comes through
//! here. [`offload_coalesced`] additionally caps each reader at one in-flight sample, so a
//! read that outlives its scrape (or never returns) cannot pile tasks onto the pool.

use anyhow::{Result, anyhow};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::debug;

/// Runs `work` on Tokio's blocking pool and awaits the result.
///
/// # Errors
///
/// Returns an error if the blocking task panicked or was cancelled; the caller reports it
/// as a collector failure rather than propagating a panic.
pub(crate) async fn offload<F, T>(collector: &'static str, work: F) -> Result<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|error| anyhow!("{collector}: OS sampling task did not complete: {error}"))
}

/// Runs `work` on the blocking pool unless the previous sample for `slot` is still
/// running, in which case it does nothing and returns `Ok(None)`.
///
/// A `spawn_blocking` task that has already started cannot be cancelled, so a scrape
/// aborted at `--scrape.timeout-ms` leaves its OS sample running. Without coalescing,
/// every later scrape enqueues *another* sample: when the read outlives the scrape
/// interval — a PSS walk slower than the timeout, or `ssl_cert_file` on a hung mount —
/// those tasks pile up on the blocking pool without bound (each started one also holds a
/// pool thread while it waits). With the slot, at most one sample per collector is ever in
/// flight; a hung read leaks exactly one blocking thread until it returns, and concurrent
/// scrapes keep the previous gauges instead of queueing duplicate work.
///
/// The guard is acquired with `try_lock` *before* spawning and moved into the closure, so
/// a hung closure holds it for as long as it is stuck and the skip path stays cheap on the
/// runtime worker. A `Mutex`, not a `Semaphore`, because `tests/collector_safety.rs`
/// reserves `Semaphore::new` in collectors for the per-database query budget.
///
/// # Errors
///
/// Same as [`offload`] when the work runs.
pub(crate) async fn offload_coalesced<F, T>(
    collector: &'static str,
    slot: &Arc<Mutex<()>>,
    work: F,
) -> Result<Option<T>>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let Ok(guard) = Arc::clone(slot).try_lock_owned() else {
        debug!(
            collector,
            "previous OS sample still running; skipping this scrape's sample"
        );
        return Ok(None);
    };
    offload(collector, move || {
        let _guard = guard;
        work()
    })
    .await
    .map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[tokio::test]
    async fn offload_returns_the_closure_result() -> Result<()> {
        assert_eq!(offload("test", || 21 * 2).await?, 42);
        Ok(())
    }

    #[tokio::test]
    async fn offload_reports_a_panicking_sample_instead_of_unwinding() {
        let result = offload("test", || {
            #[allow(clippy::panic)]
            {
                panic!("sampler exploded");
            }
        })
        .await;

        assert!(
            result.is_err(),
            "a panicking sampler must surface as a collector error"
        );
    }

    /// The core of issue #35: a long sample must not stop the runtime from making
    /// progress. Pinned to a single-threaded runtime so there is exactly one worker;
    /// running the sleep inline would monopolise it and the counter could not advance.
    #[tokio::test(flavor = "current_thread")]
    async fn a_slow_sample_does_not_starve_the_runtime() -> Result<()> {
        let progress = Arc::new(AtomicUsize::new(0));
        let ticker = Arc::clone(&progress);

        let spinner = tokio::spawn(async move {
            loop {
                ticker.fetch_add(1, Ordering::Relaxed);
                tokio::task::yield_now().await;
            }
        });

        tokio::task::yield_now().await;
        let before = progress.load(Ordering::Relaxed);

        offload("test", || std::thread::sleep(Duration::from_millis(300))).await?;

        let after = progress.load(Ordering::Relaxed);
        spinner.abort();

        assert!(
            after > before,
            "the runtime made no progress during a 300ms sample ({before} -> {after}): the \
             blocking work ran on a runtime worker, which starves every other collector and \
             surfaces as bogus 'pool timed out' errors (issue #35)"
        );
        Ok(())
    }

    /// An aborted scrape leaves its started sample running; the next scrape must skip
    /// rather than enqueue a duplicate. Regression guard for blocking-pool pile-up when a
    /// read outlives its scrape (hung `ssl_cert_file` mount, over-timeout PSS walk).
    #[tokio::test]
    async fn coalesced_skips_while_a_sample_is_in_flight() -> Result<()> {
        let slot = Arc::new(Mutex::new(()));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel::<()>();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();

        let slow_slot = Arc::clone(&slot);
        let slow_task = tokio::spawn(async move {
            offload_coalesced("test", &slow_slot, move || {
                let _ = started_tx.send(());
                let _ = release_rx.blocking_recv();
                1_u32
            })
            .await
        });

        started_rx
            .await
            .map_err(|_| anyhow!("slow sample never started"))?;

        let skipped = offload_coalesced("test", &slot, || 2_u32).await?;
        assert!(
            skipped.is_none(),
            "a second sample ran while the first was still in flight: overlapping scrapes \
             would pile unbounded work onto the blocking pool"
        );

        let _ = release_tx.send(());
        let finished = slow_task
            .await
            .map_err(|error| anyhow!("slow sample task failed: {error}"))??;
        assert_eq!(finished, Some(1));

        let after = offload_coalesced("test", &slot, || 3_u32).await?;
        assert_eq!(
            after,
            Some(3),
            "the slot must be released when the in-flight sample finishes"
        );
        Ok(())
    }

    /// A panicking sample must free the slot: one bad read cannot mute the collector for
    /// the rest of the process lifetime.
    #[tokio::test]
    async fn coalesced_releases_the_slot_after_a_panic() -> Result<()> {
        let slot = Arc::new(Mutex::new(()));

        let panicked = offload_coalesced("test", &slot, || -> u32 {
            #[allow(clippy::panic)]
            {
                panic!("sampler exploded");
            }
        })
        .await;
        assert!(panicked.is_err(), "the panic must surface as an error");

        let next = offload_coalesced("test", &slot, || 7_u32).await?;
        assert_eq!(next, Some(7), "the slot stayed held after a panic");
        Ok(())
    }
}
