//! Keeps the `system` collector's synchronous OS reads off the async runtime.
//!
//! Every sub-collector under `--collector.system` samples the operating system with
//! **blocking** APIs: `std::fs::read_to_string` on `/proc` (Linux), `sysctlbyname`
//! (FreeBSD), and `sysinfo` refreshes. None of that is async, and none of it yields.
//!
//! Running it directly inside an `async` block — which is what `collect_once` used to do
//! — occupies a Tokio worker thread for the full duration of the walk. The registry
//! launches every collector concurrently on that same runtime, so a slow walk stops the
//! `sqlx` pool's futures from being polled and *unrelated* collectors fail with
//! `pool timed out while waiting for an open connection`. That is what made issue #35 so
//! hard to attribute: a `/proc` problem presented as a `PostgreSQL` connectivity problem.
//!
//! [`offload`] moves the work to Tokio's blocking pool, where a long sample degrades into
//! a merely slow `system` collector instead of a runtime-wide outage. It also gives the
//! scrape task real await points, so the scrape timeout can actually fire and cancel it.

use anyhow::{Result, anyhow};

/// Runs `work` on Tokio's blocking pool and awaits the result.
///
/// # Errors
///
/// Returns an error if the blocking task panicked or was cancelled; the caller reports it
/// as a collector failure rather than propagating a panic.
pub(super) async fn offload<F, T>(collector: &'static str, work: F) -> Result<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|error| anyhow!("{collector}: OS sampling task did not complete: {error}"))
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
}
