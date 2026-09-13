//! Optional, registry-owned instrumentation for the global database-query limiter.
//! Task-local context must be captured before spawning: Tokio does not inherit it.
use prometheus::{Histogram, HistogramOpts, HistogramVec, Registry};
use std::future::Future;
use tokio::time::Instant;

pub(crate) const DURATION_BUCKETS: [f64; 11] = [
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
];

#[derive(Clone)]
pub(crate) struct PermitMetrics {
    wait: HistogramVec,
    hold: HistogramVec,
}

impl PermitMetrics {
    pub(crate) fn new() -> prometheus::Result<Self> {
        Ok(Self {
            wait: HistogramVec::new(
                HistogramOpts::new(
                    "pg_exporter_collector_permit_wait_seconds",
                    "Time per non-default-database permit acquisition attempt, including time until cancellation; concurrent waits overlap",
                ).buckets(DURATION_BUCKETS.to_vec()),
                &["collector"],
            )?,
            hold: HistogramVec::new(
                HistogramOpts::new(
                    "pg_exporter_collector_permit_hold_seconds",
                    "Time per held non-default-database permit, including connection setup, database work and time until cancellation; not SQL execution time alone",
                ).buckets(DURATION_BUCKETS.to_vec()),
                &["collector"],
            )?,
        })
    }

    pub(crate) fn register(&self, registry: &Registry) -> prometheus::Result<()> {
        registry.register(Box::new(self.wait.clone()))?;
        registry.register(Box::new(self.hold.clone()))?;
        Ok(())
    }

    pub(crate) fn reset(&self) {
        self.wait.reset();
        self.hold.reset();
    }

    pub(crate) fn context(&self, collector: &'static str) -> PermitContext {
        PermitContext {
            metrics: self.clone(),
            collector,
        }
    }
}

#[derive(Clone)]
pub(crate) struct PermitContext {
    metrics: PermitMetrics,
    collector: &'static str,
}

impl PermitContext {
    pub(crate) fn start_wait(&self) -> Option<PermitObservation> {
        self.metrics
            .wait
            .get_metric_with_label_values(&[self.collector])
            .ok()
            .map(PermitObservation::new)
    }

    pub(crate) fn start_hold(&self) -> Option<PermitObservation> {
        self.metrics
            .hold
            .get_metric_with_label_values(&[self.collector])
            .ok()
            .map(PermitObservation::new)
    }
}

/// Own the histogram handle: cancellation may drop this outside the task-local scope.
pub(crate) struct PermitObservation {
    histogram: Histogram,
    start: Instant,
}

impl PermitObservation {
    fn new(histogram: Histogram) -> Self {
        Self {
            histogram,
            start: Instant::now(),
        }
    }
}

impl Drop for PermitObservation {
    fn drop(&mut self) {
        self.histogram.observe(self.start.elapsed().as_secs_f64());
    }
}

tokio::task_local! {
    static CONTEXT: Option<PermitContext>;
}

pub(crate) fn current_context() -> Option<PermitContext> {
    CONTEXT.try_with(Clone::clone).ok().flatten()
}

pub(crate) fn scope<F: Future>(
    context: Option<PermitContext>,
    future: F,
) -> impl Future<Output = F::Output> {
    CONTEXT.scope(context, future)
}

/// This is deliberately not `async fn`: capture the caller's context immediately,
/// before the returned future is moved into another Tokio task.
pub(crate) fn inherit<F: Future>(future: F) -> impl Future<Output = F::Output> {
    scope(current_context(), future)
}
