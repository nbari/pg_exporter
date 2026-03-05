use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{IntGauge, Opts, Registry};
use sqlx::{PgPool, Row};
use tracing::{debug, info, info_span, instrument};
use tracing_futures::Instrument as _;

/// Tracks Citus distributed lock waits from `citus_lock_waits`
///
/// Gracefully skips on Citus versions where this view may not exist.
#[derive(Clone)]
pub struct CitusLockWaitsCollector {
    lock_waits_total: IntGauge,
}

impl Default for CitusLockWaitsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl CitusLockWaitsCollector {
    /// # Panics
    ///
    /// Panics if metric creation fails (should never happen with valid metric names)
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        let lock_waits_total = IntGauge::with_opts(Opts::new(
            "citus_lock_waits_total",
            "Total number of blocked distributed queries",
        ))
        .expect("citus_lock_waits_total metric");

        Self { lock_waits_total }
    }
}

impl Collector for CitusLockWaitsCollector {
    fn name(&self) -> &'static str {
        "citus_lock_waits"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "citus_lock_waits")
    )]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.lock_waits_total.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector = "citus_lock_waits", otel.kind = "internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let query_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT count(*)::bigint AS lock_wait_count FROM citus_lock_waits",
                db.sql.table = "citus_lock_waits"
            );

            let row = match sqlx::query(
                r"SELECT count(*)::bigint AS lock_wait_count FROM citus_lock_waits",
            )
            .fetch_one(pool)
            .instrument(query_span)
            .await
            {
                Ok(row) => row,
                Err(e) => {
                    if e.to_string().contains("citus_lock_waits") {
                        debug!("citus_lock_waits view not found, skipping");
                        return Ok(());
                    }
                    return Err(e.into());
                }
            };

            let count: i64 = row.try_get("lock_wait_count")?;
            self.lock_waits_total.set(count);

            info!("Collected citus lock waits metrics");

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_citus_lock_waits_name() {
        let collector = CitusLockWaitsCollector::new();
        assert_eq!(collector.name(), "citus_lock_waits");
    }

    #[test]
    fn test_citus_lock_waits_register_metrics() {
        let registry = Registry::new();
        let collector = CitusLockWaitsCollector::new();
        assert!(collector.register_metrics(&registry).is_ok());
    }
}
