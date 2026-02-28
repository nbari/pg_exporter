use crate::collectors::{Collector, i64_to_f64};
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{GaugeVec, IntGauge, Opts, Registry};
use sqlx::{PgPool, Row};
use tracing::{debug, info, info_span, instrument};
use tracing_futures::Instrument as _;

/// Tracks Citus distributed query activity from `citus_dist_stat_activity`
///
/// Gracefully skips on older Citus versions where this view may not exist.
#[derive(Clone)]
pub struct CitusActivityCollector {
    dist_activity_count: GaugeVec,
    dist_activity_total: IntGauge,
}

impl Default for CitusActivityCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl CitusActivityCollector {
    /// # Panics
    ///
    /// Panics if metric creation fails (should never happen with valid metric names)
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        let dist_activity_count = GaugeVec::new(
            Opts::new(
                "citus_dist_activity_count",
                "Number of distributed queries by state",
            ),
            &["state"],
        )
        .expect("citus_dist_activity_count metric");

        let dist_activity_total = IntGauge::with_opts(Opts::new(
            "citus_dist_activity_total",
            "Total number of active distributed queries",
        ))
        .expect("citus_dist_activity_total metric");

        Self {
            dist_activity_count,
            dist_activity_total,
        }
    }
}

impl Collector for CitusActivityCollector {
    fn name(&self) -> &'static str {
        "citus_activity"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "citus_activity")
    )]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.dist_activity_count.clone()))?;
        registry.register(Box::new(self.dist_activity_total.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector = "citus_activity", otel.kind = "internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let query_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT COALESCE(state, 'unknown') AS state, COUNT(*) AS count FROM citus_dist_stat_activity WHERE is_worker_query = false GROUP BY state",
                db.sql.table = "citus_dist_stat_activity"
            );

            let rows = match sqlx::query(
                r"SELECT COALESCE(state, 'unknown') AS state, COUNT(*) AS count
                  FROM citus_dist_stat_activity
                  WHERE is_worker_query = false
                  GROUP BY state",
            )
            .fetch_all(pool)
            .instrument(query_span)
            .await
            {
                Ok(rows) => rows,
                Err(e) => {
                    if e.to_string().contains("citus_dist_stat_activity") {
                        debug!("citus_dist_stat_activity view not found, skipping");
                        return Ok(());
                    }
                    return Err(e.into());
                }
            };

            self.dist_activity_count.reset();

            let mut total: i64 = 0;

            for row in &rows {
                let state: String = row.try_get("state")?;
                let count: i64 = row.try_get("count")?;

                self.dist_activity_count
                    .with_label_values(&[state.as_str()])
                    .set(i64_to_f64(count));

                total += count;

                debug!(state = %state, count, "updated citus activity metrics");
            }

            self.dist_activity_total.set(total);

            info!("Collected citus distributed activity metrics");

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_citus_activity_name() {
        let collector = CitusActivityCollector::new();
        assert_eq!(collector.name(), "citus_activity");
    }

    #[test]
    fn test_citus_activity_register_metrics() {
        let registry = Registry::new();
        let collector = CitusActivityCollector::new();
        assert!(collector.register_metrics(&registry).is_ok());
    }
}
