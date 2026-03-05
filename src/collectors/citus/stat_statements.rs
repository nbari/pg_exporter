use crate::collectors::{Collector, i64_to_f64};
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{GaugeVec, IntGauge, Opts, Registry};
use sqlx::{PgPool, Row};
use tracing::{debug, info, info_span, instrument};
use tracing_futures::Instrument as _;

/// Tracks Citus distributed statement statistics from `citus_stat_statements`
///
/// Requires `pg_stat_statements` extension. Available in Citus 11+.
/// Gracefully skips on versions where this view may not exist.
#[derive(Clone)]
pub struct CitusStatStatementsCollector {
    calls_total: GaugeVec,
    statements_total: IntGauge,
}

impl Default for CitusStatStatementsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl CitusStatStatementsCollector {
    /// # Panics
    ///
    /// Panics if metric creation fails (should never happen with valid metric names)
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        let calls_total = GaugeVec::new(
            Opts::new(
                "citus_stat_statements_calls_total",
                "Execution count per query/executor type",
            ),
            &["queryid", "executor"],
        )
        .expect("citus_stat_statements_calls_total metric");

        let statements_total = IntGauge::with_opts(Opts::new(
            "citus_stat_statements_total",
            "Total tracked statement entries",
        ))
        .expect("citus_stat_statements_total metric");

        Self {
            calls_total,
            statements_total,
        }
    }
}

impl Collector for CitusStatStatementsCollector {
    fn name(&self) -> &'static str {
        "citus_stat_statements"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "citus_stat_statements")
    )]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.calls_total.clone()))?;
        registry.register(Box::new(self.statements_total.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector = "citus_stat_statements", otel.kind = "internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let query_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT queryid::bigint, executor, calls::bigint FROM citus_stat_statements",
                db.sql.table = "citus_stat_statements"
            );

            let rows = match sqlx::query(
                r"SELECT queryid::bigint, executor, calls::bigint FROM citus_stat_statements",
            )
            .fetch_all(pool)
            .instrument(query_span)
            .await
            {
                Ok(rows) => rows,
                Err(e) => {
                    let msg = e.to_string();
                    if msg.contains("citus_stat_statements")
                        || msg.contains("pg_stat_statements")
                    {
                        debug!("citus_stat_statements view not available, skipping (requires Citus 11+ and pg_stat_statements)");
                        return Ok(());
                    }
                    return Err(e.into());
                }
            };

            self.calls_total.reset();

            let total = rows.len();

            for row in &rows {
                let queryid: i64 = row.try_get("queryid")?;
                let executor: String = row.try_get("executor")?;
                let calls: i64 = row.try_get("calls")?;

                let queryid_str = queryid.to_string();

                self.calls_total
                    .with_label_values(&[queryid_str.as_str(), executor.as_str()])
                    .set(i64_to_f64(calls));

                debug!(
                    queryid,
                    executor = %executor,
                    calls,
                    "updated citus stat statements metrics"
                );
            }

            #[allow(clippy::cast_possible_wrap)]
            self.statements_total.set(total as i64);

            info!(
                "Collected citus stat statements metrics for {} entries",
                total
            );

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_citus_stat_statements_name() {
        let collector = CitusStatStatementsCollector::new();
        assert_eq!(collector.name(), "citus_stat_statements");
    }

    #[test]
    fn test_citus_stat_statements_register_metrics() {
        let registry = Registry::new();
        let collector = CitusStatStatementsCollector::new();
        assert!(collector.register_metrics(&registry).is_ok());
    }
}
