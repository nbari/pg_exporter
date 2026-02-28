use crate::collectors::{Collector, i64_to_f64};
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{GaugeVec, Opts, Registry};
use sqlx::{PgPool, Row};
use tracing::{debug, info, info_span, instrument};
use tracing_futures::Instrument as _;

/// Tracks Citus connection and query execution statistics from `citus_stat_counters`
///
/// Available in Citus 12+. Gracefully skips on older versions.
#[derive(Clone)]
pub struct CitusStatCountersCollector {
    connection_succeeded: GaugeVec,
    connection_failed: GaugeVec,
    connection_reused: GaugeVec,
    query_single_shard: GaugeVec,
    query_multi_shard: GaugeVec,
}

impl Default for CitusStatCountersCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl CitusStatCountersCollector {
    /// # Panics
    ///
    /// Panics if metric creation fails (should never happen with valid metric names)
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        let connection_succeeded = GaugeVec::new(
            Opts::new(
                "citus_connection_establishment_succeeded_total",
                "Successful inter-node connection establishments",
            ),
            &["database"],
        )
        .expect("citus_connection_establishment_succeeded_total metric");

        let connection_failed = GaugeVec::new(
            Opts::new(
                "citus_connection_establishment_failed_total",
                "Failed inter-node connection establishments",
            ),
            &["database"],
        )
        .expect("citus_connection_establishment_failed_total metric");

        let connection_reused = GaugeVec::new(
            Opts::new(
                "citus_connection_reused_total",
                "Reused inter-node connections",
            ),
            &["database"],
        )
        .expect("citus_connection_reused_total metric");

        let query_single_shard = GaugeVec::new(
            Opts::new(
                "citus_query_execution_single_shard_total",
                "Single-shard query executions",
            ),
            &["database"],
        )
        .expect("citus_query_execution_single_shard_total metric");

        let query_multi_shard = GaugeVec::new(
            Opts::new(
                "citus_query_execution_multi_shard_total",
                "Multi-shard query executions",
            ),
            &["database"],
        )
        .expect("citus_query_execution_multi_shard_total metric");

        Self {
            connection_succeeded,
            connection_failed,
            connection_reused,
            query_single_shard,
            query_multi_shard,
        }
    }
}

impl Collector for CitusStatCountersCollector {
    fn name(&self) -> &'static str {
        "citus_stat_counters"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "citus_stat_counters")
    )]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.connection_succeeded.clone()))?;
        registry.register(Box::new(self.connection_failed.clone()))?;
        registry.register(Box::new(self.connection_reused.clone()))?;
        registry.register(Box::new(self.query_single_shard.clone()))?;
        registry.register(Box::new(self.query_multi_shard.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector = "citus_stat_counters", otel.kind = "internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let query_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT datname, connection_establishment_succeeded, connection_establishment_failed, connection_reused, query_execution_single_shard, query_execution_multi_shard FROM citus_stat_counters",
                db.sql.table = "citus_stat_counters"
            );

            let rows = match sqlx::query(
                r"SELECT datname,
                         connection_establishment_succeeded,
                         connection_establishment_failed,
                         connection_reused,
                         query_execution_single_shard,
                         query_execution_multi_shard
                  FROM citus_stat_counters",
            )
            .fetch_all(pool)
            .instrument(query_span)
            .await
            {
                Ok(rows) => rows,
                Err(e) => {
                    // citus_stat_counters may not exist on older Citus versions
                    debug!(
                        collector = "citus_stat_counters",
                        error = %e,
                        "citus_stat_counters not available, skipping"
                    );
                    return Ok(());
                }
            };

            self.connection_succeeded.reset();
            self.connection_failed.reset();
            self.connection_reused.reset();
            self.query_single_shard.reset();
            self.query_multi_shard.reset();

            for row in &rows {
                let datname: String = row.try_get("datname")?;
                let labels = [datname.as_str()];

                let conn_succeeded: i64 = row.try_get("connection_establishment_succeeded")?;
                let conn_failed: i64 = row.try_get("connection_establishment_failed")?;
                let conn_reused: i64 = row.try_get("connection_reused")?;
                let q_single: i64 = row.try_get("query_execution_single_shard")?;
                let q_multi: i64 = row.try_get("query_execution_multi_shard")?;

                self.connection_succeeded
                    .with_label_values(&labels)
                    .set(i64_to_f64(conn_succeeded));
                self.connection_failed
                    .with_label_values(&labels)
                    .set(i64_to_f64(conn_failed));
                self.connection_reused
                    .with_label_values(&labels)
                    .set(i64_to_f64(conn_reused));
                self.query_single_shard
                    .with_label_values(&labels)
                    .set(i64_to_f64(q_single));
                self.query_multi_shard
                    .with_label_values(&labels)
                    .set(i64_to_f64(q_multi));

                debug!(
                    datname = %datname,
                    conn_succeeded,
                    conn_failed,
                    conn_reused,
                    q_single,
                    q_multi,
                    "updated citus stat counters"
                );
            }

            info!(
                "Collected citus stat counters for {} databases",
                rows.len()
            );

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_citus_stat_counters_name() {
        let collector = CitusStatCountersCollector::new();
        assert_eq!(collector.name(), "citus_stat_counters");
    }

    #[test]
    fn test_citus_stat_counters_register_metrics() {
        let registry = Registry::new();
        let collector = CitusStatCountersCollector::new();
        collector.register_metrics(&registry).unwrap();
    }
}
