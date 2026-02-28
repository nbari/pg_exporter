use crate::collectors::{Collector, i64_to_f64};
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{GaugeVec, IntGaugeVec, IntGauge, Opts, Registry};
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use tracing::{debug, info, info_span, instrument};
use tracing_futures::Instrument as _;

/// Tracks individual Citus shard sizes and placement from `citus_shards`
///
/// **Cardinality warning:** `citus_shard_size_bytes` produces one time series per shard.
/// A cluster with 100 tables x 32 shards = 3,200 series for this metric alone.
/// Large clusters (1,000+ tables) may produce 100K+ series. Monitor Prometheus
/// ingestion rate and consider disabling this collector if cardinality is a concern.
///
/// Gracefully skips if `citus_shards` view is not available (requires Citus 10+).
#[derive(Clone)]
pub struct CitusShardsCollector {
    shard_size_bytes: GaugeVec,
    shards_per_node: IntGaugeVec,
    shards_total: IntGauge,
}

impl Default for CitusShardsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl CitusShardsCollector {
    /// # Panics
    ///
    /// Panics if metric creation fails (should never happen with valid metric names)
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        let shard_size_bytes = GaugeVec::new(
            Opts::new("citus_shard_size_bytes", "Size of an individual shard in bytes"),
            &["table_name", "shardid", "nodename", "nodeport"],
        )
        .expect("citus_shard_size_bytes metric");

        let shards_per_node = IntGaugeVec::new(
            Opts::new("citus_shards_per_node", "Number of shards per node"),
            &["nodename", "nodeport"],
        )
        .expect("citus_shards_per_node metric");

        let shards_total = IntGauge::with_opts(Opts::new(
            "citus_shards_total",
            "Total number of shards in the Citus cluster",
        ))
        .expect("citus_shards_total metric");

        Self {
            shard_size_bytes,
            shards_per_node,
            shards_total,
        }
    }
}

impl Collector for CitusShardsCollector {
    fn name(&self) -> &'static str {
        "citus_shards"
    }

    #[instrument(skip(self, registry), level = "info", err, fields(collector = "citus_shards"))]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.shard_size_bytes.clone()))?;
        registry.register(Box::new(self.shards_per_node.clone()))?;
        registry.register(Box::new(self.shards_total.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector = "citus_shards", otel.kind = "internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let query_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT table_name::text, shardid, nodename, nodeport, shard_size FROM citus_shards",
                db.sql.table = "citus_shards"
            );

            let rows = match sqlx::query(
                r"SELECT table_name::text, shardid, nodename, nodeport, shard_size
                  FROM citus_shards",
            )
            .fetch_all(pool)
            .instrument(query_span)
            .await
            {
                Ok(rows) => rows,
                Err(e) => {
                    if e.to_string().contains("citus_shards") {
                        debug!("citus_shards view not found, skipping (requires Citus 10+)");
                        return Ok(());
                    }
                    return Err(e.into());
                }
            };

            self.shard_size_bytes.reset();
            self.shards_per_node.reset();

            let total = rows.len();
            let mut node_shard_counts: HashMap<(String, String), i64> = HashMap::new();

            for row in &rows {
                let table_name: String = row.try_get("table_name")?;
                let shardid: i64 = row.try_get("shardid")?;
                let nodename: String = row.try_get("nodename")?;
                let nodeport: i32 = row.try_get("nodeport")?;
                let shard_size: i64 = row.try_get("shard_size")?;

                let shardid_str = shardid.to_string();
                let nodeport_str = nodeport.to_string();

                self.shard_size_bytes
                    .with_label_values(&[
                        table_name.as_str(),
                        shardid_str.as_str(),
                        nodename.as_str(),
                        nodeport_str.as_str(),
                    ])
                    .set(i64_to_f64(shard_size));

                *node_shard_counts
                    .entry((nodename.clone(), nodeport_str))
                    .or_insert(0) += 1;

                debug!(
                    table_name = %table_name,
                    shardid,
                    nodename = %nodename,
                    nodeport,
                    shard_size,
                    "updated citus shard metrics"
                );
            }

            for ((nodename, nodeport), count) in &node_shard_counts {
                self.shards_per_node
                    .with_label_values(&[nodename.as_str(), nodeport.as_str()])
                    .set(*count);
            }

            #[allow(clippy::cast_possible_wrap)]
            self.shards_total.set(total as i64);

            info!("Collected citus shard metrics for {} shards", total);

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_citus_shards_name() {
        let collector = CitusShardsCollector::new();
        assert_eq!(collector.name(), "citus_shards");
    }

    #[test]
    fn test_citus_shards_register_metrics() {
        let registry = Registry::new();
        let collector = CitusShardsCollector::new();
        assert!(collector.register_metrics(&registry).is_ok());
    }
}
