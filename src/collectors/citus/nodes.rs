use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{GaugeVec, IntGauge, Opts, Registry};
use sqlx::{PgPool, Row};
use tracing::{debug, info, info_span, instrument};
use tracing_futures::Instrument as _;

/// Tracks Citus worker node status from `pg_dist_node`
#[derive(Clone)]
pub struct CitusNodesCollector {
    node_is_active: GaugeVec,
    node_should_have_shards: GaugeVec,
    nodes_total: IntGauge,
}

impl Default for CitusNodesCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl CitusNodesCollector {
    /// # Panics
    ///
    /// Panics if metric creation fails (should never happen with valid metric names)
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        let node_is_active = GaugeVec::new(
            Opts::new("citus_node_is_active", "Whether the Citus node is active (1/0)"),
            &["nodeid", "nodename", "nodeport", "noderole"],
        )
        .expect("citus_node_is_active metric");

        let node_should_have_shards = GaugeVec::new(
            Opts::new(
                "citus_node_should_have_shards",
                "Whether the node should have shards (1/0)",
            ),
            &["nodeid", "nodename", "nodeport", "noderole"],
        )
        .expect("citus_node_should_have_shards metric");

        let nodes_total = IntGauge::with_opts(Opts::new(
            "citus_nodes_total",
            "Total number of nodes in the Citus cluster",
        ))
        .expect("citus_nodes_total metric");

        Self {
            node_is_active,
            node_should_have_shards,
            nodes_total,
        }
    }
}

impl Collector for CitusNodesCollector {
    fn name(&self) -> &'static str {
        "citus_nodes"
    }

    #[instrument(skip(self, registry), level = "info", err, fields(collector = "citus_nodes"))]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.node_is_active.clone()))?;
        registry.register(Box::new(self.node_should_have_shards.clone()))?;
        registry.register(Box::new(self.nodes_total.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector = "citus_nodes", otel.kind = "internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let query_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT nodeid, nodename, nodeport, isactive, noderole::text, shouldhaveshards FROM pg_dist_node",
                db.sql.table = "pg_dist_node"
            );

            let rows = match sqlx::query(
                r"SELECT nodeid, nodename, nodeport, isactive, noderole::text, shouldhaveshards
                  FROM pg_dist_node",
            )
            .fetch_all(pool)
            .instrument(query_span)
            .await
            {
                Ok(rows) => rows,
                Err(e) => {
                    if e.to_string().contains("pg_dist_node") {
                        debug!("pg_dist_node view not found, skipping");
                        return Ok(());
                    }
                    return Err(e.into());
                }
            };

            self.node_is_active.reset();
            self.node_should_have_shards.reset();

            let total = rows.len();

            for row in &rows {
                let nodeid: i32 = row.try_get("nodeid")?;
                let nodename: String = row.try_get("nodename")?;
                let nodeport: i32 = row.try_get("nodeport")?;
                let isactive: bool = row.try_get("isactive")?;
                let noderole: String = row.try_get("noderole")?;
                let shouldhaveshards: bool = row.try_get("shouldhaveshards")?;

                let nodeid_str = nodeid.to_string();
                let nodeport_str = nodeport.to_string();
                let labels = [
                    nodeid_str.as_str(),
                    nodename.as_str(),
                    nodeport_str.as_str(),
                    noderole.as_str(),
                ];

                self.node_is_active
                    .with_label_values(&labels)
                    .set(if isactive { 1.0 } else { 0.0 });

                self.node_should_have_shards
                    .with_label_values(&labels)
                    .set(if shouldhaveshards { 1.0 } else { 0.0 });

                debug!(
                    nodeid,
                    nodename = %nodename,
                    nodeport,
                    noderole = %noderole,
                    isactive,
                    shouldhaveshards,
                    "updated citus node metrics"
                );
            }

            #[allow(clippy::cast_possible_wrap)]
            self.nodes_total.set(total as i64);

            info!("Collected citus node metrics for {} nodes", total);

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_citus_nodes_name() {
        let collector = CitusNodesCollector::new();
        assert_eq!(collector.name(), "citus_nodes");
    }

    #[test]
    fn test_citus_nodes_register_metrics() {
        let registry = Registry::new();
        let collector = CitusNodesCollector::new();
        assert!(collector.register_metrics(&registry).is_ok());
    }
}
