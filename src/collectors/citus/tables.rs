use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{IntGaugeVec, IntGauge, Opts, Registry};
use sqlx::{PgPool, Row};
use tracing::{debug, info, info_span, instrument};
use tracing_futures::Instrument as _;

/// Tracks Citus distributed table sizes and shard counts from `citus_tables`
///
/// Uses `citus_table_size()` for accurate distributed table sizes (not `pg_table_size()`
/// which only returns the coordinator's local portion).
///
/// Gracefully skips if `citus_tables` view is not available (requires Citus 10+).
#[derive(Clone)]
pub struct CitusTablesCollector {
    table_size_bytes: IntGaugeVec,
    table_shard_count: IntGaugeVec,
    tables_total: IntGauge,
}

impl Default for CitusTablesCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl CitusTablesCollector {
    /// # Panics
    ///
    /// Panics if metric creation fails (should never happen with valid metric names)
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        let table_size_bytes = IntGaugeVec::new(
            Opts::new("citus_table_size_bytes", "Size of the distributed table in bytes"),
            &["table_name", "citus_table_type"],
        )
        .expect("citus_table_size_bytes metric");

        let table_shard_count = IntGaugeVec::new(
            Opts::new("citus_table_shard_count", "Number of shards per distributed table"),
            &["table_name", "citus_table_type"],
        )
        .expect("citus_table_shard_count metric");

        let tables_total = IntGauge::with_opts(Opts::new(
            "citus_tables_total",
            "Total number of distributed tables",
        ))
        .expect("citus_tables_total metric");

        Self {
            table_size_bytes,
            table_shard_count,
            tables_total,
        }
    }
}

impl Collector for CitusTablesCollector {
    fn name(&self) -> &'static str {
        "citus_tables"
    }

    #[instrument(skip(self, registry), level = "info", err, fields(collector = "citus_tables"))]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.table_size_bytes.clone()))?;
        registry.register(Box::new(self.table_shard_count.clone()))?;
        registry.register(Box::new(self.tables_total.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector = "citus_tables", otel.kind = "internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let query_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT table_name::text, citus_table_type, shard_count, citus_table_size(table_name) AS size_bytes FROM citus_tables",
                db.sql.table = "citus_tables"
            );

            let rows = match sqlx::query(
                r"SELECT table_name::text, citus_table_type, shard_count,
                         citus_table_size(table_name) AS size_bytes
                  FROM citus_tables",
            )
            .fetch_all(pool)
            .instrument(query_span)
            .await
            {
                Ok(rows) => rows,
                Err(e) => {
                    if e.to_string().contains("citus_tables") {
                        debug!("citus_tables view not found, skipping (requires Citus 10+)");
                        return Ok(());
                    }
                    return Err(e.into());
                }
            };

            self.table_size_bytes.reset();
            self.table_shard_count.reset();

            let total = rows.len();

            for row in &rows {
                let table_name: String = row.try_get("table_name")?;
                let table_type: String = row.try_get("citus_table_type")?;
                let shard_count: i64 = row.try_get("shard_count")?;
                let size_bytes: i64 = row.try_get("size_bytes")?;

                let labels = [table_name.as_str(), table_type.as_str()];

                self.table_size_bytes
                    .with_label_values(&labels)
                    .set(size_bytes);

                self.table_shard_count
                    .with_label_values(&labels)
                    .set(shard_count);

                debug!(
                    table_name = %table_name,
                    table_type = %table_type,
                    shard_count,
                    size_bytes,
                    "updated citus table metrics"
                );
            }

            #[allow(clippy::cast_possible_wrap)]
            self.tables_total.set(total as i64);

            info!("Collected citus table metrics for {} tables", total);

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_citus_tables_name() {
        let collector = CitusTablesCollector::new();
        assert_eq!(collector.name(), "citus_tables");
    }

    #[test]
    fn test_citus_tables_register_metrics() {
        let registry = Registry::new();
        let collector = CitusTablesCollector::new();
        assert!(collector.register_metrics(&registry).is_ok());
    }
}
