use crate::collectors::{Collector, i64_to_f64};
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{GaugeVec, IntGauge, Opts, Registry};
use sqlx::{PgPool, Row};
use tracing::{debug, info, info_span, instrument};
use tracing_futures::Instrument as _;

/// Tracks Citus distributed schema information from `citus_schemas`
///
/// Available in Citus 12+. Gracefully skips on older versions.
#[derive(Clone)]
pub struct CitusSchemasCollector {
    schema_size_bytes: GaugeVec,
    schemas_total: IntGauge,
}

impl Default for CitusSchemasCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl CitusSchemasCollector {
    /// # Panics
    ///
    /// Panics if metric creation fails (should never happen with valid metric names)
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        let schema_size_bytes = GaugeVec::new(
            Opts::new(
                "citus_schema_size_bytes",
                "Distributed schema size in bytes",
            ),
            &["schema_name", "schema_owner"],
        )
        .expect("citus_schema_size_bytes metric");

        let schemas_total = IntGauge::with_opts(Opts::new(
            "citus_schemas_total",
            "Total number of distributed schemas",
        ))
        .expect("citus_schemas_total metric");

        Self {
            schema_size_bytes,
            schemas_total,
        }
    }
}

impl Collector for CitusSchemasCollector {
    fn name(&self) -> &'static str {
        "citus_schemas"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "citus_schemas")
    )]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.schema_size_bytes.clone()))?;
        registry.register(Box::new(self.schemas_total.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector = "citus_schemas", otel.kind = "internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let query_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT schema_name::text, schema_owner::text, pg_catalog.pg_size_bytes(schema_size)::bigint AS schema_size_bytes FROM citus_schemas",
                db.sql.table = "citus_schemas"
            );

            let rows = match sqlx::query(
                r"SELECT schema_name::text, schema_owner::text,
                         pg_catalog.pg_size_bytes(schema_size)::bigint AS schema_size_bytes
                  FROM citus_schemas",
            )
            .fetch_all(pool)
            .instrument(query_span)
            .await
            {
                Ok(rows) => rows,
                Err(e) => {
                    if e.to_string().contains("citus_schemas") {
                        debug!("citus_schemas view not found, skipping (requires Citus 12+)");
                        return Ok(());
                    }
                    return Err(e.into());
                }
            };

            self.schema_size_bytes.reset();

            let total = rows.len();

            for row in &rows {
                let schema_name: String = row.try_get("schema_name")?;
                let schema_owner: String = row.try_get("schema_owner")?;
                let size_bytes: i64 = row.try_get("schema_size_bytes")?;

                self.schema_size_bytes
                    .with_label_values(&[schema_name.as_str(), schema_owner.as_str()])
                    .set(i64_to_f64(size_bytes));

                debug!(
                    schema_name = %schema_name,
                    schema_owner = %schema_owner,
                    size_bytes,
                    "updated citus schema metrics"
                );
            }

            #[allow(clippy::cast_possible_wrap)]
            self.schemas_total.set(total as i64);

            info!("Collected citus schema metrics for {} schemas", total);

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_citus_schemas_name() {
        let collector = CitusSchemasCollector::new();
        assert_eq!(collector.name(), "citus_schemas");
    }

    #[test]
    fn test_citus_schemas_register_metrics() {
        let registry = Registry::new();
        let collector = CitusSchemasCollector::new();
        assert!(collector.register_metrics(&registry).is_ok());
    }
}
