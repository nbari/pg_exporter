use crate::collectors::{Collector, i64_to_f64};
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{GaugeVec, IntGauge, Opts, Registry};
use sqlx::{PgPool, Row};
use tracing::{debug, info, info_span, instrument};
use tracing_futures::Instrument as _;

/// Tracks Citus per-tenant statistics from `citus_stat_tenants`
///
/// Gracefully skips on Citus versions where this view may not exist.
#[derive(Clone)]
pub struct CitusStatTenantsCollector {
    read_count_current: GaugeVec,
    read_count_last: GaugeVec,
    query_count_current: GaugeVec,
    query_count_last: GaugeVec,
    cpu_usage_current: GaugeVec,
    cpu_usage_last: GaugeVec,
    tenants_total: IntGauge,
}

impl Default for CitusStatTenantsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl CitusStatTenantsCollector {
    /// # Panics
    ///
    /// Panics if metric creation fails (should never happen with valid metric names)
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        let labels = &["tenant_attribute", "colocation_id"];

        let read_count_current = GaugeVec::new(
            Opts::new(
                "citus_stat_tenants_read_count_current",
                "Read count in current period per tenant",
            ),
            labels,
        )
        .expect("citus_stat_tenants_read_count_current metric");

        let read_count_last = GaugeVec::new(
            Opts::new(
                "citus_stat_tenants_read_count_last",
                "Read count in last period per tenant",
            ),
            labels,
        )
        .expect("citus_stat_tenants_read_count_last metric");

        let query_count_current = GaugeVec::new(
            Opts::new(
                "citus_stat_tenants_query_count_current",
                "Query count in current period per tenant",
            ),
            labels,
        )
        .expect("citus_stat_tenants_query_count_current metric");

        let query_count_last = GaugeVec::new(
            Opts::new(
                "citus_stat_tenants_query_count_last",
                "Query count in last period per tenant",
            ),
            labels,
        )
        .expect("citus_stat_tenants_query_count_last metric");

        let cpu_usage_current = GaugeVec::new(
            Opts::new(
                "citus_stat_tenants_cpu_usage_current",
                "CPU usage in current period per tenant",
            ),
            labels,
        )
        .expect("citus_stat_tenants_cpu_usage_current metric");

        let cpu_usage_last = GaugeVec::new(
            Opts::new(
                "citus_stat_tenants_cpu_usage_last",
                "CPU usage in last period per tenant",
            ),
            labels,
        )
        .expect("citus_stat_tenants_cpu_usage_last metric");

        let tenants_total = IntGauge::with_opts(Opts::new(
            "citus_stat_tenants_total",
            "Total number of tracked tenants",
        ))
        .expect("citus_stat_tenants_total metric");

        Self {
            read_count_current,
            read_count_last,
            query_count_current,
            query_count_last,
            cpu_usage_current,
            cpu_usage_last,
            tenants_total,
        }
    }
}

impl Collector for CitusStatTenantsCollector {
    fn name(&self) -> &'static str {
        "citus_stat_tenants"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "citus_stat_tenants")
    )]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.read_count_current.clone()))?;
        registry.register(Box::new(self.read_count_last.clone()))?;
        registry.register(Box::new(self.query_count_current.clone()))?;
        registry.register(Box::new(self.query_count_last.clone()))?;
        registry.register(Box::new(self.cpu_usage_current.clone()))?;
        registry.register(Box::new(self.cpu_usage_last.clone()))?;
        registry.register(Box::new(self.tenants_total.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector = "citus_stat_tenants", otel.kind = "internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let query_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT tenant_attribute, colocation_id::integer, read_count_in_this_period::bigint, read_count_in_last_period::bigint, query_count_in_this_period::bigint, query_count_in_last_period::bigint, cpu_usage_in_this_period::double precision, cpu_usage_in_last_period::double precision FROM citus_stat_tenants",
                db.sql.table = "citus_stat_tenants"
            );

            let rows = match sqlx::query(
                r"SELECT tenant_attribute, colocation_id::integer,
                         read_count_in_this_period::bigint, read_count_in_last_period::bigint,
                         query_count_in_this_period::bigint, query_count_in_last_period::bigint,
                         cpu_usage_in_this_period::double precision, cpu_usage_in_last_period::double precision
                  FROM citus_stat_tenants",
            )
            .fetch_all(pool)
            .instrument(query_span)
            .await
            {
                Ok(rows) => rows,
                Err(e) => {
                    if e.to_string().contains("citus_stat_tenants") {
                        debug!("citus_stat_tenants view not found, skipping");
                        return Ok(());
                    }
                    return Err(e.into());
                }
            };

            self.read_count_current.reset();
            self.read_count_last.reset();
            self.query_count_current.reset();
            self.query_count_last.reset();
            self.cpu_usage_current.reset();
            self.cpu_usage_last.reset();

            let total = rows.len();

            for row in &rows {
                let tenant_attribute: String = row.try_get("tenant_attribute")?;
                let colocation_id: i32 = row.try_get("colocation_id")?;
                let colocation_id_str = colocation_id.to_string();
                let labels = [tenant_attribute.as_str(), colocation_id_str.as_str()];

                let read_current: i64 = row.try_get("read_count_in_this_period")?;
                let read_last: i64 = row.try_get("read_count_in_last_period")?;
                let query_current: i64 = row.try_get("query_count_in_this_period")?;
                let query_last: i64 = row.try_get("query_count_in_last_period")?;
                let cpu_current: f64 = row.try_get("cpu_usage_in_this_period")?;
                let cpu_last: f64 = row.try_get("cpu_usage_in_last_period")?;

                self.read_count_current
                    .with_label_values(&labels)
                    .set(i64_to_f64(read_current));
                self.read_count_last
                    .with_label_values(&labels)
                    .set(i64_to_f64(read_last));
                self.query_count_current
                    .with_label_values(&labels)
                    .set(i64_to_f64(query_current));
                self.query_count_last
                    .with_label_values(&labels)
                    .set(i64_to_f64(query_last));
                self.cpu_usage_current
                    .with_label_values(&labels)
                    .set(cpu_current);
                self.cpu_usage_last
                    .with_label_values(&labels)
                    .set(cpu_last);

                debug!(
                    tenant_attribute = %tenant_attribute,
                    colocation_id,
                    read_current,
                    read_last,
                    query_current,
                    query_last,
                    cpu_current,
                    cpu_last,
                    "updated citus stat tenants metrics"
                );
            }

            #[allow(clippy::cast_possible_wrap)]
            self.tenants_total.set(total as i64);

            info!(
                "Collected citus stat tenants metrics for {} tenants",
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
    fn test_citus_stat_tenants_name() {
        let collector = CitusStatTenantsCollector::new();
        assert_eq!(collector.name(), "citus_stat_tenants");
    }

    #[test]
    fn test_citus_stat_tenants_register_metrics() {
        let registry = Registry::new();
        let collector = CitusStatTenantsCollector::new();
        assert!(collector.register_metrics(&registry).is_ok());
    }
}
