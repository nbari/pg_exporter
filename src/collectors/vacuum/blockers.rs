//! Cluster-wide vacuum blocker metrics from `PostgreSQL` xmin holders.
//!
//! The collector reads `pg_stat_activity`, `pg_prepared_xacts`, and
//! `pg_replication_slots` through the shared pool only. These views are
//! cluster-wide, so no per-database fan-out is needed.

use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{Gauge, IntGauge, IntGaugeVec, Opts, Registry};
use sqlx::{PgPool, Row, postgres::PgRow};
use tracing::{debug, info_span, instrument};
use tracing_futures::Instrument as _;

const BACKEND_HOLDER: &str = "backend";
const PREPARED_XACT_HOLDER: &str = "prepared_xact";
const REPLICATION_SLOT_HOLDER: &str = "replication_slot";

const BACKEND_WORST_QUERY: &str = r"
    SELECT
        age(backend_xmin)::bigint AS age_xids,
        COALESCE(application_name, '') AS identity
    FROM pg_stat_activity
    WHERE backend_xmin IS NOT NULL
    ORDER BY age(backend_xmin) DESC
    LIMIT 1
";

const PREPARED_XACTS_AGGREGATE_QUERY: &str = r"
    SELECT
        count(*)::bigint AS cnt,
        COALESCE(max(age(transaction)), 0)::bigint AS max_age_xids,
        COALESCE(EXTRACT(EPOCH FROM (now() - min(prepared))), 0)::double precision AS oldest_age_seconds
    FROM pg_prepared_xacts
";

const PREPARED_XACT_WORST_QUERY: &str = r"
    SELECT
        COALESCE(gid, '') AS identity,
        age(transaction)::bigint AS age_xids
    FROM pg_prepared_xacts
    ORDER BY age(transaction) DESC
    LIMIT 1
";

const REPLICATION_SLOT_WORST_QUERY: &str = r"
    SELECT
        COALESCE(slot_name, '') AS identity,
        GREATEST(COALESCE(age(xmin), 0), COALESCE(age(catalog_xmin), 0))::bigint AS age_xids
    FROM pg_replication_slots
    WHERE xmin IS NOT NULL OR catalog_xmin IS NOT NULL
    ORDER BY 2 DESC
    LIMIT 1
";

#[derive(Clone, Debug)]
struct HolderSample {
    identity: String,
    age_xids: i64,
}

#[derive(Clone, Copy, Debug, Default)]
struct PreparedXactsAggregate {
    count: i64,
    max_age_xids: i64,
    oldest_age_seconds: f64,
}

/// Exposes cluster-wide vacuum blocker metrics.
///
/// The fixed holder horizon metric, `pg_xmin_horizon_age_xids`, always emits
/// `backend`, `prepared_xact`, and `replication_slot` holder labels. The
/// offender metric, `pg_xmin_horizon_holder_age_xids`, is reset every scrape and
/// emits at most the single oldest identity for each holder type.
#[derive(Clone)]
pub struct VacuumBlockersCollector {
    xmin_horizon_age_xids: IntGaugeVec,
    prepared_xacts_count: IntGauge,
    prepared_xacts_oldest_age_seconds: Gauge,
    xmin_horizon_holder_age_xids: IntGaugeVec,
}

impl Default for VacuumBlockersCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl VacuumBlockersCollector {
    /// Creates a new `VacuumBlockersCollector` with all metrics initialized.
    ///
    /// # Panics
    ///
    /// Panics if metric creation fails, which only happens with an invalid
    /// metric name or label set and therefore never at runtime.
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        let xmin_horizon_age_xids = IntGaugeVec::new(
            Opts::new(
                "pg_xmin_horizon_age_xids",
                "Age in transaction IDs of the oldest xmin held by holder type",
            ),
            &["holder"],
        )
        .expect("valid pg_xmin_horizon_age_xids opts");

        let prepared_xacts_count = IntGauge::with_opts(Opts::new(
            "pg_prepared_xacts_count",
            "Number of currently prepared transactions",
        ))
        .expect("valid pg_prepared_xacts_count opts");

        let prepared_xacts_oldest_age_seconds = Gauge::with_opts(Opts::new(
            "pg_prepared_xacts_oldest_age_seconds",
            "Seconds since the oldest prepared transaction was prepared",
        ))
        .expect("valid pg_prepared_xacts_oldest_age_seconds opts");

        let xmin_horizon_holder_age_xids = IntGaugeVec::new(
            Opts::new(
                "pg_xmin_horizon_holder_age_xids",
                "Age in transaction IDs of the worst xmin holder identity per holder type",
            ),
            &["holder", "identity"],
        )
        .expect("valid pg_xmin_horizon_holder_age_xids opts");

        Self {
            xmin_horizon_age_xids,
            prepared_xacts_count,
            prepared_xacts_oldest_age_seconds,
            xmin_horizon_holder_age_xids,
        }
    }

    fn holder_sample_from_row(row: &PgRow) -> HolderSample {
        HolderSample {
            identity: row.try_get("identity").unwrap_or_default(),
            age_xids: row.try_get("age_xids").unwrap_or(0),
        }
    }

    fn prepared_aggregate_from_row(row: &PgRow) -> PreparedXactsAggregate {
        PreparedXactsAggregate {
            count: row.try_get("cnt").unwrap_or(0),
            max_age_xids: row.try_get("max_age_xids").unwrap_or(0),
            oldest_age_seconds: row.try_get("oldest_age_seconds").unwrap_or(0.0),
        }
    }

    fn set_holder_age(&self, holder: &str, age_xids: i64) {
        self.xmin_horizon_age_xids
            .with_label_values(&[holder])
            .set(age_xids);
    }

    fn set_worst_holder(&self, holder: &str, sample: &HolderSample) {
        self.xmin_horizon_holder_age_xids
            .with_label_values(&[holder, sample.identity.as_str()])
            .set(sample.age_xids);
    }
}

impl Collector for VacuumBlockersCollector {
    fn name(&self) -> &'static str {
        "vacuum_blockers"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "vacuum_blockers")
    )]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.xmin_horizon_age_xids.clone()))?;
        registry.register(Box::new(self.prepared_xacts_count.clone()))?;
        registry.register(Box::new(self.prepared_xacts_oldest_age_seconds.clone()))?;
        registry.register(Box::new(self.xmin_horizon_holder_age_xids.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector = "vacuum_blockers", otel.kind = "internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let backend_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT oldest backend_xmin FROM pg_stat_activity",
                db.sql.table = "pg_stat_activity"
            );
            let backend_worst = sqlx::query(BACKEND_WORST_QUERY)
                .fetch_optional(pool)
                .instrument(backend_span)
                .await?
                .as_ref()
                .map(Self::holder_sample_from_row);

            let prepared_aggregate_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT aggregate prepared transaction age FROM pg_prepared_xacts",
                db.sql.table = "pg_prepared_xacts"
            );
            let prepared_aggregate = sqlx::query(PREPARED_XACTS_AGGREGATE_QUERY)
                .fetch_optional(pool)
                .instrument(prepared_aggregate_span)
                .await?
                .as_ref()
                .map_or_else(PreparedXactsAggregate::default, Self::prepared_aggregate_from_row);

            let prepared_worst_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT oldest prepared transaction FROM pg_prepared_xacts",
                db.sql.table = "pg_prepared_xacts"
            );
            let prepared_worst = sqlx::query(PREPARED_XACT_WORST_QUERY)
                .fetch_optional(pool)
                .instrument(prepared_worst_span)
                .await?
                .as_ref()
                .map(Self::holder_sample_from_row);

            let replication_slot_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT oldest xmin or catalog_xmin FROM pg_replication_slots",
                db.sql.table = "pg_replication_slots"
            );
            let replication_slot_worst = sqlx::query(REPLICATION_SLOT_WORST_QUERY)
                .fetch_optional(pool)
                .instrument(replication_slot_span)
                .await?
                .as_ref()
                .map(Self::holder_sample_from_row);

            self.xmin_horizon_holder_age_xids.reset();

            self.set_holder_age(
                BACKEND_HOLDER,
                backend_worst.as_ref().map_or(0, |sample| sample.age_xids),
            );
            self.set_holder_age(PREPARED_XACT_HOLDER, prepared_aggregate.max_age_xids);
            self.set_holder_age(
                REPLICATION_SLOT_HOLDER,
                replication_slot_worst
                    .as_ref()
                    .map_or(0, |sample| sample.age_xids),
            );

            self.prepared_xacts_count.set(prepared_aggregate.count);
            self.prepared_xacts_oldest_age_seconds
                .set(prepared_aggregate.oldest_age_seconds);

            if let Some(sample) = &backend_worst {
                self.set_worst_holder(BACKEND_HOLDER, sample);
            }
            if let Some(sample) = &prepared_worst {
                self.set_worst_holder(PREPARED_XACT_HOLDER, sample);
            }
            if let Some(sample) = &replication_slot_worst {
                self.set_worst_holder(REPLICATION_SLOT_HOLDER, sample);
            }

            debug!(
                prepared_xacts = prepared_aggregate.count,
                "updated vacuum blocker metrics"
            );

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collector_name_is_vacuum_blockers() {
        assert_eq!(VacuumBlockersCollector::new().name(), "vacuum_blockers");
    }

    #[test]
    fn register_metrics_succeeds_with_unique_names() {
        let registry = Registry::new();
        assert!(VacuumBlockersCollector::new()
            .register_metrics(&registry)
            .is_ok());
    }

    #[test]
    fn reset_clears_previous_worst_holder_series() -> Result<()> {
        let collector = VacuumBlockersCollector::new();
        let registry = Registry::new();

        collector.register_metrics(&registry)?;
        collector
            .xmin_horizon_holder_age_xids
            .with_label_values(&[BACKEND_HOLDER, "app"])
            .set(42);
        collector.xmin_horizon_holder_age_xids.reset();

        let stale_series = registry
            .gather()
            .iter()
            .find(|family| family.name() == "pg_xmin_horizon_holder_age_xids")
            .is_some_and(|family| !family.get_metric().is_empty());

        assert!(!stale_series, "worst holder series should reset cleanly");
        Ok(())
    }

    #[test]
    fn blocker_queries_cast_numeric_outputs() {
        assert!(BACKEND_WORST_QUERY.contains("age(backend_xmin)::bigint AS age_xids"));
        assert!(PREPARED_XACTS_AGGREGATE_QUERY.contains("count(*)::bigint AS cnt"));
        assert!(PREPARED_XACTS_AGGREGATE_QUERY.contains("0)::bigint AS max_age_xids"));
        assert!(PREPARED_XACTS_AGGREGATE_QUERY.contains("0)::double precision AS oldest_age_seconds"));
        assert!(PREPARED_XACT_WORST_QUERY.contains("age(transaction)::bigint AS age_xids"));
        assert!(REPLICATION_SLOT_WORST_QUERY.contains(")::bigint AS age_xids"));
    }
}
