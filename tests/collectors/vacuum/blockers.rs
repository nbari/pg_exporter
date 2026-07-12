use super::super::common;
use anyhow::Result;
use pg_exporter::collectors::{Collector, vacuum::blockers::VacuumBlockersCollector};
use prometheus::{Registry, proto::MetricFamily};
use sqlx::Row;

const BLOCKER_SNAPSHOT_METRICS: [&str; 3] = [
    "pg_xmin_horizon_age_xids",
    "pg_prepared_xacts_count",
    "pg_prepared_xacts_oldest_age_seconds",
];

fn metric_family<'a>(families: &'a [MetricFamily], name: &str) -> Option<&'a MetricFamily> {
    families.iter().find(|family| family.name() == name)
}

fn gauge_value(family: &MetricFamily) -> Option<f64> {
    family
        .get_metric()
        .first()
        .map(|metric| metric.get_gauge().value())
}

#[tokio::test]
async fn test_vacuum_blockers_registers_without_error() -> Result<()> {
    let collector = VacuumBlockersCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    Ok(())
}

#[tokio::test]
async fn test_vacuum_blockers_collector_name() {
    let collector = VacuumBlockersCollector::new();
    assert_eq!(collector.name(), "vacuum_blockers");
}

#[tokio::test]
async fn test_vacuum_blockers_collect_succeeds_on_idle_database() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let collector = VacuumBlockersCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();
    for metric_name in BLOCKER_SNAPSHOT_METRICS {
        assert!(
            metric_family(&families, metric_name).is_some(),
            "expected metric family {metric_name} to be registered"
        );
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_vacuum_blockers_idle_holders_are_zero_when_absent() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let collector = VacuumBlockersCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let prepared_count: i64 = sqlx::query_scalar("SELECT count(*)::bigint FROM pg_prepared_xacts")
        .fetch_one(&pool)
        .await?;
    let slot_count: i64 = sqlx::query_scalar(
        "SELECT count(*)::bigint FROM pg_replication_slots WHERE xmin IS NOT NULL OR catalog_xmin IS NOT NULL",
    )
    .fetch_one(&pool)
    .await?;

    let families = registry.gather();
    let horizon = metric_family(&families, "pg_xmin_horizon_age_xids")
        .ok_or_else(|| anyhow::anyhow!("missing pg_xmin_horizon_age_xids"))?;

    let mut saw_backend = false;
    let mut saw_prepared = false;
    let mut saw_slot = false;

    for metric in horizon.get_metric() {
        let holder = metric
            .get_label()
            .iter()
            .find(|label| label.name() == "holder")
            .map(prometheus::proto::LabelPair::value)
            .unwrap_or_default();
        let value = common::metric_value_to_i64(metric.get_gauge().value());
        assert!(value >= 0, "holder {holder} age should be non-negative");

        match holder {
            "backend" => saw_backend = true,
            "prepared_xact" => {
                saw_prepared = true;
                if prepared_count == 0 {
                    assert_eq!(value, 0, "no prepared transactions should export zero age");
                }
            }
            "replication_slot" => {
                saw_slot = true;
                if slot_count == 0 {
                    assert_eq!(value, 0, "no xmin-holding slots should export zero age");
                }
            }
            other => anyhow::bail!("unexpected holder label {other}"),
        }
    }

    assert!(
        saw_backend,
        "backend holder series should always be present"
    );
    assert!(
        saw_prepared,
        "prepared_xact holder series should always be present"
    );
    assert!(
        saw_slot,
        "replication_slot holder series should always be present"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_vacuum_blockers_worst_offender_series_are_bounded() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let collector = VacuumBlockersCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();
    if let Some(worst) = metric_family(&families, "pg_xmin_horizon_holder_age_xids") {
        assert!(
            worst.get_metric().len() <= 3,
            "worst offender metric must emit at most one series per holder type"
        );
        for metric in worst.get_metric() {
            let labels: Vec<&str> = metric
                .get_label()
                .iter()
                .map(prometheus::proto::LabelPair::name)
                .collect();
            assert!(labels.contains(&"holder"));
            assert!(labels.contains(&"identity"));
            assert!(metric.get_gauge().value() >= 0.0);
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_vacuum_blockers_prepared_xact_type_conversions() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let row = sqlx::query(
        "SELECT
            count(*)::bigint AS cnt,
            COALESCE(max(age(transaction)), 0)::bigint AS max_age_xids,
            COALESCE(EXTRACT(EPOCH FROM (now() - min(prepared))), 0)::double precision AS oldest_age_seconds
         FROM pg_prepared_xacts",
    )
    .fetch_one(&pool)
    .await?;

    let cnt: i64 = row.try_get("cnt")?;
    let max_age_xids: i64 = row.try_get("max_age_xids")?;
    let oldest_age_seconds: f64 = row.try_get("oldest_age_seconds")?;

    assert!(cnt >= 0);
    assert!(max_age_xids >= 0);
    assert!(oldest_age_seconds.is_finite() && oldest_age_seconds >= 0.0);

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_vacuum_blockers_prepared_gauges_are_finite() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let collector = VacuumBlockersCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();
    for metric_name in [
        "pg_prepared_xacts_count",
        "pg_prepared_xacts_oldest_age_seconds",
    ] {
        let family = metric_family(&families, metric_name)
            .ok_or_else(|| anyhow::anyhow!("missing metric family {metric_name}"))?;
        let value = gauge_value(family)
            .ok_or_else(|| anyhow::anyhow!("missing sample for {metric_name}"))?;
        assert!(value.is_finite() && value >= 0.0);
    }

    pool.close().await;
    Ok(())
}
