use super::super::common;
use anyhow::{Context, Result};
use pg_exporter::collectors::{Collector, replication::replica::ReplicaCollector};
use prometheus::Registry;
use sqlx::{PgPool, Row};

// Keep in sync with postgres_exporter collector/pg_replication.go
const POSTGRES_EXPORTER_REPLICATION_QUERY: &str = r"
SELECT
    CASE
        WHEN NOT pg_is_in_recovery() THEN 0
        WHEN pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn() THEN 0
        ELSE GREATEST(0, EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp())))
    END::double precision AS lag,
    CASE
        WHEN pg_is_in_recovery() THEN 1
        ELSE 0
    END::bigint AS is_replica,
    GREATEST(0, EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp())))::double precision AS last_replay
";

fn approx_equal_seconds(left: f64, right: f64, tolerance: f64) -> bool {
    (left - right).abs() <= tolerance
}

fn get_single_gauge_value(
    families: &[prometheus::proto::MetricFamily],
    metric_name: &str,
) -> Result<f64> {
    let family = families
        .iter()
        .find(|family| family.name() == metric_name)
        .with_context(|| format!("missing metric family: {metric_name}"))?;

    let metric = family
        .get_metric()
        .first()
        .with_context(|| format!("missing metric sample: {metric_name}"))?;

    Ok(metric.get_gauge().value())
}

async fn query_postgres_exporter_replication(pool: &PgPool) -> Result<(f64, i64, f64)> {
    let row = sqlx::query(POSTGRES_EXPORTER_REPLICATION_QUERY)
        .fetch_one(pool)
        .await?;

    let lag: f64 = row.try_get("lag")?;
    let is_replica: i64 = row.try_get("is_replica")?;
    let last_replay: f64 = row.try_get("last_replay")?;

    Ok((lag, is_replica, last_replay))
}

#[tokio::test]
async fn test_replica_collector_name() {
    let collector = ReplicaCollector::new();
    assert_eq!(collector.name(), "replication_replica");
}

#[tokio::test]
async fn test_replica_collector_registers_without_error() -> Result<()> {
    let registry = Registry::new();
    let collector = ReplicaCollector::new();

    collector.register_metrics(&registry)?;
    Ok(())
}

#[tokio::test]
async fn test_replica_collector_has_all_metrics_after_collection() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = ReplicaCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();
    let expected = vec![
        "pg_replication_lag_seconds",
        "pg_replication_is_replica",
        "pg_replication_last_replay_seconds",
    ];

    for metric in expected {
        assert!(
            families.iter().any(|m| m.name() == metric),
            "Metric {} should exist. Found: {:?}",
            metric,
            families
                .iter()
                .map(prometheus::proto::MetricFamily::name)
                .collect::<Vec<_>>()
        );
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_replica_collector_is_replica_is_boolean() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = ReplicaCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    for fam in registry.gather() {
        if fam.name() == "pg_replication_is_replica" {
            for m in fam.get_metric() {
                let v = common::metric_value_to_i64(m.get_gauge().value());
                assert!(v == 0 || v == 1, "is_replica should be 0 or 1, got {v}");
            }
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_replica_collector_lag_is_non_negative() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = ReplicaCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    for fam in registry.gather() {
        if fam.name() == "pg_replication_lag_seconds" {
            for m in fam.get_metric() {
                let v = m.get_gauge().value();
                assert!(v >= 0.0, "lag_seconds should be non-negative, got {v}");
            }
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_replica_collector_primary_reports_zero_lag() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = ReplicaCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let mut is_replica = None;
    let mut lag = None;

    for fam in registry.gather() {
        if fam.name() == "pg_replication_is_replica"
            && let Some(m) = fam.get_metric().first()
        {
            is_replica = Some(common::metric_value_to_i64(m.get_gauge().value()));
        }

        if fam.name() == "pg_replication_lag_seconds"
            && let Some(m) = fam.get_metric().first()
        {
            lag = Some(common::metric_value_to_i64(m.get_gauge().value()));
        }
    }

    if let (Some(0), Some(lag_val)) = (is_replica, lag) {
        assert_eq!(
            lag_val, 0,
            "On primary (is_replica=0), lag must be 0 for postgres_exporter compatibility"
        );
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_replica_collector_handles_concurrent_collection() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let collector = ReplicaCollector::new();

    let handles: Vec<_> = (0..5)
        .map(|_| {
            let pool = pool.clone();
            let collector = collector.clone();
            tokio::spawn(async move { collector.collect(&pool).await })
        })
        .collect();

    for handle in handles {
        handle.await.unwrap()?;
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_replica_collector_matches_postgres_exporter_query() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = ReplicaCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();
    let lag_metric = get_single_gauge_value(&families, "pg_replication_lag_seconds")?;
    let is_replica_metric = common::metric_value_to_i64(get_single_gauge_value(
        &families,
        "pg_replication_is_replica",
    )?);
    let last_replay_metric =
        get_single_gauge_value(&families, "pg_replication_last_replay_seconds")?;

    let (expected_lag, expected_is_replica, expected_last_replay) =
        query_postgres_exporter_replication(&pool).await?;

    assert_eq!(
        is_replica_metric, expected_is_replica,
        "is_replica mismatch with postgres_exporter query"
    );

    assert!(
        approx_equal_seconds(lag_metric, expected_lag, 2.0),
        "lag mismatch with postgres_exporter query: ours={lag_metric}, expected={expected_lag}"
    );

    assert!(
        approx_equal_seconds(last_replay_metric, expected_last_replay, 2.0),
        "last_replay mismatch with postgres_exporter query: ours={last_replay_metric}, expected={expected_last_replay}"
    );

    pool.close().await;
    Ok(())
}
