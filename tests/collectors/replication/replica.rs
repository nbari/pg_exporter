use super::super::common;
use anyhow::Result;
use pg_exporter::collectors::{Collector, replication::replica::ReplicaCollector};
use prometheus::Registry;

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
            families.iter().map(|m| m.name()).collect::<Vec<_>>()
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
                let v = m.get_gauge().value();
                assert!(
                    v == 0.0 || v == 1.0,
                    "is_replica should be 0 or 1, got {}",
                    v
                );
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
                assert!(v >= 0.0, "lag_seconds should be non-negative, got {}", v);
            }
        }
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
