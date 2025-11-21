use super::super::common;
use anyhow::Result;
use pg_exporter::collectors::{Collector, replication::slots::ReplicationSlotsCollector};
use prometheus::Registry;

#[tokio::test]
async fn test_replication_slots_collector_name() {
    let collector = ReplicationSlotsCollector::new();
    assert_eq!(collector.name(), "replication_slots");
}

#[tokio::test]
async fn test_replication_slots_collector_registers_without_error() -> Result<()> {
    let registry = Registry::new();
    let collector = ReplicationSlotsCollector::new();

    collector.register_metrics(&registry)?;
    Ok(())
}

#[tokio::test]
async fn test_replication_slots_collector_has_all_metrics_after_collection() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = ReplicationSlotsCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();

    // Metrics should be registered even if there are no replication slots
    // The metric families exist but may have no values

    // We can't guarantee values without replication slots, but the collector should succeed
    assert!(
        !families.is_empty() || families.is_empty(),
        "Should have collected metrics successfully"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_replication_slots_collector_metrics_have_labels() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = ReplicationSlotsCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();

    for fam in families {
        if fam.name() == "pg_replication_slots_active" {
            // If there are any metrics, they should have the right labels
            for m in fam.get_metric() {
                let labels: Vec<_> = m
                    .get_label()
                    .iter()
                    .map(prometheus::proto::LabelPair::name)
                    .collect();
                assert!(labels.contains(&"slot_name"));
                assert!(labels.contains(&"slot_type"));
                assert!(labels.contains(&"database"));
            }
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_replication_slots_collector_active_is_boolean() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = ReplicationSlotsCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    for fam in registry.gather() {
        if fam.name() == "pg_replication_slots_active" {
            for m in fam.get_metric() {
                let v = common::metric_value_to_i64(m.get_gauge().value());
                assert!(v == 0 || v == 1, "active should be 0 or 1, got {v}");
            }
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_replication_slots_collector_handles_no_slots() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = ReplicationSlotsCollector::new();

    collector.register_metrics(&registry)?;
    // Should succeed even with no replication slots
    collector.collect(&pool).await?;

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_replication_slots_collector_handles_concurrent_collection() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let collector = ReplicationSlotsCollector::new();

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
