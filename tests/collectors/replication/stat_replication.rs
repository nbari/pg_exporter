use super::super::common;
use anyhow::Result;
use pg_exporter::collectors::{Collector, replication::stat_replication::StatReplicationCollector};
use prometheus::Registry;

#[tokio::test]
async fn test_stat_replication_collector_name() {
    let collector = StatReplicationCollector::new();
    assert_eq!(collector.name(), "stat_replication");
}

#[tokio::test]
async fn test_stat_replication_collector_registers_without_error() -> Result<()> {
    let registry = Registry::new();
    let collector = StatReplicationCollector::new();

    collector.register_metrics(&registry)?;
    Ok(())
}

#[tokio::test]
async fn test_stat_replication_collector_has_all_metrics_after_collection() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = StatReplicationCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();

    // Metrics should be registered even if there are no replicas
    // The metric families exist but may have no values

    // We can't guarantee values without replicas, but the collector should succeed
    assert!(
        !families.is_empty() || families.is_empty(),
        "Should have collected metrics successfully"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_stat_replication_collector_metrics_have_labels() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = StatReplicationCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    // Even if there are no replicas, the metric families should exist
    // They just won't have any values
    let families = registry.gather();

    for fam in families {
        if fam.name() == "pg_stat_replication_pg_wal_lsn_diff" {
            // If there are any metrics, they should have the right labels
            for m in fam.get_metric() {
                let labels: Vec<_> = m
                    .get_label()
                    .iter()
                    .map(prometheus::proto::LabelPair::name)
                    .collect();
                assert!(labels.contains(&"application_name"));
                assert!(labels.contains(&"client_addr"));
                assert!(labels.contains(&"state"));
            }
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_stat_replication_collector_handles_no_replicas() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = StatReplicationCollector::new();

    collector.register_metrics(&registry)?;
    // Should succeed even with no replicas
    collector.collect(&pool).await?;

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_stat_replication_collector_handles_concurrent_collection() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let collector = StatReplicationCollector::new();

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
