use super::super::common;
use anyhow::Result;
use pg_exporter::collectors::{Collector, index::UnusedIndexCollector};
use prometheus::Registry;

#[tokio::test]
async fn test_unused_index_collector_name() {
    let collector = UnusedIndexCollector::new();
    assert_eq!(collector.name(), "index_unused");
}

#[tokio::test]
async fn test_unused_index_collector_registers_without_error() -> Result<()> {
    let registry = Registry::new();
    let collector = UnusedIndexCollector::new();

    collector.register_metrics(&registry)?;
    Ok(())
}

#[tokio::test]
async fn test_unused_index_collector_has_all_metrics_after_collection() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = UnusedIndexCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();
    let expected = vec![
        "pg_index_unused_count",
        "pg_index_unused_size_bytes",
        "pg_index_invalid_count",
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
async fn test_unused_index_collector_count_is_non_negative() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = UnusedIndexCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    for fam in registry.gather() {
        if fam.name() == "pg_index_unused_count" {
            for m in fam.get_metric() {
                let v = m.get_gauge().value();
                assert!(v >= 0.0, "unused_count should be non-negative, got {}", v);
            }
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_unused_index_collector_size_is_non_negative() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = UnusedIndexCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    for fam in registry.gather() {
        if fam.name() == "pg_index_unused_size_bytes" {
            for m in fam.get_metric() {
                let v = m.get_gauge().value();
                assert!(
                    v >= 0.0,
                    "unused_size_bytes should be non-negative, got {}",
                    v
                );
            }
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_unused_index_collector_invalid_count_is_non_negative() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = UnusedIndexCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    for fam in registry.gather() {
        if fam.name() == "pg_index_invalid_count" {
            for m in fam.get_metric() {
                let v = m.get_gauge().value();
                assert!(v >= 0.0, "invalid_count should be non-negative, got {}", v);
            }
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_unused_index_collector_handles_concurrent_collection() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let collector = UnusedIndexCollector::new();

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
