use super::super::common;
use anyhow::Result;
use pg_exporter::collectors::{Collector, vacuum::progress::VacuumProgressCollector};
use prometheus::Registry;

#[tokio::test]
async fn test_vacuum_progress_collector_registers_without_error() -> Result<()> {
    let collector = VacuumProgressCollector::new();
    let registry = Registry::new();

    // Should not error when registering
    collector.register_metrics(&registry)?;

    Ok(())
}

#[tokio::test]
async fn test_vacuum_progress_collector_has_all_metrics_after_collection() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = VacuumProgressCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Should have all vacuum progress metrics
    let expected_metrics = vec![
        "pg_vacuum_in_progress",
        "pg_vacuum_heap_progress",
        "pg_vacuum_heap_vacuumed",
        "pg_vacuum_index_vacuum_count",
        "pg_vacuum_active",
    ];

    for metric_name in expected_metrics {
        let found = metric_families.iter().any(|m| m.name() == metric_name);
        assert!(
            found,
            "Metric {} should exist. Found: {:?}",
            metric_name,
            metric_families.iter().map(|m| m.name()).collect::<Vec<_>>()
        );
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_vacuum_progress_collector_handles_no_active_vacuums() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = VacuumProgressCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // When no vacuums are running, pg_vacuum_active should be 0
    let active_metric = metric_families
        .iter()
        .find(|m| m.name() == "pg_vacuum_active")
        .expect("pg_vacuum_active metric should exist");

    let value = active_metric.get_metric()[0].get_gauge().value() as i64;

    // Since we just started the test database, likely no vacuums are running
    // Value should be 0 or 1 (if autovacuum happened to start)
    assert!(
        value == 0 || value == 1,
        "pg_vacuum_active should be 0 or 1, got {}",
        value
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_vacuum_progress_collector_metrics_have_table_labels() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = VacuumProgressCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Check that table-level metrics have "table" label
    let table_metrics = vec![
        "pg_vacuum_in_progress",
        "pg_vacuum_heap_progress",
        "pg_vacuum_heap_vacuumed",
        "pg_vacuum_index_vacuum_count",
    ];

    for metric_name in table_metrics {
        if let Some(metric_family) = metric_families.iter().find(|m| m.name() == metric_name) {
            // If there are metrics, they should have a "table" label
            for metric in metric_family.get_metric() {
                let has_table_label = metric.get_label().iter().any(|l| l.name() == "table");
                assert!(
                    has_table_label,
                    "Metric {} should have 'table' label",
                    metric_name
                );
            }
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_vacuum_progress_collector_name() {
    let collector = VacuumProgressCollector::new();
    assert_eq!(collector.name(), "vacuum_progress");
}

#[tokio::test]
async fn test_vacuum_progress_collector_progress_percentage_is_valid() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = VacuumProgressCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Check that progress percentage is in valid range (0-100)
    if let Some(progress_metric) = metric_families
        .iter()
        .find(|m| m.name() == "pg_vacuum_heap_progress")
    {
        for metric in progress_metric.get_metric() {
            let value = metric.get_gauge().value() as i64;
            assert!(
                (0..=100).contains(&value),
                "Progress percentage should be 0-100, got {}",
                value
            );
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_vacuum_progress_collector_counts_are_non_negative() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = VacuumProgressCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // All vacuum progress counts should be non-negative
    for family in &metric_families {
        if family.name().starts_with("pg_vacuum_") {
            for metric in family.get_metric() {
                let value = metric.get_gauge().value();
                assert!(
                    value >= 0.0,
                    "Metric {} should be non-negative, got: {}",
                    family.name(),
                    value
                );
            }
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_vacuum_progress_collector_handles_concurrent_collection() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = VacuumProgressCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;

    // Collect concurrently
    let (r1, r2, r3) = tokio::join!(
        collector.collect(&pool),
        collector.collect(&pool),
        collector.collect(&pool)
    );

    // All should succeed
    r1?;
    r2?;
    r3?;

    pool.close().await;
    Ok(())
}
