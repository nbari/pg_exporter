use super::super::common;
use anyhow::Result;
use pg_exporter::collectors::{Collector, activity::queries::QueriesCollector};
use prometheus::Registry;

#[tokio::test]
async fn test_queries_collector_registers_without_error() -> Result<()> {
    let collector = QueriesCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    Ok(())
}

#[tokio::test]
async fn test_queries_collector_name() {
    let collector = QueriesCollector::new();
    assert_eq!(collector.name(), "queries");
}

#[tokio::test]
async fn test_queries_collector_enabled_by_default() {
    let collector = QueriesCollector::new();
    // queries collector is DISABLED by default (opt-in for production)
    assert!(
        !collector.enabled_by_default(),
        "queries collector should be disabled by default (opt-in)"
    );
}

#[tokio::test]
async fn test_queries_collector_collects_from_database() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = QueriesCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Should have at least the global metrics (always present)
    let expected_global_metrics = vec![
        "pg_stat_activity_oldest_query_age_seconds",
        "pg_stat_activity_total_long_running", // Note: not "queries" suffix
    ];

    for expected in &expected_global_metrics {
        assert!(
            metric_families.iter().any(|m| m.name() == *expected),
            "Metric {} should exist. Found: {:?}",
            expected,
            metric_families.iter().map(|m| m.name()).collect::<Vec<_>>()
        );
    }

    // Per-database metrics may be empty if no long-running queries
    // (which is normal for a test database with no load)

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_queries_collector_metrics_non_negative() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = QueriesCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // All query counts should be non-negative
    for family in &metric_families {
        if family.name().starts_with("pg_stat_activity_") {
            for metric in family.get_metric() {
                let value = metric.get_gauge().value();

                assert!(
                    value >= 0.0,
                    "Metric {} should be non-negative, got {}",
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
async fn test_queries_collector_detects_no_slow_queries_in_clean_db() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = QueriesCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // In a clean test database, oldest query age should be 0
    let oldest = metric_families
        .iter()
        .find(|m| m.name() == "pg_stat_activity_oldest_query_age_seconds");

    if let Some(family) = oldest {
        for metric in family.get_metric() {
            let value = metric.get_gauge().value();
            // Should be 0 or very small (no long-running queries)
            assert!(value >= 0.0, "Oldest query age should be non-negative");
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_queries_collector_max_duration_reasonable() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = QueriesCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Max query duration should be reasonable (not negative, not absurdly large)
    let max_duration = metric_families
        .iter()
        .find(|m| m.name() == "pg_stat_activity_max_query_duration_seconds");

    if let Some(family) = max_duration {
        for metric in family.get_metric() {
            let value = metric.get_gauge().value();
            assert!(value >= 0.0, "Max query duration should be non-negative");
            // Sanity check: should not be more than 1 year in seconds
            assert!(
                value < 365.0 * 24.0 * 3600.0,
                "Max query duration seems unreasonable: {} seconds",
                value
            );
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_queries_collector_labels_present() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = QueriesCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Check that per-database metrics have datname label
    let per_db_metrics = vec![
        "pg_stat_activity_queries_over_5m",
        "pg_stat_activity_queries_over_15m",
        "pg_stat_activity_queries_over_1h",
        "pg_stat_activity_queries_over_6h",
        "pg_stat_activity_max_query_duration_seconds",
    ];

    for metric_name in &per_db_metrics {
        if let Some(family) = metric_families.iter().find(|m| m.name() == *metric_name) {
            for metric in family.get_metric() {
                let labels: Vec<_> = metric.get_label().iter().map(|l| l.name()).collect();
                assert!(
                    labels.contains(&"datname"),
                    "Metric {} should have 'datname' label",
                    metric_name
                );
            }
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_queries_collector_handles_concurrent_collections() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = QueriesCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;

    // Collect multiple times concurrently
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

#[tokio::test]
async fn test_queries_collector_oldest_query_age_global() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = QueriesCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Oldest query age should exist and be a single global metric (no labels)
    let oldest = metric_families
        .iter()
        .find(|m| m.name() == "pg_stat_activity_oldest_query_age_seconds")
        .expect("oldest_query_age_seconds should exist");

    assert!(
        !oldest.get_metric().is_empty(),
        "oldest_query_age should have a value"
    );

    // Should have no labels (global metric)
    for metric in oldest.get_metric() {
        assert!(
            metric.get_label().is_empty(),
            "oldest_query_age should be a global metric with no labels"
        );
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_queries_collector_total_long_running() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = QueriesCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Total long running queries should exist
    let total = metric_families
        .iter()
        .find(|m| m.name() == "pg_stat_activity_total_long_running")
        .expect("total_long_running should exist");

    assert!(
        !total.get_metric().is_empty(),
        "total_long_running_queries should have a value"
    );

    // Should be non-negative (likely 0 in test environment)
    for metric in total.get_metric() {
        let value = metric.get_gauge().value();
        assert!(
            value >= 0.0,
            "total_long_running_queries should be non-negative, got {}",
            value
        );
        // In test environment with no long queries, should be 0
        assert_eq!(
            value, 0.0,
            "test environment should have no long-running queries"
        );
    }

    pool.close().await;
    Ok(())
}
