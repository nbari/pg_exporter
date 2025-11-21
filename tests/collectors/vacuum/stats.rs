use super::super::common;
use anyhow::Result;
use pg_exporter::collectors::{Collector, vacuum::stats::VacuumStatsCollector};
use prometheus::Registry;

#[tokio::test]
async fn test_vacuum_stats_collector_registers_without_error() -> Result<()> {
    let collector = VacuumStatsCollector::new();
    let registry = Registry::new();

    // Should not error when registering
    collector.register_metrics(&registry)?;

    Ok(())
}

#[tokio::test]
async fn test_vacuum_stats_collector_has_all_metrics_after_collection() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = VacuumStatsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Should have all vacuum stats metrics
    let expected_metrics = vec![
        "pg_vacuum_database_freeze_age_xids",
        "pg_vacuum_freeze_max_age_xids",
        "pg_vacuum_database_freeze_age_pct_of_max",
        "pg_vacuum_autovacuum_workers",
    ];

    for metric_name in expected_metrics {
        let found = metric_families.iter().any(|m| m.name() == metric_name);
        assert!(
            found,
            "Metric {} should exist. Found: {:?}",
            metric_name,
            metric_families
                .iter()
                .map(prometheus::proto::MetricFamily::name)
                .collect::<Vec<_>>()
        );
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_vacuum_stats_collector_freeze_max_age_is_reasonable() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = VacuumStatsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // pg_vacuum_freeze_max_age_xids should be a reasonable value (default is 200M)
    let freeze_max = metric_families
        .iter()
        .find(|m| m.name() == "pg_vacuum_freeze_max_age_xids")
        .expect("pg_vacuum_freeze_max_age_xids should exist");

    let value = common::metric_value_to_i64(freeze_max.get_metric()[0].get_gauge().value());

    // Typical values are 200,000,000 (default) to 2,000,000,000
    assert!(
        (1_000_000..=2_100_000_000).contains(&value),
        "freeze_max_age should be between 1M and 2.1B, got {value}"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_vacuum_stats_collector_freeze_age_percentage_is_valid() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = VacuumStatsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Freeze age percentage should be 0-100
    let freeze_pct = metric_families
        .iter()
        .find(|m| m.name() == "pg_vacuum_database_freeze_age_pct_of_max")
        .expect("pg_vacuum_database_freeze_age_pct_of_max should exist");

    for metric in freeze_pct.get_metric() {
        let value = common::metric_value_to_i64(metric.get_gauge().value());
        assert!(
            (0..=100).contains(&value),
            "Freeze age percentage should be 0-100, got {value}"
        );
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_vacuum_stats_collector_database_freeze_age_is_non_negative() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = VacuumStatsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Database freeze age should be non-negative
    let db_freeze_age = metric_families
        .iter()
        .find(|m| m.name() == "pg_vacuum_database_freeze_age_xids")
        .expect("pg_vacuum_database_freeze_age_xids should exist");

    for metric in db_freeze_age.get_metric() {
        let value = common::metric_value_to_i64(metric.get_gauge().value());
        assert!(
            value >= 0,
            "Database freeze age should be non-negative, got {value}"
        );
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_vacuum_stats_collector_autovacuum_workers_is_non_negative() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = VacuumStatsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Autovacuum workers should be non-negative
    let workers = metric_families
        .iter()
        .find(|m| m.name() == "pg_vacuum_autovacuum_workers")
        .expect("pg_vacuum_autovacuum_workers should exist");

    for metric in workers.get_metric() {
        let value = common::metric_value_to_i64(metric.get_gauge().value());
        assert!(
            value >= 0,
            "Autovacuum workers should be non-negative, got {value}"
        );
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_vacuum_stats_collector_metrics_have_datname_labels() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = VacuumStatsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Check that per-database metrics have "datname" label
    let db_metrics = vec![
        "pg_vacuum_database_freeze_age_xids",
        "pg_vacuum_database_freeze_age_pct_of_max",
        "pg_vacuum_autovacuum_workers",
    ];

    for metric_name in db_metrics {
        let metric_family = metric_families
            .iter()
            .find(|m| m.name() == metric_name)
            .unwrap_or_else(|| panic!("{metric_name} should exist"));

        for metric in metric_family.get_metric() {
            let has_datname = metric.get_label().iter().any(|l| l.name() == "datname");
            assert!(
                has_datname,
                "Metric {metric_name} should have 'datname' label"
            );
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_vacuum_stats_collector_name() {
    let collector = VacuumStatsCollector::new();
    assert_eq!(collector.name(), "vacuum_stats");
}

#[tokio::test]
async fn test_vacuum_stats_collector_handles_concurrent_collection() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = VacuumStatsCollector::new();
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

#[tokio::test]
async fn test_vacuum_stats_collector_collects_from_test_database() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = VacuumStatsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Should have at least one database with freeze age
    let db_freeze_age = metric_families
        .iter()
        .find(|m| m.name() == "pg_vacuum_database_freeze_age_xids")
        .expect("pg_vacuum_database_freeze_age_xids should exist");

    assert!(
        !db_freeze_age.get_metric().is_empty(),
        "Should have freeze age metrics for at least one database"
    );

    pool.close().await;
    Ok(())
}
