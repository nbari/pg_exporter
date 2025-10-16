use super::super::common;
use anyhow::Result;
use pg_exporter::collectors::{Collector, default::postmaster::PostmasterCollector};
use prometheus::Registry;

#[tokio::test]
async fn test_postmaster_collector_returns_start_time() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = PostmasterCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();
    let postmaster_start = metric_families
        .iter()
        .find(|m| m.name() == "pg_postmaster_start_time_seconds")
        .expect("pg_postmaster_start_time_seconds should exist");

    assert_eq!(
        postmaster_start.get_field_type(),
        prometheus::proto::MetricType::GAUGE
    );

    let metric = &postmaster_start.get_metric()[0];
    let start_time = metric.get_gauge().value() as i64;

    // Should be a valid Unix timestamp
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    // Basic sanity checks:
    // 1. Should be positive
    assert!(
        start_time > 0,
        "start_time should be positive, got: {}",
        start_time
    );

    // 2. Should not be in the future
    assert!(
        start_time <= now,
        "start_time should not be in the future. start_time: {}, now: {}",
        start_time,
        now
    );

    // 3. Should be a reasonable Unix timestamp (after year 2000)
    let year_2000 = 946684800; // 2000-01-01 00:00:00 UTC
    assert!(
        start_time >= year_2000,
        "start_time should be after year 2000 ({}), got: {}",
        year_2000,
        start_time
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_postmaster_collector_is_idempotent() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = PostmasterCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;

    // Collect twice
    collector.collect(&pool).await?;
    let first_value = {
        let metrics = registry.gather();
        let metric = metrics
            .iter()
            .find(|m| m.name() == "pg_postmaster_start_time_seconds")
            .unwrap();
        metric.get_metric()[0].get_gauge().value() as i64
    };

    // Small delay
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    collector.collect(&pool).await?;
    let second_value = {
        let metrics = registry.gather();
        let metric = metrics
            .iter()
            .find(|m| m.name() == "pg_postmaster_start_time_seconds")
            .unwrap();
        metric.get_metric()[0].get_gauge().value() as i64
    };

    // Start time should be the same (PostgreSQL didn't restart)
    assert_eq!(
        first_value, second_value,
        "Postmaster start time should be consistent"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_postmaster_collector_name() {
    let collector = PostmasterCollector::new();
    assert_eq!(collector.name(), "postmaster");
}

#[tokio::test]
async fn test_postmaster_collector_enabled_by_default() {
    let collector = PostmasterCollector::new();
    assert!(collector.enabled_by_default());
}

#[tokio::test]
async fn test_postmaster_collector_metric_has_correct_type() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let collector = PostmasterCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;

    // Before collection, metric should be registered but value might be 0
    let metrics = registry.gather();
    let metric = metrics
        .iter()
        .find(|m| m.name() == "pg_postmaster_start_time_seconds")
        .expect("Metric should be registered");

    assert_eq!(
        metric.get_field_type(),
        prometheus::proto::MetricType::GAUGE,
        "Should be a gauge metric"
    );

    pool.close().await;
    Ok(())
}
