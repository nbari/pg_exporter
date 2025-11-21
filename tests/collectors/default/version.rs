use super::super::common;
use anyhow::Result;
use pg_exporter::collectors::{Collector, default::version::VersionCollector};
use prometheus::Registry;

#[tokio::test]
async fn test_version_collector_queries_database() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = VersionCollector::new();
    let registry = Registry::new();

    // Register metrics
    collector.register_metrics(&registry)?;

    // Collect from database
    collector.collect(&pool).await?;

    // Verify metrics were collected
    let metric_families = registry.gather();

    // Should have pg_version_info
    let version_info = metric_families
        .iter()
        .find(|m| m.name() == "pg_version_info")
        .expect("pg_version_info metric should exist");

    assert_eq!(
        version_info.get_field_type(),
        prometheus::proto::MetricType::GAUGE
    );
    assert!(
        !version_info.get_metric().is_empty(),
        "Should have at least one version metric"
    );

    // Check that version label exists and is not empty
    let metric = &version_info.get_metric()[0];
    let labels: Vec<_> = metric.get_label().iter().collect();

    let version_label = labels
        .iter()
        .find(|l| l.name() == "version")
        .expect("version label should exist");

    assert!(
        !version_label.value().is_empty(),
        "version label should have a value"
    );

    // Should have pg_settings_server_version_num
    let version_num = metric_families
        .iter()
        .find(|m| m.name() == "pg_settings_server_version_num")
        .expect("pg_settings_server_version_num metric should exist");

    assert_eq!(
        version_num.get_field_type(),
        prometheus::proto::MetricType::GAUGE
    );

    let metric = &version_num.get_metric()[0];
    let gauge_value = common::metric_value_to_i64(metric.get_gauge().value());

    // server_version_num should be >= 140_000 (PostgreSQL 14+) based on our test matrix
    assert!(
        gauge_value >= 140_000,
        "server_version_num should be >= 140_000, got {gauge_value}"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_version_collector_normalizes_versions() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = VersionCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();
    let version_info = metric_families
        .iter()
        .find(|m| m.name() == "pg_version_info")
        .unwrap();

    let metric = &version_info.get_metric()[0];
    let labels: Vec<_> = metric.get_label().iter().collect();

    let short_version = labels
        .iter()
        .find(|l| l.name() == "short_version")
        .unwrap()
        .value();

    // Should be in format X.Y.Z
    let parts: Vec<&str> = short_version.split('.').collect();
    assert_eq!(
        parts.len(),
        3,
        "short_version should have 3 parts (X.Y.Z), got: {short_version}"
    );

    // Each part should be a number
    for part in parts {
        assert!(
            part.parse::<u32>().is_ok(),
            "version part '{part}' should be a number"
        );
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_version_collector_name() {
    let collector = VersionCollector::new();
    assert_eq!(collector.name(), "version");
}

#[tokio::test]
async fn test_version_collector_enabled_by_default() {
    let collector = VersionCollector::new();
    assert!(collector.enabled_by_default());
}

#[tokio::test]
async fn test_version_collector_handles_different_version_formats() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let collector = VersionCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();
    let version_info = metric_families
        .iter()
        .find(|m| m.name() == "pg_version_info")
        .unwrap();

    let metric = &version_info.get_metric()[0];
    let labels: Vec<_> = metric.get_label().iter().collect();

    // Full version should exist
    let full_version = labels
        .iter()
        .find(|l| l.name() == "version")
        .unwrap()
        .value();

    // Should contain "PostgreSQL" and a version number
    assert!(
        full_version.contains("PostgreSQL") || full_version.contains("postgres"),
        "Full version should contain 'PostgreSQL', got: {full_version}"
    );

    pool.close().await;
    Ok(())
}
