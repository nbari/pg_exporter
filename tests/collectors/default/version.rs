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

/// Issue 42: `short_version` was padded to three parts (`18.6.0`), a version
/// `PostgreSQL` has not used since release 10. It must be `major.minor`.
#[tokio::test]
async fn test_version_collector_reports_major_and_minor() -> Result<()> {
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
    let short_version = metric
        .get_label()
        .iter()
        .find(|l| l.name() == "short_version")
        .unwrap()
        .value()
        .to_owned();

    let parts: Vec<&str> = short_version.split('.').collect();
    assert_eq!(
        parts.len(),
        2,
        "short_version should be major.minor, got: {short_version}"
    );
    for part in &parts {
        assert!(
            part.parse::<u32>().is_ok(),
            "version part '{part}' should be a number"
        );
    }

    // It is the server's own numeric version.
    let server_version_num: i32 =
        sqlx::query_scalar("SELECT current_setting('server_version_num')::int4")
            .fetch_one(&pool)
            .await?;
    assert_eq!(
        short_version,
        format!(
            "{}.{}",
            server_version_num / 10_000,
            server_version_num % 10_000
        ),
        "short_version should follow server_version_num {server_version_num}"
    );

    // On a released server it is also what PostgreSQL calls itself: the leading
    // token of `server_version` ("18.6", or "18.6 (Debian 18.6-1.pgdg13+1)").
    // A pre-release ("19beta1") has no minor there and reports minor 0 instead.
    let server_version: String = sqlx::query_scalar("SHOW server_version")
        .fetch_one(&pool)
        .await?;
    let leading: String = server_version
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '.')
        .collect();
    if leading.contains('.') {
        assert_eq!(
            short_version, leading,
            "short_version should match server_version '{server_version}'"
        );
    } else {
        assert_eq!(
            short_version,
            format!("{leading}.0"),
            "a pre-release '{server_version}' should report minor 0"
        );
    }

    pool.close().await;
    Ok(())
}

/// The full banner stays in the `version` label and still agrees with the short one.
#[tokio::test]
async fn test_version_collector_short_version_agrees_with_the_full_banner() -> Result<()> {
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
    let label = |name: &str| {
        metric
            .get_label()
            .iter()
            .find(|l| l.name() == name)
            .unwrap()
            .value()
            .to_owned()
    };
    let full_version = label("version");
    let short_version = label("short_version");

    let banner: String = sqlx::query_scalar("SELECT version()")
        .fetch_one(&pool)
        .await?;
    assert_eq!(full_version, banner, "version label is version() verbatim");

    let major = short_version.split('.').next().unwrap();
    assert!(
        full_version.starts_with(&format!("PostgreSQL {major}")),
        "'{full_version}' should start with the major of short_version '{short_version}'"
    );

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

#[tokio::test]
async fn test_version_collector_no_duplicate_versions() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = VersionCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;

    // Simulate multiple collections (as would happen during version upgrade)
    collector.collect(&pool).await?;
    collector.collect(&pool).await?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Check pg_version_info - should have exactly ONE metric (not duplicates)
    let version_info = metric_families
        .iter()
        .find(|m| m.name() == "pg_version_info")
        .expect("pg_version_info metric should exist");

    assert_eq!(
        version_info.get_metric().len(),
        1,
        "Should have exactly one pg_version_info metric, found {}",
        version_info.get_metric().len()
    );

    // Check pg_settings_server_version_num - should have exactly ONE metric
    let version_num = metric_families
        .iter()
        .find(|m| m.name() == "pg_settings_server_version_num")
        .expect("pg_settings_server_version_num metric should exist");

    assert_eq!(
        version_num.get_metric().len(),
        1,
        "Should have exactly one pg_settings_server_version_num metric, found {}",
        version_num.get_metric().len()
    );

    pool.close().await;
    Ok(())
}
