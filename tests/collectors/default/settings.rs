use super::super::common;
use anyhow::Result;
use pg_exporter::collectors::{Collector, default::settings::SettingsCollector};
use prometheus::Registry;

#[tokio::test]
async fn test_settings_collector_returns_key_settings() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = SettingsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Check for some key settings that should always exist
    let expected_settings = vec![
        "pg_settings_max_connections",
        "pg_settings_shared_buffers_bytes",
        "pg_settings_work_mem_bytes",
        "pg_settings_fsync",
        "pg_settings_autovacuum",
    ];

    for setting_name in expected_settings {
        let setting = metric_families
            .iter()
            .find(|m| m.name() == setting_name)
            .unwrap_or_else(|| panic!("{setting_name} should exist"));

        assert_eq!(
            setting.get_field_type(),
            prometheus::proto::MetricType::GAUGE
        );
        assert!(
            !setting.get_metric().is_empty(),
            "{setting_name} should have a value"
        );

        let metric = &setting.get_metric()[0];
        let value = common::metric_value_to_i64(metric.get_gauge().value());

        // Sanity checks
        if setting_name == "pg_settings_max_connections" {
            assert!(
                value >= 1,
                "max_connections should be at least 1, got {value}"
            );
        }

        if setting_name == "pg_settings_fsync" || setting_name == "pg_settings_autovacuum" {
            // Boolean settings should be 0 or 1
            assert!(
                value == 0 || value == 1,
                "{setting_name} should be 0 or 1, got {value}"
            );
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_settings_collector_handles_on_off_values() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = SettingsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Check boolean settings are properly converted
    let autovacuum = metric_families
        .iter()
        .find(|m| m.name() == "pg_settings_autovacuum")
        .expect("pg_settings_autovacuum should exist");

    let value = common::metric_value_to_i64(autovacuum.get_metric()[0].get_gauge().value());

    // Should be either 0 (off) or 1 (on), not a string
    assert!(
        value == 0 || value == 1,
        "Boolean setting should be 0 or 1, got {value}"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_settings_collector_all_registered_settings_have_values() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = SettingsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // All pg_settings_* metrics should have values after collection
    let settings_metrics: Vec<_> = metric_families
        .iter()
        .filter(|m| m.name().starts_with("pg_settings_"))
        .collect();

    assert!(
        !settings_metrics.is_empty(),
        "Should have collected settings metrics"
    );

    for metric in settings_metrics {
        assert!(
            !metric.get_metric().is_empty(),
            "Metric {} should have a value",
            metric.name()
        );
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_settings_collector_name() {
    let collector = SettingsCollector::new();
    assert_eq!(collector.name(), "settings");
}

#[tokio::test]
async fn test_settings_collector_enabled_by_default() {
    let collector = SettingsCollector::new();
    assert!(collector.enabled_by_default());
}

#[tokio::test]
async fn test_settings_collector_memory_settings_are_reasonable() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let collector = SettingsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Check memory-related settings have reasonable values
    let work_mem = metric_families
        .iter()
        .find(|m| m.name() == "pg_settings_work_mem_bytes")
        .unwrap();

    let value = common::metric_value_to_i64(work_mem.get_metric()[0].get_gauge().value());
    assert!(value > 0, "work_mem should be positive, got {value}");

    pool.close().await;
    Ok(())
}
