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
        "pg_settings_data_checksums",
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

        if setting_name == "pg_settings_fsync"
            || setting_name == "pg_settings_autovacuum"
            || setting_name == "pg_settings_data_checksums"
        {
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

#[tokio::test]
async fn test_settings_collector_exposes_wal_size_settings() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let collector = SettingsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // max_wal_size and min_wal_size are reported by pg_settings in MB and must be
    // converted to bytes; both should be present and positive.
    for setting_name in [
        "pg_settings_max_wal_size_bytes",
        "pg_settings_min_wal_size_bytes",
    ] {
        let fam = metric_families
            .iter()
            .find(|m| m.name() == setting_name)
            .unwrap_or_else(|| panic!("{setting_name} should exist"));

        let value = common::metric_value_to_i64(fam.get_metric()[0].get_gauge().value());
        assert!(
            value > 0,
            "{setting_name} should be positive (bytes), got {value}"
        );
        // A sane lower bound: the PostgreSQL default min_wal_size is 80MB and
        // max_wal_size is 1GB, so after MB->bytes conversion both exceed 1 MiB.
        assert!(
            value >= 1024 * 1024,
            "{setting_name} should be converted to bytes (>= 1 MiB), got {value}"
        );
    }

    pool.close().await;
    Ok(())
}

/// The temp-file safeguards from issue #32. `temp_file_limit` and `log_temp_files`
/// are reported by `pg_settings` in kB, but `-1` is a sentinel (unlimited /
/// disabled) that must survive the unit conversion untouched.
#[tokio::test]
async fn test_settings_collector_exposes_temp_safeguards() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let collector = SettingsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();
    let value_of = |name: &str| -> Option<i64> {
        metric_families
            .iter()
            .find(|family| family.name() == name)
            .and_then(|family| family.get_metric().first())
            .map(|metric| common::metric_value_to_i64(metric.get_gauge().value()))
    };

    let block_size = value_of("pg_settings_block_size_bytes")
        .expect("pg_settings_block_size_bytes should exist");
    assert!(
        block_size >= 1024 && block_size.count_ones() == 1,
        "block_size must be a power-of-two byte count, got {block_size}"
    );

    for setting_name in [
        "pg_settings_temp_file_limit_bytes",
        "pg_settings_log_temp_files_bytes",
    ] {
        let value = value_of(setting_name).unwrap_or_else(|| panic!("{setting_name} should exist"));
        assert!(
            value == -1 || value >= 0,
            "{setting_name} must be -1 or a non-negative byte count, got {value}"
        );
        assert_ne!(
            value, -1024,
            "{setting_name}: the -1 sentinel must not be scaled by the kB unit"
        );
    }

    pool.close().await;
    Ok(())
}

/// A positive `temp_file_limit` must be converted from kB to bytes, while `-1`
/// must stay `-1`.
#[tokio::test]
async fn test_settings_collector_converts_temp_file_limit_to_bytes() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let raw: i64 = sqlx::query_scalar(
        "SELECT setting::bigint FROM pg_settings WHERE name = 'temp_file_limit'",
    )
    .fetch_one(&pool)
    .await?;

    let collector = SettingsCollector::new();
    let registry = Registry::new();
    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let exported = registry
        .gather()
        .iter()
        .find(|family| family.name() == "pg_settings_temp_file_limit_bytes")
        .and_then(|family| family.get_metric().first())
        .map(|metric| common::metric_value_to_i64(metric.get_gauge().value()))
        .expect("pg_settings_temp_file_limit_bytes should exist");

    let expected = if raw < 0 { raw } else { raw * 1024 };
    assert_eq!(
        exported, expected,
        "temp_file_limit is reported in kB; only non-negative values are scaled"
    );

    pool.close().await;
    Ok(())
}
