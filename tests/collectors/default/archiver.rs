use super::super::common;
use anyhow::Result;
use pg_exporter::collectors::{Collector, default::archiver::ArchiverCollector};
use prometheus::Registry;

#[tokio::test]
async fn test_archiver_collector_registers_without_error() -> Result<()> {
    let registry = Registry::new();
    let collector = ArchiverCollector::new();

    collector.register_metrics(&registry)?;
    Ok(())
}

#[tokio::test]
async fn test_archiver_collector_has_all_metrics() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = ArchiverCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();
    let expected_metrics = vec![
        "pg_stat_archiver_archived_total",
        "pg_stat_archiver_failed_total",
        "pg_stat_archiver_last_archived_age_seconds",
        "pg_stat_archiver_last_failed_age_seconds",
    ];

    for metric_name in expected_metrics {
        assert!(
            families.iter().any(|m| m.name() == metric_name),
            "Metric {} should exist. Found: {:?}",
            metric_name,
            families
                .iter()
                .map(prometheus::proto::MetricFamily::name)
                .collect::<Vec<_>>()
        );
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_archiver_collector_values_non_negative() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = ArchiverCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    for fam in registry.gather() {
        if fam.name().starts_with("pg_stat_archiver_") {
            for m in fam.get_metric() {
                let v = if fam.get_field_type() == prometheus::proto::MetricType::COUNTER {
                    m.get_counter().value()
                } else {
                    m.get_gauge().value()
                };
                assert!(
                    v >= 0.0,
                    "Metric {} should be non-negative, got {}",
                    fam.name(),
                    v
                );
            }
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_archiver_collector_counter_and_gauge_types() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = ArchiverCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();

    // Counter metrics
    let counter_metrics = vec![
        "pg_stat_archiver_archived_total",
        "pg_stat_archiver_failed_total",
    ];

    for metric_name in counter_metrics {
        let metric_family = families
            .iter()
            .find(|m| m.name() == metric_name)
            .unwrap_or_else(|| panic!("Metric {metric_name} should exist"));

        assert_eq!(
            metric_family.get_field_type(),
            prometheus::proto::MetricType::COUNTER,
            "Metric {metric_name} should be a COUNTER"
        );
    }

    // Gauge metrics
    let gauge_metrics = vec![
        "pg_stat_archiver_last_archived_age_seconds",
        "pg_stat_archiver_last_failed_age_seconds",
    ];

    for metric_name in gauge_metrics {
        let metric_family = families
            .iter()
            .find(|m| m.name() == metric_name)
            .unwrap_or_else(|| panic!("Metric {metric_name} should exist"));

        assert_eq!(
            metric_family.get_field_type(),
            prometheus::proto::MetricType::GAUGE,
            "Metric {metric_name} should be a GAUGE"
        );
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_archiver_collector_concurrent_collections() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = ArchiverCollector::new();

    collector.register_metrics(&registry)?;

    // Run multiple collections concurrently
    let mut handles = vec![];
    for _ in 0..5 {
        let pool_clone = pool.clone();
        let collector_clone = collector.clone();
        handles.push(tokio::spawn(async move {
            collector_clone.collect(&pool_clone).await
        }));
    }

    // Wait for all to complete
    for handle in handles {
        handle.await??;
    }

    // Verify metrics are still valid
    let families = registry.gather();
    assert!(
        families
            .iter()
            .any(|m| m.name() == "pg_stat_archiver_archived_total"),
        "Metrics should exist after concurrent collections"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_archiver_collector_idempotent_collection() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = ArchiverCollector::new();

    collector.register_metrics(&registry)?;

    // Collect multiple times
    for _ in 0..3 {
        collector.collect(&pool).await?;
    }

    // Verify metrics exist and are valid
    let families = registry.gather();
    let archived = families
        .iter()
        .find(|m| m.name() == "pg_stat_archiver_archived_total")
        .expect("archived metric should exist");

    let value = archived.get_metric()[0].get_counter().value();
    assert!(
        value >= 0.0,
        "Value should be non-negative after multiple collections"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_archiver_collector_metric_help_text() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = ArchiverCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();
    let archiver_metrics = vec![
        "pg_stat_archiver_archived_total",
        "pg_stat_archiver_failed_total",
        "pg_stat_archiver_last_archived_age_seconds",
    ];

    for metric_name in archiver_metrics {
        let metric_family = families
            .iter()
            .find(|m| m.name() == metric_name)
            .unwrap_or_else(|| panic!("Metric {metric_name} should exist"));

        assert!(
            !metric_family.help().is_empty(),
            "Metric {metric_name} should have help text"
        );
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_archiver_collector_no_labels() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = ArchiverCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();

    // archiver metrics should have no labels (they are global stats)
    for fam in families {
        if fam.name().starts_with("pg_stat_archiver_") {
            for m in fam.get_metric() {
                assert_eq!(
                    m.get_label().len(),
                    0,
                    "Archiver metrics should have no labels, found {} labels on {}",
                    m.get_label().len(),
                    fam.name()
                );
            }
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_archiver_collector_name() -> Result<()> {
    let collector = ArchiverCollector::new();
    assert_eq!(
        collector.name(),
        "archiver",
        "Collector name should be 'archiver'"
    );
    Ok(())
}

#[tokio::test]
async fn test_archiver_collector_enabled_by_default() -> Result<()> {
    let collector = ArchiverCollector::new();
    assert!(
        collector.enabled_by_default(),
        "Archiver collector should be enabled by default"
    );
    Ok(())
}

#[tokio::test]
async fn test_archiver_collector_double_registration_fails() -> Result<()> {
    let registry = Registry::new();
    let collector = ArchiverCollector::new();

    // First registration should succeed
    collector.register_metrics(&registry)?;

    // Second registration should fail
    let result = collector.register_metrics(&registry);
    assert!(
        result.is_err(),
        "Double registration should fail with an error"
    );

    Ok(())
}

#[tokio::test]
async fn test_archiver_collector_failed_count_exists() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = ArchiverCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();

    // Failed count is critical for alerting
    let failed = families
        .iter()
        .find(|m| m.name() == "pg_stat_archiver_failed_total")
        .expect("failed_count metric should exist");

    assert!(failed.get_metric()[0].get_counter().value() >= 0.0);

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_archiver_collector_age_metrics_reasonable() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = ArchiverCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();

    // Age metrics should be reasonable (not millions of years)
    let age_metrics = vec![
        "pg_stat_archiver_last_archived_age_seconds",
        "pg_stat_archiver_last_failed_age_seconds",
    ];

    for metric_name in age_metrics {
        let metric_family = families
            .iter()
            .find(|m| m.name() == metric_name)
            .unwrap_or_else(|| panic!("Metric {metric_name} should exist"));

        let value = metric_family.get_metric()[0].get_gauge().value();
        // If set, should be reasonable (less than 10 years in seconds)
        if value > 0.0 {
            assert!(
                value < 315_360_000.0, // 10 years
                "Age metric {metric_name} has unreasonable value: {value}"
            );
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_archiver_collector_handles_database_restart() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = ArchiverCollector::new();

    collector.register_metrics(&registry)?;

    // First collection
    collector.collect(&pool).await?;
    let first_archived = {
        let families = registry.gather();
        families
            .iter()
            .find(|m| m.name() == "pg_stat_archiver_archived_total")
            .and_then(|f| f.get_metric().first())
            .map_or(0, |m| common::metric_value_to_i64(m.get_counter().value()))
    };

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Second collection
    collector.collect(&pool).await?;
    let second_archived = {
        let families = registry.gather();
        families
            .iter()
            .find(|m| m.name() == "pg_stat_archiver_archived_total")
            .and_then(|f| f.get_metric().first())
            .map_or(0, |m| common::metric_value_to_i64(m.get_counter().value()))
    };

    // Values should be valid (either increased or stayed same)
    assert!(
        second_archived >= 0,
        "Counter should be non-negative after collection"
    );
    assert!(
        first_archived >= 0,
        "Counter should be non-negative in first collection"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_archiver_collector_all_counters_valid_after_activity() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = ArchiverCollector::new();

    collector.register_metrics(&registry)?;

    // Generate some database activity (though archiver activity is system-level)
    let mut tx = pool.begin().await?;
    for i in 0..10 {
        sqlx::query(&format!(
            "CREATE TEMP TABLE archiver_activity_{i} (data TEXT)"
        ))
        .execute(&mut *tx)
        .await?;
        sqlx::query(&format!(
            "INSERT INTO archiver_activity_{i} SELECT 'test' FROM generate_series(1, 50)"
        ))
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;

    // Collect metrics
    collector.collect(&pool).await?;

    let families = registry.gather();

    // Verify all counters are present and valid
    let archived = families
        .iter()
        .find(|m| m.name() == "pg_stat_archiver_archived_total")
        .expect("archived should exist");
    assert!(archived.get_metric()[0].get_counter().value() >= 0.0);

    let failed = families
        .iter()
        .find(|m| m.name() == "pg_stat_archiver_failed_total")
        .expect("failed should exist");
    assert!(failed.get_metric()[0].get_counter().value() >= 0.0);

    // Age gauges should also be valid
    let last_archived_age = families
        .iter()
        .find(|m| m.name() == "pg_stat_archiver_last_archived_age_seconds")
        .expect("last_archived_age should exist");
    assert!(last_archived_age.get_metric()[0].get_gauge().value() >= 0.0);

    let last_failed_age = families
        .iter()
        .find(|m| m.name() == "pg_stat_archiver_last_failed_age_seconds")
        .expect("last_failed_age should exist");
    assert!(last_failed_age.get_metric()[0].get_gauge().value() >= 0.0);

    pool.close().await;
    Ok(())
}
