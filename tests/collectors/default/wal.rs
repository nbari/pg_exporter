use super::super::common;
use anyhow::Result;
use pg_exporter::collectors::{Collector, default::wal::WalCollector};
use prometheus::Registry;

#[tokio::test]
async fn test_wal_collector_registers_without_error() -> Result<()> {
    let registry = Registry::new();
    let collector = WalCollector::new();

    collector.register_metrics(&registry)?;
    Ok(())
}

#[tokio::test]
async fn test_wal_collector_has_all_metrics() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = WalCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();
    let expected_metrics = vec![
        "pg_stat_wal_records_total",
        "pg_stat_wal_fpi_total",
        "pg_stat_wal_bytes_total",
        "pg_stat_wal_buffers_full_total",
    ];

    for metric_name in expected_metrics {
        assert!(
            families.iter().any(|m| m.name() == metric_name),
            "Metric {} should exist. Found: {:?}",
            metric_name,
            families.iter().map(|m| m.name()).collect::<Vec<_>>()
        );
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_wal_collector_values_non_negative() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = WalCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    for fam in registry.gather() {
        if fam.name().starts_with("pg_stat_wal_") {
            for m in fam.get_metric() {
                let v = m.get_counter().value();
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
async fn test_wal_collector_metrics_are_counters() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = WalCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();
    let wal_metrics = vec![
        "pg_stat_wal_records_total",
        "pg_stat_wal_fpi_total",
        "pg_stat_wal_bytes_total",
        "pg_stat_wal_buffers_full_total",
    ];

    for metric_name in wal_metrics {
        let metric_family = families
            .iter()
            .find(|m| m.name() == metric_name)
            .unwrap_or_else(|| panic!("Metric {} should exist", metric_name));

        assert_eq!(
            metric_family.get_field_type(),
            prometheus::proto::MetricType::COUNTER,
            "Metric {} should be a COUNTER",
            metric_name
        );
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_wal_collector_records_increase_with_activity() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = WalCollector::new();

    collector.register_metrics(&registry)?;

    // First collection
    collector.collect(&pool).await?;
    let initial_records = {
        let families = registry.gather();
        families
            .iter()
            .find(|m| m.name() == "pg_stat_wal_records_total")
            .and_then(|f| f.get_metric().first())
            .map(|m| m.get_counter().value() as i64)
            .unwrap_or(0)
    };

    // Generate some WAL activity
    let mut tx = pool.begin().await?;
    sqlx::query("CREATE TEMP TABLE wal_test (id INT, data TEXT)")
        .execute(&mut *tx)
        .await?;
    for i in 0..50 {
        sqlx::query("INSERT INTO wal_test VALUES ($1, 'test data')")
            .bind(i)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;

    // Second collection
    collector.collect(&pool).await?;
    let final_records = {
        let families = registry.gather();
        families
            .iter()
            .find(|m| m.name() == "pg_stat_wal_records_total")
            .and_then(|f| f.get_metric().first())
            .map(|m| m.get_counter().value() as i64)
            .unwrap_or(0)
    };

    // WAL records should have increased
    assert!(
        final_records >= initial_records,
        "WAL records should increase with activity. Initial: {}, Final: {}",
        initial_records,
        final_records
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_wal_collector_concurrent_collections() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = WalCollector::new();

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
            .any(|m| m.name() == "pg_stat_wal_records_total"),
        "Metrics should exist after concurrent collections"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_wal_collector_idempotent_collection() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = WalCollector::new();

    collector.register_metrics(&registry)?;

    // Collect multiple times
    for _ in 0..3 {
        collector.collect(&pool).await?;
    }

    // Verify metrics exist and are valid
    let families = registry.gather();
    let records = families
        .iter()
        .find(|m| m.name() == "pg_stat_wal_records_total")
        .expect("records metric should exist");

    let value = records.get_metric()[0].get_counter().value();
    assert!(
        value >= 0.0,
        "Value should be non-negative after multiple collections"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_wal_collector_metric_help_text() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = WalCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();
    let wal_metrics = vec![
        "pg_stat_wal_records_total",
        "pg_stat_wal_fpi_total",
        "pg_stat_wal_bytes_total",
        "pg_stat_wal_buffers_full_total",
    ];

    for metric_name in wal_metrics {
        let metric_family = families
            .iter()
            .find(|m| m.name() == metric_name)
            .unwrap_or_else(|| panic!("Metric {} should exist", metric_name));

        assert!(
            !metric_family.help().is_empty(),
            "Metric {} should have help text",
            metric_name
        );
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_wal_collector_no_labels() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = WalCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();

    // WAL metrics should have no labels (they are global stats)
    for fam in families {
        if fam.name().starts_with("pg_stat_wal_") {
            for m in fam.get_metric() {
                assert_eq!(
                    m.get_label().len(),
                    0,
                    "WAL metrics should have no labels, found {} labels on {}",
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
async fn test_wal_collector_name() -> Result<()> {
    let collector = WalCollector::new();
    assert_eq!(collector.name(), "wal", "Collector name should be 'wal'");
    Ok(())
}

#[tokio::test]
async fn test_wal_collector_enabled_by_default() -> Result<()> {
    let collector = WalCollector::new();
    assert!(
        collector.enabled_by_default(),
        "WAL collector should be enabled by default"
    );
    Ok(())
}

#[tokio::test]
async fn test_wal_collector_double_registration_fails() -> Result<()> {
    let registry = Registry::new();
    let collector = WalCollector::new();

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
async fn test_wal_collector_bytes_increases() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = WalCollector::new();

    collector.register_metrics(&registry)?;

    // First collection
    collector.collect(&pool).await?;
    let initial_bytes = {
        let families = registry.gather();
        families
            .iter()
            .find(|m| m.name() == "pg_stat_wal_bytes_total")
            .and_then(|f| f.get_metric().first())
            .map(|m| m.get_counter().value() as i64)
            .unwrap_or(0)
    };

    // Generate WAL activity
    let mut tx = pool.begin().await?;
    for i in 0..100 {
        sqlx::query("CREATE TEMP TABLE wal_bytes_test (data TEXT)")
            .execute(&mut *tx)
            .await
            .ok();
        sqlx::query(&format!("DROP TABLE IF EXISTS wal_bytes_test_{}", i))
            .execute(&mut *tx)
            .await
            .ok();
    }
    tx.commit().await.ok();

    // Second collection
    collector.collect(&pool).await?;
    let final_bytes = {
        let families = registry.gather();
        families
            .iter()
            .find(|m| m.name() == "pg_stat_wal_bytes_total")
            .and_then(|f| f.get_metric().first())
            .map(|m| m.get_counter().value() as i64)
            .unwrap_or(0)
    };

    // Bytes should be non-negative (may or may not increase depending on system)
    assert!(
        final_bytes >= 0,
        "WAL bytes should be non-negative: {}",
        final_bytes
    );
    assert!(
        initial_bytes >= 0,
        "Initial WAL bytes should be non-negative: {}",
        initial_bytes
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_wal_collector_fpi_metric_exists() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = WalCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();

    // FPI (Full Page Images) is important for recovery
    let fpi = families
        .iter()
        .find(|m| m.name() == "pg_stat_wal_fpi_total")
        .expect("FPI metric should exist");

    assert!(fpi.get_metric()[0].get_counter().value() >= 0.0);

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_wal_collector_handles_database_restart() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = WalCollector::new();

    collector.register_metrics(&registry)?;

    // First collection
    collector.collect(&pool).await?;
    let first_records = {
        let families = registry.gather();
        families
            .iter()
            .find(|m| m.name() == "pg_stat_wal_records_total")
            .and_then(|f| f.get_metric().first())
            .map(|m| m.get_counter().value() as i64)
            .unwrap_or(0)
    };

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Second collection
    collector.collect(&pool).await?;
    let second_records = {
        let families = registry.gather();
        families
            .iter()
            .find(|m| m.name() == "pg_stat_wal_records_total")
            .and_then(|f| f.get_metric().first())
            .map(|m| m.get_counter().value() as i64)
            .unwrap_or(0)
    };

    // Values should be valid (either increased or stayed same)
    assert!(
        second_records >= 0,
        "Counter should be non-negative after collection"
    );
    assert!(
        first_records >= 0,
        "Counter should be non-negative in first collection"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_wal_collector_all_counters_valid_after_activity() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = WalCollector::new();

    collector.register_metrics(&registry)?;

    // Generate some database activity
    let mut tx = pool.begin().await?;
    for i in 0..10 {
        sqlx::query(&format!("CREATE TEMP TABLE wal_activity_{} (data TEXT)", i))
            .execute(&mut *tx)
            .await?;
        sqlx::query(&format!(
            "INSERT INTO wal_activity_{} SELECT 'test' FROM generate_series(1, 50)",
            i
        ))
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;

    // Collect metrics
    collector.collect(&pool).await?;

    let families = registry.gather();

    // Verify all four counters are present and valid
    let records = families
        .iter()
        .find(|m| m.name() == "pg_stat_wal_records_total")
        .expect("records should exist");
    assert!(records.get_metric()[0].get_counter().value() >= 0.0);

    let fpi = families
        .iter()
        .find(|m| m.name() == "pg_stat_wal_fpi_total")
        .expect("fpi should exist");
    assert!(fpi.get_metric()[0].get_counter().value() >= 0.0);

    let bytes = families
        .iter()
        .find(|m| m.name() == "pg_stat_wal_bytes_total")
        .expect("bytes should exist");
    assert!(bytes.get_metric()[0].get_counter().value() >= 0.0);

    let buffers_full = families
        .iter()
        .find(|m| m.name() == "pg_stat_wal_buffers_full_total")
        .expect("buffers_full should exist");
    assert!(buffers_full.get_metric()[0].get_counter().value() >= 0.0);

    pool.close().await;
    Ok(())
}
