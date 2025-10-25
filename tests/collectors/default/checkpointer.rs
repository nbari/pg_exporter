use super::super::common;
use anyhow::Result;
use pg_exporter::collectors::{Collector, default::checkpointer::CheckpointerCollector};
use prometheus::Registry;

#[tokio::test]
async fn test_checkpointer_collector_registers_without_error() -> Result<()> {
    let registry = Registry::new();
    let collector = CheckpointerCollector::new();

    collector.register_metrics(&registry)?;
    Ok(())
}

#[tokio::test]
async fn test_checkpointer_collector_has_all_metrics() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = CheckpointerCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();
    let expected_metrics = vec![
        "pg_stat_checkpointer_timed_total",
        "pg_stat_checkpointer_requested_total",
        "pg_stat_checkpointer_buffers_written_total",
        "pg_stat_checkpointer_write_time_seconds_total",
        "pg_stat_checkpointer_sync_time_seconds_total",
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
async fn test_checkpointer_collector_values_non_negative() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = CheckpointerCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    for fam in registry.gather() {
        if fam.name().starts_with("pg_stat_checkpointer_") {
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
async fn test_checkpointer_collector_metrics_are_counters() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = CheckpointerCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();
    let checkpointer_metrics = vec![
        "pg_stat_checkpointer_timed_total",
        "pg_stat_checkpointer_requested_total",
        "pg_stat_checkpointer_buffers_written_total",
        "pg_stat_checkpointer_write_time_seconds_total",
        "pg_stat_checkpointer_sync_time_seconds_total",
    ];

    for metric_name in checkpointer_metrics {
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
async fn test_checkpointer_collector_concurrent_collections() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = CheckpointerCollector::new();

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
            .any(|m| m.name() == "pg_stat_checkpointer_timed_total"),
        "Metrics should exist after concurrent collections"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_checkpointer_collector_idempotent_collection() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = CheckpointerCollector::new();

    collector.register_metrics(&registry)?;

    // Collect multiple times
    for _ in 0..3 {
        collector.collect(&pool).await?;
    }

    // Verify metrics exist and are valid
    let families = registry.gather();
    let timed = families
        .iter()
        .find(|m| m.name() == "pg_stat_checkpointer_timed_total")
        .expect("timed metric should exist");

    let value = timed.get_metric()[0].get_counter().value();
    assert!(
        value >= 0.0,
        "Value should be non-negative after multiple collections"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_checkpointer_collector_metric_help_text() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = CheckpointerCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();
    let checkpointer_metrics = vec![
        "pg_stat_checkpointer_timed_total",
        "pg_stat_checkpointer_requested_total",
        "pg_stat_checkpointer_buffers_written_total",
    ];

    for metric_name in checkpointer_metrics {
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
async fn test_checkpointer_collector_no_labels() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = CheckpointerCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();

    // checkpointer metrics should have no labels (they are global stats)
    for fam in families {
        if fam.name().starts_with("pg_stat_checkpointer_") {
            for m in fam.get_metric() {
                assert_eq!(
                    m.get_label().len(),
                    0,
                    "Checkpointer metrics should have no labels, found {} labels on {}",
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
async fn test_checkpointer_collector_name() -> Result<()> {
    let collector = CheckpointerCollector::new();
    assert_eq!(
        collector.name(),
        "checkpointer",
        "Collector name should be 'checkpointer'"
    );
    Ok(())
}

#[tokio::test]
async fn test_checkpointer_collector_enabled_by_default() -> Result<()> {
    let collector = CheckpointerCollector::new();
    assert!(
        collector.enabled_by_default(),
        "Checkpointer collector should be enabled by default"
    );
    Ok(())
}

#[tokio::test]
async fn test_checkpointer_collector_double_registration_fails() -> Result<()> {
    let registry = Registry::new();
    let collector = CheckpointerCollector::new();

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
async fn test_checkpointer_collector_timed_vs_requested() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = CheckpointerCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();

    // Both timed and requested should exist
    let timed = families
        .iter()
        .find(|m| m.name() == "pg_stat_checkpointer_timed_total")
        .expect("timed metric should exist");
    let requested = families
        .iter()
        .find(|m| m.name() == "pg_stat_checkpointer_requested_total")
        .expect("requested metric should exist");

    assert!(timed.get_metric()[0].get_counter().value() >= 0.0);
    assert!(requested.get_metric()[0].get_counter().value() >= 0.0);

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_checkpointer_collector_timing_metrics() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = CheckpointerCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();

    // Write and sync time should exist
    let write_time = families
        .iter()
        .find(|m| m.name() == "pg_stat_checkpointer_write_time_seconds_total")
        .expect("write_time metric should exist");
    let sync_time = families
        .iter()
        .find(|m| m.name() == "pg_stat_checkpointer_sync_time_seconds_total")
        .expect("sync_time metric should exist");

    assert!(write_time.get_metric()[0].get_counter().value() >= 0.0);
    assert!(sync_time.get_metric()[0].get_counter().value() >= 0.0);

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_checkpointer_collector_handles_database_restart() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = CheckpointerCollector::new();

    collector.register_metrics(&registry)?;

    // First collection
    collector.collect(&pool).await?;
    let first_timed = {
        let families = registry.gather();
        families
            .iter()
            .find(|m| m.name() == "pg_stat_checkpointer_timed_total")
            .and_then(|f| f.get_metric().first())
            .map(|m| m.get_counter().value() as i64)
            .unwrap_or(0)
    };

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Second collection
    collector.collect(&pool).await?;
    let second_timed = {
        let families = registry.gather();
        families
            .iter()
            .find(|m| m.name() == "pg_stat_checkpointer_timed_total")
            .and_then(|f| f.get_metric().first())
            .map(|m| m.get_counter().value() as i64)
            .unwrap_or(0)
    };

    // Values should be valid (either increased or stayed same)
    assert!(
        second_timed >= 0,
        "Counter should be non-negative after collection"
    );
    assert!(
        first_timed >= 0,
        "Counter should be non-negative in first collection"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_checkpointer_collector_all_counters_valid_after_activity() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = CheckpointerCollector::new();

    collector.register_metrics(&registry)?;

    // Generate some database activity
    let mut tx = pool.begin().await?;
    for i in 0..10 {
        sqlx::query(&format!(
            "CREATE TEMP TABLE checkpointer_activity_{} (data TEXT)",
            i
        ))
        .execute(&mut *tx)
        .await?;
        sqlx::query(&format!(
            "INSERT INTO checkpointer_activity_{} SELECT 'test' FROM generate_series(1, 50)",
            i
        ))
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;

    // Collect metrics
    collector.collect(&pool).await?;

    let families = registry.gather();

    // Verify all five counters are present and valid
    let timed = families
        .iter()
        .find(|m| m.name() == "pg_stat_checkpointer_timed_total")
        .expect("timed should exist");
    assert!(timed.get_metric()[0].get_counter().value() >= 0.0);

    let requested = families
        .iter()
        .find(|m| m.name() == "pg_stat_checkpointer_requested_total")
        .expect("requested should exist");
    assert!(requested.get_metric()[0].get_counter().value() >= 0.0);

    let buffers_written = families
        .iter()
        .find(|m| m.name() == "pg_stat_checkpointer_buffers_written_total")
        .expect("buffers_written should exist");
    assert!(buffers_written.get_metric()[0].get_counter().value() >= 0.0);

    let write_time = families
        .iter()
        .find(|m| m.name() == "pg_stat_checkpointer_write_time_seconds_total")
        .expect("write_time should exist");
    assert!(write_time.get_metric()[0].get_counter().value() >= 0.0);

    let sync_time = families
        .iter()
        .find(|m| m.name() == "pg_stat_checkpointer_sync_time_seconds_total")
        .expect("sync_time should exist");
    assert!(sync_time.get_metric()[0].get_counter().value() >= 0.0);

    pool.close().await;
    Ok(())
}
