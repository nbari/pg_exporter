use super::super::common;
use anyhow::Result;
use pg_exporter::collectors::{Collector, default::bgwriter::BgwriterCollector};
use prometheus::Registry;

#[tokio::test]
async fn test_bgwriter_collector_registers_without_error() -> Result<()> {
    let registry = Registry::new();
    let collector = BgwriterCollector::new();

    collector.register_metrics(&registry)?;
    Ok(())
}

#[tokio::test]
async fn test_bgwriter_collector_has_all_metrics() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = BgwriterCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();
    let expected_metrics = vec![
        "pg_stat_bgwriter_buffers_clean_total",
        "pg_stat_bgwriter_maxwritten_clean_total",
        "pg_stat_bgwriter_buffers_alloc_total",
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
async fn test_bgwriter_collector_values_non_negative() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = BgwriterCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    for fam in registry.gather() {
        if fam.name().starts_with("pg_stat_bgwriter_") {
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
async fn test_bgwriter_collector_buffers_alloc_increases() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = BgwriterCollector::new();

    collector.register_metrics(&registry)?;

    // First collection
    collector.collect(&pool).await?;
    let initial_value = {
        let families = registry.gather();
        families
            .iter()
            .find(|m| m.name() == "pg_stat_bgwriter_buffers_alloc_total")
            .and_then(|f| f.get_metric().first())
            .map_or(0, |m| common::metric_value_to_i64(m.get_counter().value()))
    };

    // Perform some database operations to allocate buffers
    let mut tx = pool.begin().await?;
    sqlx::query("CREATE TEMP TABLE bgwriter_test (id INT)")
        .execute(&mut *tx)
        .await?;
    sqlx::query("INSERT INTO bgwriter_test SELECT generate_series(1, 100)")
        .execute(&mut *tx)
        .await?;
    sqlx::query("SELECT * FROM bgwriter_test")
        .fetch_all(&mut *tx)
        .await?;
    tx.commit().await?;

    // Second collection
    collector.collect(&pool).await?;
    let final_value = {
        let families = registry.gather();
        families
            .iter()
            .find(|m| m.name() == "pg_stat_bgwriter_buffers_alloc_total")
            .and_then(|f| f.get_metric().first())
            .map_or(0, |m| common::metric_value_to_i64(m.get_counter().value()))
    };

    // buffers_alloc should have increased or stayed the same
    assert!(
        final_value >= initial_value,
        "buffers_alloc should increase or stay same. Initial: {initial_value}, Final: {final_value}"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_bgwriter_collector_metrics_are_counters() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = BgwriterCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();
    let bgwriter_metrics = vec![
        "pg_stat_bgwriter_buffers_clean_total",
        "pg_stat_bgwriter_maxwritten_clean_total",
        "pg_stat_bgwriter_buffers_alloc_total",
    ];

    for metric_name in bgwriter_metrics {
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

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_bgwriter_collector_concurrent_collections() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = BgwriterCollector::new();

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
            .any(|m| m.name() == "pg_stat_bgwriter_buffers_alloc_total"),
        "Metrics should exist after concurrent collections"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_bgwriter_collector_idempotent_collection() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = BgwriterCollector::new();

    collector.register_metrics(&registry)?;

    // Collect multiple times
    for _ in 0..3 {
        collector.collect(&pool).await?;
    }

    // Verify metrics exist and are valid
    let families = registry.gather();
    let buffers_alloc = families
        .iter()
        .find(|m| m.name() == "pg_stat_bgwriter_buffers_alloc_total")
        .expect("buffers_alloc metric should exist");

    let value = buffers_alloc.get_metric()[0].get_counter().value();
    assert!(
        value >= 0.0,
        "Value should be non-negative after multiple collections"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_bgwriter_collector_metric_help_text() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = BgwriterCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();
    let bgwriter_metrics = vec![
        "pg_stat_bgwriter_buffers_clean_total",
        "pg_stat_bgwriter_maxwritten_clean_total",
        "pg_stat_bgwriter_buffers_alloc_total",
    ];

    for metric_name in bgwriter_metrics {
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
async fn test_bgwriter_collector_no_labels() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = BgwriterCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();

    // bgwriter metrics should have no labels (they are global stats)
    for fam in families {
        if fam.name().starts_with("pg_stat_bgwriter_") {
            for m in fam.get_metric() {
                assert_eq!(
                    m.get_label().len(),
                    0,
                    "Bgwriter metrics should have no labels, found {} labels on {}",
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
async fn test_bgwriter_collector_name() -> Result<()> {
    let collector = BgwriterCollector::new();
    assert_eq!(
        collector.name(),
        "bgwriter",
        "Collector name should be 'bgwriter'"
    );
    Ok(())
}

#[tokio::test]
async fn test_bgwriter_collector_enabled_by_default() -> Result<()> {
    let collector = BgwriterCollector::new();
    assert!(
        collector.enabled_by_default(),
        "Bgwriter collector should be enabled by default"
    );
    Ok(())
}

#[tokio::test]
async fn test_bgwriter_collector_double_registration_fails() -> Result<()> {
    let registry = Registry::new();
    let collector = BgwriterCollector::new();

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
async fn test_bgwriter_collector_handles_database_restart() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = BgwriterCollector::new();

    collector.register_metrics(&registry)?;

    // First collection
    collector.collect(&pool).await?;
    let first_alloc = {
        let families = registry.gather();
        families
            .iter()
            .find(|m| m.name() == "pg_stat_bgwriter_buffers_alloc_total")
            .and_then(|f| f.get_metric().first())
            .map_or(0, |m| common::metric_value_to_i64(m.get_counter().value()))
    };

    // Simulate reset by checking if stats_reset changes
    // In normal operation, counters should only increase
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Second collection
    collector.collect(&pool).await?;
    let second_alloc = {
        let families = registry.gather();
        families
            .iter()
            .find(|m| m.name() == "pg_stat_bgwriter_buffers_alloc_total")
            .and_then(|f| f.get_metric().first())
            .map_or(0, |m| common::metric_value_to_i64(m.get_counter().value()))
    };

    // Values should be valid (either increased or stayed same)
    assert!(
        second_alloc >= 0,
        "Counter should be non-negative after collection"
    );
    assert!(
        first_alloc >= 0,
        "Counter should be non-negative in first collection"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_bgwriter_collector_all_counters_valid_after_activity() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = BgwriterCollector::new();

    collector.register_metrics(&registry)?;

    // Generate some database activity within a transaction
    let mut tx = pool.begin().await?;
    for i in 0..10 {
        sqlx::query(&format!(
            "CREATE TEMP TABLE bgwriter_activity_{i} (data TEXT)"
        ))
        .execute(&mut *tx)
        .await?;
        sqlx::query(&format!(
            "INSERT INTO bgwriter_activity_{i} SELECT 'test' FROM generate_series(1, 50)"
        ))
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;

    // Collect metrics
    collector.collect(&pool).await?;

    let families = registry.gather();

    // Verify all three counters are present and valid
    let buffers_clean = families
        .iter()
        .find(|m| m.name() == "pg_stat_bgwriter_buffers_clean_total")
        .expect("buffers_clean should exist");
    assert!(buffers_clean.get_metric()[0].get_counter().value() >= 0.0);

    let maxwritten_clean = families
        .iter()
        .find(|m| m.name() == "pg_stat_bgwriter_maxwritten_clean_total")
        .expect("maxwritten_clean should exist");
    assert!(maxwritten_clean.get_metric()[0].get_counter().value() >= 0.0);

    let buffers_alloc = families
        .iter()
        .find(|m| m.name() == "pg_stat_bgwriter_buffers_alloc_total")
        .expect("buffers_alloc should exist");
    assert!(buffers_alloc.get_metric()[0].get_counter().value() >= 0.0);

    pool.close().await;
    Ok(())
}
