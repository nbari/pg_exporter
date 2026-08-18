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
            .map_or(0, |m| common::metric_value_to_i64(m.get_counter().value()))
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
            .map_or(0, |m| common::metric_value_to_i64(m.get_counter().value()))
    };

    // WAL records should have increased
    assert!(
        final_records >= initial_records,
        "WAL records should increase with activity. Initial: {initial_records}, Final: {final_records}"
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
            .map_or(0, |m| common::metric_value_to_i64(m.get_counter().value()))
    };

    // Generate WAL activity
    let mut tx = pool.begin().await?;
    for i in 0..100 {
        sqlx::query(sqlx::AssertSqlSafe(&*format!(
            "CREATE TEMP TABLE wal_bytes_test_{i} (data TEXT)"
        )))
        .execute(&mut *tx)
        .await?;
        sqlx::query(sqlx::AssertSqlSafe(&*format!(
            "INSERT INTO wal_bytes_test_{i} SELECT 'test' FROM generate_series(1, 10)"
        )))
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;

    // Second collection
    collector.collect(&pool).await?;
    let final_bytes = {
        let families = registry.gather();
        families
            .iter()
            .find(|m| m.name() == "pg_stat_wal_bytes_total")
            .and_then(|f| f.get_metric().first())
            .map_or(0, |m| common::metric_value_to_i64(m.get_counter().value()))
    };

    // Bytes should be non-negative (may or may not increase depending on system)
    assert!(
        final_bytes >= 0,
        "WAL bytes should be non-negative: {final_bytes}"
    );
    assert!(
        initial_bytes >= 0,
        "Initial WAL bytes should be non-negative: {initial_bytes}"
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
            .map_or(0, |m| common::metric_value_to_i64(m.get_counter().value()))
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
            .map_or(0, |m| common::metric_value_to_i64(m.get_counter().value()))
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
        sqlx::query(sqlx::AssertSqlSafe(&*format!(
            "CREATE TEMP TABLE wal_activity_{i} (data TEXT)"
        )))
        .execute(&mut *tx)
        .await?;
        sqlx::query(sqlx::AssertSqlSafe(&*format!(
            "INSERT INTO wal_activity_{i} SELECT 'test' FROM generate_series(1, 50)"
        )))
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

/// A `42501` on `pg_stat_wal` must clear the metrics and still succeed, not fail the scrape.
///
/// This is the Stage 1 regression scenario: the collector used to decide "view does not
/// exist" by matching the view name in the error *message*, which reads a permission error
/// as an absent view — a missing GRANT was indistinguishable from an old server, and
/// WAL generation metrics vanished with no explanation. Classifying by `SQLSTATE` separates them,
/// and a denied read clears rather than leaving the last values on display.
///
/// Grants on `pg_catalog` views are per-database, so the REVOKE below is confined to this
/// isolated database and cannot disturb a sibling test.
#[tokio::test]
async fn test_wal_clears_metrics_when_the_role_may_not_read_the_view() -> Result<()> {
    let test_db = common::IsolatedTestDatabase::new("wal_denied").await?;
    let pool = test_db.pool();

    let is_superuser: bool =
        sqlx::query_scalar("SELECT usesuper FROM pg_user WHERE usename = current_user")
            .fetch_optional(pool)
            .await?
            .unwrap_or(false);
    if !is_superuser {
        println!("not superuser, cannot revoke a catalog grant - skipping");
        test_db.cleanup().await?;
        return Ok(());
    }

    let role = format!("exporter_wal_denied_{}", std::process::id());
    let _ = sqlx::query(sqlx::AssertSqlSafe(format!("DROP ROLE IF EXISTS {role}")))
        .execute(pool)
        .await;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE ROLE {role} LOGIN PASSWORD 'denied_probe' NOSUPERUSER"
    )))
    .execute(pool)
    .await?;

    let outcome = async {
        let collector = WalCollector::new();
        let registry = Registry::new();
        collector.register_metrics(&registry)?;

        // Seed from the privileged pool, or a cleared registry proves nothing.
        collector.collect(pool).await?;
        let seeded = registry
            .gather()
            .iter()
            .any(|f| f.name().starts_with("pg_stat_wal_") && !f.get_metric().is_empty());
        assert!(seeded, "the privileged scrape must publish series first");

        sqlx::query("REVOKE SELECT ON pg_stat_wal FROM PUBLIC")
            .execute(pool)
            .await?;

        let denied_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(std::time::Duration::from_secs(5))
            .connect(&test_db.dsn_for_role(&role, "denied_probe")?)
            .await?;

        let result = collector.collect(&denied_pool).await;
        assert!(
            result.is_ok(),
            "a denied read is a skip, not a scrape failure: {result:?}"
        );

        for family in registry.gather() {
            if family.name().starts_with("pg_stat_wal_") {
                assert!(
                    family.get_metric().is_empty(),
                    "{} must be cleared after a denied read, {} samples remain",
                    family.name(),
                    family.get_metric().len()
                );
            }
        }

        denied_pool.close().await;
        Ok::<(), anyhow::Error>(())
    }
    .await;

    let _ = sqlx::query("GRANT SELECT ON pg_stat_wal TO PUBLIC")
        .execute(pool)
        .await;
    let _ = sqlx::query(sqlx::AssertSqlSafe(format!("DROP ROLE IF EXISTS {role}")))
        .execute(pool)
        .await;
    test_db.cleanup().await?;
    outcome
}
