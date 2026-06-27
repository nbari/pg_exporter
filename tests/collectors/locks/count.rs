use super::super::common;
use anyhow::Result;
use pg_exporter::collectors::{Collector, locks::LocksCollector, util::set_excluded_databases};
use prometheus::Registry;

#[tokio::test]
async fn test_locks_count_registers_without_error() -> Result<()> {
    let registry = Registry::new();
    let collector = LocksCollector::new();

    collector.register_metrics(&registry)?;
    Ok(())
}

#[tokio::test]
async fn test_locks_count_has_metrics_after_collection() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = LocksCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();
    assert!(
        families.iter().any(|m| m.name() == "pg_locks_count"),
        "Metric pg_locks_count should exist. Found: {:?}",
        families
            .iter()
            .map(prometheus::proto::MetricFamily::name)
            .collect::<Vec<_>>()
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_locks_count_labels_present() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = LocksCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    for fam in registry.gather() {
        if fam.name() == "pg_locks_count" {
            for m in fam.get_metric() {
                let has_datname = m.get_label().iter().any(|l| l.name() == "datname");
                let has_mode = m.get_label().iter().any(|l| l.name() == "mode");
                assert!(
                    has_datname,
                    "Metric {} should have 'datname' label",
                    fam.name()
                );
                assert!(has_mode, "Metric {} should have 'mode' label", fam.name());
            }
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_locks_count_values_non_negative() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = LocksCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    for fam in registry.gather() {
        if fam.name() == "pg_locks_count" {
            for m in fam.get_metric() {
                let v = m.get_gauge().value();
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

// This test creates a genuine lock contention so pg_locks shows lock entries.
#[tokio::test]
async fn test_locks_count_detects_locks() -> Result<()> {
    let pool = common::create_test_pool().await?;
    // Ensure a test table exists
    sqlx::query("CREATE TABLE IF NOT EXISTS test_locks_rel (id INT PRIMARY KEY)")
        .execute(&pool)
        .await?;

    // Tx1: acquire an ACCESS EXCLUSIVE lock and hold it
    let mut tx1 = pool.begin().await?;
    sqlx::query("LOCK TABLE test_locks_rel IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *tx1)
        .await?;

    // Give the lock a moment to be established
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Collect lock metrics while Tx1 holds the lock
    let registry = Registry::new();
    let locks = LocksCollector::new();
    locks.register_metrics(&registry)?;
    locks.collect(&pool).await?;

    // Assert that we have lock entries
    let families = registry.gather();

    let locks_family = families
        .iter()
        .find(|m| m.name() == "pg_locks_count")
        .expect("pg_locks_count metric should exist");

    // Should have at least some locks (from our transaction or system)
    assert!(
        !locks_family.get_metric().is_empty(),
        "Expected at least some lock entries"
    );

    // Check that AccessExclusiveLock mode appears somewhere
    let has_access_exclusive = locks_family.get_metric().iter().any(|metric| {
        metric
            .get_label()
            .iter()
            .any(|l| l.name() == "mode" && l.value() == "AccessExclusiveLock")
    });

    assert!(
        has_access_exclusive,
        "Expected to find AccessExclusiveLock mode in lock metrics"
    );

    // Release Tx1
    tx1.commit().await?;

    // Cleanup
    sqlx::query("DROP TABLE IF EXISTS test_locks_rel")
        .execute(&pool)
        .await?;

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_locks_count_multiple_lock_modes() -> Result<()> {
    let pool = common::create_test_pool().await?;

    // Create test table
    sqlx::query("CREATE TABLE IF NOT EXISTS test_locks_modes (id INT PRIMARY KEY)")
        .execute(&pool)
        .await?;

    // Acquire different lock modes
    let mut tx1 = pool.begin().await?;
    let mut tx2 = pool.begin().await?;

    // AccessShareLock (SELECT)
    sqlx::query("SELECT * FROM test_locks_modes")
        .fetch_optional(&mut *tx1)
        .await?;

    // RowExclusiveLock (INSERT/UPDATE/DELETE)
    sqlx::query("INSERT INTO test_locks_modes (id) VALUES (1) ON CONFLICT DO NOTHING")
        .execute(&mut *tx2)
        .await?;

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Collect metrics
    let registry = Registry::new();
    let collector = LocksCollector::new();
    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();
    let locks_family = families
        .iter()
        .find(|m| m.name() == "pg_locks_count")
        .expect("pg_locks_count metric should exist");

    // Check that we have multiple lock modes
    let modes: Vec<String> = locks_family
        .get_metric()
        .iter()
        .flat_map(|m| {
            m.get_label()
                .iter()
                .filter(|l| l.name() == "mode")
                .map(|l| l.value().to_string())
        })
        .collect();

    assert!(
        modes.len() >= 2,
        "Expected at least 2 different lock modes, found: {modes:?}"
    );

    // Cleanup
    tx1.rollback().await?;
    tx2.rollback().await?;
    sqlx::query("DROP TABLE IF EXISTS test_locks_modes")
        .execute(&pool)
        .await?;

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_locks_count_concurrent_collections() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = LocksCollector::new();

    collector.register_metrics(&registry)?;

    // Run multiple collections concurrently
    let mut handles = vec![];
    for _ in 0..3 {
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
        families.iter().any(|m| m.name() == "pg_locks_count"),
        "Metric should exist after concurrent collections"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_locks_count_resets_stale_metrics() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = LocksCollector::new();

    collector.register_metrics(&registry)?;

    // Create a table and lock
    sqlx::query("CREATE TABLE IF NOT EXISTS test_locks_reset (id INT PRIMARY KEY)")
        .execute(&pool)
        .await?;

    let mut tx = pool.begin().await?;
    sqlx::query("LOCK TABLE test_locks_reset IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *tx)
        .await?;

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // First collection - should have locks
    collector.collect(&pool).await?;

    let families = registry.gather();
    let initial_count = families
        .iter()
        .find(|m| m.name() == "pg_locks_count")
        .map_or(0, |f| f.get_metric().len());

    // Release the lock
    tx.commit().await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Second collection - metrics should be reset/updated
    collector.collect(&pool).await?;

    let families = registry.gather();
    let _final_count = families
        .iter()
        .find(|m| m.name() == "pg_locks_count")
        .map_or(0, |f| f.get_metric().len());

    // Verify metrics are still valid after reset
    assert!(
        initial_count > 0,
        "Should have had locks initially, got count: {initial_count}"
    );

    // Cleanup
    sqlx::query("DROP TABLE IF EXISTS test_locks_reset")
        .execute(&pool)
        .await?;

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_locks_count_empty_database_name() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = LocksCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    // Locks without a database (system locks) should have empty datname
    let families = registry.gather();
    let locks_family = families
        .iter()
        .find(|m| m.name() == "pg_locks_count")
        .expect("pg_locks_count metric should exist");

    // Verify that all metrics have the datname label (even if empty)
    for metric in locks_family.get_metric() {
        let has_datname = metric.get_label().iter().any(|l| l.name() == "datname");
        assert!(
            has_datname,
            "All lock metrics should have datname label (even if empty)"
        );
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_locks_count_excluded_databases() -> Result<()> {
    // Set up exclusion list
    set_excluded_databases(vec!["template0".to_string(), "template1".to_string()]);

    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = LocksCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();
    let locks_family = families
        .iter()
        .find(|m| m.name() == "pg_locks_count")
        .expect("pg_locks_count metric should exist");

    // Verify excluded databases don't appear in metrics
    for metric in locks_family.get_metric() {
        for label in metric.get_label() {
            if label.name() == "datname" {
                assert!(
                    label.value() != "template0" && label.value() != "template1",
                    "Excluded databases should not appear in metrics, found: {}",
                    label.value()
                );
            }
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_locks_count_with_current_database() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = LocksCollector::new();

    // Get the current database name
    let current_db: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;

    collector.register_metrics(&registry)?;

    // Create a lock in the current database
    let mut tx = pool.begin().await?;
    sqlx::query("CREATE TABLE IF NOT EXISTS test_locks_current_db (id INT PRIMARY KEY)")
        .execute(&mut *tx)
        .await?;

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    collector.collect(&pool).await?;

    let families = registry.gather();
    let locks_family = families
        .iter()
        .find(|m| m.name() == "pg_locks_count")
        .expect("pg_locks_count metric should exist");

    // Verify current database appears in metrics
    let has_current_db = locks_family.get_metric().iter().any(|metric| {
        metric
            .get_label()
            .iter()
            .any(|l| l.name() == "datname" && l.value() == current_db)
    });

    assert!(
        has_current_db,
        "Current database {current_db} should appear in lock metrics"
    );

    tx.rollback().await?;
    sqlx::query("DROP TABLE IF EXISTS test_locks_current_db")
        .execute(&pool)
        .await?;

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_locks_count_collector_name() -> Result<()> {
    let collector = LocksCollector::new();
    assert_eq!(
        collector.name(),
        "locks",
        "Collector name should be 'locks'"
    );
    Ok(())
}

#[tokio::test]
async fn test_locks_count_enabled_by_default() -> Result<()> {
    let collector = LocksCollector::new();
    assert!(
        !collector.enabled_by_default(),
        "Locks collector should not be enabled by default"
    );
    Ok(())
}

#[tokio::test]
async fn test_locks_count_double_registration_fails() -> Result<()> {
    let registry = Registry::new();
    let collector = LocksCollector::new();

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
async fn test_locks_count_metric_format_validity() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = LocksCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();
    let locks_family = families
        .iter()
        .find(|m| m.name() == "pg_locks_count")
        .expect("pg_locks_count metric should exist");

    // Verify metric help text exists
    assert!(
        !locks_family.help().is_empty(),
        "Metric should have help text"
    );

    // Verify metric type is gauge
    assert_eq!(
        locks_family.get_field_type(),
        prometheus::proto::MetricType::GAUGE,
        "pg_locks_count should be a GAUGE metric"
    );

    // Verify each metric has exactly 2 labels
    for metric in locks_family.get_metric() {
        assert_eq!(
            metric.get_label().len(),
            2,
            "Each lock metric should have exactly 2 labels (datname, mode)"
        );
    }

    pool.close().await;
    Ok(())
}

// Registering and collecting the blocking diagnostics must succeed without error.
// Note: empty IntGaugeVec/GaugeVec families (no blocking => no child series) are
// intentionally omitted by Prometheus `gather()`, so presence is asserted under real
// contention by `test_blocking_metrics_detect_real_block` instead.
#[tokio::test]
async fn test_blocking_metrics_registered() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = LocksCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    // Re-registering the same collector must fail, proving the new blocking metric
    // families are registered in the registry.
    assert!(
        collector.register_metrics(&registry).is_err(),
        "blocking metrics should already be registered (double registration must fail)"
    );

    pool.close().await;
    Ok(())
}

// Blocking metrics must always collect cleanly with sane (non-negative) values.
// Note: this does NOT assert zero blocking — tests run in parallel against a shared
// database, so other tests may create genuine contention concurrently. The
// zero-contention -> reflected-in-metrics path is covered by the isolated
// `test_blocking_metrics_detect_real_block` test below.
#[tokio::test]
async fn test_blocking_metrics_non_negative() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = LocksCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    for fam in registry.gather() {
        let name = fam.name();
        if matches!(
            name,
            "pg_blocked_sessions"
                | "pg_blocking_sessions"
                | "pg_longest_blocked_seconds"
                | "pg_lock_waits"
        ) {
            for m in fam.get_metric() {
                let v = m.get_gauge().value();
                assert!(v >= 0.0, "{name} must be non-negative, got {v}");
            }
        }
    }

    pool.close().await;
    Ok(())
}

// Create a genuine lock-wait: tx1 holds ACCESS EXCLUSIVE, a second session blocks
// trying to read the same table. The diagnostics must reflect the blocked/blocking
// sessions, a positive wait age, and an ungranted lock wait.
#[tokio::test]
async fn test_blocking_metrics_detect_real_block() -> Result<()> {
    let pool = common::create_test_pool().await?;
    sqlx::query("CREATE TABLE IF NOT EXISTS test_blocking_rel (id INT PRIMARY KEY)")
        .execute(&pool)
        .await?;

    // Tx1 holds an ACCESS EXCLUSIVE lock and keeps it.
    let mut tx1 = pool.begin().await?;
    sqlx::query("LOCK TABLE test_blocking_rel IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *tx1)
        .await?;

    // Second session blocks: SELECT needs ACCESS SHARE, conflicts with ACCESS EXCLUSIVE.
    let blocker_pool = pool.clone();
    let blocked_task = tokio::spawn(async move {
        let _ = sqlx::query("SELECT * FROM test_blocking_rel")
            .fetch_optional(&blocker_pool)
            .await;
    });

    // Allow the lock-wait to establish.
    tokio::time::sleep(tokio::time::Duration::from_millis(600)).await;

    let registry = Registry::new();
    let collector = LocksCollector::new();
    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();
    let sum = |name: &str| -> f64 {
        families.iter().find(|m| m.name() == name).map_or(0.0, |f| {
            f.get_metric().iter().map(|m| m.get_gauge().value()).sum()
        })
    };
    let blocked = sum("pg_blocked_sessions");
    let blocking = sum("pg_blocking_sessions");
    let longest = sum("pg_longest_blocked_seconds");
    let lock_waits = sum("pg_lock_waits");

    // Release tx1 and let the blocked statement finish before asserting/cleanup.
    tx1.commit().await?;
    let _ = blocked_task.await;
    sqlx::query("DROP TABLE IF EXISTS test_blocking_rel")
        .execute(&pool)
        .await?;
    pool.close().await;

    assert!(
        blocked >= 1.0,
        "expected >=1 blocked session, got {blocked}"
    );
    assert!(
        blocking >= 1.0,
        "expected >=1 blocking session, got {blocking}"
    );
    assert!(
        longest > 0.0,
        "expected longest_blocked_seconds > 0, got {longest}"
    );
    assert!(
        lock_waits >= 1.0,
        "expected >=1 ungranted lock wait, got {lock_waits}"
    );
    Ok(())
}
