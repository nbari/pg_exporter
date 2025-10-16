use super::super::common;
use anyhow::Result;
use pg_exporter::collectors::{Collector, locks::LocksCollector};
use prometheus::Registry;

#[tokio::test]
async fn test_locks_relations_registers_without_error() -> Result<()> {
    let registry = Registry::new();
    let collector = LocksCollector::new();

    collector.register_metrics(&registry)?;
    Ok(())
}

#[tokio::test]
async fn test_locks_relations_has_metrics_after_collection() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = LocksCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();
    let expected = vec!["pg_locks_waiting", "pg_locks_granted"];

    for metric in expected {
        assert!(
            families.iter().any(|m| m.name() == metric),
            "Metric {} should exist. Found: {:?}",
            metric,
            families.iter().map(|m| m.name()).collect::<Vec<_>>()
        );
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_locks_relations_labels_present() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = LocksCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    for fam in registry.gather() {
        if fam.name() == "pg_locks_waiting" || fam.name() == "pg_locks_granted" {
            for m in fam.get_metric() {
                let has_relation = m.get_label().iter().any(|l| l.name() == "relation");
                assert!(
                    has_relation,
                    "Metric {} should have 'relation' label",
                    fam.name()
                );
            }
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_locks_relations_values_non_negative() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = LocksCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    for fam in registry.gather() {
        if fam.name() == "pg_locks_waiting" || fam.name() == "pg_locks_granted" {
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

// This test creates a genuine lock contention so pg_locks shows a waiting entry.
#[tokio::test]
async fn test_locks_relations_detects_waiting_lock() -> Result<()> {
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

    // Tx2: attempt the same lock in a background task (will wait until tx1 commits)
    let pool2 = pool.clone();
    let waiter = tokio::spawn(async move {
        let mut tx2 = pool2.begin().await.unwrap();
        // This will block until tx1 commits
        let _ = sqlx::query("LOCK TABLE test_locks_rel IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *tx2)
            .await;
        // Commit to release any acquired lock when unblocked
        let _ = tx2.commit().await;
    });

    // Give Tx2 a moment to reach the waiting state
    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

    // Collect lock metrics while Tx2 is waiting
    let registry = Registry::new();
    let locks = LocksCollector::new();
    locks.register_metrics(&registry)?;
    locks.collect(&pool).await?;

    // Assert that waiting > 0 for relation "test_locks_rel"
    let families = registry.gather();

    let waiting_family = families
        .iter()
        .find(|m| m.name() == "pg_locks_waiting")
        .expect("pg_locks_waiting metric should exist");

    // Find our table's label
    let maybe_metric = waiting_family.get_metric().iter().find(|metric| {
        metric
            .get_label()
            .iter()
            .any(|l| l.name() == "relation" && l.value() == "test_locks_rel")
    });

    if let Some(m) = maybe_metric {
        let val = m.get_gauge().value() as i64;
        assert!(
            val >= 1,
            "Expected at least one waiting lock for relation=test_locks_rel, got {}",
            val
        );
    } else {
        panic!("Expected a waiting lock entry for relation=test_locks_rel");
    }

    // Release Tx1 to let the waiter finish
    tx1.commit().await?;

    // Ensure the waiter completes
    let _ = waiter.await;

    // Cleanup
    sqlx::query("DROP TABLE IF EXISTS test_locks_rel")
        .execute(&pool)
        .await?;

    pool.close().await;
    Ok(())
}
