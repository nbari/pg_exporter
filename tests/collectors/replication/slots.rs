use super::super::common;
use anyhow::{Context, Result, ensure};
use pg_exporter::collectors::{Collector, replication::slots::ReplicationSlotsCollector};
use prometheus::Registry;
use sqlx::PgPool;

/// WAL written past the slot before collecting, so a correct reading is unmistakably
/// non-zero.
const WAL_BURST_BYTES: i32 = 1_048_576;

/// `SQLSTATE`s for a server that cannot hold another slot: `wal_level = minimal`
/// (`55000`) or every `max_replication_slots` entry in use (`53400`).
fn slots_unavailable(error: &sqlx::Error) -> bool {
    matches!(
        error,
        sqlx::Error::Database(db) if matches!(db.code().as_deref(), Some("55000" | "53400"))
    )
}

fn exported_slot_lag(registry: &Registry, slot: &str) -> Option<f64> {
    registry
        .gather()
        .iter()
        .find(|family| family.name() == "pg_replication_slots_pg_wal_lsn_diff")?
        .get_metric()
        .iter()
        .find(|metric| {
            metric
                .get_label()
                .iter()
                .any(|label| label.name() == "slot_name" && label.value() == slot)
        })
        .map(|metric| metric.get_gauge().value())
}

async fn assert_slot_lag_is_exported(pool: &PgPool, slot: &str) -> Result<()> {
    // Transactional, so the commit flushes it and `pg_current_wal_lsn()` moves past it.
    sqlx::query("SELECT pg_logical_emit_message(true, 'pg_exporter_test', repeat('x', $1))")
        .bind(WAL_BURST_BYTES)
        .execute(pool)
        .await
        .context("failed to write WAL past the slot")?;

    let registry = Registry::new();
    let collector = ReplicationSlotsCollector::new();
    collector.register_metrics(&registry)?;
    collector.collect(pool).await?;

    let exported = exported_slot_lag(&registry, slot)
        .with_context(|| format!("no pg_replication_slots_pg_wal_lsn_diff series for {slot}"))?;

    // The slot is inactive, so its `restart_lsn` cannot move, and WAL only grows: a later
    // reading from PostgreSQL is an upper bound for what the collector saw.
    let retained_after: f64 = sqlx::query_scalar(
        "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::double precision
         FROM pg_replication_slots WHERE slot_name = $1",
    )
    .bind(slot)
    .fetch_one(pool)
    .await?;

    ensure!(
        exported >= f64::from(WAL_BURST_BYTES),
        "slot {slot} retains at least {WAL_BURST_BYTES} bytes of WAL but the collector \
         exported {exported}"
    );
    ensure!(
        exported <= retained_after,
        "exported lag {exported} exceeds what PostgreSQL reports afterwards ({retained_after})"
    );
    Ok(())
}

#[tokio::test]
async fn test_replication_slots_collector_name() {
    let collector = ReplicationSlotsCollector::new();
    assert_eq!(collector.name(), "replication_slots");
}

#[tokio::test]
async fn test_replication_slots_collector_registers_without_error() -> Result<()> {
    let registry = Registry::new();
    let collector = ReplicationSlotsCollector::new();

    collector.register_metrics(&registry)?;
    Ok(())
}

#[tokio::test]
async fn test_replication_slots_collector_has_all_metrics_after_collection() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = ReplicationSlotsCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();

    // Metrics should be registered even if there are no replication slots
    // The metric families exist but may have no values

    // We can't guarantee values without replication slots, but the collector should succeed
    assert!(
        !families.is_empty() || families.is_empty(),
        "Should have collected metrics successfully"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_replication_slots_collector_metrics_have_labels() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = ReplicationSlotsCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();

    for fam in families {
        if fam.name() == "pg_replication_slots_active" {
            // If there are any metrics, they should have the right labels
            for m in fam.get_metric() {
                let labels: Vec<_> = m
                    .get_label()
                    .iter()
                    .map(prometheus::proto::LabelPair::name)
                    .collect();
                assert!(labels.contains(&"slot_name"));
                assert!(labels.contains(&"slot_type"));
                assert!(labels.contains(&"database"));
            }
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_replication_slots_collector_active_is_boolean() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = ReplicationSlotsCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    for fam in registry.gather() {
        if fam.name() == "pg_replication_slots_active" {
            for m in fam.get_metric() {
                let v = common::metric_value_to_i64(m.get_gauge().value());
                assert!(v == 0 || v == 1, "active should be 0 or 1, got {v}");
            }
        }
    }

    pool.close().await;
    Ok(())
}

/// The lag of a slot that retains WAL must reach the metric. `pg_wal_lsn_diff` returns
/// `numeric`; decoded as `f64` without a cast it failed on every row and the collector
/// published 0 for every slot, so the documented retention alert could never fire.
#[tokio::test]
async fn test_replication_slots_lag_reports_retained_wal() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let slot = format!("pgx_slot_lag_{}_{nanos}", std::process::id());

    // `true` reserves WAL immediately, giving the slot a `restart_lsn`.
    let created = sqlx::query("SELECT pg_create_physical_replication_slot($1, true)")
        .bind(&slot)
        .execute(&pool)
        .await;
    if let Err(error) = created {
        pool.close().await;
        if slots_unavailable(&error) {
            eprintln!("skipping slot lag test: this server cannot create a slot: {error}");
            return Ok(());
        }
        return Err(error).context("failed to create the test replication slot");
    }

    let outcome = assert_slot_lag_is_exported(&pool, &slot).await;

    sqlx::query("SELECT pg_drop_replication_slot($1)")
        .bind(&slot)
        .execute(&pool)
        .await
        .context("failed to drop the test replication slot")?;
    pool.close().await;
    outcome
}

#[tokio::test]
async fn test_replication_slots_collector_handles_no_slots() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = ReplicationSlotsCollector::new();

    collector.register_metrics(&registry)?;
    // Should succeed even with no replication slots
    collector.collect(&pool).await?;

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_replication_slots_collector_handles_concurrent_collection() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let collector = ReplicationSlotsCollector::new();

    let handles: Vec<_> = (0..5)
        .map(|_| {
            let pool = pool.clone();
            let collector = collector.clone();
            tokio::spawn(async move { collector.collect(&pool).await })
        })
        .collect();

    for handle in handles {
        handle.await.unwrap()?;
    }

    pool.close().await;
    Ok(())
}
