use super::super::common;
use anyhow::{Context, Result, bail, ensure};
use pg_exporter::collectors::{Collector, activity::wait::WaitEventsCollector};
use prometheus::Registry;
use sqlx::PgPool;
use std::time::Duration;

#[tokio::test]
async fn test_wait_events_collector_registers_without_error() -> Result<()> {
    let collector = WaitEventsCollector::new();
    let registry = Registry::new();

    // Should not error when registering
    collector.register_metrics(&registry)?;

    Ok(())
}

#[tokio::test]
async fn test_wait_events_collector_has_all_metrics_after_collection() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = WaitEventsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Should register wait event metrics
    let expected_metrics = vec!["pg_wait_event_type", "pg_wait_event"];

    for metric_name in expected_metrics {
        assert!(
            metric_families.iter().any(|m| m.name() == metric_name),
            "Metric {} should be registered. Found: {:?}",
            metric_name,
            metric_families
                .iter()
                .map(prometheus::proto::MetricFamily::name)
                .collect::<Vec<_>>()
        );
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_wait_events_collector_handles_no_wait_events() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = WaitEventsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // If no wait events, should have 'none' label
    let wait_event_type = metric_families
        .iter()
        .find(|m| m.name() == "pg_wait_event_type")
        .unwrap();

    // Check if 'none' label exists (when no sessions are waiting)
    let has_none_or_events = wait_event_type.get_metric().iter().any(|m| {
        m.get_label()
            .iter()
            .any(|l| l.name() == "type" && (l.value() == "none" || !l.value().is_empty()))
    });

    assert!(
        has_none_or_events,
        "Should have either 'none' or actual wait event types"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_wait_events_collector_metrics_have_labels() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = WaitEventsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // pg_wait_event_type should have 'type' label
    let wait_event_type = metric_families
        .iter()
        .find(|m| m.name() == "pg_wait_event_type")
        .unwrap();

    for metric in wait_event_type.get_metric() {
        let has_type_label = metric.get_label().iter().any(|l| l.name() == "type");

        assert!(has_type_label, "Metric should have 'type' label");
    }

    // pg_wait_event should have 'event' label
    let wait_event = metric_families
        .iter()
        .find(|m| m.name() == "pg_wait_event")
        .unwrap();

    for metric in wait_event.get_metric() {
        let has_event_label = metric.get_label().iter().any(|l| l.name() == "event");

        assert!(has_event_label, "Metric should have 'event' label");
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_wait_events_collector_name() {
    let collector = WaitEventsCollector::new();
    assert_eq!(collector.name(), "wait_events");
}

#[tokio::test]
async fn test_wait_events_collector_counts_are_non_negative() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = WaitEventsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // All wait event counts should be non-negative
    for family in &metric_families {
        if family.name().starts_with("pg_wait_event") {
            for metric in family.get_metric() {
                let value = metric.get_gauge().value();
                assert!(
                    value >= 0.0,
                    "Wait event count should be non-negative, got: {value}"
                );
            }
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_wait_events_collector_is_idempotent() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = WaitEventsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;

    // Collect twice and compare
    collector.collect(&pool).await?;
    let first_metrics = registry.gather();
    let first_count: usize = first_metrics
        .iter()
        .find(|m| m.name() == "pg_wait_event_type")
        .map_or(0, |m| m.get_metric().len());

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    collector.collect(&pool).await?;
    let second_metrics = registry.gather();
    let second_count: usize = second_metrics
        .iter()
        .find(|m| m.name() == "pg_wait_event_type")
        .map_or(0, |m| m.get_metric().len());

    // Should have similar structure (count may vary slightly due to timing)
    assert!(
        first_count > 0 && second_count > 0,
        "Should have metrics in both collections"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_wait_events_collector_handles_concurrent_collection() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = WaitEventsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;

    // Collect concurrently
    let (r1, r2, r3) = tokio::join!(
        collector.collect(&pool),
        collector.collect(&pool),
        collector.collect(&pool)
    );

    // All should succeed
    r1?;
    r2?;
    r3?;

    pool.close().await;
    Ok(())
}

/// Waits until this database has sessions blocked on two different `Lock` wait events,
/// then returns what the collector exports for `pg_wait_event_type{type="Lock"}`.
async fn collect_lock_wait_total(pool: &PgPool) -> Result<f64> {
    let mut distinct_events = 0_i64;
    for _ in 0..100 {
        distinct_events = sqlx::query_scalar(
            "SELECT count(DISTINCT wait_event)::bigint
             FROM pg_stat_activity
             WHERE datname = current_database()
               AND state = 'active'
               AND wait_event_type = 'Lock'",
        )
        .fetch_one(pool)
        .await?;
        if distinct_events >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    ensure!(
        distinct_events >= 2,
        "expected sessions blocked on two different Lock wait events, saw {distinct_events}"
    );

    let collector = WaitEventsCollector::new();
    let registry = Registry::new();
    collector.register_metrics(&registry)?;
    collector.collect(pool).await?;

    registry
        .gather()
        .iter()
        .find(|family| family.name() == "pg_wait_event_type")
        .context("pg_wait_event_type was not exported")?
        .get_metric()
        .iter()
        .find(|metric| {
            metric
                .get_label()
                .iter()
                .any(|label| label.name() == "type" && label.value() == "Lock")
        })
        .map(|metric| metric.get_gauge().value())
        .context("pg_wait_event_type has no Lock series")
}

/// Blocks one session on a table lock (`Lock`/`relation`) and another on a row lock
/// (`Lock`/`transactionid`), and checks that both are counted under the `Lock` type.
async fn assert_lock_waiters_are_summed(pool: &PgPool) -> Result<()> {
    sqlx::query("CREATE TABLE wait_relation_target (id int)")
        .execute(pool)
        .await?;
    sqlx::query("CREATE TABLE wait_row_target (id int PRIMARY KEY, v int)")
        .execute(pool)
        .await?;
    sqlx::query("INSERT INTO wait_row_target VALUES (1, 0)")
        .execute(pool)
        .await?;

    let mut holder = pool.begin().await?;
    sqlx::query("LOCK TABLE wait_relation_target IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *holder)
        .await?;
    sqlx::query("UPDATE wait_row_target SET v = v + 1 WHERE id = 1")
        .execute(&mut *holder)
        .await?;

    let relation_waiter = tokio::spawn({
        let pool = pool.clone();
        async move {
            sqlx::query("SELECT count(*) FROM wait_relation_target")
                .execute(&pool)
                .await
        }
    });
    let row_waiter = tokio::spawn({
        let pool = pool.clone();
        async move {
            sqlx::query("UPDATE wait_row_target SET v = v + 1 WHERE id = 1")
                .execute(&pool)
                .await
        }
    });

    let exported = collect_lock_wait_total(pool).await;

    // Release the waiters before judging the result, so a failure cannot leave them
    // blocked behind the holder while the database is dropped.
    holder.rollback().await?;
    relation_waiter.await??;
    row_waiter.await??;

    let exported = exported?;
    if exported < 2.0 {
        bail!(
            "two sessions wait on different Lock events, but pg_wait_event_type{{type=\"Lock\"}} \
             exported {exported}: a type's events must be summed, not overwritten"
        );
    }
    Ok(())
}

/// `pg_wait_event_type` must count every session of a type. The query yields one row per
/// (type, event); publishing each row with `set` left only the last event's count, so a
/// type under-reported whenever it had more than one distinct wait event.
#[tokio::test]
async fn test_wait_event_type_sums_distinct_events_of_one_type() -> Result<()> {
    let test_db = common::IsolatedTestDatabase::new("wait_event_type_sum").await?;
    let outcome = assert_lock_waiters_are_summed(test_db.pool()).await;
    test_db.cleanup().await?;
    outcome
}
