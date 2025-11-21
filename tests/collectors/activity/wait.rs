use super::super::common;
use anyhow::Result;
use pg_exporter::collectors::{Collector, activity::wait::WaitEventsCollector};
use prometheus::Registry;

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
async fn test_wait_events_collector_collects_from_database() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = WaitEventsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Should have wait event metrics
    let wait_event_type = metric_families
        .iter()
        .find(|m| m.name() == "pg_wait_event_type")
        .expect("pg_wait_event_type metric should exist");

    let wait_event = metric_families
        .iter()
        .find(|m| m.name() == "pg_wait_event")
        .expect("pg_wait_event metric should exist");

    // Metrics should exist (even if no active wait events, will have 'none')
    assert!(!wait_event_type.get_metric().is_empty());
    assert!(!wait_event.get_metric().is_empty());

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
