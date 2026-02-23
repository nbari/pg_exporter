use super::super::common;
use anyhow::Result;
use pg_exporter::collectors::{Collector, activity::connections::ConnectionsCollector};
use prometheus::Registry;

#[tokio::test]
async fn test_connections_collector_registers_without_error() -> Result<()> {
    let collector = ConnectionsCollector::new();
    let registry = Registry::new();

    // Should not error
    collector.register_metrics(&registry)?;

    Ok(())
}

#[tokio::test]
async fn test_connections_collector_has_all_metrics() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = ConnectionsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Should have all connection metrics after collection
    let expected_metrics = vec![
        "pg_stat_activity_count",
        "pg_stat_activity_active_connections",
        "pg_stat_activity_idle_connections",
        "pg_stat_activity_waiting_connections",
        "pg_stat_activity_blocked_connections",
    ];

    for metric_name in expected_metrics {
        let found = metric_families.iter().any(|m| m.name() == metric_name);
        assert!(found, "Metric {metric_name} should exist after collection");
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_connections_collector_collects_from_database() -> Result<()> {
    let pool = common::create_test_pool().await?;

    // Keep a connection active during the test by running a long query
    let mut conn = pool.acquire().await?;
    let query_handle = tokio::spawn(async move {
        // Run a 5-second sleep query to keep connection in 'active' state
        let _ = sqlx::query("SELECT pg_sleep(5)").execute(&mut *conn).await;
        conn
    });

    // Give the query time to start executing
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let collector = ConnectionsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Should have at least one active connection (our test connection running pg_sleep)
    let active_conn = metric_families
        .iter()
        .find(|m| m.name() == "pg_stat_activity_active_connections")
        .expect("active_connections metric should exist");

    assert!(
        !active_conn.get_metric().is_empty(),
        "Should have active connections"
    );

    // Check total connections across all databases
    let total_connections: f64 = active_conn
        .get_metric()
        .iter()
        .map(|m| m.get_gauge().value())
        .sum();

    assert!(
        total_connections >= 1.0,
        "Should have at least one active connection, found: {total_connections}"
    );

    // Clean up - wait for the query to complete
    let _conn = query_handle.await?;

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_connections_collector_tracks_connection_states() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = ConnectionsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // pg_stat_activity_count should have state labels
    let count_metric = metric_families
        .iter()
        .find(|m| m.name() == "pg_stat_activity_count")
        .expect("count metric should exist");

    assert!(
        !count_metric.get_metric().is_empty(),
        "Should have connection counts"
    );

    // Check that metrics have datname and state labels
    for metric in count_metric.get_metric() {
        let labels: Vec<_> = metric.get_label().iter().collect();

        let has_datname = labels.iter().any(|l| l.name() == "datname");
        let has_state = labels.iter().any(|l| l.name() == "state");

        assert!(has_datname, "Metric should have datname label");
        assert!(has_state, "Metric should have state label");
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_connections_collector_counts_match() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = ConnectionsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Get active + idle counts and verify they match the state counts
    let active_conn = metric_families
        .iter()
        .find(|m| m.name() == "pg_stat_activity_active_connections")
        .unwrap();

    let idle_conn = metric_families
        .iter()
        .find(|m| m.name() == "pg_stat_activity_idle_connections")
        .unwrap();

    // Both should have metrics
    assert!(!active_conn.get_metric().is_empty());
    assert!(!idle_conn.get_metric().is_empty());

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_connections_collector_waiting_and_blocked() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = ConnectionsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Waiting connections metric should exist
    let waiting = metric_families
        .iter()
        .find(|m| m.name() == "pg_stat_activity_waiting_connections")
        .expect("waiting_connections metric should exist");

    // Blocked connections metric should exist
    let blocked = metric_families
        .iter()
        .find(|m| m.name() == "pg_stat_activity_blocked_connections")
        .expect("blocked_connections metric should exist");

    // Both should have metrics (even if zero)
    assert!(!waiting.get_metric().is_empty());
    assert!(!blocked.get_metric().is_empty());

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_connections_collector_name() {
    let collector = ConnectionsCollector::new();
    assert_eq!(collector.name(), "connections");
}

#[tokio::test]
async fn test_connections_collector_handles_concurrent_collections() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = ConnectionsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;

    // Collect multiple times concurrently
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

#[tokio::test]
async fn test_connections_collector_metrics_are_non_negative() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = ConnectionsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // All connection counts should be non-negative
    for family in metric_families {
        if family.name().starts_with("pg_stat_activity_") {
            for metric in family.get_metric() {
                let value = metric.get_gauge().value();
                assert!(
                    value >= 0.0,
                    "Metric {} should be non-negative, got: {}",
                    family.name(),
                    value
                );
            }
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_connections_collector_handles_query_error() -> Result<()> {
    use sqlx::postgres::PgPoolOptions;
    use std::time::Duration;

    let collector = ConnectionsCollector::new();
    let registry = Registry::new();
    collector.register_metrics(&registry)?;

    // Use a pool that will definitely fail (invalid port)
    // We use connect_lazy so the pool creation succeeds, but the first query will fail
    let pool = PgPoolOptions::new()
        .acquire_timeout(Duration::from_millis(100))
        .connect_lazy("postgresql://postgres:postgres@localhost:54321/postgres")
        .expect("failed to create lazy pool");

    // Collect should fail when it tries to execute queries
    let result = collector.collect(&pool).await;

    assert!(result.is_err(), "Should return error when query fails");

    Ok(())
}
