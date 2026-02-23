use super::super::common;
use anyhow::Result;
use pg_exporter::collectors::{Collector, vacuum::progress::VacuumProgressCollector};
use prometheus::Registry;

#[tokio::test]
async fn test_vacuum_progress_collector_registers_without_error() -> Result<()> {
    let collector = VacuumProgressCollector::new();
    let registry = Registry::new();

    // Should not error when registering
    collector.register_metrics(&registry)?;

    Ok(())
}

#[tokio::test]
async fn test_vacuum_progress_collector_has_all_metrics_after_collection() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = VacuumProgressCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Should have all vacuum progress metrics
    let expected_metrics = vec![
        "pg_vacuum_in_progress",
        "pg_vacuum_heap_progress",
        "pg_vacuum_heap_vacuumed",
        "pg_vacuum_index_vacuum_count",
        "pg_vacuum_active",
        "pg_vacuum_is_autovacuum",
        "pg_vacuum_duration_seconds",
    ];

    for metric_name in expected_metrics {
        let found = metric_families.iter().any(|m| m.name() == metric_name);
        assert!(
            found,
            "Metric {} should exist. Found: {:?}",
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
async fn test_vacuum_progress_collector_handles_no_active_vacuums() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = VacuumProgressCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // When no vacuums are running, pg_vacuum_active should be 0
    let active_metric = metric_families
        .iter()
        .find(|m| m.name() == "pg_vacuum_active")
        .expect("pg_vacuum_active metric should exist");

    let value = common::metric_value_to_i64(active_metric.get_metric()[0].get_gauge().value());

    // Since we just started the test database, likely no vacuums are running
    // Value should be 0 or 1 (if autovacuum happened to start)
    assert!(
        value == 0 || value == 1,
        "pg_vacuum_active should be 0 or 1, got {value}"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_vacuum_progress_collector_metrics_have_database_and_table_labels() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = VacuumProgressCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Check that table-level metrics have both "database" and "table" labels
    let table_metrics = vec![
        "pg_vacuum_in_progress",
        "pg_vacuum_heap_progress",
        "pg_vacuum_heap_vacuumed",
        "pg_vacuum_index_vacuum_count",
    ];

    for metric_name in table_metrics {
        if let Some(metric_family) = metric_families.iter().find(|m| m.name() == metric_name) {
            // If there are metrics, they should have "database" and "table" labels
            for metric in metric_family.get_metric() {
                let has_database_label = metric.get_label().iter().any(|l| l.name() == "database");
                let has_table_label = metric.get_label().iter().any(|l| l.name() == "table");
                assert!(
                    has_database_label,
                    "Metric {metric_name} should have 'database' label"
                );
                assert!(
                    has_table_label,
                    "Metric {metric_name} should have 'table' label"
                );
            }
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_vacuum_progress_collector_name() {
    let collector = VacuumProgressCollector::new();
    assert_eq!(collector.name(), "vacuum_progress");
}

#[tokio::test]
async fn test_vacuum_progress_collector_progress_percentage_is_valid() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = VacuumProgressCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Check that progress percentage is in valid range (0-100)
    if let Some(progress_metric) = metric_families
        .iter()
        .find(|m| m.name() == "pg_vacuum_heap_progress")
    {
        for metric in progress_metric.get_metric() {
            let value = common::metric_value_to_i64(metric.get_gauge().value());
            assert!(
                (0..=100).contains(&value),
                "Progress percentage should be 0-100, got {value}"
            );
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_vacuum_progress_collector_counts_are_non_negative() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = VacuumProgressCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // All vacuum progress counts should be non-negative
    for family in &metric_families {
        if family.name().starts_with("pg_vacuum_") {
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
async fn test_vacuum_progress_collector_handles_concurrent_collection() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = VacuumProgressCollector::new();
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

#[tokio::test]
async fn test_vacuum_progress_collector_captures_actual_vacuum() -> Result<()> {
    let pool = common::create_test_pool().await?;

    // Create a test table with data (split into separate statements)
    sqlx::query("DROP TABLE IF EXISTS test_vacuum_progress_table")
        .execute(&pool)
        .await?;

    sqlx::query(
        "CREATE TABLE test_vacuum_progress_table (
            id SERIAL PRIMARY KEY,
            data TEXT
        )",
    )
    .execute(&pool)
    .await?;

    sqlx::query(
        "INSERT INTO test_vacuum_progress_table (data)
        SELECT 'test_data_' || generate_series(1, 1000)",
    )
    .execute(&pool)
    .await?;

    let collector = VacuumProgressCollector::new();
    let registry = Registry::new();
    collector.register_metrics(&registry)?;

    // Spawn a vacuum in a separate task (it will run for a moment)
    let pool_clone = pool.clone();
    let vacuum_task = tokio::spawn(async move {
        // VACUUM VERBOSE to make it take longer
        let _ = sqlx::query("VACUUM (VERBOSE) test_vacuum_progress_table")
            .execute(&pool_clone)
            .await;
    });

    // Give vacuum a moment to start
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Collect metrics while vacuum might be running
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Check that we have the database label with value "postgres"
    let in_progress_metric = metric_families
        .iter()
        .find(|m| m.name() == "pg_vacuum_in_progress");

    if let Some(metric_family) = in_progress_metric {
        for metric in metric_family.get_metric() {
            let database_label = metric
                .get_label()
                .iter()
                .find(|l| l.name() == "database")
                .map(prometheus::proto::LabelPair::value);

            // If we caught a vacuum in progress, verify database label exists
            if let Some(db) = database_label {
                // Should have database label set to "postgres", "pgbench_test" or "none"
                assert!(
                    db == "postgres" || db == "pgbench_test" || db == "none",
                    "Database label should be 'postgres', 'pgbench_test' or 'none', got: {db}"
                );
            }
        }
    }

    // Wait for vacuum to complete
    let _ = vacuum_task.await;

    // Clean up
    sqlx::query("DROP TABLE IF EXISTS test_vacuum_progress_table")
        .execute(&pool)
        .await?;

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_vacuum_progress_collector_database_label_format() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = VacuumProgressCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Verify that all vacuum metrics have properly formatted labels
    let vacuum_metrics = vec![
        "pg_vacuum_in_progress",
        "pg_vacuum_heap_progress",
        "pg_vacuum_heap_vacuumed",
        "pg_vacuum_index_vacuum_count",
        "pg_vacuum_is_autovacuum",
        "pg_vacuum_duration_seconds",
    ];

    for metric_name in vacuum_metrics {
        if let Some(metric_family) = metric_families.iter().find(|m| m.name() == metric_name) {
            for metric in metric_family.get_metric() {
                let labels: Vec<_> = metric
                    .get_label()
                    .iter()
                    .map(prometheus::proto::LabelPair::name)
                    .collect();

                // Should have exactly database and table labels (in that order)
                assert_eq!(
                    labels.len(),
                    2,
                    "Metric {metric_name} should have exactly 2 labels, got: {labels:?}"
                );

                assert_eq!(
                    labels[0], "database",
                    "First label should be 'database' for metric {metric_name}"
                );

                assert_eq!(
                    labels[1], "table",
                    "Second label should be 'table' for metric {metric_name}"
                );
            }
        }
    }

    pool.close().await;
    Ok(())
}
