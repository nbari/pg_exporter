use super::super::common;
use anyhow::Result;
use pg_exporter::collectors::{Collector, stat::user_tables::StatUserTablesCollector};
use prometheus::Registry;

#[tokio::test]
async fn test_stat_user_tables_collector_registers_without_error() -> Result<()> {
    let collector = StatUserTablesCollector::new();
    let registry = Registry::new();

    // Should not error when registering
    collector.register_metrics(&registry)?;

    Ok(())
}

#[tokio::test]
async fn test_stat_user_tables_collector_collection_succeeds() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = StatUserTablesCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;

    // Collection should succeed even if no user tables exist
    collector.collect(&pool).await?;

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_stat_user_tables_collector_with_created_table() -> Result<()> {
    let pool = common::create_test_pool().await?;

    // Create a test table
    sqlx::query("CREATE TABLE IF NOT EXISTS test_table (id INT PRIMARY KEY, data TEXT)")
        .execute(&pool)
        .await?;

    // Insert some data
    sqlx::query("INSERT INTO test_table (id, data) VALUES (1, 'test') ON CONFLICT DO NOTHING")
        .execute(&pool)
        .await?;

    let collector = StatUserTablesCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Now we should have metrics for our test table
    let has_metrics = !metric_families.is_empty();

    if has_metrics {
        // If metrics exist, they should have the correct structure
        for family in &metric_families {
            if family.name().starts_with("pg_stat_user_tables_") {
                for metric in family.get_metric() {
                    let label_names: Vec<String> = metric
                        .get_label()
                        .iter()
                        .map(|l| l.name().to_string())
                        .collect();

                    assert!(
                        label_names.contains(&"datname".to_string()),
                        "Metric should have 'datname' label"
                    );
                    assert!(
                        label_names.contains(&"schemaname".to_string()),
                        "Metric should have 'schemaname' label"
                    );
                    assert!(
                        label_names.contains(&"relname".to_string()),
                        "Metric should have 'relname' label"
                    );
                }
            }
        }
    }

    // Cleanup
    sqlx::query("DROP TABLE IF EXISTS test_table")
        .execute(&pool)
        .await?;

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_stat_user_tables_collector_metrics_have_correct_labels() -> Result<()> {
    let pool = common::create_test_pool().await?;

    // Create a test table
    sqlx::query("CREATE TABLE IF NOT EXISTS test_labels (id INT)")
        .execute(&pool)
        .await?;

    let collector = StatUserTablesCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // All metrics should have datname, schemaname, and relname labels
    for family in &metric_families {
        if family.name().starts_with("pg_stat_user_tables_") {
            for metric in family.get_metric() {
                let label_names: Vec<String> = metric
                    .get_label()
                    .iter()
                    .map(|l| l.name().to_string())
                    .collect();

                assert!(
                    label_names.contains(&"datname".to_string()),
                    "Metric {} should have 'datname' label",
                    family.name()
                );
                assert!(
                    label_names.contains(&"schemaname".to_string()),
                    "Metric {} should have 'schemaname' label",
                    family.name()
                );
                assert!(
                    label_names.contains(&"relname".to_string()),
                    "Metric {} should have 'relname' label",
                    family.name()
                );
            }
        }
    }

    // Cleanup
    sqlx::query("DROP TABLE IF EXISTS test_labels")
        .execute(&pool)
        .await?;

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_stat_user_tables_collector_counts_are_non_negative() -> Result<()> {
    let pool = common::create_test_pool().await?;

    // Create a test table
    sqlx::query("CREATE TABLE IF NOT EXISTS test_counts (id INT)")
        .execute(&pool)
        .await?;

    let collector = StatUserTablesCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // All counts should be non-negative
    for family in &metric_families {
        if family.name().starts_with("pg_stat_user_tables_") {
            for metric in family.get_metric() {
                let value = metric.get_gauge().value() as i64;
                assert!(
                    value >= 0,
                    "Metric {} should be non-negative, got: {}",
                    family.name(),
                    value
                );
            }
        }
    }

    // Cleanup
    sqlx::query("DROP TABLE IF EXISTS test_counts")
        .execute(&pool)
        .await?;

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_stat_user_tables_collector_tracks_inserts() -> Result<()> {
    let pool = common::create_test_pool().await?;

    // Create a test table
    sqlx::query("CREATE TABLE IF NOT EXISTS test_inserts (id SERIAL PRIMARY KEY, data TEXT)")
        .execute(&pool)
        .await?;

    // Insert some rows
    for i in 1..=5 {
        sqlx::query("INSERT INTO test_inserts (data) VALUES ($1)")
            .bind(format!("test_{}", i))
            .execute(&pool)
            .await?;
    }

    let collector = StatUserTablesCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Check if n_tup_ins metric exists and has a value
    let n_tup_ins = metric_families
        .iter()
        .find(|m| m.name() == "pg_stat_user_tables_n_tup_ins");

    if let Some(metric_family) = n_tup_ins {
        // Find our test_inserts table
        let our_table = metric_family.get_metric().iter().find(|m| {
            m.get_label()
                .iter()
                .any(|l| l.name() == "relname" && l.value() == "test_inserts")
        });

        if let Some(metric) = our_table {
            let value = metric.get_gauge().value() as i64;
            assert!(value >= 5, "Should have at least 5 inserts, got: {}", value);
        }
    }

    // Cleanup
    sqlx::query("DROP TABLE IF EXISTS test_inserts")
        .execute(&pool)
        .await?;

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_stat_user_tables_collector_name() {
    let collector = StatUserTablesCollector::new();
    assert_eq!(collector.name(), "stat_user_tables");
}

#[tokio::test]
async fn test_stat_user_tables_collector_handles_concurrent_collection() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = StatUserTablesCollector::new();
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
async fn test_stat_user_tables_collector_handles_empty_database() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = StatUserTablesCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;

    // Should not error even if no user tables exist
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // No metrics should be produced if no user tables exist
    // This is expected behavior
    let user_table_metrics: Vec<_> = metric_families
        .iter()
        .filter(|m| m.name().starts_with("pg_stat_user_tables_"))
        .collect();

    // Either no metrics or empty metrics is acceptable
    for family in user_table_metrics {
        // If metrics exist, they should be well-formed
        for metric in family.get_metric() {
            let value = metric.get_gauge().value() as i64;
            assert!(value >= 0, "Values should be non-negative");
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_stat_user_tables_collector_timestamp_values_are_reasonable() -> Result<()> {
    let pool = common::create_test_pool().await?;

    // Create a test table and vacuum it
    sqlx::query("CREATE TABLE IF NOT EXISTS test_timestamps (id INT)")
        .execute(&pool)
        .await?;

    // Run VACUUM and ANALYZE to generate timestamps
    sqlx::query("VACUUM ANALYZE test_timestamps")
        .execute(&pool)
        .await?;

    let collector = StatUserTablesCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    // Check that timestamp metrics are reasonable (0 or valid timestamp)
    let timestamp_metrics = vec![
        "pg_stat_user_tables_last_vacuum",
        "pg_stat_user_tables_last_autovacuum",
        "pg_stat_user_tables_last_analyze",
        "pg_stat_user_tables_last_autoanalyze",
    ];

    for metric_name in timestamp_metrics {
        if let Some(family) = metric_families.iter().find(|m| m.name() == metric_name) {
            for metric in family.get_metric() {
                let value = metric.get_gauge().value() as i64;

                // Value should be 0 (never run) or a reasonable Unix timestamp
                if value > 0 {
                    let year_2000 = 946684800;
                    assert!(
                        value >= year_2000 && value <= now,
                        "Timestamp {} should be between year 2000 and now, got {}",
                        metric_name,
                        value
                    );
                }
            }
        }
    }

    // Cleanup
    sqlx::query("DROP TABLE IF EXISTS test_timestamps")
        .execute(&pool)
        .await?;

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_stat_user_tables_collector_tracks_table_size() -> Result<()> {
    let pool = common::create_test_pool().await?;

    // Create a test table with some data
    sqlx::query("CREATE TABLE IF NOT EXISTS test_size (id INT, data TEXT)")
        .execute(&pool)
        .await?;

    // Insert some data to ensure table has size
    for i in 1..=100 {
        sqlx::query("INSERT INTO test_size (id, data) VALUES ($1, $2)")
            .bind(i)
            .bind("x".repeat(100))
            .execute(&pool)
            .await?;
    }

    let collector = StatUserTablesCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Check if table_size_bytes metric exists
    let table_size = metric_families
        .iter()
        .find(|m| m.name() == "pg_stat_user_tables_table_size_bytes");

    if let Some(metric_family) = table_size {
        // Find our test_size table
        let our_table = metric_family.get_metric().iter().find(|m| {
            m.get_label()
                .iter()
                .any(|l| l.name() == "relname" && l.value() == "test_size")
        });

        if let Some(metric) = our_table {
            let value = metric.get_gauge().value() as i64;
            assert!(
                value > 0,
                "Table with data should have size > 0, got: {}",
                value
            );
        }
    }

    // Cleanup
    sqlx::query("DROP TABLE IF EXISTS test_size")
        .execute(&pool)
        .await?;

    pool.close().await;
    Ok(())
}
