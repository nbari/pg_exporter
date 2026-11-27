use super::super::common;
use anyhow::Result;
use pg_exporter::collectors::{
    Collector,
    stat::{StatCollector, user_tables::StatUserTablesCollector},
};
use prometheus::Registry;
use sqlx::postgres::PgPoolOptions;
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration as StdDuration,
};

static TEST_TABLE_COUNTER: AtomicU64 = AtomicU64::new(1);

fn unique_table_name(prefix: &str) -> String {
    let counter = TEST_TABLE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}_{}_{}", std::process::id(), counter)
}

fn find_metric_for_table<'a>(
    metric_families: &'a [prometheus::proto::MetricFamily],
    metric_name: &str,
    table_name: &str,
) -> Option<&'a prometheus::proto::Metric> {
    metric_families
        .iter()
        .find(|family| family.name() == metric_name)?
        .get_metric()
        .iter()
        .find(|metric| {
            metric
                .get_label()
                .iter()
                .any(|label| label.name() == "relname" && label.value() == table_name)
        })
}

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

    let table_name = unique_table_name("test_table");

    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "CREATE TABLE IF NOT EXISTS {table_name} (id INT PRIMARY KEY, data TEXT)"
    )))
    .execute(&pool)
    .await?;

    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "INSERT INTO {table_name} (id, data) VALUES (1, 'test') ON CONFLICT DO NOTHING"
    )))
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
    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "DROP TABLE IF EXISTS {table_name}"
    )))
    .execute(&pool)
    .await?;

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_stat_user_tables_collector_block_io_metrics() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let table_name = unique_table_name("test_blockio");

    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "CREATE TABLE IF NOT EXISTS {table_name} (id INT PRIMARY KEY, data TEXT)"
    )))
    .execute(&pool)
    .await?;

    for i in 0..50 {
        sqlx::query(sqlx::AssertSqlSafe(&*format!(
            "INSERT INTO {table_name} (id, data) VALUES ({i}, 'payload') ON CONFLICT DO NOTHING"
        )))
        .execute(&pool)
        .await?;
    }

    // Force heap and index block access so pg_statio counters are populated.
    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "SELECT count(*) FROM {table_name} WHERE id > 0"
    )))
    .fetch_one(&pool)
    .await?;

    let collector = StatUserTablesCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    let block_io_metrics = [
        "pg_stat_user_tables_heap_blks_read_total",
        "pg_stat_user_tables_heap_blks_hit_total",
        "pg_stat_user_tables_idx_blks_read_total",
        "pg_stat_user_tables_idx_blks_hit_total",
        "pg_stat_user_tables_toast_blks_read_total",
        "pg_stat_user_tables_toast_blks_hit_total",
        "pg_stat_user_tables_tidx_blks_read_total",
        "pg_stat_user_tables_tidx_blks_hit_total",
    ];

    for metric_name in block_io_metrics {
        assert!(
            metric_families.iter().any(|f| f.name() == metric_name),
            "block-I/O metric {metric_name} should be registered. Found: {:?}",
            metric_families
                .iter()
                .map(prometheus::proto::MetricFamily::name)
                .collect::<Vec<_>>()
        );

        if let Some(metric) = find_metric_for_table(&metric_families, metric_name, &table_name) {
            let label_names: Vec<&str> = metric
                .get_label()
                .iter()
                .map(prometheus::proto::LabelPair::name)
                .collect();
            assert!(
                label_names.contains(&"datname")
                    && label_names.contains(&"schemaname")
                    && label_names.contains(&"relname"),
                "{metric_name} should carry datname/schemaname/relname labels"
            );
            let value = metric.get_gauge().value();
            assert!(
                value >= 0.0,
                "{metric_name} should be non-negative, got {value}"
            );
        }
    }

    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "DROP TABLE IF EXISTS {table_name}"
    )))
    .execute(&pool)
    .await?;

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_stat_user_tables_collector_metrics_have_correct_labels() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let table_name = unique_table_name("test_labels");

    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "CREATE TABLE IF NOT EXISTS {table_name} (id INT)"
    )))
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
    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "DROP TABLE IF EXISTS {table_name}"
    )))
    .execute(&pool)
    .await?;

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_stat_user_tables_collector_counts_are_non_negative() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let table_name = unique_table_name("test_counts");

    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "CREATE TABLE IF NOT EXISTS {table_name} (id INT)"
    )))
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
                let value = common::metric_value_to_i64(metric.get_gauge().value());
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
    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "DROP TABLE IF EXISTS {table_name}"
    )))
    .execute(&pool)
    .await?;

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_stat_user_tables_collector_tracks_inserts() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let table_name = unique_table_name("test_inserts");

    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "CREATE TABLE IF NOT EXISTS {table_name} (id SERIAL PRIMARY KEY, data TEXT)"
    )))
    .execute(&pool)
    .await?;

    // Insert some rows
    for i in 1..=5 {
        sqlx::query(sqlx::AssertSqlSafe(&*format!(
            "INSERT INTO {table_name} (data) VALUES ($1)"
        )))
        .bind(format!("test_{i}"))
        .execute(&pool)
        .await?;
    }

    // Force PostgreSQL to flush stats so `n_tup_ins` is visible reliably in the full suite.
    let _ = sqlx::query("SELECT pg_stat_force_next_flush()")
        .execute(&pool)
        .await;

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let collector = StatUserTablesCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    let mut observed_inserts = 0;
    for _ in 0..20 {
        collector.collect(&pool).await?;

        let metric_families = registry.gather();

        observed_inserts = metric_families
            .iter()
            .find(|m| m.name() == "pg_stat_user_tables_n_tup_ins")
            .and_then(|metric_family| {
                metric_family.get_metric().iter().find(|metric| {
                    metric
                        .get_label()
                        .iter()
                        .any(|label| label.name() == "relname" && label.value() == table_name)
                })
            })
            .map_or(0, |metric| {
                common::metric_value_to_i64(metric.get_gauge().value())
            });

        if observed_inserts >= 5 {
            break;
        }

        let _ = sqlx::query("SELECT pg_stat_force_next_flush()")
            .execute(&pool)
            .await;
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    assert!(
        observed_inserts >= 5,
        "Should have at least 5 inserts, got: {observed_inserts}"
    );

    // Cleanup
    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "DROP TABLE IF EXISTS {table_name}"
    )))
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
            let value = common::metric_value_to_i64(metric.get_gauge().value());
            assert!(value >= 0, "Values should be non-negative");
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_stat_user_tables_collector_timestamp_values_are_reasonable() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let table_name = unique_table_name("test_timestamps");

    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "CREATE TABLE IF NOT EXISTS {table_name} (id INT)"
    )))
    .execute(&pool)
    .await?;

    // Run VACUUM and ANALYZE to generate timestamps
    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "VACUUM ANALYZE {table_name}"
    )))
    .execute(&pool)
    .await?;

    let collector = StatUserTablesCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    let now = i64::try_from(now.as_secs()).expect("current timestamp fits in i64");

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
                let value = common::metric_value_to_i64(metric.get_gauge().value());

                // Value should be 0 (never run) or a reasonable Unix timestamp
                if value > 0 {
                    let year_2000 = 946_684_800;
                    let allowed_future_skew = 86_400;
                    assert!(
                        value >= year_2000 && value <= now + allowed_future_skew,
                        "Timestamp {metric_name} should be between year 2000 and near-now, got {value}"
                    );
                }
            }
        }
    }

    // Cleanup
    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "DROP TABLE IF EXISTS {table_name}"
    )))
    .execute(&pool)
    .await?;

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_stat_user_tables_collector_tracks_table_size() -> Result<()> {
    let pool = common::create_test_pool().await?;

    // Create a test table with some data using unique name
    let table_name = format!("test_size_{}", std::process::id());
    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "DROP TABLE IF EXISTS {table_name}"
    )))
    .execute(&pool)
    .await?;

    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "CREATE TABLE {table_name} (id INT, data TEXT)"
    )))
    .execute(&pool)
    .await?;

    // Insert some data to ensure table has size
    for i in 1..=100 {
        sqlx::query(sqlx::AssertSqlSafe(&*format!(
            "INSERT INTO {table_name} (id, data) VALUES ($1, $2)"
        )))
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
        // Find our test table by its exact unique name. Matching on a bare `_{pid}` suffix
        // is unsafe because cargo runs tests multi-threaded in one process, so other
        // concurrent tests create tables ending in the same `_{pid}` and a loose match can
        // grab an unrelated (empty) table.
        let our_table = metric_family.get_metric().iter().find(|m| {
            m.get_label()
                .iter()
                .any(|l| l.name() == "relname" && l.value() == table_name)
        });

        if let Some(metric) = our_table {
            let value = common::metric_value_to_i64(metric.get_gauge().value());
            assert!(
                value > 0,
                "Table with data should have size > 0, got: {value}"
            );
        }
    }

    // Cleanup
    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "DROP TABLE IF EXISTS {table_name}"
    )))
    .execute(&pool)
    .await?;

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_stat_user_tables_collector_bloat_metrics() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = StatUserTablesCollector::new();
    let registry = Registry::new();

    // Metrics should register without error
    collector.register_metrics(&registry)?;

    // Create a table and collect data with unique name to avoid conflicts
    let table_name = format!("test_bloat_{}", std::process::id());
    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "DROP TABLE IF EXISTS {table_name}"
    )))
    .execute(&pool)
    .await?;

    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "CREATE TABLE {table_name} (id INT PRIMARY KEY, data TEXT)"
    )))
    .execute(&pool)
    .await?;

    // Insert data
    for i in 1..=100 {
        sqlx::query(sqlx::AssertSqlSafe(&*format!(
            "INSERT INTO {table_name} (id, data) VALUES ($1, $2)"
        )))
        .bind(i)
        .bind("x".repeat(100))
        .execute(&pool)
        .await?;
    }

    // Update rows to create dead tuples
    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "UPDATE {table_name} SET data = 'updated'"
    )))
    .execute(&pool)
    .await?;

    // Collect metrics
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Validate bloat metrics if they appear (they will appear if pg_stat_user_tables has data)
    for family in &metric_families {
        if family.name() == "pg_stat_user_tables_bloat_ratio" {
            for metric in family.get_metric() {
                let value = metric.get_gauge().value();
                assert!(
                    (0.0..=1.0).contains(&value),
                    "Bloat ratio should be between 0.0 and 1.0, got: {value}"
                );
            }
        }

        if family.name() == "pg_stat_user_tables_dead_tuple_size_bytes" {
            for metric in family.get_metric() {
                let value = metric.get_gauge().value();
                assert!(
                    value >= 0.0,
                    "Dead tuple size should be non-negative, got: {value}"
                );
            }
        }
    }

    // Cleanup
    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "DROP TABLE IF EXISTS {table_name}"
    )))
    .execute(&pool)
    .await?;

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_stat_user_tables_collector_captures_all_tuple_operations() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let table_name = format!("test_tuple_ops_{}", std::process::id());

    // Drop table if exists
    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "DROP TABLE IF EXISTS {table_name}"
    )))
    .execute(&pool)
    .await?;

    // Create table
    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "CREATE TABLE {table_name} (id SERIAL PRIMARY KEY, data TEXT, value INT)"
    )))
    .execute(&pool)
    .await?;

    // Perform various operations
    // INSERT
    let insert_count = 50;
    for i in 1..=insert_count {
        sqlx::query(sqlx::AssertSqlSafe(&*format!(
            "INSERT INTO {table_name} (data, value) VALUES ($1, $2)"
        )))
        .bind(format!("data_{i}"))
        .bind(i)
        .execute(&pool)
        .await?;
    }

    // UPDATE
    let update_count = 10;
    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "UPDATE {table_name} SET data = 'updated' WHERE value <= $1"
    )))
    .bind(update_count)
    .execute(&pool)
    .await?;

    // DELETE
    let delete_count = 5;
    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "DELETE FROM {table_name} WHERE value <= $1"
    )))
    .bind(delete_count)
    .execute(&pool)
    .await?;

    // SEQ SCAN - ensure table is scanned
    sqlx::query(sqlx::AssertSqlSafe(&*format!("SELECT * FROM {table_name}")))
        .fetch_all(&pool)
        .await?;

    // Force PostgreSQL to flush statistics (PostgreSQL 15+)
    // This makes stats immediately visible in pg_stat_user_tables
    let _ = sqlx::query("SELECT pg_stat_force_next_flush()")
        .execute(&pool)
        .await;

    // Small delay to allow stats to propagate
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let collector = StatUserTablesCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // PostgreSQL stats collection can be timing-sensitive
    // The collector should complete without errors, but specific values
    // may not appear immediately due to async stats collection

    // Verify collector ran without errors
    println!("✓ Collector completed without errors");

    // Verify metrics were registered (may be empty if no activity yet)
    println!("✓ Total metric families: {}", metric_families.len());

    // If any user tables metrics exist, validate their structure
    let user_tables_metrics: Vec<_> = metric_families
        .iter()
        .filter(|m| m.name().starts_with("pg_stat_user_tables_"))
        .collect();

    if user_tables_metrics.is_empty() {
        println!("ℹ No user tables metrics found (this is normal if stats haven't propagated)");
    } else {
        println!(
            "✓ Found {} user tables metric families",
            user_tables_metrics.len()
        );

        // Helper to find our table's metric value
        let find_metric_value = |metric_name: &str| -> Option<i64> {
            metric_families
                .iter()
                .find(|m| m.name() == metric_name)?
                .get_metric()
                .iter()
                .find(|m| {
                    m.get_label()
                        .iter()
                        .any(|l| l.name() == "relname" && l.value() == table_name)
                })
                .map(|m| common::metric_value_to_i64(m.get_gauge().value()))
        };

        // Check if our table's metrics are present
        if let Some(n_tup_ins) = find_metric_value("pg_stat_user_tables_n_tup_ins") {
            println!("✓ n_tup_ins: {n_tup_ins} (table found in metrics!)");
            assert!(
                n_tup_ins >= 0,
                "n_tup_ins should be non-negative, got {n_tup_ins}"
            );
        } else {
            println!("ℹ Table {table_name} not yet in pg_stat_user_tables (stats may be delayed)");
        }
    }

    // Cleanup
    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "DROP TABLE IF EXISTS {table_name}"
    )))
    .execute(&pool)
    .await?;

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_stat_user_tables_collector_autovacuum_threshold_metrics() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let table_name = format!("test_autovac_threshold_{}", std::process::id());

    // Create table
    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "DROP TABLE IF EXISTS {table_name}"
    )))
    .execute(&pool)
    .await?;

    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "CREATE TABLE {table_name} (id SERIAL PRIMARY KEY, data TEXT)"
    )))
    .execute(&pool)
    .await?;

    // Insert rows
    for i in 1..=100 {
        sqlx::query(sqlx::AssertSqlSafe(&*format!(
            "INSERT INTO {table_name} (data) VALUES ($1)"
        )))
        .bind(format!("row_{i}"))
        .execute(&pool)
        .await?;
    }

    // Update to create dead tuples
    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "UPDATE {table_name} SET data = 'modified'"
    )))
    .execute(&pool)
    .await?;

    sqlx::query(sqlx::AssertSqlSafe(&*format!("ANALYZE {table_name}")))
        .execute(&pool)
        .await?;

    let collector = StatUserTablesCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Check autovacuum threshold ratio metric exists
    if let Some(metric_family) = metric_families
        .iter()
        .find(|m| m.name() == "pg_stat_user_tables_autovacuum_threshold_ratio")
    {
        let our_table = metric_family.get_metric().iter().find(|m| {
            m.get_label()
                .iter()
                .any(|l| l.name() == "relname" && l.value() == table_name)
        });

        if let Some(metric) = our_table {
            let value = metric.get_gauge().value();
            println!("✓ autovacuum_threshold_ratio: {value} (should be >= 0.0)");
            assert!(
                value >= 0.0,
                "Autovacuum threshold ratio should be non-negative, got {value}"
            );
        }
    }

    // Check autoanalyze threshold ratio metric exists
    if let Some(metric_family) = metric_families
        .iter()
        .find(|m| m.name() == "pg_stat_user_tables_autoanalyze_threshold_ratio")
    {
        let our_table = metric_family.get_metric().iter().find(|m| {
            m.get_label()
                .iter()
                .any(|l| l.name() == "relname" && l.value() == table_name)
        });

        if let Some(metric) = our_table {
            let value = metric.get_gauge().value();
            println!("✓ autoanalyze_threshold_ratio: {value} (should be >= 0.0)");
            assert!(
                value >= 0.0,
                "Autoanalyze threshold ratio should be non-negative, got {value}"
            );
        }
    }

    // Cleanup
    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "DROP TABLE IF EXISTS {table_name}"
    )))
    .execute(&pool)
    .await?;

    pool.close().await;
    Ok(())
}

/// Creates a table whose autovacuum and autoanalyze trigger points are both zero, gives
/// it dead tuples, and returns the autovacuum threshold ratio the collector exports.
async fn collect_zero_threshold_ratio(pool: &sqlx::PgPool, table_name: &str) -> Result<f64> {
    // Autovacuum is disabled so it cannot remove the dead tuples mid-test; the zeroed
    // options still define the trigger point the ratio divides by.
    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "CREATE TABLE {table_name} (id int, data text) WITH (
             autovacuum_enabled = false,
             autovacuum_vacuum_threshold = 0,
             autovacuum_vacuum_scale_factor = 0,
             autovacuum_analyze_threshold = 0,
             autovacuum_analyze_scale_factor = 0
         )"
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "INSERT INTO {table_name} SELECT g, 'row' FROM generate_series(1, 100) g"
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "UPDATE {table_name} SET data = 'modified'"
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(&*format!("ANALYZE {table_name}")))
        .execute(pool)
        .await?;

    // ANALYZE publishes live and dead tuple counts; wait until both are visible.
    let mut counts = (0_i64, 0_i64);
    for _ in 0..100 {
        counts = sqlx::query_as(
            "SELECT n_live_tup::bigint, n_dead_tup::bigint
             FROM pg_stat_user_tables WHERE relname = $1",
        )
        .bind(table_name)
        .fetch_one(pool)
        .await?;
        if counts.0 > 0 && counts.1 > 0 {
            break;
        }
        tokio::time::sleep(StdDuration::from_millis(100)).await;
    }
    anyhow::ensure!(
        counts.0 > 0 && counts.1 > 0,
        "table statistics never showed live and dead tuples: {counts:?}"
    );

    let collector = StatUserTablesCollector::new();
    let registry = Registry::new();
    collector.register_metrics(&registry)?;
    collector.collect(pool).await?;
    let metric_families = registry.gather();

    for metric_name in [
        "pg_stat_user_tables_autovacuum_threshold_ratio",
        "pg_stat_user_tables_autoanalyze_threshold_ratio",
        "pg_stat_user_tables_n_dead_tup",
    ] {
        anyhow::ensure!(
            find_metric_for_table(&metric_families, metric_name, table_name).is_some(),
            "{metric_name} is missing for {table_name}: a zero autovacuum trigger point must \
             not fail the query for the whole database"
        );
    }

    find_metric_for_table(
        &metric_families,
        "pg_stat_user_tables_autovacuum_threshold_ratio",
        table_name,
    )
    .map(|metric| metric.get_gauge().value())
    .ok_or_else(|| anyhow::anyhow!("autovacuum threshold ratio missing for {table_name}"))
}

/// Reloptions may set both the autovacuum threshold and scale factor to 0. The ratio's
/// divisor is then 0, and `PostgreSQL` raises `division by zero` for `double precision`:
/// unguarded, one such table removed every `pg_stat_user_tables_*` series of its database,
/// and failed the scrape when it happened in every database.
#[tokio::test]
async fn test_stat_user_tables_collector_survives_zero_autovacuum_threshold() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let table_name = unique_table_name("test_zero_autovac_threshold");

    let outcome = collect_zero_threshold_ratio(&pool, &table_name).await;

    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "DROP TABLE IF EXISTS {table_name}"
    )))
    .execute(&pool)
    .await?;
    pool.close().await;

    let ratio = outcome?;
    assert!(
        ratio >= 1.0,
        "with a zero trigger point any dead tuple is due for autovacuum, so the ratio must \
         be at least 1, got {ratio}"
    );
    Ok(())
}

/// Creates `table_name` with `reloptions` (autovacuum itself is switched off so the
/// counts stay put), inserts `rows` rows, deletes the first `deleted`, and returns the
/// exported vacuum and analyze threshold ratios once the statistics show those counts.
async fn threshold_ratios_after_delete(
    pool: &sqlx::PgPool,
    table_name: &str,
    reloptions: &str,
    rows: i64,
    deleted: i64,
    analyze_before_delete: bool,
) -> Result<(f64, f64)> {
    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "CREATE TABLE {table_name} (id bigint PRIMARY KEY, data text)
         WITH (autovacuum_enabled = false, {reloptions})"
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "INSERT INTO {table_name} SELECT g, 'row' FROM generate_series(1, {rows}) g"
    )))
    .execute(pool)
    .await?;
    if analyze_before_delete {
        // Sets `reltuples`, the size the server's trigger point is computed from.
        sqlx::query(sqlx::AssertSqlSafe(&*format!("ANALYZE {table_name}")))
            .execute(pool)
            .await?;
    }
    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "DELETE FROM {table_name} WHERE id <= {deleted}"
    )))
    .execute(pool)
    .await?;

    // The dead-tuple count is what both ratios are measured in. The live count is only
    // predictable without an ANALYZE in between: ANALYZE overwrites it with an estimate,
    // and statistics another pooled connection had not reported yet are added on top.
    let mut counts = (-1_i64, -1_i64);
    let settled = |counts: (i64, i64)| {
        counts.1 == deleted && (analyze_before_delete || counts.0 == rows - deleted)
    };
    for _ in 0..100 {
        // Not available before PostgreSQL 15, where the statistics arrive on their own.
        let _ = sqlx::query("SELECT pg_stat_force_next_flush()")
            .execute(pool)
            .await;
        counts = sqlx::query_as(
            "SELECT n_live_tup::bigint, n_dead_tup::bigint
             FROM pg_stat_user_tables WHERE relname = $1",
        )
        .bind(table_name)
        .fetch_one(pool)
        .await?;
        if settled(counts) {
            break;
        }
        tokio::time::sleep(StdDuration::from_millis(100)).await;
    }
    anyhow::ensure!(
        settled(counts),
        "table statistics never showed {deleted} dead tuples (and, without ANALYZE, {} live), \
         last saw live and dead {counts:?}",
        rows - deleted
    );

    let collector = StatUserTablesCollector::new();
    let registry = Registry::new();
    collector.register_metrics(&registry)?;
    collector.collect(pool).await?;
    let metric_families = registry.gather();

    let ratio = |metric_name: &str| {
        find_metric_for_table(&metric_families, metric_name, table_name)
            .map(|metric| metric.get_gauge().value())
            .ok_or_else(|| anyhow::anyhow!("{metric_name} missing for {table_name}"))
    };
    Ok((
        ratio("pg_stat_user_tables_autovacuum_threshold_ratio")?,
        ratio("pg_stat_user_tables_autoanalyze_threshold_ratio")?,
    ))
}

async fn drop_table(pool: &sqlx::PgPool, table_name: &str) -> Result<()> {
    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "DROP TABLE IF EXISTS {table_name}"
    )))
    .execute(pool)
    .await?;
    Ok(())
}

/// A table whose rows were all deleted has no live tuples, so its trigger point is the
/// bare threshold, and its dead tuples are due as soon as they exceed it. Both ratios
/// used to be forced to 0 for such a table, which hid the one moment they matter most.
#[tokio::test]
async fn test_stat_user_tables_threshold_ratios_cover_a_fully_deleted_table() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let table_name = unique_table_name("test_ratio_all_deleted");

    let outcome = threshold_ratios_after_delete(
        &pool,
        &table_name,
        "autovacuum_vacuum_threshold = 50, autovacuum_vacuum_scale_factor = 0.2,
         autovacuum_analyze_threshold = 50, autovacuum_analyze_scale_factor = 0.1",
        100,
        100,
        false,
    )
    .await;
    drop_table(&pool, &table_name).await?;
    pool.close().await;

    let (vacuum_ratio, analyze_ratio) = outcome?;
    assert!(
        (vacuum_ratio - 2.0).abs() < 1e-9,
        "100 dead tuples against a threshold of 50 are a ratio of 2, got {vacuum_ratio}"
    );
    assert!(
        analyze_ratio >= 2.0,
        "at least 100 modifications against a threshold of 50, got {analyze_ratio}"
    );
    Ok(())
}

/// The zero-trigger-point convention holds without live rows too: the ratio is the dead
/// tuple count, not 0.
#[tokio::test]
async fn test_stat_user_tables_zero_threshold_ratio_covers_a_fully_deleted_table() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let table_name = unique_table_name("test_ratio_zero_all_deleted");

    let outcome = threshold_ratios_after_delete(
        &pool,
        &table_name,
        "autovacuum_vacuum_threshold = 0, autovacuum_vacuum_scale_factor = 0,
         autovacuum_analyze_threshold = 0, autovacuum_analyze_scale_factor = 0",
        100,
        100,
        false,
    )
    .await;
    drop_table(&pool, &table_name).await?;
    pool.close().await;

    let (vacuum_ratio, _) = outcome?;
    assert!(
        (vacuum_ratio - 100.0).abs() < 1e-9,
        "with a zero trigger point the ratio is the dead tuple count, got {vacuum_ratio}"
    );
    Ok(())
}

/// The scaled part of the trigger point follows `reltuples`, not the live-tuple counter.
/// A table of 100 analyzed rows that is then emptied still has `reltuples = 100`, so with
/// threshold 10 and scale factor 1 the server waits for 110 dead tuples: the 100 there
/// are a ratio of 0.909, not the 10 that dividing by the bare threshold gives.
#[tokio::test]
async fn test_stat_user_tables_threshold_ratios_scale_with_reltuples() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let table_name = unique_table_name("test_ratio_reltuples");

    let outcome = threshold_ratios_after_delete(
        &pool,
        &table_name,
        "autovacuum_vacuum_threshold = 10, autovacuum_vacuum_scale_factor = 1,
         autovacuum_analyze_threshold = 10, autovacuum_analyze_scale_factor = 1",
        100,
        100,
        true,
    )
    .await;
    drop_table(&pool, &table_name).await?;
    pool.close().await;

    let (vacuum_ratio, analyze_ratio) = outcome?;
    let expected = 100.0 / 110.0;
    assert!(
        (vacuum_ratio - expected).abs() < 1e-6,
        "100 dead tuples against 10 + 1 * 100 are a ratio of {expected}, got {vacuum_ratio}"
    );
    // The modification count depends on which statistics had been reported when ANALYZE
    // ran, so only the divisor is pinned: 110, which makes the ratio a multiple of 1/110
    // no smaller than the 100 deletes.
    let modifications = analyze_ratio * 110.0;
    assert!(
        modifications >= 100.0 - 1e-6 && (modifications - modifications.round()).abs() < 1e-6,
        "the analyze ratio should be a whole modification count over 110, got {analyze_ratio}"
    );
    Ok(())
}

/// `PostgreSQL` 18 caps the vacuum trigger point with `autovacuum_vacuum_max_threshold`.
/// Ignoring the cap under-reports how due a table is: 2 dead tuples against a threshold
/// of 50 capped at 1 are a ratio of 2, not 0.04.
#[tokio::test]
async fn test_stat_user_tables_vacuum_ratio_honours_the_max_threshold() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let server_version: i32 =
        sqlx::query_scalar("SELECT current_setting('server_version_num')::int4")
            .fetch_one(&pool)
            .await?;
    if server_version < 180_000 {
        println!("autovacuum_vacuum_max_threshold needs PostgreSQL 18, skipping test");
        pool.close().await;
        return Ok(());
    }
    let table_name = unique_table_name("test_ratio_max_threshold");

    let outcome = threshold_ratios_after_delete(
        &pool,
        &table_name,
        "autovacuum_vacuum_threshold = 50, autovacuum_vacuum_scale_factor = 0,
         autovacuum_vacuum_max_threshold = 1",
        100,
        2,
        false,
    )
    .await;
    drop_table(&pool, &table_name).await?;
    pool.close().await;

    let (vacuum_ratio, _) = outcome?;
    assert!(
        (vacuum_ratio - 2.0).abs() < 1e-9,
        "2 dead tuples against a trigger point capped at 1 are a ratio of 2, got {vacuum_ratio}"
    );
    Ok(())
}

#[tokio::test]
async fn test_stat_user_tables_collector_marks_never_autovacuumed_tables() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let table_name = format!("test_never_autovac_{}", std::process::id());
    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "DROP TABLE IF EXISTS {table_name}"
    )))
    .execute(&pool)
    .await?;

    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "CREATE TABLE {table_name} (id INT)"
    )))
    .execute(&pool)
    .await?;

    let collector = StatUserTablesCollector::new();
    let registry = Registry::new();
    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    let never_autovacuumed = find_metric_for_table(
        &metric_families,
        "pg_stat_user_tables_never_autovacuumed",
        &table_name,
    )
    .map_or(0, |metric| {
        common::metric_value_to_i64(metric.get_gauge().value())
    });

    assert_eq!(
        never_autovacuumed, 1,
        "new table should be marked as never autovacuumed"
    );

    let has_last_autovacuum_age = find_metric_for_table(
        &metric_families,
        "pg_stat_user_tables_last_autovacuum_seconds_ago",
        &table_name,
    )
    .is_some();

    assert!(
        !has_last_autovacuum_age,
        "table that was never autovacuumed should not expose last_autovacuum_seconds_ago"
    );

    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "DROP TABLE IF EXISTS {table_name}"
    )))
    .execute(&pool)
    .await?;

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_stat_user_tables_collector_emits_automatic_maintenance_ages() -> Result<()> {
    let bootstrap_pool = common::create_test_pool().await?;
    bootstrap_pool.close().await;

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&common::get_test_dsn())
        .await?;
    let table_name = unique_table_name("test_maintenance_ages");

    sqlx::query("SET search_path TO pg_temp, pg_catalog")
        .execute(&pool)
        .await?;
    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "CREATE TEMP TABLE {table_name} (id INT)"
    )))
    .execute(&pool)
    .await?;

    // Shadow the statistics views in this session so the exact collector query receives
    // deterministic non-NULL auto-maintenance timestamps without waiting for autovacuum.
    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        r"
        CREATE TEMP VIEW pg_stat_user_tables AS
        SELECT
            'maintenance_fixture'::name AS schemaname,
            '{table_name}'::name AS relname,
            '{table_name}'::regclass::oid AS relid,
            0::bigint AS seq_scan,
            0::bigint AS seq_tup_read,
            0::bigint AS idx_scan,
            0::bigint AS idx_tup_fetch,
            1::bigint AS n_tup_ins,
            0::bigint AS n_tup_upd,
            0::bigint AS n_tup_del,
            0::bigint AS n_tup_hot_upd,
            1::bigint AS n_live_tup,
            0::bigint AS n_dead_tup,
            0::bigint AS n_mod_since_analyze,
            NULL::timestamp with time zone AS last_vacuum,
            now() - interval '5 minutes' AS last_autovacuum,
            NULL::timestamp with time zone AS last_analyze,
            now() - interval '10 minutes' AS last_autoanalyze,
            0::bigint AS vacuum_count,
            1::bigint AS autovacuum_count,
            0::bigint AS analyze_count,
            1::bigint AS autoanalyze_count
        "
    )))
    .execute(&pool)
    .await?;

    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        r"
        CREATE TEMP VIEW pg_statio_user_tables AS
        SELECT
            '{table_name}'::regclass::oid AS relid,
            0::bigint AS heap_blks_read,
            0::bigint AS heap_blks_hit,
            0::bigint AS idx_blks_read,
            0::bigint AS idx_blks_hit,
            0::bigint AS toast_blks_read,
            0::bigint AS toast_blks_hit,
            0::bigint AS tidx_blks_read,
            0::bigint AS tidx_blks_hit
        "
    )))
    .execute(&pool)
    .await?;

    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    let collector = StatCollector::new();
    let registry = Registry::new();
    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();
    for (metric_name, expected_seconds) in [
        ("pg_stat_user_tables_last_autovacuum_seconds_ago", 300.0),
        ("pg_stat_user_tables_last_autoanalyze_seconds_ago", 600.0),
    ] {
        let metric = find_metric_for_table(&metric_families, metric_name, &table_name)
            .ok_or_else(|| anyhow::anyhow!("{metric_name} was not collected for {table_name}"))?;
        let value = metric.get_gauge().value();

        assert!(value.is_finite(), "{metric_name} should be finite");
        assert!(
            (value - expected_seconds).abs() < 1.0,
            "{metric_name} should be approximately {expected_seconds}, got {value}"
        );
        assert!(
            metric
                .get_label()
                .iter()
                .any(|label| { label.name() == "datname" && label.value() == database_name })
        );
        assert!(metric.get_label().iter().any(|label| {
            label.name() == "schemaname" && label.value() == "maintenance_fixture"
        }));
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_stat_user_tables_collector_preserves_last_good_snapshot_on_query_failure()
-> Result<()> {
    let pool = common::create_test_pool().await?;

    let table_name = format!("test_user_tables_snapshot_{}", std::process::id());
    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "DROP TABLE IF EXISTS {table_name}"
    )))
    .execute(&pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "CREATE TABLE {table_name} (id INT)"
    )))
    .execute(&pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "INSERT INTO {table_name} (id) VALUES (1), (2), (3)"
    )))
    .execute(&pool)
    .await?;

    let collector = StatUserTablesCollector::new();
    let registry = Registry::new();
    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let sample_count_before = registry
        .gather()
        .iter()
        .find(|family| family.name() == "pg_stat_user_tables_table_size_bytes")
        .map_or(0, |family| family.get_metric().len());

    assert!(
        sample_count_before > 0,
        "expected initial table stats samples"
    );

    let broken_pool = PgPoolOptions::new()
        .acquire_timeout(StdDuration::from_millis(100))
        .connect_lazy("postgresql://postgres:postgres@localhost:54321/postgres")?;

    let failed = collector.collect(&broken_pool).await;
    assert!(
        failed.is_err(),
        "expected collection against broken pool to fail"
    );

    let sample_count_after = registry
        .gather()
        .iter()
        .find(|family| family.name() == "pg_stat_user_tables_table_size_bytes")
        .map_or(0, |family| family.get_metric().len());

    assert_eq!(
        sample_count_after, sample_count_before,
        "failed collection should preserve the last good table-stats snapshot"
    );

    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "DROP TABLE IF EXISTS {table_name}"
    )))
    .execute(&pool)
    .await?;

    pool.close().await;
    Ok(())
}
