use super::super::common;
use anyhow::Result;
use pg_exporter::collectors::Collector;
use pg_exporter::collectors::statements::pg_statements::PgStatementsCollector;
use prometheus::Registry;

#[tokio::test]
async fn test_pg_statements_collector_registers_without_error() -> Result<()> {
    let collector = PgStatementsCollector::new();
    let registry = Registry::new();

    // Should not error when registering
    collector.register_metrics(&registry)?;

    Ok(())
}

#[tokio::test]
async fn test_pg_statements_collector_has_all_metrics_after_collection() -> Result<()> {
    let pool = common::create_test_pool().await?;

    // First, ensure pg_stat_statements extension exists (may not be available in test env)
    let ext_check = sqlx::query("SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'")
        .fetch_optional(&pool)
        .await?;

    if ext_check.is_none() {
        // Skip test if extension not available
        println!("pg_stat_statements extension not installed, skipping test");
        pool.close().await;
        return Ok(());
    }

    let collector = PgStatementsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Should have all pg_stat_statements metrics
    let expected_metrics = vec![
        "postgres_pg_stat_statements_total_exec_time_seconds",
        "postgres_pg_stat_statements_mean_exec_time_seconds",
        "postgres_pg_stat_statements_max_exec_time_seconds",
        "postgres_pg_stat_statements_stddev_exec_time_seconds",
        "postgres_pg_stat_statements_calls_total",
        "postgres_pg_stat_statements_rows_total",
        "postgres_pg_stat_statements_shared_blks_hit_total",
        "postgres_pg_stat_statements_shared_blks_read_total",
        "postgres_pg_stat_statements_cache_hit_ratio",
    ];

    for metric_name in expected_metrics {
        let found = metric_families.iter().any(|m| m.name() == metric_name);
        assert!(
            found,
            "Metric {} should exist. Found: {:?}",
            metric_name,
            metric_families.iter().map(|m| m.name()).collect::<Vec<_>>()
        );
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_pg_statements_collector_gracefully_handles_missing_extension() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = PgStatementsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;

    // Should not panic even if extension is missing
    // The collector should just log a warning and continue
    let result = collector.collect(&pool).await;
    assert!(
        result.is_ok(),
        "Collector should handle missing extension gracefully"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_pg_statements_collector_with_top_n_configuration() -> Result<()> {
    let pool = common::create_test_pool().await?;

    // Test with custom top_n value
    let collector = PgStatementsCollector::with_top_n(50);
    let registry = Registry::new();

    collector.register_metrics(&registry)?;

    // Should not error with custom configuration
    let result = collector.collect(&pool).await;
    assert!(result.is_ok());

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_pg_statements_collector_metrics_have_proper_labels() -> Result<()> {
    let pool = common::create_test_pool().await?;

    // Check if extension exists
    let ext_check = sqlx::query("SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'")
        .fetch_optional(&pool)
        .await?;

    if ext_check.is_none() {
        println!("pg_stat_statements extension not installed, skipping test");
        pool.close().await;
        return Ok(());
    }

    // Generate some test queries to ensure we have data
    let _ = sqlx::query("SELECT 1").execute(&pool).await;
    let _ = sqlx::query("SELECT current_timestamp").execute(&pool).await;

    let collector = PgStatementsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Find a metric with labels
    let total_time_metric = metric_families
        .iter()
        .find(|m| m.name() == "postgres_pg_stat_statements_total_exec_time_seconds");

    if let Some(metric) = total_time_metric
        && !metric.get_metric().is_empty()
    {
        let labels = metric.get_metric()[0].get_label();

        // Should have expected label names
        let label_names: Vec<&str> = labels.iter().map(|l| l.name()).collect();

        assert!(
            label_names.contains(&"queryid"),
            "Should have queryid label"
        );
        assert!(
            label_names.contains(&"datname"),
            "Should have datname label"
        );
        assert!(
            label_names.contains(&"usename"),
            "Should have usename label"
        );
        assert!(
            label_names.contains(&"query_short"),
            "Should have query_short label"
        );
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_pg_statements_collector_cache_hit_ratio_is_valid() -> Result<()> {
    let pool = common::create_test_pool().await?;

    // Check if extension exists
    let ext_check = sqlx::query("SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'")
        .fetch_optional(&pool)
        .await?;

    if ext_check.is_none() {
        println!("pg_stat_statements extension not installed, skipping test");
        pool.close().await;
        return Ok(());
    }

    let collector = PgStatementsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Cache hit ratio should be between 0.0 and 1.0
    let cache_hit_ratio = metric_families
        .iter()
        .find(|m| m.name() == "postgres_pg_stat_statements_cache_hit_ratio");

    if let Some(metric) = cache_hit_ratio {
        for m in metric.get_metric() {
            let value = m.get_gauge().value();
            assert!(
                (0.0..=1.0).contains(&value),
                "Cache hit ratio should be between 0.0 and 1.0, got {}",
                value
            );
        }
    }

    pool.close().await;
    Ok(())
}

/// Test that utility statements (VACUUM, ANALYZE, etc.) with NULL query text are handled properly
#[tokio::test]
async fn test_pg_statements_handles_utility_statements() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let ext_check = sqlx::query("SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'")
        .fetch_optional(&pool)
        .await?;

    if ext_check.is_none() {
        println!("pg_stat_statements extension not installed, skipping test");
        pool.close().await;
        return Ok(());
    }

    // Generate utility statements that may have NULL query text
    let _ = sqlx::query("VACUUM").execute(&pool).await;
    let _ = sqlx::query("ANALYZE").execute(&pool).await;

    let collector = PgStatementsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;

    // Should not panic with utility statements
    let result = collector.collect(&pool).await;
    assert!(
        result.is_ok(),
        "Should handle utility statements without panicking"
    );

    pool.close().await;
    Ok(())
}

/// Test that the collector handles queries with various types correctly
/// This specifically tests for the NUMERIC vs BIGINT type mismatch issue
#[tokio::test]
async fn test_pg_statements_handles_numeric_types_correctly() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let ext_check = sqlx::query("SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'")
        .fetch_optional(&pool)
        .await?;

    if ext_check.is_none() {
        println!("pg_stat_statements extension not installed, skipping test");
        pool.close().await;
        return Ok(());
    }

    // Generate diverse queries to ensure pg_stat_statements has data with various numeric types
    for _ in 0..10 {
        let _ = sqlx::query("SELECT 1").execute(&pool).await;
        let _ = sqlx::query("SELECT COUNT(*) FROM pg_stat_statements")
            .execute(&pool)
            .await;
        let _ = sqlx::query("SELECT * FROM pg_stat_statements WHERE queryid IS NOT NULL LIMIT 1")
            .execute(&pool)
            .await;
    }

    let collector = PgStatementsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;

    // Should not panic on type conversions
    let result = collector.collect(&pool).await;
    assert!(
        result.is_ok(),
        "Should handle NUMERIC type conversions without panicking: {:?}",
        result.err()
    );

    // Verify metrics were actually collected
    let metric_families = registry.gather();
    let has_data = metric_families.iter().any(|m| {
        m.name().starts_with("postgres_pg_stat_statements_") && !m.get_metric().is_empty()
    });

    // It's okay if there's no data, but if there is data, it should be valid
    if has_data {
        println!("Successfully collected pg_stat_statements metrics with numeric types");
    }

    pool.close().await;
    Ok(())
}

/// Test that all metrics handle zero/NULL values gracefully
#[tokio::test]
async fn test_pg_statements_handles_edge_case_values() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let ext_check = sqlx::query("SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'")
        .fetch_optional(&pool)
        .await?;

    if ext_check.is_none() {
        println!("pg_stat_statements extension not installed, skipping test");
        pool.close().await;
        return Ok(());
    }

    // Reset stats to ensure we're testing edge cases
    let _ = sqlx::query("SELECT pg_stat_statements_reset()")
        .execute(&pool)
        .await;

    // Generate a minimal query
    let _ = sqlx::query("SELECT 1").execute(&pool).await;

    let collector = PgStatementsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Verify all numeric metrics handle zero/small values correctly
    for family in &metric_families {
        if family.name().starts_with("postgres_pg_stat_statements_") {
            for metric in family.get_metric() {
                // Check that we don't have NaN or Inf values
                let value = metric.get_gauge().value();
                assert!(
                    value.is_finite(),
                    "Metric {} should not have NaN/Inf values, got {}",
                    family.name(),
                    value
                );
            }
        }
    }

    pool.close().await;
    Ok(())
}

/// Test that the collector works correctly with a realistic workload
#[tokio::test]
async fn test_pg_statements_with_realistic_workload() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let ext_check = sqlx::query("SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'")
        .fetch_optional(&pool)
        .await?;

    if ext_check.is_none() {
        println!("pg_stat_statements extension not installed, skipping test");
        pool.close().await;
        return Ok(());
    }

    // Create a test table
    let _ = sqlx::query("CREATE TEMP TABLE test_table (id SERIAL PRIMARY KEY, data TEXT)")
        .execute(&pool)
        .await;

    // Generate a realistic workload with different query types
    for i in 0..20 {
        let _ = sqlx::query("INSERT INTO test_table (data) VALUES ($1)")
            .bind(format!("data_{}", i))
            .execute(&pool)
            .await;
    }

    for _ in 0..30 {
        let _ = sqlx::query("SELECT * FROM test_table WHERE id > $1")
            .bind(5)
            .execute(&pool)
            .await;
    }

    for _ in 0..15 {
        let _ = sqlx::query("UPDATE test_table SET data = $1 WHERE id = $2")
            .bind("updated")
            .bind(1)
            .execute(&pool)
            .await;
    }

    let collector = PgStatementsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Verify we collected metrics
    let calls_metric = metric_families
        .iter()
        .find(|m| m.name() == "postgres_pg_stat_statements_calls_total");

    assert!(calls_metric.is_some(), "Should have calls_total metric");

    if let Some(metric) = calls_metric {
        let total_calls: i64 = metric
            .get_metric()
            .iter()
            .map(|m| m.get_gauge().value() as i64)
            .sum();

        assert!(
            total_calls > 0,
            "Should have recorded some calls, got {}",
            total_calls
        );
    }

    pool.close().await;
    Ok(())
}
