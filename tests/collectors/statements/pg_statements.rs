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
