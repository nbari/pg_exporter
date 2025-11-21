#[allow(clippy::duplicate_mod)]
#[path = "../../common/mod.rs"]
mod common;

use anyhow::Result;
use pg_exporter::collectors::Collector;
use pg_exporter::collectors::tls::connection_stats::ConnectionTlsCollector;
use prometheus::Registry;

#[tokio::test]
async fn test_connection_tls_collector_registers_without_error() -> Result<()> {
    let collector = ConnectionTlsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;

    Ok(())
}

#[tokio::test]
async fn test_connection_tls_collector_name() {
    let collector = ConnectionTlsCollector::new();
    assert_eq!(collector.name(), "tls.connection_stats");
}

#[tokio::test]
async fn test_connection_tls_collector_disabled_by_default() {
    let collector = ConnectionTlsCollector::new();
    assert!(!collector.enabled_by_default());
}

#[tokio::test]
async fn test_connection_tls_collector_has_all_metrics() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let collector = ConnectionTlsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Only Gauge metrics appear immediately after registration
    // GaugeVec metrics (with labels) only appear after at least one label value is set
    let expected_gauge_metrics = vec!["pg_ssl_connections_total", "pg_ssl_connection_bits_avg"];

    for metric_name in expected_gauge_metrics {
        let found = metric_families.iter().any(|m| m.name() == metric_name);
        assert!(found, "Metric {metric_name} should be registered");
    }

    // Note: pg_ssl_connections_by_version and pg_ssl_connections_by_cipher are GaugeVec
    // They won't appear in registry.gather() until they have at least one label value set
    // which only happens when there are active SSL connections

    Ok(())
}

#[tokio::test]
async fn test_connection_tls_collector_handles_old_postgres_gracefully() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = ConnectionTlsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;

    // Should not panic even on PostgreSQL versions without pg_stat_ssl
    let result = collector.collect(&pool).await;
    assert!(
        result.is_ok(),
        "Collector should handle missing pg_stat_ssl view gracefully"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_connection_tls_collector_collects_from_database() -> Result<()> {
    let pool = common::create_test_pool().await?;

    // Check if pg_stat_ssl view exists (PostgreSQL 9.5+)
    let has_pg_stat_ssl = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM pg_catalog.pg_class WHERE relname = 'pg_stat_ssl'",
    )
    .fetch_one(&pool)
    .await?
        > 0;

    if !has_pg_stat_ssl {
        println!("pg_stat_ssl view not available, skipping detailed test");
        pool.close().await;
        return Ok(());
    }

    let collector = ConnectionTlsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Verify metrics exist
    let connections_total = metric_families
        .iter()
        .find(|m| m.name() == "pg_ssl_connections_total");

    assert!(
        connections_total.is_some(),
        "pg_ssl_connections_total metric should exist"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_connection_tls_collector_metrics_have_correct_types() -> Result<()> {
    let collector = ConnectionTlsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;

    let metric_families = registry.gather();

    for family in &metric_families {
        assert_eq!(
            family.get_field_type(),
            prometheus::proto::MetricType::GAUGE,
            "Metric {} should be a GAUGE",
            family.name()
        );
    }

    Ok(())
}
