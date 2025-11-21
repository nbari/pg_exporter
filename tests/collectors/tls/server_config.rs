#[allow(clippy::duplicate_mod)]
#[path = "../../common/mod.rs"]
mod common;

use anyhow::Result;
use pg_exporter::collectors::Collector;
use pg_exporter::collectors::tls::server_config::ServerTlsConfigCollector;
use prometheus::Registry;

#[tokio::test]
async fn test_server_tls_config_collector_registers_without_error() -> Result<()> {
    let collector = ServerTlsConfigCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;

    Ok(())
}

#[tokio::test]
async fn test_server_tls_config_collector_name() {
    let collector = ServerTlsConfigCollector::new();
    assert_eq!(collector.name(), "tls.server_config");
}

#[tokio::test]
async fn test_server_tls_config_collector_disabled_by_default() {
    let collector = ServerTlsConfigCollector::new();
    assert!(!collector.enabled_by_default());
}

#[tokio::test]
async fn test_server_tls_config_collector_collects_ssl_status() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = ServerTlsConfigCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Find pg_ssl_enabled metric
    let ssl_enabled = metric_families
        .iter()
        .find(|m| m.name() == "pg_ssl_enabled")
        .expect("pg_ssl_enabled metric should exist");

    assert_eq!(
        ssl_enabled.get_field_type(),
        prometheus::proto::MetricType::GAUGE
    );
    assert!(!ssl_enabled.get_metric().is_empty());

    // Value should be either 0 or 1
    let value = common::metric_value_to_i64(ssl_enabled.get_metric()[0].get_gauge().value());
    assert!(value == 0 || value == 1);

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_server_tls_config_collector_handles_query_errors_gracefully() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = ServerTlsConfigCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;

    // Should not panic even if database connection has issues
    let result = collector.collect(&pool).await;
    assert!(result.is_ok());

    pool.close().await;
    Ok(())
}
