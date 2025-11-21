#[allow(clippy::duplicate_mod)]
#[path = "../../common/mod.rs"]
mod common;

use anyhow::Result;
use pg_exporter::collectors::Collector;
use pg_exporter::collectors::tls::certificate::CertificateCollector;
use prometheus::Registry;

#[tokio::test]
async fn test_certificate_collector_registers_without_error() -> Result<()> {
    let collector = CertificateCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;

    Ok(())
}

#[tokio::test]
async fn test_certificate_collector_name() {
    let collector = CertificateCollector::new();
    assert_eq!(collector.name(), "tls.certificate");
}

#[tokio::test]
async fn test_certificate_collector_disabled_by_default() {
    let collector = CertificateCollector::new();
    assert!(!collector.enabled_by_default());
}

#[tokio::test]
async fn test_certificate_collector_has_all_metrics() -> Result<()> {
    let collector = CertificateCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;

    let metric_families = registry.gather();

    let expected_metrics = vec![
        "pg_ssl_certificate_expiry_seconds",
        "pg_ssl_certificate_valid",
        "pg_ssl_certificate_not_before_timestamp",
        "pg_ssl_certificate_not_after_timestamp",
    ];

    for metric_name in expected_metrics {
        let found = metric_families.iter().any(|m| m.name() == metric_name);
        assert!(found, "Metric {metric_name} should be registered");
    }

    Ok(())
}

#[tokio::test]
async fn test_certificate_collector_handles_missing_cert_gracefully() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = CertificateCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;

    // Should not panic even if certificate file is not configured or doesn't exist
    let result = collector.collect(&pool).await;
    assert!(
        result.is_ok(),
        "Collector should handle missing certificate gracefully"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_certificate_collector_collects_from_database() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = CertificateCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Verify metrics were registered (they may not have values if SSL is not configured)
    assert!(!metric_families.is_empty());

    pool.close().await;
    Ok(())
}
