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

/// Registration must accept all four metric names, proven by a second registration of the
/// same names failing rather than by looking for them in `gather()`.
///
/// The metrics are zero-label vectors, so nothing is exposed until a value is set. Asserting
/// on `gather()` right after registration is what the previous version of this test did, and
/// it passed for the wrong reason: the scalars existed at `0` from registration alone, which
/// is precisely the false "certificate invalid" reading this design removes.
#[tokio::test]
async fn test_certificate_collector_registers_all_metric_names() -> Result<()> {
    let registry = Registry::new();
    CertificateCollector::new().register_metrics(&registry)?;

    assert!(
        CertificateCollector::new()
            .register_metrics(&registry)
            .is_err(),
        "re-registering the same metric names must conflict, proving they were registered"
    );

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

/// With no certificate configured, the collector must publish **nothing** rather than a
/// zeroed snapshot.
///
/// A zeroed `pg_ssl_certificate_valid` reads as "the certificate is invalid" and a zeroed
/// `pg_ssl_certificate_expiry_seconds` as "it expires now" — both false alarms, and both
/// worse than no data. Absence is the honest answer, which is why these metrics are
/// zero-label vectors: they can be removed, not just set to zero.
#[tokio::test]
async fn test_certificate_collector_publishes_nothing_without_a_certificate() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = CertificateCollector::new();
    let registry = Registry::new();
    collector.register_metrics(&registry)?;

    let configured: Option<String> = sqlx::query_scalar("SHOW ssl_cert_file")
        .fetch_one(&pool)
        .await
        .ok()
        .filter(|path: &String| !path.is_empty());

    collector.collect(&pool).await?;

    if configured.is_some() {
        println!("ssl_cert_file is configured on this server, skipping the absence assertion");
        pool.close().await;
        return Ok(());
    }

    for family in registry.gather() {
        assert!(
            family.get_metric().is_empty(),
            "{} must expose no sample without a configured certificate, got {:?}",
            family.name(),
            family
                .get_metric()
                .iter()
                .map(|m| m.get_gauge().value())
                .collect::<Vec<_>>()
        );
    }

    pool.close().await;
    Ok(())
}
