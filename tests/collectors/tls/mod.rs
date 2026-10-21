mod certificate;
mod connection_stats;
mod server_config;

#[allow(clippy::duplicate_mod)]
#[path = "../../common/mod.rs"]
mod common;

use anyhow::Result;
use pg_exporter::collectors::Collector;
use pg_exporter::collectors::tls::TlsCollector;
use prometheus::Registry;

/// Test that TLS collector registers metrics without error
#[tokio::test]
async fn test_tls_collector_registers_without_error() -> Result<()> {
    let collector = TlsCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;

    Ok(())
}

/// Test TLS collector name
#[tokio::test]
async fn test_tls_collector_name() {
    let collector = TlsCollector::new();
    assert_eq!(collector.name(), "tls");
}

/// Test TLS collector is disabled by default
#[tokio::test]
async fn test_tls_collector_disabled_by_default() {
    let collector = TlsCollector::new();
    assert!(!collector.enabled_by_default());
}
