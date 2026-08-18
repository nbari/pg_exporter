#[allow(clippy::duplicate_mod)]
#[path = "../../common/mod.rs"]
mod common;

use anyhow::Result;
use pg_exporter::collectors::Collector;
use pg_exporter::collectors::tls::server_config::ServerTlsConfigCollector;
use prometheus::Registry;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

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

/// `pg_ssl_enabled` must never be published as `0` when the server could not be asked, and
/// a genuine fault must **preserve** the previous reading rather than clear it.
///
/// Three outcomes, deliberately distinct:
///   - the setting reads fine        -> publish it
///   - absent or permission denied   -> skip, which clears the series
///   - anything else (a fault)       -> propagate, so the last good value survives
///
/// A zeroed `pg_ssl_enabled` would assert "TLS is disabled" when the truth is "unknown",
/// which is a security-relevant false negative. Filing a transient fault as a skip would be
/// the opposite error: it destroys the last good snapshot instead of keeping it.
#[tokio::test]
async fn test_server_tls_config_collector_never_claims_tls_is_off() -> Result<()> {
    let good = common::create_test_pool().await?;
    let collector = ServerTlsConfigCollector::new();
    let registry = Registry::new();
    collector.register_metrics(&registry)?;

    collector.collect(&good).await?;
    let seeded = registry
        .gather()
        .iter()
        .find(|f| f.name() == "pg_ssl_enabled")
        .map_or(0, |f| f.get_metric().len());
    assert_eq!(
        seeded, 1,
        "expected pg_ssl_enabled to be published from a live server"
    );
    good.close().await;

    // An unreachable server is a fault, not a known absence.
    let broken = PgPoolOptions::new()
        .acquire_timeout(Duration::from_millis(100))
        .connect_lazy("postgresql://postgres:postgres@127.0.0.1:1/postgres")?;

    let result = collector.collect(&broken).await;
    assert!(
        result.is_err(),
        "an unreachable server is a genuine fault and must propagate, not skip"
    );

    let after = registry
        .gather()
        .iter()
        .find(|f| f.name() == "pg_ssl_enabled")
        .map_or(0, |f| f.get_metric().len());
    assert_eq!(
        after, 1,
        "a fault must preserve the last good reading, not clear it"
    );

    let value = registry
        .gather()
        .iter()
        .find(|f| f.name() == "pg_ssl_enabled")
        .and_then(|f| f.get_metric().first().map(|m| m.get_gauge().value()));
    assert!(
        value.is_some(),
        "pg_ssl_enabled must still hold its previous value after a fault"
    );

    Ok(())
}
