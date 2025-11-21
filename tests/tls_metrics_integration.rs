#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::panic)]
#![allow(clippy::indexing_slicing)]
//! Integration test for TLS collector metrics endpoint
//! This test verifies that TLS metrics are properly exposed via the /metrics endpoint

#[allow(clippy::duplicate_mod)]
#[path = "common/mod.rs"]
mod common;

use anyhow::Result;

#[tokio::test]
async fn test_tls_metrics_endpoint_returns_ssl_metrics() -> Result<()> {
    let port = common::get_available_port();
    let dsn = common::get_test_dsn_secret();

    let collectors = vec!["default".to_string(), "tls".to_string()];

    let handle =
        tokio::spawn(async move { pg_exporter::exporter::new(port, None, dsn, collectors).await });

    assert!(
        common::wait_for_server(port, 50).await,
        "Server failed to start on port {port}"
    );

    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/metrics", common::get_test_url(port)))
        .send()
        .await?;

    assert_eq!(response.status(), 200);

    let body = response.text().await?;

    // Verify TLS metrics are present (or at least registered)
    let expected_metrics = vec![
        "pg_ssl_enabled",
        "pg_ssl_certificate_expiry_seconds",
        "pg_ssl_certificate_valid",
        "pg_ssl_connections_total",
    ];

    for metric_name in expected_metrics {
        assert!(
            body.contains(metric_name),
            "Metric {metric_name} should be in metrics output"
        );
    }

    // Verify it's in Prometheus format
    assert!(body.contains("# HELP"));
    assert!(body.contains("# TYPE"));

    handle.abort();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    Ok(())
}

#[tokio::test]
async fn test_tls_collector_can_be_enabled() -> Result<()> {
    let port = common::get_available_port();
    let dsn = common::get_test_dsn_secret();

    // Enable only TLS collector
    let collectors = vec!["tls".to_string()];

    let handle =
        tokio::spawn(async move { pg_exporter::exporter::new(port, None, dsn, collectors).await });

    assert!(
        common::wait_for_server(port, 50).await,
        "Server failed to start with TLS collector"
    );

    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/metrics", common::get_test_url(port)))
        .send()
        .await?;

    assert_eq!(response.status(), 200);

    let body = response.text().await?;

    // Should have TLS metrics
    assert!(body.contains("pg_ssl") || body.contains("pg_tls"));

    // Should have pg_up metric (always present)
    assert!(body.contains("pg_up"));

    handle.abort();

    Ok(())
}

#[tokio::test]
async fn test_metrics_format_is_valid_prometheus() -> Result<()> {
    let port = common::get_available_port();
    let dsn = common::get_test_dsn_secret();

    let collectors = vec!["tls".to_string()];

    let handle =
        tokio::spawn(async move { pg_exporter::exporter::new(port, None, dsn, collectors).await });

    assert!(common::wait_for_server(port, 50).await);

    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/metrics", common::get_test_url(port)))
        .send()
        .await?;

    assert_eq!(response.status(), 200);

    let content_type = response
        .headers()
        .get("content-type")
        .expect("Content-Type header should be present");
    assert_eq!(content_type, "text/plain; charset=utf-8");

    let body = response.text().await?;

    // Verify Prometheus format
    assert!(body.contains("# HELP"));
    assert!(body.contains("# TYPE"));

    // Verify at least one TLS metric is documented
    let has_tls_help = body.contains("# HELP pg_ssl") || body.contains("# HELP pg_tls");
    assert!(has_tls_help, "Should have HELP text for TLS metrics");

    handle.abort();

    Ok(())
}
