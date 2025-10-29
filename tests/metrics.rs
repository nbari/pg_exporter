use anyhow::Result;

mod common;

#[tokio::test]
async fn test_metrics_endpoint_returns_prometheus_format() -> Result<()> {
    let port = common::get_available_port();
    let dsn = common::get_test_dsn_secret();

    let handle = tokio::spawn(async move {
        pg_exporter::exporter::new(port, None, dsn, vec!["default".to_string()]).await
    });

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

    assert!(body.contains("# HELP"));
    assert!(body.contains("# TYPE"));
    assert!(body.contains("pg_up"));

    handle.abort();

    Ok(())
}

#[tokio::test]
async fn test_metrics_endpoint_with_multiple_collectors() -> Result<()> {
    let port = common::get_available_port();
    let dsn = common::get_test_dsn_secret();

    let collectors = vec![
        "default".to_string(),
        "activity".to_string(),
        "vacuum".to_string(),
        "database".to_string(),
        "locks".to_string(),
        "stat".to_string(),
        "replication".to_string(),
        "index".to_string(),
        "statements".to_string(),
    ];

    let handle =
        tokio::spawn(async move { pg_exporter::exporter::new(port, None, dsn, collectors).await });

    assert!(common::wait_for_server(port, 50).await);

    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/metrics", common::get_test_url(port)))
        .send()
        .await?;

    assert_eq!(response.status(), 200);

    let body = response.text().await?;

    assert!(body.contains("pg_up"));
    assert!(body.contains("pg_stat_activity") || body.contains("pg_connections"));

    handle.abort();

    Ok(())
}

#[tokio::test]
async fn test_metrics_endpoint_performance() -> Result<()> {
    let port = common::get_available_port();
    let dsn = common::get_test_dsn_secret();

    let handle = tokio::spawn(async move {
        pg_exporter::exporter::new(port, None, dsn, vec!["default".to_string()]).await
    });

    assert!(common::wait_for_server(port, 50).await);

    let client = reqwest::Client::new();

    for _ in 0..3 {
        let start = std::time::Instant::now();
        let response = client
            .get(format!("{}/metrics", common::get_test_url(port)))
            .send()
            .await?;

        let duration = start.elapsed();

        assert_eq!(response.status(), 200);

        assert!(
            duration.as_secs() < 5,
            "Metrics collection took too long: {:?}",
            duration
        );
    }

    handle.abort();

    Ok(())
}
