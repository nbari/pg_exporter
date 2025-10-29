use anyhow::Result;
use serde_json::Value;

mod common;

#[tokio::test]
async fn test_health_endpoint_returns_ok() -> Result<()> {
    let port = common::get_available_port();
    let dsn = common::get_test_dsn_secret();

    let handle = tokio::spawn(async move {
        pg_exporter::exporter::new(port, None, dsn, vec!["default".to_string()]).await
    });

    assert!(
        common::wait_for_server(port, 50).await,
        "Server failed to start"
    );

    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/health", common::get_test_url(port)))
        .send()
        .await?;

    assert_eq!(response.status(), 200);

    let body: Value = response.json().await?;
    assert_eq!(body["name"], env!("CARGO_PKG_NAME"));
    assert_eq!(body["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(body["database"], "ok");
    assert!(body["commit"].is_string());

    handle.abort();

    Ok(())
}

#[tokio::test]
async fn test_health_endpoint_options_request() -> Result<()> {
    let port = common::get_available_port();
    let dsn = common::get_test_dsn_secret();

    let handle = tokio::spawn(async move {
        pg_exporter::exporter::new(port, None, dsn, vec!["default".to_string()]).await
    });

    assert!(common::wait_for_server(port, 50).await);

    let client = reqwest::Client::new();
    let response = client
        .request(
            reqwest::Method::OPTIONS,
            format!("{}/health", common::get_test_url(port)),
        )
        .send()
        .await?;

    assert_eq!(response.status(), 200);

    handle.abort();

    Ok(())
}

#[tokio::test]
async fn test_health_endpoint_has_x_app_header() -> Result<()> {
    let port = common::get_available_port();
    let dsn = common::get_test_dsn_secret();

    let handle = tokio::spawn(async move {
        pg_exporter::exporter::new(port, None, dsn, vec!["default".to_string()]).await
    });

    assert!(common::wait_for_server(port, 50).await);

    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/health", common::get_test_url(port)))
        .send()
        .await?;

    let x_app = response
        .headers()
        .get("X-App")
        .expect("X-App header should be present");

    let x_app_str = x_app.to_str()?;
    assert!(x_app_str.contains(env!("CARGO_PKG_NAME")));
    assert!(x_app_str.contains(env!("CARGO_PKG_VERSION")));

    handle.abort();

    Ok(())
}
