use anyhow::Result;

mod common;

#[tokio::test]
async fn test_exporter_database_connection() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let row: (i32,) = sqlx::query_as("SELECT 1").fetch_one(&pool).await?;

    assert_eq!(row.0, 1);

    pool.close().await;

    Ok(())
}

#[tokio::test]
async fn test_exporter_starts_and_stops() -> Result<()> {
    let port = common::get_available_port();
    let dsn = common::get_test_dsn_secret();

    let handle = tokio::spawn(async move {
        pg_exporter::exporter::new(port, dsn, vec!["default".to_string()]).await
    });

    assert!(
        common::wait_for_server(port, 50).await,
        "Server failed to start on port {}",
        port
    );

    handle.abort();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let result = tokio::net::TcpStream::connect(format!("localhost:{}", port)).await;
    assert!(result.is_err(), "Server should be stopped");

    Ok(())
}

#[tokio::test]
async fn test_exporter_with_excluded_databases() -> Result<()> {
    use pg_exporter::collectors::util::set_excluded_databases;

    set_excluded_databases(vec!["template0".to_string(), "template1".to_string()]);

    let port = common::get_available_port();
    let dsn = common::get_test_dsn_secret();

    let handle = tokio::spawn(async move {
        pg_exporter::exporter::new(port, dsn, vec!["default".to_string()]).await
    });

    assert!(
        common::wait_for_server(port, 50).await,
        "Server failed to start on port {}",
        port
    );

    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/metrics", common::get_test_url(port)))
        .send()
        .await?;

    assert_eq!(response.status(), 200);

    let body = response.text().await?;
    assert!(!body.is_empty());

    handle.abort();

    Ok(())
}
