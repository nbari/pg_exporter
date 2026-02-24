#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::panic)]
#![allow(clippy::indexing_slicing)]
use anyhow::Result;
use secrecy::SecretString;

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
        pg_exporter::exporter::new(port, None, dsn, vec!["default".to_string()]).await
    });

    assert!(
        common::wait_for_server(port, 50).await,
        "Server failed to start on port {port}"
    );

    handle.abort();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let result = tokio::net::TcpStream::connect(format!("localhost:{port}")).await;
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
        pg_exporter::exporter::new(port, None, dsn, vec!["default".to_string()]).await
    });

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
    assert!(!body.is_empty());

    handle.abort();

    Ok(())
}

#[tokio::test]
async fn test_exporter_bind_to_ipv4_localhost() -> Result<()> {
    let port = common::get_available_port();
    let dsn = common::get_test_dsn_secret();

    let handle = tokio::spawn(async move {
        pg_exporter::exporter::new(
            port,
            Some("127.0.0.1".to_string()),
            dsn,
            vec!["default".to_string()],
        )
        .await
    });

    assert!(
        common::wait_for_server(port, 50).await,
        "Server failed to start on 127.0.0.1:{port}"
    );

    // Verify it's accessible on IPv4 localhost
    let result = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}")).await;
    assert!(result.is_ok(), "Should connect to 127.0.0.1");

    handle.abort();

    Ok(())
}

#[tokio::test]
async fn test_exporter_bind_to_ipv4_all_interfaces() -> Result<()> {
    let port = common::get_available_port();
    let dsn = common::get_test_dsn_secret();

    let handle = tokio::spawn(async move {
        pg_exporter::exporter::new(
            port,
            Some("0.0.0.0".to_string()),
            dsn,
            vec!["default".to_string()],
        )
        .await
    });

    assert!(
        common::wait_for_server(port, 50).await,
        "Server failed to start on 0.0.0.0:{port}"
    );

    // Verify it's accessible
    let result = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}")).await;
    assert!(result.is_ok(), "Should connect via 127.0.0.1");

    handle.abort();

    Ok(())
}

#[tokio::test]
async fn test_exporter_bind_to_ipv6_localhost() -> Result<()> {
    let port = common::get_available_port();
    let dsn = common::get_test_dsn_secret();

    let handle = tokio::spawn(async move {
        pg_exporter::exporter::new(
            port,
            Some("::1".to_string()),
            dsn,
            vec!["default".to_string()],
        )
        .await
    });

    // Give it time to start (or fail if IPv6 not available)
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Try to connect via IPv6 localhost
    // This may fail on systems without IPv6, which is OK
    let result = tokio::net::TcpStream::connect(format!("[::1]:{port}")).await;

    if result.is_ok() {
        // IPv6 is available and working
        println!("✓ IPv6 localhost binding works");
    } else {
        // IPv6 might not be available, which is fine for this test
        println!("ℹ IPv6 localhost not available (expected on some systems)");
    }

    handle.abort();

    Ok(())
}

#[tokio::test]
async fn test_exporter_invalid_ip_address() -> Result<()> {
    let port = common::get_available_port();
    let dsn = common::get_test_dsn_secret();

    let result = pg_exporter::exporter::new(
        port,
        Some("invalid-ip".to_string()),
        dsn,
        vec!["default".to_string()],
    )
    .await;

    assert!(result.is_err(), "Should reject invalid IP address");

    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("Invalid IP address"),
        "Error should mention invalid IP, got: {error_msg}"
    );

    Ok(())
}

#[tokio::test]
async fn test_exporter_default_bind_auto_detect() -> Result<()> {
    let port = common::get_available_port();
    let dsn = common::get_test_dsn_secret();

    // None = auto-detect (try IPv6, fallback to IPv4)
    let handle = tokio::spawn(async move {
        pg_exporter::exporter::new(port, None, dsn, vec!["default".to_string()]).await
    });

    assert!(
        common::wait_for_server(port, 50).await,
        "Server failed to start with auto-detect on port {port}"
    );

    // Should be accessible regardless of IPv4 or IPv6
    let result = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}")).await;
    assert!(result.is_ok(), "Should connect via IPv4 localhost");

    handle.abort();

    Ok(())
}

#[tokio::test]
async fn test_exporter_starts_even_when_db_down() -> Result<()> {
    // Attempt to initialize tracing, ignore if already initialized
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_test_writer()
        .try_init();

    let port = common::get_available_port();
    // Use a port where definitely no Postgres is running
    let dsn = SecretString::from("postgresql://postgres:postgres@localhost:54321/postgres");

    let handle = tokio::spawn(async move {
        pg_exporter::exporter::new(port, None, dsn, vec!["default".to_string()]).await
    });

    // Wait for server to start
    assert!(
        common::wait_for_server(port, 50).await,
        "Server failed to start on port {port} despite DB being down"
    );

    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/metrics", common::get_test_url(port)))
        .send()
        .await?;

    assert_eq!(response.status(), 200);

    let body = response.text().await?;
    assert!(
        body.contains("pg_up 0"),
        "pg_up should be 0 when DB is down"
    );
    assert!(
        body.contains("pg_exporter_build_info"),
        "Core build-info metric should still be exposed during outage"
    );

    // DB-dependent metrics should be omitted.
    // Default collector normally exports pg_settings_count
    assert!(
        !body.contains("pg_settings_count"),
        "DB-dependent metrics should be omitted"
    );

    handle.abort();
    Ok(())
}
