#![allow(dead_code)]

use anyhow::Result;
use secrecy::SecretString;
use sqlx::PgPool;
use std::env;

/// Get the test database DSN from environment
///
/// SAFETY: Tests should ALWAYS run against localhost to avoid accidentally
/// running against production databases. If PG_EXPORTER_DSN is set (e.g., in .envrc),
/// we verify it points to localhost. Use 'just test' which handles this automatically.
pub fn get_test_dsn() -> String {
    let dsn = env::var("PG_EXPORTER_DSN")
        .unwrap_or_else(|_| "postgresql://postgres:postgres@localhost:5432/postgres".to_string());

    // Safety check: ensure we're not accidentally testing against a remote database
    if !dsn.contains("localhost") && !dsn.contains("127.0.0.1") && !dsn.contains("::1") {
        eprintln!("⚠️  WARNING: PG_EXPORTER_DSN points to a remote database!");
        eprintln!("⚠️  DSN: {}", dsn.replace(char::is_alphanumeric, "*"));
        eprintln!("⚠️  Tests should run against localhost only.");
        eprintln!("⚠️  Use: just test (handles this automatically)");
        eprintln!(
            "⚠️  Or:   PG_EXPORTER_DSN='postgresql://postgres:postgres@localhost:5432/postgres' cargo test"
        );
        panic!("Refusing to run tests against remote database. Use localhost.");
    }

    dsn
}

/// Create a test database pool
pub async fn create_test_pool() -> Result<PgPool> {
    let dsn = get_test_dsn();
    let pool = PgPool::connect(&dsn).await?;
    Ok(pool)
}

/// Get test DSN as SecretString
pub fn get_test_dsn_secret() -> SecretString {
    SecretString::from(get_test_dsn())
}

/// Find an available port for testing (returns port > 1024)
pub fn get_available_port() -> u16 {
    use std::net::TcpListener;

    // Bind to port 0 lets the OS assign an available ephemeral port
    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind to random port");
    let port = listener
        .local_addr()
        .expect("Failed to get local addr")
        .port();

    // Ephemeral ports are typically > 1024, usually 32768-60999 on Linux
    assert!(port > 1024, "Assigned port {} should be > 1024", port);

    port
}

/// Wait for server to be ready on the given port
///
/// # Arguments
/// * `port` - The port number to connect to (should be > 1024)
/// * `max_attempts` - Maximum number of connection attempts (e.g., 50 = 5 seconds at 100ms intervals)
pub async fn wait_for_server(port: u16, max_attempts: u32) -> bool {
    use tokio::time::{Duration, sleep};

    for attempt in 1..=max_attempts {
        // Use localhost which will try both IPv4 and IPv6
        if tokio::net::TcpStream::connect(format!("localhost:{}", port))
            .await
            .is_ok()
        {
            return true;
        }

        if attempt % 10 == 0 {
            eprintln!(
                "Still waiting for server on port {} (attempt {}/{})",
                port, attempt, max_attempts
            );
        }

        sleep(Duration::from_millis(100)).await;
    }

    eprintln!(
        "Failed to connect to server on port {} after {} attempts",
        port, max_attempts
    );
    false
}

/// Get base URL for test server
pub fn get_test_url(port: u16) -> String {
    format!("http://localhost:{}", port)
}
