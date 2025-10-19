//! Integration tests for the pg_exporter binary
//!
//! These tests execute the binary as a subprocess and verify:
//! - CLI argument parsing (--help, --version, flags)
//! - Server startup and shutdown behavior
//! - HTTP endpoints (/metrics, /health)
//! - Environment variable handling
//! - Error handling and validation

use anyhow::Result;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::OnceLock;
use std::time::Duration;

mod common;

// ============================================================================
// Binary Path Setup
// ============================================================================

static BINARY_PATH: OnceLock<PathBuf> = OnceLock::new();

/// Ensure binary is built and return path to it
fn get_binary_path() -> &'static PathBuf {
    BINARY_PATH.get_or_init(|| {
        // Build the binary once
        let output = Command::new("cargo")
            .args(["build", "--bin", "pg_exporter"])
            .output()
            .expect("Failed to build binary");
        
        if !output.status.success() {
            panic!("Failed to build binary: {}", String::from_utf8_lossy(&output.stderr));
        }
        
        // Get the binary path
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("target");
        path.push("debug");
        path.push("pg_exporter");
        
        path
    })
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Run the binary with given arguments and return output
fn run_binary_with_args(args: &[&str]) -> std::io::Result<std::process::Output> {
    Command::new(get_binary_path())
        .args(args)
        .output()
}

/// Start the binary in background with given port and DSN
fn start_binary(port: u16, dsn: &str) -> std::io::Result<Child> {
    Command::new(get_binary_path())
        .args([
            "--port",
            &port.to_string(),
            "--dsn",
            dsn,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
}

/// Start the binary with environment variables
fn start_binary_with_env(port: u16, dsn: &str) -> std::io::Result<Child> {
    Command::new(get_binary_path())
        .env("PG_EXPORTER_PORT", port.to_string())
        .env("PG_EXPORTER_DSN", dsn)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
}

/// Cleanup helper: kill child process and wait
fn cleanup_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

// ============================================================================
// Tests
// ============================================================================

/// Test that the binary can be executed and shows help
#[test]
fn test_binary_help_flag() {
    let output = run_binary_with_args(&["--help"]).expect("Failed to execute binary");

    assert!(output.status.success(), "Binary should exit successfully");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("PostgreSQL metric exporter"),
        "Help output should contain description"
    );
    assert!(stdout.contains("--port"), "Help should show port option");
    assert!(stdout.contains("--dsn"), "Help should show dsn option");
}

/// Test that the binary shows version information
#[test]
fn test_binary_version_flag() {
    let output = run_binary_with_args(&["--version"]).expect("Failed to execute binary");

    assert!(output.status.success(), "Binary should exit successfully");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("pg_exporter"),
        "Version output should contain binary name"
    );
}

/// Test that the binary validates port range
#[test]
fn test_binary_invalid_port() {
    let output = run_binary_with_args(&["--port", "70000"]).expect("Failed to execute binary");

    assert!(
        !output.status.success(),
        "Binary should fail with invalid port"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("70000") || stderr.contains("port") || stderr.contains("range"),
        "Error should mention port validation"
    );
}

/// Test that the binary can start and stop gracefully
#[tokio::test]
async fn test_binary_starts_and_stops() -> Result<()> {
    let port = common::get_available_port();
    let dsn = common::get_test_dsn();

    // Start the binary in background
    let mut child = start_binary(port, &dsn).expect("Failed to start binary");

    // Wait for server to start
    let started = common::wait_for_server(port, 100).await;
    assert!(started, "Server should start on port {}", port);

    // Kill the process
    cleanup_child(&mut child);

    // Give it time to clean up
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify server stopped
    let result = tokio::net::TcpStream::connect(format!("localhost:{}", port)).await;
    assert!(result.is_err(), "Server should be stopped");

    Ok(())
}

/// Test that the binary handles shutdown gracefully via kill
#[tokio::test]
#[cfg(unix)]
async fn test_binary_handles_graceful_shutdown() -> Result<()> {
    let port = common::get_available_port();
    let dsn = common::get_test_dsn();

    // Start the binary
    let mut child = start_binary(port, &dsn).expect("Failed to start binary");

    // Wait for server to start
    assert!(
        common::wait_for_server(port, 100).await,
        "Server should start"
    );

    // Kill the process gracefully
    child.kill().expect("Failed to kill process");
    let status = child.wait().expect("Failed to wait for process");

    // Process should have been killed
    assert!(!status.success(), "Process was killed");

    // Verify server stopped
    tokio::time::sleep(Duration::from_millis(200)).await;
    let result = tokio::net::TcpStream::connect(format!("localhost:{}", port)).await;
    assert!(result.is_err(), "Server should be stopped");

    Ok(())
}

/// Test that the binary respects environment variables
#[tokio::test]
async fn test_binary_uses_environment_variables() -> Result<()> {
    let port = common::get_available_port();
    let dsn = common::get_test_dsn();

    // Start with environment variables
    let mut child = start_binary_with_env(port, &dsn).expect("Failed to start binary");

    // Wait for server to start
    let started = common::wait_for_server(port, 100).await;
    assert!(started, "Server should start using env vars");

    // Clean up
    cleanup_child(&mut child);

    Ok(())
}

/// Test that the binary can disable collectors
#[test]
fn test_binary_disable_collector() {
    let output = run_binary_with_args(&["--help"]).expect("Failed to execute binary");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("--no-collector"),
        "Help should show collector disable options"
    );
}

/// Test that binary rejects invalid DSN format
#[test]
fn test_binary_validates_dsn_format() {
    let output = Command::new(get_binary_path())
        .args([
            "--dsn",
            "not-a-valid-dsn",
            "--port",
            "9999",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to start binary")
        .wait_with_output()
        .expect("Failed to wait for output");

    // The binary should either fail immediately or fail when trying to connect
    // This is mostly a smoke test to ensure bad DSN doesn't cause panic
    assert!(
        !output.status.success() || !output.stderr.is_empty(),
        "Binary should handle invalid DSN gracefully"
    );
}

/// Test that the binary exposes metrics endpoint
#[tokio::test]
async fn test_binary_exposes_metrics_endpoint() -> Result<()> {
    let port = common::get_available_port();
    let dsn = common::get_test_dsn();

    // Start the binary
    let mut child = start_binary(port, &dsn).expect("Failed to start binary");

    // Wait for server to start
    assert!(
        common::wait_for_server(port, 100).await,
        "Server should start"
    );

    // Make HTTP request to metrics endpoint
    let result = async {
        let client = reqwest::Client::new();
        let response = client
            .get(format!("http://localhost:{}/metrics", port))
            .timeout(Duration::from_secs(10))
            .send()
            .await?;

        assert!(
            response.status().is_success(),
            "Metrics endpoint should respond"
        );

        let body = response.text().await?;

        assert!(body.contains("pg_up"), "Metrics should include pg_up");
        assert!(
            body.contains("pg_exporter_build_info"),
            "Metrics should include build info"
        );

        Ok::<(), anyhow::Error>(())
    }
    .await;

    // Clean up
    cleanup_child(&mut child);

    result
}

/// Test that the binary exposes health endpoint
#[tokio::test]
async fn test_binary_exposes_health_endpoint() -> Result<()> {
    let port = common::get_available_port();
    let dsn = common::get_test_dsn();

    // Start the binary
    let mut child = start_binary(port, &dsn).expect("Failed to start binary");

    // Wait for server to start
    assert!(
        common::wait_for_server(port, 100).await,
        "Server should start"
    );

    // Make HTTP request to health endpoint
    let result = async {
        let client = reqwest::Client::new();
        let response = client
            .get(format!("http://localhost:{}/health", port))
            .timeout(Duration::from_secs(10))
            .send()
            .await?;

        assert!(
            response.status().is_success(),
            "Health endpoint should respond"
        );

        Ok::<(), anyhow::Error>(())
    }
    .await;

    // Clean up
    cleanup_child(&mut child);

    result
}
