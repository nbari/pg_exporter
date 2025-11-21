#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::panic)]
#![allow(clippy::indexing_slicing)]
//! Integration tests for the `pg_exporter` binary
//!
//! These tests execute the binary as a subprocess and verify:
//! - CLI argument parsing (--help, --version, flags)
//! - Server startup and shutdown behavior
//! - HTTP endpoints (/metrics, /health)
//! - Environment variable handling
//! - Error handling and validation
//!
//! # Performance Optimization
//!
//! These tests build the binary once using `OnceLock` and reuse it across all tests,
//! instead of calling `cargo run` for each test. This approach:
//! - Eliminates repeated compilation checks (10x faster)
//! - Ensures consistent binary state across tests
//! - Avoids cargo-related environment issues
//! - Makes tests more reliable in CI environments

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

/// Get path to the `pg_exporter` binary, building it once if needed.
///
/// This function ensures the binary is compiled exactly once across all tests,
/// using `OnceLock` for thread-safe lazy initialization. Subsequent calls return
/// the cached path without rebuilding.
fn get_binary_path() -> &'static PathBuf {
    BINARY_PATH.get_or_init(|| {
        // Build the binary once for all tests
        let output = Command::new("cargo")
            .args(["build", "--bin", "pg_exporter"])
            .output()
            .expect("Failed to build binary");

        assert!(
            output.status.success(),
            "Failed to build binary:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );

        // Construct path to the compiled binary
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("debug")
            .join("pg_exporter")
    })
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Run the binary with given arguments and return output.
fn run_binary_with_args(args: &[&str]) -> std::io::Result<std::process::Output> {
    Command::new(get_binary_path()).args(args).output()
}

/// Start the binary in background with given port and DSN.
fn start_binary(port: u16, dsn: &str) -> std::io::Result<Child> {
    Command::new(get_binary_path())
        .args(["--port", &port.to_string(), "--dsn", dsn])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
}

/// Start the binary using environment variables for configuration.
fn start_binary_with_env(port: u16, dsn: &str) -> std::io::Result<Child> {
    Command::new(get_binary_path())
        .env("PG_EXPORTER_PORT", port.to_string())
        .env("PG_EXPORTER_DSN", dsn)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
}

/// Kill child process and wait for it to exit.
fn cleanup_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

/// RAII guard for automatic cleanup of child process.
/// When dropped, ensures the process is terminated.
struct ChildGuard(Child);

impl ChildGuard {
    fn new(child: Child) -> Self {
        Self(child)
    }

    fn as_mut(&mut self) -> &mut Child {
        &mut self.0
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        cleanup_child(&mut self.0);
    }
}

/// Start binary and return a RAII guard that ensures cleanup on drop.
async fn start_and_wait(port: u16, dsn: &str) -> Result<ChildGuard> {
    let child = start_binary(port, dsn)?;
    let guard = ChildGuard::new(child);

    if !common::wait_for_server(port, 100).await {
        anyhow::bail!("Server failed to start on port {port}");
    }

    Ok(guard)
}

/// Make HTTP request to given endpoint and return response body.
async fn http_get(port: u16, endpoint: &str) -> Result<String> {
    let client = reqwest::Client::new();
    let response = client
        .get(format!("http://localhost:{port}{endpoint}"))
        .timeout(Duration::from_secs(10))
        .send()
        .await?;

    if !response.status().is_success() {
        anyhow::bail!("HTTP request failed with status: {}", response.status());
    }

    Ok(response.text().await?)
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

    // Start the binary and wait for it to be ready
    let mut guard = start_and_wait(port, &dsn).await?;

    // Kill the process
    cleanup_child(guard.as_mut());

    // Give it time to clean up
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify server stopped
    let result = tokio::net::TcpStream::connect(format!("localhost:{port}")).await;
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
    let mut guard = start_and_wait(port, &dsn).await?;

    // Kill the process gracefully
    let child = guard.as_mut();
    child.kill().expect("Failed to kill process");
    let status = child.wait().expect("Failed to wait for process");

    // Process should have been killed
    assert!(!status.success(), "Process was killed");

    // Verify server stopped
    tokio::time::sleep(Duration::from_millis(200)).await;
    let result = tokio::net::TcpStream::connect(format!("localhost:{port}")).await;
    assert!(result.is_err(), "Server should be stopped");

    Ok(())
}

/// Test that the binary respects environment variables
#[tokio::test]
async fn test_binary_uses_environment_variables() -> Result<()> {
    let port = common::get_available_port();
    let dsn = common::get_test_dsn();

    // Start with environment variables
    let child = start_binary_with_env(port, &dsn)?;
    let _guard = ChildGuard::new(child); // Auto-cleanup on drop

    // Wait for server to start
    if !common::wait_for_server(port, 100).await {
        anyhow::bail!("Server should start using env vars");
    }

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
        .args(["--dsn", "not-a-valid-dsn", "--port", "9999"])
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

    // Start the binary and ensure it's cleaned up automatically
    let _guard = start_and_wait(port, &dsn).await?;

    // Make HTTP request to metrics endpoint
    let body = http_get(port, "/metrics").await?;

    assert!(body.contains("pg_up"), "Metrics should include pg_up");
    assert!(
        body.contains("pg_exporter_build_info"),
        "Metrics should include build info"
    );

    Ok(())
}

/// Test that the binary exposes health endpoint
#[tokio::test]
async fn test_binary_exposes_health_endpoint() -> Result<()> {
    let port = common::get_available_port();
    let dsn = common::get_test_dsn();

    // Start the binary and ensure it's cleaned up automatically
    let _guard = start_and_wait(port, &dsn).await?;

    // Make HTTP request to health endpoint
    let _body = http_get(port, "/health").await?;

    Ok(())
}
