//! Exercise the actual binary in each Cargo feature configuration. Each subprocess
//! owns its subscriber/provider, so no global telemetry state leaks between tests.
use anyhow::{Context as _, Result, ensure};
use std::{process::Stdio, time::Duration};
use tokio::process::Command;

mod common;

const REMOTE_TRACE_ID: &str = "11111111111111111111111111111111";
const TRACEPARENT: &str = "00-11111111111111111111111111111111-2222222222222222-01";

#[cfg(not(feature = "telemetry"))]
#[tokio::test]
async fn disabled_telemetry_warns_even_with_logging_off_without_leaking_config() -> Result<()> {
    for log_filter in [None, Some("off")] {
        for endpoint_configured in [false, true] {
            let port = common::get_available_port();
            let mut command = Command::new(env!("CARGO_BIN_EXE_pg_exporter"));
            command
                .args(["--listen", "127.0.0.1", "--port", &port.to_string()])
                .env("PG_EXPORTER_DSN", common::get_test_dsn())
                .env_remove("PG_EXPORTER_DSN_FILE")
                .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
                .env("OTEL_EXPORTER_OTLP_HEADERS", "authorization=private-token")
                .env_remove("RUST_LOG")
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true);
            if let Some(filter) = log_filter {
                command.env("RUST_LOG", filter);
            }
            if endpoint_configured {
                command.env(
                    "OTEL_EXPORTER_OTLP_ENDPOINT",
                    "http://private-user:private-password@127.0.0.1:1",
                );
            }
            let mut child = command.spawn()?;
            let result = check_http(port, endpoint_configured).await;
            child.kill().await?;
            let output = child.wait_with_output().await?;
            result?;
            let stderr = String::from_utf8(output.stderr)?;
            ensure!(
                stderr.matches("built without telemetry").count()
                    == usize::from(endpoint_configured),
                "unexpected startup warning with RUST_LOG={log_filter:?}: {stderr}"
            );
            ensure!(
                !stderr.contains("private-"),
                "configuration leaked: {stderr}"
            );
        }
    }
    Ok(())
}

#[test]
fn version_reports_compiled_telemetry_capability() -> Result<()> {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_exporter"))
        .arg("--version")
        .output()?;
    ensure!(output.status.success(), "--version failed");
    let capability = if cfg!(feature = "telemetry") {
        "enabled"
    } else {
        "disabled"
    };
    ensure!(String::from_utf8(output.stdout)?.contains(&format!("telemetry: {capability}")));
    Ok(())
}

#[tokio::test]
async fn binary_preserves_local_logging_and_gates_remote_trace_context() -> Result<()> {
    for endpoint_configured in [false, true] {
        let port = common::get_available_port();
        let mut command = Command::new(env!("CARGO_BIN_EXE_pg_exporter"));
        command
            .args(["--listen", "127.0.0.1", "--port", &port.to_string()])
            .env("PG_EXPORTER_DSN", common::get_test_dsn())
            .env_remove("PG_EXPORTER_DSN_FILE")
            .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
            .env_remove("OTEL_EXPORTER_OTLP_PROTOCOL")
            .env("RUST_LOG", "info")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        // A disabled build must ignore even malformed exporter configuration.
        command.env(
            "OTEL_EXPORTER_OTLP_HEADERS",
            if cfg!(feature = "telemetry") {
                ""
            } else {
                "custom-bin=not-valid-base64!!!"
            },
        );
        if endpoint_configured {
            command.env("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1");
        }
        let mut child = command.spawn()?;
        let result = check_http(port, endpoint_configured).await;
        child.kill().await?;
        let output = child.wait_with_output().await?;
        let stdout = String::from_utf8(output.stdout)?;
        let stderr = String::from_utf8(output.stderr)?;
        result.with_context(|| format!("exporter stdout: {stdout}\nstderr: {stderr}"))?;
        ensure!(
            stderr.contains("built without telemetry")
                == (!cfg!(feature = "telemetry") && endpoint_configured)
        );
        ensure!(
            stdout.contains("request completed") && stdout.contains("feature-probe"),
            "local request logs lost their correlation fields: {stdout}"
        );
    }
    Ok(())
}

async fn check_http(port: u16, endpoint_configured: bool) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    let url = format!("http://127.0.0.1:{port}/metrics");
    let response = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Ok(response) = client
                .get(&url)
                .header("x-request-id", "feature-probe")
                .header("traceparent", TRACEPARENT)
                .send()
                .await
            {
                break response;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .context("exporter did not become ready")?;
    ensure!(response.status().is_success(), "metrics scrape failed");
    ensure!(
        response
            .headers()
            .get("x-request-id")
            .is_some_and(|id| id == "feature-probe")
    );
    let trace_id = response
        .headers()
        .get("x-trace-id")
        .map(|id| id.to_str())
        .transpose()?;
    let expected = (cfg!(feature = "telemetry") && endpoint_configured).then_some(REMOTE_TRACE_ID);
    ensure!(
        trace_id == expected,
        "unexpected trace header: {trace_id:?}, expected {expected:?}"
    );
    ensure!(response.text().await?.lines().any(|line| line == "pg_up 1"));

    let generated = client.get(&url).send().await?;
    ensure!(generated.status().is_success());
    let request_id = generated
        .headers()
        .get("x-request-id")
        .context("generated request ID missing")?
        .to_str()?;
    ulid::Ulid::from_string(request_id)?;
    Ok(())
}
