use anyhow::{Context, Result};
use tokio::signal;

/// Wait for shutdown signal (SIGINT, SIGTERM on Unix; Ctrl+C on Windows)
///
/// Returns Result to allow proper error handling during signal handler installation.
/// The shutdown_signal_handler() wrapper provides a simple () return for graceful shutdown.
pub async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut sigint = signal::unix::signal(signal::unix::SignalKind::interrupt())
            .context("Failed to install SIGINT handler")?;

        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
            .context("Failed to install SIGTERM handler")?;

        tokio::select! {
            _ = sigint.recv()  => {
                tracing::info!("Received SIGINT signal");
            },
            _ = sigterm.recv() => {
                tracing::info!("Received SIGTERM signal");
            },
        }
    }

    #[cfg(not(unix))]
    {
        // Fallback to Ctrl+C only on non-Unix systems
        signal::ctrl_c()
            .await
            .context("Failed to install Ctrl+C handler")?;
        tracing::info!("Received Ctrl+C signal");
    }

    Ok(())
}

/// Wrapper that provides () return for axum's graceful shutdown
/// This logs any errors in signal handling instead of propagating them
pub async fn shutdown_signal_handler() {
    if let Err(e) = shutdown_signal().await {
        tracing::error!("Error setting up shutdown handler: {}", e);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    #[cfg(unix)]
    async fn test_shutdown_signal_with_timeout() {
        // This test verifies the shutdown signal function compiles and runs
        // We can't actually test signal handling without sending real signals

        let result = timeout(Duration::from_millis(100), shutdown_signal()).await;

        // Should timeout because no signal was sent
        assert!(result.is_err(), "Should timeout waiting for signal");
    }

    // This test demonstrates the function signature is correct
    #[test]
    fn test_shutdown_signal_signature() {
        // Verify the function returns a future
        let _future = shutdown_signal();
    }
}
