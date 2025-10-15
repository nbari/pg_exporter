use tokio::signal;

pub async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut sigint = signal::unix::signal(signal::unix::SignalKind::interrupt())
            .expect("install SIGINT handler");

        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");

        tokio::select! {
            _ = sigint.recv()  => {},
            _ = sigterm.recv() => {},
        }
    }

    #[cfg(not(unix))]
    {
        // Fallback to Ctrl+C only
        let _ = signal::ctrl_c().await;
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
