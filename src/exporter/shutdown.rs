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
