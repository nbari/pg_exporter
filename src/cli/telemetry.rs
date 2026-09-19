//! Local logging is always available. OTLP export additionally requires a build
//! with the `telemetry` feature and an endpoint configured at startup.
use anyhow::Result;
use tracing::Level;
use tracing_subscriber::{EnvFilter, Registry, fmt, layer::SubscriberExt};

#[cfg(feature = "telemetry")]
mod otlp;

/// Initialize local logging and, when compiled in and configured, OTLP tracing.
///
/// Default builds ignore OTEL environment variables and warn on stderr if an
/// endpoint is set. Telemetry-enabled builds create an exporter only when
/// `OTEL_EXPORTER_OTLP_ENDPOINT` is set.
///
/// # Errors
///
/// Returns an error if tracer or subscriber initialization fails.
pub fn init(verbosity_level: Option<Level>) -> Result<()> {
    let verbosity_level = verbosity_level.unwrap_or(Level::ERROR);
    let fmt_layer = fmt::layer()
        .with_file(false)
        .with_line_number(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_target(false)
        .pretty();

    let filter = EnvFilter::builder()
        .with_default_directive(verbosity_level.into())
        .from_env_lossy()
        .add_directive("hyper=error".parse()?)
        .add_directive("tokio=error".parse()?);

    #[cfg(feature = "telemetry")]
    let filter = filter.add_directive("opentelemetry_sdk=warn".parse()?);

    #[cfg(feature = "telemetry")]
    if std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_ok() {
        let tracer = otlp::init_tracer()?;
        let subscriber = Registry::default()
            .with(fmt_layer)
            .with(tracing_opentelemetry::layer().with_tracer(tracer))
            .with(filter);
        tracing::subscriber::set_global_default(subscriber)?;
        return Ok(());
    }

    let subscriber = Registry::default().with(fmt_layer).with(filter);
    tracing::subscriber::set_global_default(subscriber)?;
    #[cfg(not(feature = "telemetry"))]
    if std::env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT").is_some() {
        use std::io::Write as _;

        // This migration warning must survive the default ERROR filter (and off).
        // Do not disclose endpoint/header values or panic if stderr is unavailable.
        let _ = writeln!(
            std::io::stderr().lock(),
            "warning: pg_exporter was built without telemetry; OTEL_EXPORTER_OTLP_ENDPOINT is ignored. Rebuild with --features telemetry to enable OTLP tracing."
        );
    }
    Ok(())
}

/// Shut down the optional tracer provider; a no-op in default builds.
pub fn shutdown_tracer() {
    #[cfg(feature = "telemetry")]
    otlp::shutdown_tracer();
}
