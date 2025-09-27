use anyhow::Result;
use opentelemetry::{KeyValue, global, trace::TracerProvider as _};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    Resource,
    trace::{SdkTracerProvider, Tracer},
};
use std::time::Duration;
use tracing::Level;
use tracing_subscriber::{EnvFilter, Registry, fmt, layer::SubscriberExt};

fn init_tracer(endpoint: &str) -> Result<Tracer> {
    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(
            opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .with_timeout(Duration::from_secs(3))
                .build()?,
        )
        .with_resource(
            Resource::builder_empty()
                .with_attributes(vec![
                    KeyValue::new("service.name", env!("CARGO_PKG_NAME")),
                    KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
                ])
                .build(),
        )
        .build();

    global::set_tracer_provider(tracer_provider.clone());

    Ok(tracer_provider.tracer(env!("CARGO_PKG_NAME")))
}

/// Start the telemetry layer
/// # Errors
/// Will return an error if the telemetry layer fails to start
pub fn init(verbosity_level: Option<Level>) -> Result<()> {
    let verbosity_level = verbosity_level.unwrap_or(Level::ERROR);

    let fmt_layer = fmt::layer()
        .with_file(false)
        .with_line_number(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_target(false)
        .pretty();

    // RUST_LOG=
    let filter = EnvFilter::builder()
        .with_default_directive(verbosity_level.into())
        .from_env_lossy()
        .add_directive("hyper=error".parse()?)
        .add_directive("tokio=error".parse()?)
        .add_directive("reqwest=error".parse()?)
        .add_directive("opentelemetry_sdk=warn".parse()?);

    // Read optional endpoint from env
    let otlp_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok();

    if let Some(endpoint) = otlp_endpoint {
        // Initialize OpenTelemetry only if endpoint is set
        let tracer = init_tracer(&endpoint)?;
        let otel_tracer_layer = tracing_opentelemetry::layer().with_tracer(tracer);
        let subscriber = Registry::default()
            .with(fmt_layer)
            .with(otel_tracer_layer)
            .with(filter);
        tracing::subscriber::set_global_default(subscriber)?;
    } else {
        // Skip tracing setup if no endpoint is configured
        let subscriber = Registry::default().with(fmt_layer).with(filter);
        tracing::subscriber::set_global_default(subscriber)?;
    }

    Ok(())
}
