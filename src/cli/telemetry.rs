use anyhow::{Result, anyhow};
use base64::{Engine, engine::general_purpose};
use opentelemetry::propagation::TextMapCompositePropagator;
use opentelemetry::{KeyValue, global, trace::TracerProvider as _};
use opentelemetry_otlp::{Compression, WithExportConfig, WithHttpConfig, WithTonicConfig};
use opentelemetry_sdk::propagation::{BaggagePropagator, TraceContextPropagator};
use opentelemetry_sdk::{
    Resource,
    trace::{SdkTracerProvider, Tracer},
};
use std::{collections::HashMap, env::var, time::Duration};
use tonic::{metadata::*, transport::ClientTlsConfig};
use tracing::Level;
use tracing_subscriber::{EnvFilter, Registry, fmt, layer::SubscriberExt};
use ulid::Ulid;

fn parse_headers_env(headers_str: &str) -> HashMap<String, String> {
    headers_str
        .split(',')
        .filter_map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let key = parts.next()?.trim().to_string();
            let value = parts.next()?.trim().to_string();
            Some((key, value))
        })
        .collect()
}

// Convert HashMap<String, String> into tonic::MetadataMap
// - Supports ASCII metadata (normal keys)
// - Supports binary metadata keys (ending with "-bin"), values must be base64-encoded
fn headers_to_metadata(headers: &std::collections::HashMap<String, String>) -> Result<MetadataMap> {
    let mut meta = MetadataMap::with_capacity(headers.len());

    for (k, v) in headers {
        // gRPC metadata keys must be lowercase ASCII. Normalize to be safe.
        let key_str = k.to_ascii_lowercase();

        if key_str.ends_with("-bin") {
            // Binary metadata: value must be bytes. Expect base64 in env, decode here.
            let bytes = general_purpose::STANDARD
                .decode(v.as_bytes())
                .map_err(|e| anyhow!("failed to base64-decode value for key {}: {}", key_str, e))?;

            // Build an owned binary metadata key (no 'static required)
            let key = MetadataKey::<Binary>::from_bytes(key_str.as_bytes())
                .map_err(|e| anyhow!("invalid binary metadata key {}: {}", key_str, e))?;

            let val = MetadataValue::from_bytes(&bytes);
            meta.insert_bin(key, val);
        } else {
            // ASCII metadata
            let key = MetadataKey::<Ascii>::from_bytes(key_str.as_bytes())
                .map_err(|e| anyhow!("invalid ASCII metadata key {}: {}", key_str, e))?;

            let val: MetadataValue<_> = v
                .parse()
                .map_err(|e| anyhow!("invalid ASCII metadata value for key {}: {}", key_str, e))?;

            meta.insert(key, val); // uses owned key, no 'static
        }
    }

    Ok(meta)
}

fn normalize_endpoint(ep: String) -> String {
    if ep.starts_with("http://") || ep.starts_with("https://") {
        ep
    } else {
        format!("https://{}", ep.trim_end_matches('/'))
    }
}

fn init_tracer() -> Result<Tracer> {
    // Read protocol from environment (default to grpc if not set)
    let protocol = var("OTEL_EXPORTER_OTLP_PROTOCOL").unwrap_or_else(|_| "grpc".to_string());

    // Protocol-specific sensible default
    let default_ep = match protocol.as_str() {
        "http/protobuf" => "http://localhost:4318",
        _ => "http://localhost:4317",
    };

    let endpoint = var("OTEL_EXPORTER_OTLP_ENDPOINT").unwrap_or_else(|_| default_ep.to_string());
    let endpoint = normalize_endpoint(endpoint);

    let headers = var("OTEL_EXPORTER_OTLP_HEADERS")
        .ok()
        .map(|s| parse_headers_env(&s))
        .unwrap_or_default();

    let exporter = match protocol.as_str() {
        "http/protobuf" => {
            // Build a reqwest HTTP client (you can customize timeouts, TLS, proxies, etc.)
            let http_client = reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()?;

            let mut builder = opentelemetry_otlp::SpanExporter::builder()
                .with_http()
                .with_compression(Compression::Gzip)
                .with_http_client(http_client);

            builder = builder.with_endpoint(endpoint);

            if !headers.is_empty() {
                builder = builder.with_headers(headers);
            }

            match builder.with_timeout(Duration::from_secs(3)).build() {
                Ok(exporter) => exporter,
                Err(e) => {
                    eprintln!("Failed to create OTLP HTTP exporter: {}", e);
                    return Err(e.into());
                }
            }
        }
        _ => {
            // gRPC mode (default) - headers not supported
            let mut builder = opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_endpoint(&endpoint);

            // Optional: explicit TLS config (SNI)
            if let Some(host) = &endpoint
                .strip_prefix("https://")
                .and_then(|s| s.split('/').next())
                .and_then(|h| h.split(':').next())
            {
                let tls = ClientTlsConfig::new()
                    .domain_name(host.to_string())
                    .with_native_roots();

                builder = builder.with_tls_config(tls);
            }

            builder = builder
                .with_compression(Compression::Gzip)
                .with_timeout(Duration::from_secs(3));

            if !headers.is_empty() {
                let metadata = headers_to_metadata(&headers)?;
                builder = builder.with_metadata(metadata);
            }

            builder.build()?
        }
    };

    let instance_id = var("OTEL_SERVICE_INSTANCE_ID").unwrap_or_else(|_| Ulid::new().to_string());

    let trace_provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(
            Resource::builder_empty()
                .with_attributes(vec![
                    KeyValue::new("service.name", env!("CARGO_PKG_NAME")),
                    KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
                    KeyValue::new("service.instance.id", instance_id),
                ])
                .build(),
        )
        .build();

    global::set_tracer_provider(trace_provider.clone());

    global::set_text_map_propagator(TextMapCompositePropagator::new(vec![
        Box::new(TraceContextPropagator::new()),
        Box::new(BaggagePropagator::new()),
    ]));

    Ok(trace_provider.tracer(env!("CARGO_PKG_NAME")))
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
        // .add_directive("reqwest=error".parse()?)
        .add_directive("opentelemetry_sdk=warn".parse()?);

    // Start the tracer if the endpoint is defined
    if var("OTEL_EXPORTER_OTLP_ENDPOINT").is_ok() {
        // Initialize OpenTelemetry only if endpoint is set
        let tracer = init_tracer()?;
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
