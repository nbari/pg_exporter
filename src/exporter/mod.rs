use crate::{
    cli::telemetry::shutdown_tracer,
    collectors::{
        config::CollectorConfig,
        registry::CollectorRegistry,
        util::{get_excluded_databases, set_base_connect_options_from_dsn},
    },
};
use anyhow::{Context, Result, anyhow};
use axum::{
    Extension, Router,
    body::Body,
    http::{HeaderName, HeaderValue, Request},
    middleware::{Next, from_fn},
    response::Response,
    routing::get,
};
use opentelemetry::global;
use opentelemetry::trace::{TraceContextExt, TraceId};
use opentelemetry_http::HeaderExtractor;
use secrecy::{ExposeSecret, SecretString};
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;
use tokio::{net::TcpListener, time::timeout};
use tower::ServiceBuilder;
use tower_http::{
    request_id::PropagateRequestIdLayer, set_header::SetRequestHeaderLayer, trace::TraceLayer,
};
use tracing::{Span, error, info, info_span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use ulid::Ulid;

mod handlers;
mod shutdown;

pub mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

pub const GIT_COMMIT_HASH: &str = if let Some(hash) = built_info::GIT_COMMIT_HASH {
    hash
} else {
    ":-("
};

pub async fn new(port: u16, dsn: SecretString, collectors: Vec<String>) -> Result<()> {
    let db_dsn = dsn.expose_secret().to_string();

    let pool = match timeout(
        Duration::from_secs(2),
        PgPoolOptions::new()
            .min_connections(1)
            .max_connections(3)
            .max_lifetime(Duration::from_secs(60 * 2))
            .test_before_acquire(true)
            .connect(&db_dsn),
    )
    .await
    {
        Ok(Ok(pool)) => pool,
        Ok(Err(err)) => return Err(err).context("Failed to connect to database"),
        Err(_) => return Err(anyhow!("Failed to connect to database: timed out after 2s")),
    };

    info!("Connected to database");

    // Initialize base connect options for cross-DB collectors (idempotent).
    let _ = set_base_connect_options_from_dsn(&dsn);

    let config = CollectorConfig::new().with_enabled(&collectors);
    let registry = CollectorRegistry::new(config);

    let trace_layer = TraceLayer::new_for_http()
        .make_span_with(make_span)
        .on_response(on_response);

    let app = Router::new()
        .route("/metrics", get(handlers::metrics))
        .route("/health", get(handlers::health).options(handlers::health))
        .layer(
            ServiceBuilder::new()
                .layer(SetRequestHeaderLayer::if_not_present(
                    HeaderName::from_static("x-request-id"),
                    |_req: &_| HeaderValue::from_str(Ulid::new().to_string().as_str()).ok(),
                ))
                .layer(PropagateRequestIdLayer::new(HeaderName::from_static(
                    "x-request-id",
                )))
                .layer(trace_layer)
                .layer(from_fn(add_trace_headers))
                .layer(Extension(pool.clone()))
                .layer(Extension(registry)),
        );

    let listener = TcpListener::bind(format!("::0:{port}")).await?;

    println!(
        "{} {} - Listening on [::]:{port}\n\nEnabled collectors:\n{}",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION"),
        format_list(&collectors),
    );

    let excluded = get_excluded_databases();

    if !excluded.is_empty() {
        println!("\nExcluded databases:\n{}", format_list(excluded));
    }

    if let Err(e) = axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(shutdown::shutdown_signal())
        .await
    {
        error!(error=%e, "server error");
    }

    info!("shutting down");

    shutdown_tracer();

    Ok(())
}

// Helper to format a list of items with a leading dash and indentation for the
// start up message
fn format_list<T: std::fmt::Display>(items: &[T]) -> String {
    items
        .iter()
        .map(|i| format!("  - {i}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn make_span(request: &Request<Body>) -> Span {
    let parent_cx =
        global::get_text_map_propagator(|prop| prop.extract(&HeaderExtractor(request.headers())));

    let method = request.method().as_str();

    let path = request.uri().path();

    let target = request.uri().to_string();

    let scheme = request.uri().scheme_str().unwrap_or("http");

    let request_id = request
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("none");

    let user_agent = request
        .headers()
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown");

    let span = info_span!(
        "http.server.request",
        otel.kind = "server",
        http.method = method,
        http.route = path,
        http.target = target,
        http.scheme = scheme,
        http.user_agent = user_agent,
        request_id = request_id,
    );

    let _ = span.set_parent(parent_cx);

    span
}

fn on_response<B>(response: &axum::http::Response<B>, latency: Duration, span: &Span) {
    if response.status().is_server_error() {
        span.record("otel.status_code", "ERROR");
    } else {
        span.record("otel.status_code", "OK");
    }

    let cx = span.context();
    let trace_id = cx.span().span_context().trace_id();

    if trace_id != TraceId::INVALID {
        info!(
            parent: span,
            status = response.status().as_u16(),
            elapsed_ms = latency.as_millis() as u64,
            trace_id = %trace_id,
            "request completed"
        );
    } else {
        info!(
            parent: span,
            status = response.status().as_u16(),
            elapsed_ms = latency.as_millis() as u64,
            "request completed"
        );
    }
}

async fn add_trace_headers(req: Request<Body>, next: Next) -> Response {
    let mut res = next.run(req).await;

    let span = Span::current();

    let cx = span.context();

    // CLONE the SpanContext to avoid borrowing a temporary
    let span_context = cx.span().span_context().clone();

    if span_context.is_valid()
        && let Ok(val) = HeaderValue::from_str(&span_context.trace_id().to_string())
    {
        res.headers_mut()
            .insert(HeaderName::from_static("x-trace-id"), val);
    }

    res
}
