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

pub async fn new(
    port: u16,
    listen: Option<String>,
    dsn: SecretString,
    collectors: Vec<String>,
) -> Result<()> {
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

    let (listener, bind_addr) = match listen {
        Some(addr) => {
            // Try to parse as IpAddr to validate and determine type
            match addr.parse::<std::net::IpAddr>() {
                Ok(ip) => {
                    let bind_addr = format!("{ip}:{port}");
                    (
                        TcpListener::bind(&bind_addr)
                            .await
                            .with_context(|| format!("Failed to bind to {bind_addr}"))?,
                        if ip.is_ipv6() {
                            format!("[{ip}]:{port}")
                        } else {
                            bind_addr.clone()
                        },
                    )
                }
                Err(_) => {
                    return Err(anyhow!(
                        "Invalid IP address: '{}'. Expected IPv4 (e.g., 0.0.0.0, 127.0.0.1) or IPv6 (e.g., ::, ::1)",
                        addr
                    ));
                }
            }
        }
        None => {
            // Auto: try IPv6 first, fallback to IPv4
            match TcpListener::bind(format!("::0:{port}")).await {
                Ok(l) => (l, format!("[::]:{port}")),
                Err(_) => {
                    // If IPv6 fails, fall back to binding to IPv4 address
                    (
                        TcpListener::bind(format!("0.0.0.0:{port}")).await?,
                        format!("0.0.0.0:{port}"),
                    )
                }
            }
        }
    };

    println!(
        "{} {} - Listening on {bind_addr}\n\nEnabled collectors:\n{}",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION"),
        format_list(&collectors),
    );

    let excluded = get_excluded_databases();

    if !excluded.is_empty() {
        println!("\nExcluded databases:\n{}", format_list(excluded));
    }

    if let Err(e) = axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(shutdown::shutdown_signal_handler())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_git_commit_hash_exists() {
        // GIT_COMMIT_HASH is a compile-time constant, either a git hash or ":-("
        // We can verify it's one of the expected patterns
        assert!(
            GIT_COMMIT_HASH.len() >= 3,
            "Git commit hash should be at least 3 chars (even ':-(' is 3 chars)"
        );

        // It should be either a hex string (git hash) or the fallback
        let is_hex = GIT_COMMIT_HASH.chars().all(|c| c.is_ascii_hexdigit());
        let is_fallback = GIT_COMMIT_HASH == ":-(";

        assert!(
            is_hex || is_fallback,
            "Git commit hash should be hex digits or the fallback ':-(' pattern"
        );
    }

    #[test]
    fn test_format_list_empty() {
        let items: Vec<String> = vec![];
        let result = format_list(&items);
        assert_eq!(result, "");
    }

    #[test]
    fn test_format_list_single_item() {
        let items = vec!["item1"];
        let result = format_list(&items);
        assert_eq!(result, "  - item1");
    }

    #[test]
    fn test_format_list_multiple_items() {
        let items = vec!["item1", "item2", "item3"];
        let result = format_list(&items);
        assert_eq!(result, "  - item1\n  - item2\n  - item3");
    }

    #[test]
    fn test_format_list_with_numbers() {
        let items = vec![1, 2, 3];
        let result = format_list(&items);
        assert_eq!(result, "  - 1\n  - 2\n  - 3");
    }

    #[test]
    fn test_format_list_formatting() {
        let items = vec!["collector1", "collector2"];
        let result = format_list(&items);

        // Should start with two spaces and a dash
        assert!(result.starts_with("  - "));

        // Should contain both items
        assert!(result.contains("collector1"));
        assert!(result.contains("collector2"));

        // Should have newline between items
        assert!(result.contains("\n"));
    }

    // Test the on_response function behavior
    #[test]
    fn test_on_response_status_codes() {
        use axum::http::{Response, StatusCode};
        use std::time::Duration;
        use tracing::info_span;

        let span = info_span!("test");

        // Test with 200 OK
        let response_ok = Response::builder().status(StatusCode::OK).body(()).unwrap();

        let latency = Duration::from_millis(100);

        // This should not panic
        on_response(&response_ok, latency, &span);

        // Test with 500 error
        let response_err = Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(())
            .unwrap();

        on_response(&response_err, latency, &span);
    }

    #[test]
    fn test_make_span_creates_span() {
        use axum::body::Body;
        use axum::http::Request;

        let request = Request::builder()
            .method("GET")
            .uri("/metrics")
            .header("user-agent", "test-client")
            .body(Body::empty())
            .unwrap();

        let span = make_span(&request);

        // Verify span was created with correct metadata
        assert_eq!(
            span.metadata().map(|m| m.name()),
            Some("http.server.request")
        );
    }

    #[test]
    fn test_make_span_with_request_id() {
        use axum::body::Body;
        use axum::http::Request;

        let request = Request::builder()
            .method("POST")
            .uri("/health")
            .header("x-request-id", "test-id-12345")
            .header("user-agent", "Mozilla/5.0")
            .body(Body::empty())
            .unwrap();

        let span = make_span(&request);

        // Just verify it doesn't panic and the span has the correct name
        assert_eq!(
            span.metadata().map(|m| m.name()),
            Some("http.server.request")
        );
    }

    #[test]
    fn test_make_span_without_optional_headers() {
        use axum::body::Body;
        use axum::http::Request;

        let request = Request::builder()
            .method("GET")
            .uri("/")
            .body(Body::empty())
            .unwrap();

        let span = make_span(&request);

        // Should still create a valid span even without optional headers
        assert_eq!(
            span.metadata().map(|m| m.name()),
            Some("http.server.request")
        );
    }
}
