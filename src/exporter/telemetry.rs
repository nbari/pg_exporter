//! HTTP propagation compiled only into telemetry-enabled builds.
use axum::{
    body::Body,
    http::{HeaderName, HeaderValue, Request},
    middleware::Next,
    response::Response,
};
use opentelemetry::{
    global,
    trace::{TraceContextExt as _, TraceId},
};
use opentelemetry_http::HeaderExtractor;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

pub(super) fn set_parent(request: &Request<Body>, span: &Span) {
    let parent =
        global::get_text_map_propagator(|prop| prop.extract(&HeaderExtractor(request.headers())));
    let _ = span.set_parent(parent);
}

pub(super) fn trace_id(span: &Span) -> Option<TraceId> {
    let context = span.context();
    let span_context = context.span().span_context().clone();
    span_context.is_valid().then_some(span_context.trace_id())
}

pub(super) async fn add_trace_headers(req: Request<Body>, next: Next) -> Response {
    let mut response = next.run(req).await;
    if let Some(trace_id) = trace_id(&Span::current())
        && let Ok(value) = HeaderValue::from_str(&trace_id.to_string())
    {
        response
            .headers_mut()
            .insert(HeaderName::from_static("x-trace-id"), value);
    }
    response
}
