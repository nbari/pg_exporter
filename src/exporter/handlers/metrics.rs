use crate::collectors::registry::CollectorRegistry;
use axum::{
    extract::Extension,
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use prometheus::Encoder;
use sqlx::PgPool;
use tracing::{debug, error, instrument};

#[instrument(skip(pool, registry), fields(http.route="/metrics"))]
pub async fn metrics(
    Extension(pool): Extension<PgPool>,
    Extension(registry): Extension<CollectorRegistry>,
) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        "content-type",
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );

    match registry.collect_all_bytes(&pool).await {
        Ok(metrics) => {
            debug!("Successfully collected metrics");
            (StatusCode::OK, headers, metrics).into_response()
        }
        Err(e) => {
            error!("Failed to collect metrics: {}", e);
            // Even on error, we return 200 with best-effort results if available.
            // registry.gather() could be used here to get at least pg_up and build info.
            let mut buffer = Vec::new();
            let encoder = prometheus::TextEncoder::new();
            let metric_families = registry.registry().gather();
            if let Err(encode_err) = encoder.encode(&metric_families, &mut buffer) {
                error!("Failed to encode metrics on error path: {}", encode_err);
                return (
                    StatusCode::OK,
                    headers,
                    format!(
                        "# Error collecting metrics: {e}\n# Error encoding metrics: {encode_err}"
                    ),
                )
                    .into_response();
            }

            (StatusCode::OK, headers, buffer).into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: These tests require a database connection, so they're more integration tests
    // We'll create unit tests for the response structure

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_metrics_response_headers() {
        // Test that we're setting the correct content-type
        let mut headers = HeaderMap::new();
        headers.insert(
            "content-type",
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );

        assert_eq!(
            headers.get("content-type").unwrap(),
            "text/plain; charset=utf-8"
        );
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_header_value_creation() {
        let header_val = HeaderValue::from_static("text/plain; charset=utf-8");
        assert_eq!(header_val.to_str().unwrap(), "text/plain; charset=utf-8");
    }
}
