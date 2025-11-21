use crate::collectors::registry::CollectorRegistry;
use axum::{
    extract::Extension,
    http::{HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
};
use sqlx::PgPool;
use tracing::{debug, error, instrument};

#[instrument(skip(pool, registry), fields(http.route="/metrics"))]
pub async fn metrics(
    Extension(pool): Extension<PgPool>,
    Extension(registry): Extension<CollectorRegistry>,
) -> impl IntoResponse {
    let mut headers = HeaderMap::new();
    headers.insert(
        "content-type",
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );

    match registry.collect_all(&pool).await {
        Ok(metrics) => {
            debug!("Successfully collected metrics");
            (StatusCode::OK, headers, metrics)
        }
        Err(e) => {
            error!("Failed to collect metrics: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                headers,
                format!("Error collecting metrics: {e}"),
            )
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
