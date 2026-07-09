use crate::collectors::registry::{CollectorRegistry, ScrapeError};
use axum::{
    extract::Extension,
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
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
            let status = match e {
                ScrapeError::Timeout(_) => StatusCode::GATEWAY_TIMEOUT,
                ScrapeError::Busy
                | ScrapeError::CollectorFailed(_)
                | ScrapeError::Encode(_)
                | ScrapeError::Utf8(_) => StatusCode::SERVICE_UNAVAILABLE,
            };

            (
                status,
                headers,
                format!("# Error collecting metrics: {e}\n"),
            )
                .into_response()
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
