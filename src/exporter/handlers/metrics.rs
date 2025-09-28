use crate::collectors::registry::CollectorRegistry;
use axum::{
    extract::Extension,
    http::{HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
};
use sqlx::PgPool;
use tracing::{debug, error};

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
                format!("Error collecting metrics: {}", e),
            )
        }
    }
}
