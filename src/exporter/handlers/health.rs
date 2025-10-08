use crate::exporter::GIT_COMMIT_HASH;
use axum::{
    body::Body,
    extract::Extension,
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Json},
};
use serde::{Deserialize, Serialize};
use sqlx::{Connection, PgPool};
use tracing::{debug, error, info_span, instrument};
use tracing_futures::Instrument as _;

#[derive(Serialize, Deserialize, Debug)]
pub struct Health {
    commit: String,
    name: String,
    version: String,
    database: String,
}

// Check database health
#[instrument(skip(pool), err, fields(db.system="postgresql", db.operation="ping", otel.kind="client"))]
async fn check_database_health(pool: &PgPool) -> Result<(), StatusCode> {
    // Acquire connection
    let acquire_span = info_span!("db.acquire");

    let mut conn = pool
        .acquire()
        .instrument(acquire_span)
        .await
        .map_err(|error| {
            error!(%error, "Failed to acquire database connection");
            StatusCode::SERVICE_UNAVAILABLE
        })?;

    // Ping
    let ping_span = info_span!("db.ping");
    conn.ping().instrument(ping_span).await.map_err(|error| {
        error!(%error, "Failed to ping database");
        StatusCode::SERVICE_UNAVAILABLE
    })
}

// Create health struct based on database status
fn create_health_response(db_result: &Result<(), StatusCode>) -> Health {
    Health {
        commit: GIT_COMMIT_HASH.to_string(),
        name: env!("CARGO_PKG_NAME").to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        database: if db_result.is_ok() {
            "ok".to_string()
        } else {
            "error".to_string()
        },
    }
}

// Create response body based on method
fn create_response_body(method: Method, health: &Health) -> Body {
    if method == Method::GET {
        Json(health).into_response().into_body()
    } else {
        Body::empty()
    }
}

// Create X-App header
fn create_app_headers(health: &Health) -> HeaderMap {
    let short_hash = if health.commit.len() > 7 {
        &health.commit[0..7]
    } else {
        ""
    };

    let header_value = format!("{}:{}:{}", health.name, health.version, short_hash);

    match header_value.parse::<HeaderValue>() {
        Ok(x_app_header_value) => {
            debug!("X-App header: {:?}", x_app_header_value);
            let mut headers = HeaderMap::new();
            headers.insert("X-App", x_app_header_value);
            headers
        }
        Err(err) => {
            debug!("Failed to parse X-App header: {}", err);
            HeaderMap::new()
        }
    }
}

// Main axum handler for health
#[instrument(skip(pool), fields(http.route="/health"))]
pub async fn health(method: Method, pool: Extension<PgPool>) -> impl IntoResponse {
    let db_result = check_database_health(&pool.0).await;
    let health = create_health_response(&db_result);
    let body = create_response_body(method, &health);
    let headers = create_app_headers(&health);

    match db_result {
        Ok(()) => {
            debug!("Database connection is healthy");
            (StatusCode::OK, headers, body)
        }
        Err(status_code) => {
            debug!("Database connection is unhealthy");
            (status_code, headers, body)
        }
    }
}
