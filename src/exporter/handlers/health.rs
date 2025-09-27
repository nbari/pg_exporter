use crate::exporter::GIT_COMMIT_HASH;
use axum::{
    body::Body,
    extract::Extension,
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Json},
};
use serde::{Deserialize, Serialize};
use sqlx::{Connection, PgPool};
use tracing::{debug, error};

#[derive(Serialize, Deserialize, Debug)]
pub struct Health {
    commit: String,
    name: String,
    version: String,
    database: String,
}

// axum handler for health
pub async fn health(method: Method, pool: Extension<PgPool>) -> impl IntoResponse {
    let result = match pool.0.acquire().await {
        Ok(mut conn) => match conn.ping().await {
            Ok(()) => Ok(()),
            Err(error) => {
                error!("Failed to ping database: {}", error);

                Err(StatusCode::SERVICE_UNAVAILABLE)
            }
        },

        Err(error) => {
            error!("Failed to acquire database connection: {}", error);

            Err(StatusCode::SERVICE_UNAVAILABLE)
        }
    };

    // Create a health struct
    let health = Health {
        commit: GIT_COMMIT_HASH.to_string(),
        name: env!("CARGO_PKG_NAME").to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        database: if result.is_ok() {
            "ok".to_string()
        } else {
            "error".to_string()
        },
    };

    let body = if method == Method::GET {
        Json(&health).into_response()
    } else {
        Body::empty().into_response()
    };

    let short_hash = if health.commit.len() > 7 {
        &health.commit[0..7]
    } else {
        ""
    };

    // Create headers using the map method
    let headers = format!("{}:{}:{}", health.name, health.version, short_hash)
        .parse::<HeaderValue>()
        .map(|x_app_header_value| {
            debug!("X-App header: {:?}", x_app_header_value);

            let mut headers = HeaderMap::new();

            headers.insert("X-App", x_app_header_value);

            headers
        })
        .map_err(|err| {
            debug!("Failed to parse X-App header: {}", err);
        });

    // Unwrap the headers or provide a default value (empty headers) in case of an error
    let headers = headers.unwrap_or_else(|()| HeaderMap::new());

    match result {
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
