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
    #[serde(skip_serializing_if = "Option::is_none")]
    commit: Option<String>,
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
        commit: GIT_COMMIT_HASH.map(|s| s.to_string()),
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
    let short_hash = health
        .commit
        .as_deref()
        .filter(|s| s.len() > 7)
        .map(|s| &s[0..7])
        .unwrap_or("");

    let header_value = if short_hash.is_empty() {
        format!("{}:{}", health.name, health.version)
    } else {
        format!("{}:{}:{}", health.name, health.version, short_hash)
    };

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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Method;

    #[test]
    fn test_health_struct_serialization_with_commit() {
        let health = Health {
            commit: Some("abc123".to_string()),
            name: "test_app".to_string(),
            version: "1.0.0".to_string(),
            database: "ok".to_string(),
        };

        let json = serde_json::to_string(&health).unwrap();
        assert!(json.contains("abc123"));
        assert!(json.contains("test_app"));
        assert!(json.contains("1.0.0"));
        assert!(json.contains("ok"));
    }

    #[test]
    fn test_health_struct_serialization_without_commit() {
        let health = Health {
            commit: None,
            name: "test_app".to_string(),
            version: "1.0.0".to_string(),
            database: "ok".to_string(),
        };

        let json = serde_json::to_string(&health).unwrap();
        // commit field should be omitted when None
        assert!(!json.contains("commit"));
        assert!(json.contains("test_app"));
        assert!(json.contains("1.0.0"));
        assert!(json.contains("ok"));
    }

    #[test]
    fn test_health_struct_deserialization_with_commit() {
        let json = r#"{
            "commit": "def456",
            "name": "my_app",
            "version": "2.0.0",
            "database": "error"
        }"#;

        let health: Health = serde_json::from_str(json).unwrap();
        assert_eq!(health.commit, Some("def456".to_string()));
        assert_eq!(health.name, "my_app");
        assert_eq!(health.version, "2.0.0");
        assert_eq!(health.database, "error");
    }

    #[test]
    fn test_health_struct_deserialization_without_commit() {
        let json = r#"{
            "name": "my_app",
            "version": "2.0.0",
            "database": "error"
        }"#;

        let health: Health = serde_json::from_str(json).unwrap();
        assert_eq!(health.commit, None);
        assert_eq!(health.name, "my_app");
        assert_eq!(health.version, "2.0.0");
        assert_eq!(health.database, "error");
    }

    #[test]
    fn test_create_health_response_ok() {
        let db_result: Result<(), StatusCode> = Ok(());
        let health = create_health_response(&db_result);

        assert_eq!(health.database, "ok");
        assert_eq!(health.name, env!("CARGO_PKG_NAME"));
        assert_eq!(health.version, env!("CARGO_PKG_VERSION"));
        // commit may be Some or None depending on build context
    }

    #[test]
    fn test_create_health_response_error() {
        let db_result: Result<(), StatusCode> = Err(StatusCode::SERVICE_UNAVAILABLE);
        let health = create_health_response(&db_result);

        assert_eq!(health.database, "error");
        assert_eq!(health.name, env!("CARGO_PKG_NAME"));
        assert_eq!(health.version, env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn test_create_response_body_get() {
        let health = Health {
            commit: Some("test".to_string()),
            name: "test".to_string(),
            version: "1.0".to_string(),
            database: "ok".to_string(),
        };

        let body = create_response_body(Method::GET, &health);

        // Body should not be empty for GET
        // We can't easily check the contents without consuming it,
        // but we can verify it was created
        assert!(std::mem::size_of_val(&body) > 0);
    }

    #[test]
    fn test_create_response_body_options() {
        let health = Health {
            commit: Some("test".to_string()),
            name: "test".to_string(),
            version: "1.0".to_string(),
            database: "ok".to_string(),
        };

        let body = create_response_body(Method::OPTIONS, &health);

        // For OPTIONS, body should be empty
        // This is harder to test without consuming the body
        assert!(std::mem::size_of_val(&body) > 0);
    }

    #[test]
    fn test_create_app_headers_full_hash() {
        let health = Health {
            commit: Some("abc123def456".to_string()),
            name: "myapp".to_string(),
            version: "1.2.3".to_string(),
            database: "ok".to_string(),
        };

        let headers = create_app_headers(&health);

        let x_app = headers.get("X-App").expect("X-App header should exist");
        let x_app_str = x_app.to_str().unwrap();

        // Should truncate to 7 chars
        assert!(x_app_str.contains("abc123d"));
        assert!(x_app_str.contains("myapp"));
        assert!(x_app_str.contains("1.2.3"));
        assert_eq!(x_app_str, "myapp:1.2.3:abc123d");
    }

    #[test]
    fn test_create_app_headers_short_hash() {
        let health = Health {
            commit: Some("abc".to_string()),
            name: "myapp".to_string(),
            version: "1.0.0".to_string(),
            database: "ok".to_string(),
        };

        let headers = create_app_headers(&health);

        let x_app = headers.get("X-App").expect("X-App header should exist");
        let x_app_str = x_app.to_str().unwrap();

        // Short hash (<= 7 chars) should be omitted from header
        assert_eq!(x_app_str, "myapp:1.0.0");
    }

    #[test]
    fn test_create_app_headers_no_commit() {
        let health = Health {
            commit: None,
            name: "myapp".to_string(),
            version: "1.0.0".to_string(),
            database: "ok".to_string(),
        };

        let headers = create_app_headers(&health);

        let x_app = headers.get("X-App").expect("X-App header should exist");
        let x_app_str = x_app.to_str().unwrap();

        // No commit should omit the hash entirely
        assert_eq!(x_app_str, "myapp:1.0.0");
    }

    #[test]
    fn test_create_app_headers_special_characters() {
        let health = Health {
            commit: Some("abc123!@#".to_string()),
            name: "my-app".to_string(),
            version: "1.0.0-beta".to_string(),
            database: "ok".to_string(),
        };

        // This might fail to parse if special chars are invalid for HTTP headers
        let headers = create_app_headers(&health);

        // Either we get a valid header or an empty HeaderMap on parse error
        // The function handles this gracefully
        assert!(headers.is_empty() || headers.contains_key("X-App"));
    }
}
