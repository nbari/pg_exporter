use anyhow::{Context, Result};
use axum::{
    Extension, Router,
    body::Body,
    http::{HeaderName, HeaderValue, Request},
    routing::get,
};
use secrecy::{ExposeSecret, SecretString};
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::{
    request_id::PropagateRequestIdLayer, set_header::SetRequestHeaderLayer, trace::TraceLayer,
};
use tracing::{Span, debug_span, info};
use ulid::Ulid;

mod handlers;

pub mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

pub const GIT_COMMIT_HASH: &str = if let Some(hash) = built_info::GIT_COMMIT_HASH {
    hash
} else {
    ":-("
};

/// router
/// # Errors
/// Returns an error if the server fails to start
pub async fn new(port: u16, dsn: SecretString) -> Result<()> {
    let db_dsn = dsn.expose_secret().to_string();

    // Connect to database
    let pool = PgPoolOptions::new()
        .min_connections(1)
        .max_connections(3)
        .max_lifetime(Duration::from_secs(60 * 2))
        .test_before_acquire(true)
        .connect(&db_dsn)
        .await
        .context("Failed to connect to database")?;

    info!("Connected to database");

    let app = Router::new()
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
                .layer(TraceLayer::new_for_http().make_span_with(make_span))
                .layer(Extension(pool.clone())),
        )
        .layer(Extension(pool));

    let listener = TcpListener::bind(format!("::0:{port}")).await?;

    println!("Listening on [::]:{}", port);

    axum::serve(listener, app.into_make_service()).await?;

    Ok(())
}

// span
fn make_span(request: &Request<Body>) -> Span {
    let headers = request.headers();
    let path = request.uri().path();
    let request_id = headers
        .get("x-request-id")
        .and_then(|val| val.to_str().ok())
        .unwrap_or("none");

    debug_span!("http-request", path, ?headers, request_id)
}
