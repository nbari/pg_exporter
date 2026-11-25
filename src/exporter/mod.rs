use crate::{
    cli::telemetry::shutdown_tracer,
    collectors::{
        SHARED_POOL_MAX_CONNECTIONS,
        config::CollectorConfig,
        registry::CollectorRegistry,
        util::{
            apply_connection_hardening, get_connect_timeout, get_excluded_databases,
            set_base_connect_options_from_dsn, set_pg_version, validate_connect_timeout_budget,
        },
    },
};
use anyhow::{Context, Result, anyhow};
use axum::{
    Extension, Router,
    body::Body,
    http::{HeaderName, HeaderValue, Request},
    routing::get,
};
use secrecy::{ExposeSecret, SecretString};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::{net::IpAddr, str::FromStr, time::Duration};
use tokio::{net::TcpListener, time::timeout};
use tower::ServiceBuilder;
use tower_http::{
    request_id::PropagateRequestIdLayer, set_header::SetRequestHeaderLayer, trace::TraceLayer,
};
use tracing::{Span, error, info, info_span, warn};
use ulid::Ulid;

mod handlers;
mod shutdown;
#[cfg(feature = "telemetry")]
mod telemetry;

pub mod built_info {
    #![allow(clippy::doc_markdown)]
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

pub const GIT_COMMIT_HASH: Option<&str> = built_info::GIT_COMMIT_HASH;

/// Starts the `PostgreSQL` metrics exporter
///
/// # Errors
///
/// Returns an error if database connection, HTTP server setup, or shutdown handling fails
pub async fn new(
    port: u16,
    listen: Option<String>,
    dsn: SecretString,
    collector_config: CollectorConfig,
) -> Result<()> {
    let pool = connect_pool(&dsn)?;

    // Try to initialize version, but don't block startup if DB is down
    let _ = timeout(Duration::from_secs(1), initialize_version(&pool)).await;

    set_base_connect_options_from_dsn(&dsn).context("Failed to parse base DSN options")?;
    let enabled_collectors = collector_config.enabled_collectors_in_order();
    warn_if_system_collector_remote(&dsn, &enabled_collectors);
    let registry = CollectorRegistry::new(&collector_config);

    let app = build_router(pool.clone(), registry);

    let (listener, bind_addr) = bind_listener(port, listen).await?;

    let excluded = get_excluded_databases();

    print_startup(&bind_addr, &enabled_collectors, excluded);

    run_server(listener, app).await;

    info!("shutting down");

    let close_timeout = get_connect_timeout();
    let closed = timeout(close_timeout, pool.close())
        .await
        .with_context(|| format!("closing PostgreSQL pool exceeded timeout of {close_timeout:?}"));

    shutdown_tracer();

    closed
}

fn connect_pool(dsn: &SecretString) -> Result<sqlx::PgPool> {
    validate_connect_timeout_budget()?;

    let opts = apply_connection_hardening(
        PgConnectOptions::from_str(dsn.expose_secret())
            .context("Failed to parse base DSN options")?,
    )?;

    // Idle connections are validated on checkout (the sqlx default). After a restart or
    // failover every pooled connection is dead; without the check each scrape drew one,
    // failed its connectivity query and reported `pg_up 0`, once per pooled connection,
    // while the server was already accepting connections again. With it, a dead
    // connection is replaced inside the same acquire, at the cost of one Sync round trip
    // per checkout.
    let pool = PgPoolOptions::new()
        .min_connections(0)
        .max_connections(SHARED_POOL_MAX_CONNECTIONS)
        .acquire_timeout(get_connect_timeout())
        .max_lifetime(Duration::from_mins(2))
        .connect_lazy_with(opts);

    info!("Database connection pool initialized (lazy)");

    Ok(pool)
}

async fn initialize_version(pool: &sqlx::PgPool) -> Result<()> {
    let version_num: String = sqlx::query_scalar("SHOW server_version_num")
        .fetch_one(pool)
        .await
        .context("Failed to get PostgreSQL version")?;

    let version: i32 = version_num
        .parse()
        .context("Failed to parse PostgreSQL version")?;
    set_pg_version(version);
    info!(version, "PostgreSQL version detected");
    Ok(())
}

fn build_router(pool: sqlx::PgPool, registry: CollectorRegistry) -> Router {
    let trace_layer = TraceLayer::new_for_http()
        .make_span_with(make_span)
        .on_response(on_response);

    let middleware = ServiceBuilder::new()
        .layer(SetRequestHeaderLayer::if_not_present(
            HeaderName::from_static("x-request-id"),
            |_req: &_| HeaderValue::from_str(Ulid::generate().to_string().as_str()).ok(),
        ))
        .layer(PropagateRequestIdLayer::new(HeaderName::from_static(
            "x-request-id",
        )))
        .layer(trace_layer);
    #[cfg(feature = "telemetry")]
    let middleware = middleware.layer(axum::middleware::from_fn(telemetry::add_trace_headers));

    Router::new()
        .route("/metrics", get(handlers::metrics))
        .route("/health", get(handlers::health).options(handlers::health))
        .layer(middleware.layer(Extension(pool)).layer(Extension(registry)))
}

async fn bind_listener(port: u16, listen: Option<String>) -> Result<(TcpListener, String)> {
    if let Some(addr) = listen {
        let ip = addr.parse::<std::net::IpAddr>().map_err(|_| {
            anyhow!(
                "Invalid IP address: '{addr}'. Expected IPv4 (e.g., 0.0.0.0, 127.0.0.1) or IPv6 (e.g., ::, ::1)"
            )
        })?;
        let bind_addr = format!("{ip}:{port}");
        let listener = TcpListener::bind(&bind_addr)
            .await
            .with_context(|| format!("Failed to bind to {bind_addr}"))?;
        let display = if ip.is_ipv6() {
            format!("[{ip}]:{port}")
        } else {
            bind_addr.clone()
        };
        Ok((listener, display))
    } else {
        if let Ok(listener) = TcpListener::bind(format!("::0:{port}")).await {
            return Ok((listener, format!("[::]:{port}")));
        }
        let listener = TcpListener::bind(format!("0.0.0.0:{port}")).await?;
        Ok((listener, format!("0.0.0.0:{port}")))
    }
}

fn print_startup(bind_addr: &str, collectors: &[String], excluded: &[String]) {
    println!(
        "{} {} - Listening on {bind_addr}\n\nEnabled collectors:\n{}",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION"),
        format_list(collectors),
    );

    if !excluded.is_empty() {
        println!("\nExcluded databases:\n{}", format_list(excluded));
    }
}

/// Returns `true` when `host` is a loopback address or a Unix-domain socket
/// path, i.e. the database is on the same machine as the exporter.
fn is_local_db_host(host: &str) -> bool {
    host.is_empty()
        || host.starts_with('/')
        || host.eq_ignore_ascii_case("localhost")
        || host == "::1"
        || host == "[::1]"
        || host.starts_with("127.")
}

/// The address `hostaddr` sends the TCP connection to, if one is set: the DSN parameter,
/// or else `PGHOSTADDR`, which is the precedence `SQLx` applies.
///
/// Since `SQLx` 0.9.1 `hostaddr` no longer replaces `host`, and the parsed options expose
/// no getter for it. It is read from the DSN text because the one way to get it back out
/// of the options, `to_url_lossy`, panics on options it cannot format as a URL (an empty
/// host, or a user name containing `/`).
fn connect_hostaddr(dsn: &str, env_hostaddr: Option<&str>) -> Option<IpAddr> {
    // The last `hostaddr` wins when the parameter is repeated, as it does in `SQLx`.
    let from_dsn = url::Url::parse(dsn).ok().and_then(|url| {
        url.query_pairs()
            .filter(|(key, _)| key == "hostaddr")
            .last()
            .map(|(_, value)| value.into_owned())
    });

    from_dsn
        .as_deref()
        .or(env_hostaddr)
        .and_then(|value| value.parse().ok())
}

/// Returns `true` when the DSN connects to the machine the exporter runs on.
///
/// A Unix socket is always local and ignores `hostaddr`. Otherwise `hostaddr` decides
/// where the connection goes and `host` only names the server for TLS and `.pgpass`,
/// so `host=localhost&hostaddr=10.0.0.5` is a remote database.
fn is_local_db_target(opts: &PgConnectOptions, hostaddr: Option<IpAddr>) -> bool {
    let host = opts.get_host();
    if opts.get_socket().is_some() || host.starts_with('/') {
        return true;
    }

    hostaddr.map_or_else(|| is_local_db_host(host), |addr| addr.is_loopback())
}

/// Warns when `--collector.system` is enabled but the DSN points at a remote
/// host. The host CPU/memory metrics describe the exporter's machine, so on a
/// managed service (RDS/Aurora) or any non-co-located setup they do not reflect
/// the database server and are misleading.
fn warn_if_system_collector_remote(dsn: &SecretString, enabled_collectors: &[String]) {
    if !enabled_collectors.iter().any(|c| c == "system") {
        return;
    }

    let Ok(opts) = PgConnectOptions::from_str(dsn.expose_secret()) else {
        return;
    };

    let env_hostaddr = std::env::var("PGHOSTADDR").ok();
    let hostaddr = connect_hostaddr(dsn.expose_secret(), env_hostaddr.as_deref());

    if !is_local_db_target(&opts, hostaddr) {
        warn!(
            host = opts.get_host(),
            "--collector.system reports host CPU/memory for the exporter's own \
             machine, but the DSN points at a remote database host; enable it \
             only when the exporter is co-located with PostgreSQL (not on managed \
             services such as RDS/Aurora)"
        );
    }
}

async fn run_server(listener: TcpListener, app: Router) {
    if let Err(e) = axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(shutdown::shutdown_signal_handler())
        .await
    {
        error!(error=%e, "server error");
    }
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
        otel.status_code = tracing::field::Empty,
        http.method = method,
        http.route = path,
        http.target = target,
        http.scheme = scheme,
        http.user_agent = user_agent,
        request_id = request_id,
    );

    #[cfg(feature = "telemetry")]
    telemetry::set_parent(request, &span);

    span
}

fn on_response<B>(response: &axum::http::Response<B>, latency: Duration, span: &Span) {
    if response.status().is_server_error() {
        span.record("otel.status_code", "ERROR");
    } else {
        span.record("otel.status_code", "OK");
    }

    #[allow(clippy::cast_possible_truncation)]
    let elapsed_ms = latency.as_millis() as u64;

    #[cfg(feature = "telemetry")]
    if let Some(trace_id) = telemetry::trace_id(span) {
        info!(
            parent: span,
            status = response.status().as_u16(),
            elapsed_ms,
            trace_id = %trace_id,
            "request completed"
        );
        return;
    }
    info!(parent: span, status = response.status().as_u16(), elapsed_ms, "request completed");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_records_status_on_request_span() -> Result<()> {
        use crate::trace_capture;
        use anyhow::{Context as _, ensure};

        let (subscriber, observations) = trace_capture::subscriber();
        tracing::subscriber::with_default(subscriber, || -> Result<()> {
            for (status, expected) in [(200, "OK"), (400, "OK"), (500, "ERROR"), (503, "ERROR")] {
                let request = Request::builder()
                    .uri("/metrics")
                    .header("x-request-id", "status-probe")
                    .body(Body::empty())?;
                let span = make_span(&request);
                let id = span.id().context("request span disabled")?;
                let response = axum::http::Response::builder().status(status).body(())?;
                on_response(&response, Duration::from_millis(1), &span);
                let event = observations
                    .try_iter()
                    .find(|event| {
                        event
                            .fields
                            .get("message")
                            .is_some_and(|message| message == "request completed")
                    })
                    .context("completion log missing")?;
                trace_capture::assert_request(&event, &id, "status-probe")?;
                ensure!(
                    event
                        .scope
                        .first()
                        .and_then(|root| root.fields.get("otel.status_code"))
                        .map(String::as_str)
                        == Some(expected),
                    "status {status} was not recorded: {event:?}"
                );
            }
            Ok(())
        })
    }

    /// A connection the server closed while it sat idle in the pool must be replaced
    /// on checkout, not handed to the next query. A restart or failover closes all of
    /// them at once, and each one that reached a scrape's connectivity check turned
    /// into a false `pg_up 0` after the server was back.
    #[tokio::test]
    async fn pool_replaces_idle_connections_the_server_closed() -> Result<()> {
        use anyhow::Context as _;
        use sqlx::Connection as _;

        let dsn = std::env::var("PG_EXPORTER_DSN").unwrap_or_else(|_| {
            "postgresql://postgres:postgres@localhost:5432/postgres".to_owned()
        });
        let pool = connect_pool(&SecretString::from(dsn.clone()))?;
        if sqlx::query("SELECT 1").fetch_one(&pool).await.is_err() {
            eprintln!("skipping: PostgreSQL is not reachable");
            return Ok(());
        }

        // Check out every pooled connection at once so all of them exist, then let
        // them go idle.
        let mut held = Vec::new();
        let mut pids: Vec<i32> = Vec::new();
        for _ in 0..SHARED_POOL_MAX_CONNECTIONS {
            let mut conn = pool.acquire().await?;
            pids.push(
                sqlx::query_scalar("SELECT pg_backend_pid()")
                    .fetch_one(&mut *conn)
                    .await?,
            );
            held.push(conn);
        }
        drop(held);
        for _ in 0..200 {
            if pool.num_idle() == pids.len() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        anyhow::ensure!(
            pool.num_idle() == pids.len(),
            "pooled connections did not return to idle"
        );

        // What a restart or failover does to them: the server ends the backends.
        let mut admin = sqlx::PgConnection::connect(&dsn).await?;
        sqlx::query("SELECT pg_terminate_backend(pid) FROM unnest($1::int[]) AS pid")
            .bind(&pids)
            .execute(&mut admin)
            .await?;
        let mut alive = pids.len();
        for _ in 0..200 {
            let remaining: i64 =
                sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE pid = ANY($1)")
                    .bind(&pids)
                    .fetch_one(&mut admin)
                    .await?;
            alive = usize::try_from(remaining)?;
            if alive == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        admin.close().await?;
        anyhow::ensure!(alive == 0, "{alive} terminated backends are still running");

        // One query per connection that died: every one must succeed first time.
        for attempt in 1..=pids.len() {
            sqlx::query("SELECT 1")
                .fetch_one(&pool)
                .await
                .with_context(|| {
                    format!("query {attempt} was given a pooled connection the server had closed")
                })?;
        }

        pool.close().await;
        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_git_commit_hash_is_valid_if_present() {
        // GIT_COMMIT_HASH is an Option - either Some(hash) or None
        if let Some(hash) = GIT_COMMIT_HASH {
            // If present, should be a valid git hash (hex string)
            assert!(
                hash.len() >= 7,
                "Git commit hash should be at least 7 chars, got: {hash}"
            );
            assert!(
                hash.chars().all(|c| c.is_ascii_hexdigit()),
                "Git commit hash should be hex digits, got: {hash}"
            );
        } else {
            // None is valid when not built from git (e.g., cargo install from crates.io)
            println!("No git commit hash available (normal for crates.io installs)");
        }
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_format_list_empty() {
        let items: Vec<String> = vec![];
        let result = format_list(&items);
        assert_eq!(result, "");
    }

    #[test]
    fn test_is_local_db_host() {
        for host in [
            "localhost",
            "LOCALHOST",
            "127.0.0.1",
            "127.5.6.7",
            "::1",
            "",
            "/tmp",
            "/var/run/postgresql",
        ] {
            assert!(is_local_db_host(host), "expected {host:?} to be local");
        }
        for host in [
            "db.example.com",
            "10.0.0.5",
            "192.168.1.10",
            "my-rds.abc123.us-east-1.rds.amazonaws.com",
            "::2",
        ] {
            assert!(!is_local_db_host(host), "expected {host:?} to be remote");
        }
    }

    /// `hostaddr` decides where the connection goes. `SQLx` 0.9.0 folded it into `host`;
    /// 0.9.1 keeps them apart, so judging locality by `host` alone called
    /// `host=localhost&hostaddr=10.0.0.5` local and dropped the system-collector warning.
    #[test]
    fn test_is_local_db_target_follows_hostaddr() -> Result<()> {
        // (DSN, PGHOSTADDR, expected local)
        for (dsn, env_hostaddr, local) in [
            ("postgresql://user@localhost:5432/postgres", None, true),
            (
                "postgresql://user@db.example.com:5432/postgres",
                None,
                false,
            ),
            (
                "postgresql://user@localhost:5432/postgres?hostaddr=10.0.0.5",
                None,
                false,
            ),
            (
                "postgresql://user@db.example.com:5432/postgres?hostaddr=127.0.0.1",
                None,
                true,
            ),
            (
                "postgresql://user@db.example.com:5432/postgres?hostaddr=::1",
                None,
                true,
            ),
            // `PGHOSTADDR` applies when the DSN sets no `hostaddr`, and loses to one.
            (
                "postgresql://user@localhost:5432/postgres",
                Some("10.0.0.5"),
                false,
            ),
            (
                "postgresql://user@db.example.com:5432/postgres?hostaddr=127.0.0.1",
                Some("10.0.0.5"),
                true,
            ),
            // A repeated `hostaddr`: the last one is where the connection goes.
            (
                "postgresql://user@localhost:5432/postgres?hostaddr=127.0.0.1&hostaddr=10.0.0.5",
                None,
                false,
            ),
            (
                "postgresql://user@localhost:5432/postgres?hostaddr=10.0.0.5&hostaddr=127.0.0.1",
                None,
                true,
            ),
            // A Unix socket ignores `hostaddr`.
            (
                "postgresql:///postgres?host=/var/run/postgresql&hostaddr=10.0.0.5",
                None,
                true,
            ),
            // A bracketed IPv6 loopback literal, as a URL requires it.
            ("postgresql://user@[::1]:5432/postgres", None, true),
            ("postgresql://user@[2001:db8::1]:5432/postgres", None, false),
        ] {
            let opts = PgConnectOptions::from_str(dsn)?;
            let hostaddr = connect_hostaddr(dsn, env_hostaddr);
            assert_eq!(
                is_local_db_target(&opts, hostaddr),
                local,
                "{dsn} with PGHOSTADDR={env_hostaddr:?} should be {}",
                if local { "local" } else { "remote" }
            );
        }
        Ok(())
    }

    /// These DSNs parse, but `SQLx` cannot format them back into a URL and panics trying.
    /// The startup warning must not go through that path: a panic here would take the
    /// exporter down before it serves a single scrape.
    #[test]
    fn test_remote_warning_survives_dsns_that_do_not_round_trip() {
        let system = vec!["system".to_owned()];
        for dsn in [
            // Explicitly empty host.
            "postgresql:///postgres?host=",
            // A user name that is only valid in a URL when percent-encoded.
            "postgresql://user%2Frole@localhost:5432/postgres",
            "postgresql://user%3Fname@db.example.com:5432/postgres?hostaddr=10.0.0.5",
        ] {
            assert!(
                PgConnectOptions::from_str(dsn).is_ok(),
                "{dsn} should parse; the test is only meaningful for DSNs that do"
            );
            warn_if_system_collector_remote(&SecretString::from(dsn.to_owned()), &system);
        }
    }

    #[test]
    fn test_warn_if_system_collector_remote_is_noop_when_disabled() {
        let dsn = SecretString::from("postgresql://user@db.example.com:5432/postgres".to_owned());
        // No "system" collector -> must not panic and simply return.
        warn_if_system_collector_remote(&dsn, &["default".to_owned(), "vacuum".to_owned()]);
    }

    #[test]
    fn test_warn_if_system_collector_remote_handles_local_and_remote() {
        let system = vec!["system".to_owned()];
        // Both branches must be panic-free regardless of host locality.
        warn_if_system_collector_remote(
            &SecretString::from("postgresql://user@localhost:5432/postgres".to_owned()),
            &system,
        );
        warn_if_system_collector_remote(
            &SecretString::from("postgresql://user@db.example.com:5432/postgres".to_owned()),
            &system,
        );
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_format_list_single_item() {
        let items = vec!["item1"];
        let result = format_list(&items);
        assert_eq!(result, "  - item1");
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_format_list_multiple_items() {
        let items = vec!["item1", "item2", "item3"];
        let result = format_list(&items);
        assert_eq!(result, "  - item1\n  - item2\n  - item3");
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_format_list_with_numbers() {
        let items = vec![1, 2, 3];
        let result = format_list(&items);
        assert_eq!(result, "  - 1\n  - 2\n  - 3");
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_format_list_formatting() {
        let items = vec!["collector1", "collector2"];
        let result = format_list(&items);

        // Should start with two spaces and a dash
        assert!(result.starts_with("  - "));

        // Should contain both items
        assert!(result.contains("collector1"));
        assert!(result.contains("collector2"));

        // Should have newline between items
        assert!(result.contains('\n'));
    }

    // Test the on_response function behavior
    #[test]
    #[allow(clippy::unwrap_used)]
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
    #[allow(clippy::unwrap_used)]
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
            span.metadata().map(tracing::Metadata::name),
            Some("http.server.request")
        );
    }

    #[test]
    #[allow(clippy::unwrap_used)]
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
            span.metadata().map(tracing::Metadata::name),
            Some("http.server.request")
        );
    }

    #[test]
    #[allow(clippy::unwrap_used)]
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
            span.metadata().map(tracing::Metadata::name),
            Some("http.server.request")
        );
    }
}
