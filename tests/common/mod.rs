#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::panic)]
#![allow(clippy::indexing_slicing)]
#![allow(dead_code)]

use anyhow::{Context, Result};
use secrecy::SecretString;
use sqlx::PgPool;
use std::{
    env,
    sync::atomic::{AtomicU64, Ordering},
};
use url::Url;

static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Convert metric values (stored as f64) back to i64 safely for assertions.
///
/// Metrics in the codebase are always integer counters or gauges, but Prometheus
/// stores them internally as `f64`. To avoid lossy `as i64` casts in tests, we
/// round and parse through a string representation. Panic if the value is not a
/// whole number or does not fit in `i64` (which would indicate a bug in the test).
#[must_use]
pub fn metric_value_to_i64(value: f64) -> i64 {
    assert!(
        value.is_finite(),
        "metric values must be finite, got {value}"
    );
    let rounded = value.round();
    let as_string = format!("{rounded:.0}");
    as_string
        .parse::<i64>()
        .unwrap_or_else(|_| panic!("metric value {value} does not fit in i64"))
}

/// Get the test database DSN from environment
///
/// SAFETY: Tests should ALWAYS run against localhost to avoid accidentally
/// running against production databases. If `PG_EXPORTER_DSN` is set (e.g., in .envrc),
/// we verify it points to localhost. Use 'just test' which handles this automatically.
pub fn get_test_dsn() -> String {
    let dsn = env::var("PG_EXPORTER_DSN")
        .unwrap_or_else(|_| "postgresql://postgres:postgres@localhost:5432/postgres".to_string());

    // Safety check: ensure we're not accidentally testing against a remote database
    if !dsn.contains("localhost") && !dsn.contains("127.0.0.1") && !dsn.contains("::1") {
        eprintln!("WARNING: PG_EXPORTER_DSN points to a remote database!");
        eprintln!("DSN: {}", dsn.replace(char::is_alphanumeric, "*"));
        eprintln!("Tests should run against localhost only.");
        eprintln!("Use: just test (handles this automatically)");
        eprintln!(
            "Or:   PG_EXPORTER_DSN='postgresql://postgres:postgres@localhost:5432/postgres' cargo test"
        );
        panic!("Refusing to run tests against remote database. Use localhost.");
    }

    dsn
}

/// Create a test database pool
pub async fn create_test_pool() -> Result<PgPool> {
    let dsn = get_test_dsn();
    let pool = PgPool::connect(&dsn).await?;
    Ok(pool)
}

fn quoted_identifier(identifier: &str) -> String {
    format!("\"{identifier}\"")
}

fn sanitize_database_prefix(prefix: &str) -> String {
    let sanitized: String = prefix
        .chars()
        .map(|ch| {
            if ch.is_ascii_lowercase() || ch.is_ascii_digit() {
                ch
            } else if ch.is_ascii_uppercase() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect();

    let trimmed = sanitized.trim_matches('_');
    let collapsed = trimmed
        .split('_')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
        .join("_");

    if collapsed.is_empty() {
        "db".to_string()
    } else {
        collapsed.chars().take(16).collect()
    }
}

fn next_test_database_name(prefix: &str) -> String {
    let counter = TEST_DB_COUNTER.fetch_add(1, Ordering::Relaxed);
    let prefix = sanitize_database_prefix(prefix);
    format!("test_{prefix}_{}_{}", std::process::id(), counter)
}

fn dsn_for_database(base_dsn: &str, database_name: &str) -> Result<String> {
    let mut url = Url::parse(base_dsn).context("Failed to parse test DSN")?;
    url.set_path(&format!("/{database_name}"));
    Ok(url.to_string())
}

async fn extension_available(pool: &PgPool, extension_name: &str) -> Result<bool> {
    sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM pg_available_extensions WHERE name = $1)",
    )
    .bind(extension_name)
    .fetch_one(pool)
    .await
    .context("Failed to query available PostgreSQL extensions")
}

fn pg_stat_statements_requires_preload(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .to_string()
            .contains(r#"pg_stat_statements must be loaded via "shared_preload_libraries""#)
    })
}

async fn drop_test_database(admin_dsn: &str, database_name: &str) -> Result<()> {
    let admin_pool = PgPool::connect(admin_dsn)
        .await
        .context("Failed to connect to administrative test database")?;

    sqlx::query(
        "SELECT pg_terminate_backend(pid)
         FROM pg_stat_activity
         WHERE datname = $1
           AND pid <> pg_backend_pid()",
    )
    .bind(database_name)
    .execute(&admin_pool)
    .await
    .with_context(|| format!("Failed to terminate connections to database {database_name}"))?;

    sqlx::query(&format!(
        "DROP DATABASE IF EXISTS {}",
        quoted_identifier(database_name)
    ))
    .execute(&admin_pool)
    .await
    .with_context(|| format!("Failed to drop database {database_name}"))?;

    admin_pool.close().await;
    Ok(())
}

async fn cleanup_isolated_database(
    admin_dsn: &str,
    database_name: &str,
    pool: PgPool,
) -> Result<()> {
    pool.close().await;
    drop_test_database(admin_dsn, database_name).await
}

pub struct IsolatedTestDatabase {
    admin_dsn: String,
    database_name: String,
    pool: Option<PgPool>,
}

impl IsolatedTestDatabase {
    pub async fn new(prefix: &str) -> Result<Self> {
        let admin_dsn = get_test_dsn();
        let admin_pool = PgPool::connect(&admin_dsn)
            .await
            .context("Failed to connect to administrative test database")?;
        let database_name = next_test_database_name(prefix);

        sqlx::query(&format!(
            "CREATE DATABASE {} TEMPLATE template0",
            quoted_identifier(&database_name)
        ))
        .execute(&admin_pool)
        .await
        .with_context(|| format!("Failed to create database {database_name}"))?;

        admin_pool.close().await;

        let database_dsn = dsn_for_database(&admin_dsn, &database_name)?;
        let pool = PgPool::connect(&database_dsn)
            .await
            .with_context(|| format!("Failed to connect to isolated database {database_name}"))?;

        Ok(Self {
            admin_dsn,
            database_name,
            pool: Some(pool),
        })
    }

    pub async fn with_pg_stat_statements(prefix: &str) -> Result<Option<Self>> {
        let test_db = Self::new(prefix).await?;

        if !extension_available(test_db.pool(), "pg_stat_statements").await? {
            test_db.cleanup().await?;
            return Ok(None);
        }

        sqlx::query("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
            .execute(test_db.pool())
            .await
            .context("Failed to create pg_stat_statements extension in test database")?;

        if let Err(error) = reset_pg_stat_statements_current_database(test_db.pool()).await {
            if pg_stat_statements_requires_preload(&error) {
                test_db.cleanup().await?;
                return Ok(None);
            }

            return Err(error);
        }

        Ok(Some(test_db))
    }

    #[must_use]
    pub fn pool(&self) -> &PgPool {
        self.pool
            .as_ref()
            .expect("isolated test database pool should exist until cleanup")
    }

    #[must_use]
    pub fn database_name(&self) -> &str {
        &self.database_name
    }

    pub async fn cleanup(mut self) -> Result<()> {
        if let Some(pool) = self.pool.take() {
            cleanup_isolated_database(&self.admin_dsn, &self.database_name, pool).await?;
        }

        Ok(())
    }
}

impl Drop for IsolatedTestDatabase {
    fn drop(&mut self) {
        let Some(pool) = self.pool.take() else {
            return;
        };

        let admin_dsn = self.admin_dsn.clone();
        let database_name = self.database_name.clone();

        let _ = std::thread::Builder::new()
            .name("isolated-test-db-cleanup".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build();

                match runtime {
                    Ok(runtime) => {
                        if let Err(error) = runtime.block_on(cleanup_isolated_database(
                            &admin_dsn,
                            &database_name,
                            pool,
                        )) {
                            eprintln!(
                                "Failed to clean up isolated test database {database_name}: {error}"
                            );
                        }
                    }
                    Err(error) => {
                        eprintln!(
                            "Failed to build cleanup runtime for isolated test database {database_name}: {error}"
                        );
                    }
                }
            });
    }
}

pub async fn create_pg_statements_test_database(
    prefix: &str,
) -> Result<Option<IsolatedTestDatabase>> {
    IsolatedTestDatabase::with_pg_stat_statements(prefix).await
}

pub async fn reset_pg_stat_statements_current_database(pool: &PgPool) -> Result<()> {
    let server_version_num =
        sqlx::query_scalar::<_, i32>("SELECT current_setting('server_version_num')::int")
            .fetch_one(pool)
            .await
            .context("Failed to determine PostgreSQL server version")?;

    if server_version_num >= 170_000 {
        sqlx::query(
            "SELECT pg_stat_statements_reset(
                0::oid,
                (SELECT oid FROM pg_database WHERE datname = current_database()),
                0::bigint,
                false
            )",
        )
        .execute(pool)
        .await
        .context("Failed to reset pg_stat_statements for current database")?;
    } else {
        sqlx::query(
            "SELECT pg_stat_statements_reset(
                0::oid,
                (SELECT oid FROM pg_database WHERE datname = current_database()),
                0::bigint
            )",
        )
        .execute(pool)
        .await
        .context("Failed to reset pg_stat_statements for current database")?;
    }

    Ok(())
}

/// Get test DSN as `SecretString`
pub fn get_test_dsn_secret() -> SecretString {
    SecretString::from(get_test_dsn())
}

/// Find an available port for testing (returns port > 1024)
pub fn get_available_port() -> u16 {
    use std::net::TcpListener;

    // Bind to port 0 lets the OS assign an available ephemeral port
    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind to random port");
    let port = listener
        .local_addr()
        .expect("Failed to get local addr")
        .port();

    // Ephemeral ports are typically > 1024, usually 32768-60999 on Linux
    assert!(port > 1024, "Assigned port {port} should be > 1024");

    port
}

/// Wait for server to be ready on the given port
///
/// # Arguments
/// * `port` - The port number to connect to (should be > 1024)
/// * `max_attempts` - Maximum number of connection attempts (e.g., 50 = 5 seconds at 100ms intervals)
pub async fn wait_for_server(port: u16, max_attempts: u32) -> bool {
    use tokio::time::{Duration, sleep};

    for attempt in 1..=max_attempts {
        // Use localhost which will try both IPv4 and IPv6
        if tokio::net::TcpStream::connect(format!("localhost:{port}"))
            .await
            .is_ok()
        {
            return true;
        }

        if attempt % 10 == 0 {
            eprintln!("Still waiting for server on port {port} (attempt {attempt}/{max_attempts})");
        }

        sleep(Duration::from_millis(100)).await;
    }

    eprintln!("Failed to connect to server on port {port} after {max_attempts} attempts");
    false
}

/// Get base URL for test server
pub fn get_test_url(port: u16) -> String {
    format!("http://localhost:{port}")
}
