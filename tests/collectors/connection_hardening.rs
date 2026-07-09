//! Integration test for the scrape-connection hardening that prevents the exporter from
//! exhausting `max_connections`.
//!
//! Regression context: scrape queries only take a weak `AccessShareLock`, but if another
//! session holds an `AccessExclusiveLock` on a queried relation (routine DDL such as
//! `ALTER TABLE`, `VACUUM FULL`, `REINDEX`, `TRUNCATE`, or an abandoned transaction) the
//! scrape blocks. A client-side timeout only drops the client future — the backend keeps
//! waiting server-side, holding its connection slot — so over successive scrapes blocked
//! backends accumulate until the whole cluster stops accepting connections. Setting a
//! server-side `lock_timeout` on every scrape connection makes a blocked scrape abort and
//! release its slot instead of queuing.
//!
//! This test proves the mechanism against a real `PostgreSQL`:
//! - a connection with `lock_timeout` set aborts a lock-blocked query with SQLSTATE 55P03,
//! - the exporter's real `apply_connection_hardening` helper behaves the same way,
//! - a connection WITHOUT `lock_timeout` stays blocked (the pre-fix behaviour).

use super::common;
use anyhow::{Result, bail};
use pg_exporter::collectors::config::CollectorConfig;
use secrecy::SecretString;
use sqlx::postgres::{PgConnectOptions, PgConnection};
use sqlx::{Connection, Executor};
use std::str::FromStr;
use std::time::{Duration, Instant};
use testcontainers_modules::testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::time::{sleep, timeout};

const POSTGRES_TAG: &str = "16";
const CONNECT_ATTEMPTS: u32 = 60;

/// `PostgreSQL` SQLSTATE raised when a statement aborts because it could not acquire a lock
/// within `lock_timeout`.
const LOCK_NOT_AVAILABLE: &str = "55P03";

async fn start_postgres() -> Result<ContainerAsync<GenericImage>> {
    GenericImage::new("postgres", POSTGRES_TAG)
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_DB", "postgres")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .start()
        .await
        .map_err(|error| anyhow::anyhow!("failed to start postgres container: {error}"))
}

async fn container_dsn(container: &ContainerAsync<GenericImage>) -> Result<String> {
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(5432.tcp()).await?;
    Ok(format!(
        "postgresql://postgres:postgres@{host}:{port}/postgres"
    ))
}

async fn connect_with_retry(opts: &PgConnectOptions) -> Result<PgConnection> {
    let mut last_error = None;
    for _ in 0..CONNECT_ATTEMPTS {
        match PgConnection::connect_with(opts).await {
            Ok(conn) => return Ok(conn),
            Err(error) => {
                last_error = Some(error);
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
    bail!("failed to connect to postgres; last_error={last_error:?}")
}

/// SQLSTATE returned by a query, or `None` when it did not fail with a database error.
fn database_error_code(result: &Result<u64, sqlx::Error>) -> Option<String> {
    match result {
        Err(sqlx::Error::Database(db_error)) => db_error.code().map(std::borrow::Cow::into_owned),
        _ => None,
    }
}

#[tokio::test]
async fn lock_timeout_aborts_blocked_scrape_instead_of_queuing() -> Result<()> {
    let test_name = "lock_timeout_aborts_blocked_scrape_instead_of_queuing";
    if !common::ensure_container_runtime_for_test(test_name)? {
        return Ok(());
    }

    let container = match start_postgres().await {
        Ok(container) => container,
        Err(error) => {
            if common::should_require_container_runtime() {
                return Err(error);
            }
            eprintln!("Skipping {test_name}: {error}");
            return Ok(());
        }
    };

    let dsn = container_dsn(&container).await?;
    let base_opts = PgConnectOptions::from_str(&dsn)?;

    // Set up a table and, on a dedicated session, hold an AccessExclusiveLock on it so any
    // concurrent SELECT (which needs an AccessShareLock) blocks — exactly the situation a
    // long DDL or abandoned transaction creates in production.
    let mut setup = connect_with_retry(&base_opts).await?;
    setup
        .execute("CREATE TABLE hardening_probe (id int)")
        .await?;

    let mut lock_holder = connect_with_retry(&base_opts).await?;
    lock_holder.execute("BEGIN").await?;
    lock_holder
        .execute("LOCK TABLE hardening_probe IN ACCESS EXCLUSIVE MODE")
        .await?;

    // 1) A connection with an explicit lock_timeout must fail fast with 55P03 rather than
    //    waiting indefinitely on the exclusive lock.
    let hardened_opts = base_opts.clone().options([("lock_timeout", "250")]);
    let mut hardened = connect_with_retry(&hardened_opts).await?;
    let hardened_result = timeout(
        Duration::from_secs(10),
        hardened.execute("SELECT count(*) FROM hardening_probe"),
    )
    .await
    .map(|res| res.map(|done| done.rows_affected()));
    let hardened_result = hardened_result
        .map_err(|_| anyhow::anyhow!("hardened query hung; lock_timeout was not enforced"))?;
    assert_eq!(
        database_error_code(&hardened_result).as_deref(),
        Some(LOCK_NOT_AVAILABLE),
        "hardened scrape query should abort with lock_not_available (55P03), got {hardened_result:?}"
    );

    // 2) The exporter's real hardening helper (default lock_timeout) must behave the same
    //    way, proving the shipping code path — not just a hand-built option — is protected.
    let exporter_opts =
        pg_exporter::collectors::util::apply_connection_hardening(base_opts.clone())?;
    let mut exporter_conn = connect_with_retry(&exporter_opts).await?;
    let exporter_result = timeout(
        Duration::from_secs(10),
        exporter_conn.execute("SELECT count(*) FROM hardening_probe"),
    )
    .await
    .map(|res| res.map(|done| done.rows_affected()))
    .map_err(|_| anyhow::anyhow!("exporter-hardened query hung; lock_timeout was not enforced"))?;
    assert_eq!(
        database_error_code(&exporter_result).as_deref(),
        Some(LOCK_NOT_AVAILABLE),
        "apply_connection_hardening scrape query should abort with 55P03, got {exporter_result:?}"
    );

    // 3) Control: without lock_timeout the query stays blocked (the pre-fix behaviour that
    //    let blocked backends pile up). We only wait briefly and assert it is still stuck.
    let mut unhardened = connect_with_retry(&base_opts).await?;
    let control = timeout(
        Duration::from_secs(2),
        unhardened.execute("SELECT count(*) FROM hardening_probe"),
    )
    .await;
    assert!(
        control.is_err(),
        "without lock_timeout the query must remain blocked on the exclusive lock, but it returned {control:?}"
    );

    // Release the lock and clean up.
    lock_holder.execute("ROLLBACK").await?;
    let _ = hardened.close().await;
    let _ = exporter_conn.close().await;
    let _ = unhardened.close().await;
    let _ = lock_holder.close().await;
    let _ = setup.close().await;

    Ok(())
}

#[tokio::test]
async fn metrics_scrape_fails_fast_and_connections_stay_bounded_when_table_is_locked() -> Result<()>
{
    let test_name = "metrics_scrape_fails_fast_and_connections_stay_bounded_when_table_is_locked";
    if !common::ensure_container_runtime_for_test(test_name)? {
        return Ok(());
    }

    let container = match start_postgres().await {
        Ok(container) => container,
        Err(error) => {
            if common::should_require_container_runtime() {
                return Err(error);
            }
            eprintln!("Skipping {test_name}: {error}");
            return Ok(());
        }
    };

    let dsn = container_dsn(&container).await?;
    let base_opts = PgConnectOptions::from_str(&dsn)?;

    let mut setup = connect_with_retry(&base_opts).await?;
    setup
        .execute("CREATE TABLE hardening_probe (id int)")
        .await?;
    setup
        .execute("INSERT INTO hardening_probe VALUES (1)")
        .await?;

    let mut lock_holder = connect_with_retry(&base_opts).await?;
    lock_holder.execute("BEGIN").await?;
    lock_holder
        .execute("LOCK TABLE hardening_probe IN ACCESS EXCLUSIVE MODE")
        .await?;

    let hardened_dsn =
        format!("{dsn}?options=-c%20lock_timeout%3D250%20-c%20statement_timeout%3D5000");
    let port = common::get_available_port();
    let collector_config = CollectorConfig::new(25).with_enabled(&["stat".to_string()]);

    let handle = tokio::spawn(async move {
        pg_exporter::exporter::new(
            port,
            None,
            SecretString::from(hardened_dsn),
            collector_config,
        )
        .await
    });

    assert!(
        common::wait_for_server(port, 50).await,
        "exporter did not start on port {port}"
    );

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    for scrape in 1..=3 {
        let started = Instant::now();
        let response = client
            .get(format!("{}/metrics", common::get_test_url(port)))
            .send()
            .await?;
        let elapsed = started.elapsed();
        let status = response.status();
        let body = response.text().await?;

        assert_eq!(
            status,
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
            "locked-table scrape {scrape} should fail visibly, body: {body}"
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "locked-table scrape {scrape} took {elapsed:?}; lock_timeout did not fail fast"
        );
        assert!(
            body.contains("Error collecting metrics"),
            "locked-table scrape {scrape} should return an error exposition, got: {body}"
        );
    }

    let exporter_connections: i64 = sqlx::query_scalar(
        "SELECT count(*)::bigint
         FROM pg_stat_activity
         WHERE application_name = $1",
    )
    .bind(env!("CARGO_PKG_NAME"))
    .fetch_one(&mut setup)
    .await?;
    assert!(
        exporter_connections <= 3,
        "exporter connections should stay bounded by the shared pool cap (3), got {exporter_connections}"
    );

    lock_holder.execute("ROLLBACK").await?;
    let _ = lock_holder.close().await;
    let _ = setup.close().await;
    handle.abort();

    Ok(())
}
