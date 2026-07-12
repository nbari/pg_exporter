#![allow(clippy::expect_used)]
#![allow(clippy::indexing_slicing)]
#![allow(clippy::panic)]
#![allow(clippy::unwrap_used)]

//! End-to-end regression coverage for the five-connection exporter budget.

mod common;

use anyhow::{Context, Result, anyhow, bail};
use pg_exporter::collectors::{COLLECTOR_NAMES, config::CollectorConfig};
use secrecy::SecretString;
use sqlx::postgres::{PgConnectOptions, PgConnection};
use sqlx::{Connection, Executor};
use std::str::FromStr;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicI64, Ordering},
};
use std::time::{Duration, Instant};
use testcontainers_modules::testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::time::{sleep, timeout};

/// Default image tag when `PG_EXPORTER_TEST_POSTGRES_TAG` is unset. CI overrides the
/// tag from its version matrix so this test covers every supported `PostgreSQL` major.
const POSTGRES_TAG: &str = "16";
const EXPORTER_ROLE: &str = "postgres_exporter_budget";
const CONNECTION_BUDGET: i64 = 5;

fn postgres_tag() -> String {
    std::env::var("PG_EXPORTER_TEST_POSTGRES_TAG").unwrap_or_else(|_| POSTGRES_TAG.to_string())
}

async fn start_postgres() -> Result<ContainerAsync<GenericImage>> {
    GenericImage::new("postgres", &postgres_tag())
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_DB", "postgres")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_cmd(vec![
            "postgres",
            "-c",
            "shared_preload_libraries=pg_stat_statements",
        ])
        .start()
        .await
        .map_err(|error| anyhow!("failed to start postgres container: {error}"))
}

async fn container_endpoint(container: &ContainerAsync<GenericImage>) -> Result<(String, u16)> {
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(5432.tcp()).await?;
    Ok((host, port))
}

/// Resolve the `PostgreSQL` server this test runs against.
///
/// Prefers a dedicated testcontainers instance (deterministic, isolated). When no
/// container runtime socket is available — for example inside the compose devpod,
/// which already runs a trusted local `postgres` sidecar — fall back to the server
/// from `PG_EXPORTER_DSN` (validated by `common::get_test_dsn`, so the localhost /
/// trusted-compose guard still applies). Returns `Ok(None)` when the test should skip.
async fn resolve_admin_server(
    test_name: &str,
) -> Result<Option<(PgConnectOptions, Option<ContainerAsync<GenericImage>>)>> {
    if common::container_runtime_available() {
        match start_postgres().await {
            Ok(container) => {
                let (host, port) = container_endpoint(&container).await?;
                let admin_dsn = format!("postgresql://postgres:postgres@{host}:{port}/postgres");
                return Ok(Some((
                    PgConnectOptions::from_str(&admin_dsn)?,
                    Some(container),
                )));
            }
            Err(error) => {
                if common::should_require_container_runtime() {
                    return Err(error);
                }
                eprintln!("{test_name}: container start failed ({error}); trying PG_EXPORTER_DSN");
            }
        }
    }

    if std::env::var("PG_EXPORTER_DSN").is_ok() {
        let dsn = common::get_test_dsn();
        eprintln!(
            "{test_name}: no container runtime; using existing PostgreSQL from PG_EXPORTER_DSN"
        );
        return Ok(Some((PgConnectOptions::from_str(&dsn)?, None)));
    }

    if common::should_require_container_runtime() {
        bail!("no container runtime socket and no PG_EXPORTER_DSN available for {test_name}");
    }
    eprintln!("Skipping {test_name}: no container runtime socket and no PG_EXPORTER_DSN");
    Ok(None)
}

/// Remove every cluster-level object this test creates so it can run repeatedly
/// against a shared, long-lived server (the devpod `postgres` sidecar) and leave
/// no residue: role, budget databases, lock-target table, and the CONNECT revoke
/// on the default database are all restored.
async fn cleanup_budget_state(admin: &mut PgConnection) -> Result<()> {
    admin
        .execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
             WHERE usename = 'postgres_exporter_budget'",
        )
        .await?;
    admin
        .execute("DROP DATABASE IF EXISTS budget_db_1 WITH (FORCE)")
        .await?;
    admin
        .execute("DROP DATABASE IF EXISTS budget_db_2 WITH (FORCE)")
        .await?;
    admin
        .execute("DROP TABLE IF EXISTS exporter_budget_lock_target")
        .await?;
    admin
        .execute(
            "DO $$ BEGIN \
                 IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'postgres_exporter_budget') THEN \
                     EXECUTE 'DROP OWNED BY postgres_exporter_budget'; \
                 END IF; \
             END $$",
        )
        .await?;
    admin
        .execute("DROP ROLE IF EXISTS postgres_exporter_budget")
        .await?;
    admin
        .execute("GRANT CONNECT ON DATABASE postgres TO PUBLIC")
        .await?;
    Ok(())
}

async fn connect_with_retry(options: &PgConnectOptions) -> Result<PgConnection> {
    let mut last_error = None;
    for _ in 0..60 {
        match PgConnection::connect_with(options).await {
            Ok(connection) => return Ok(connection),
            Err(error) => {
                last_error = Some(error);
                sleep(Duration::from_millis(100)).await;
            }
        }
    }
    bail!("failed to connect to PostgreSQL; last_error={last_error:?}")
}

async fn exporter_connection_count(admin: &mut PgConnection) -> Result<i64> {
    sqlx::query_scalar("SELECT count(*)::bigint FROM pg_stat_activity WHERE usename = $1")
        .bind(EXPORTER_ROLE)
        .fetch_one(admin)
        .await
        .map_err(Into::into)
}

async fn exporter_lock_waiter_count(admin: &mut PgConnection) -> Result<i64> {
    sqlx::query_scalar(
        "SELECT count(*)::bigint
         FROM pg_stat_activity
         WHERE usename = $1 AND wait_event_type = 'Lock'",
    )
    .bind(EXPORTER_ROLE)
    .fetch_one(admin)
    .await
    .map_err(Into::into)
}

async fn wait_for_lock_waiter(admin: &mut PgConnection) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if exporter_lock_waiter_count(admin).await? > 0 {
            return Ok(());
        }
        sleep(Duration::from_millis(20)).await;
    }
    bail!("exporter never reached the held ACCESS EXCLUSIVE lock")
}

async fn wait_for_connection_baseline(admin: &mut PgConnection) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let connections = exporter_connection_count(admin).await?;
        let waiters = exporter_lock_waiter_count(admin).await?;
        if connections <= 3 && waiters == 0 {
            return Ok(());
        }
        sleep(Duration::from_millis(25)).await;
    }
    bail!("exporter connections did not return to the shared-pool baseline")
}

/// Poll `/metrics` until the exporter reports `200 OK`, within a bounded window.
///
/// After a scrape exceeds the scrape timeout the exporter intentionally keeps the
/// scrape-gate permit held until the detached scrape task unwinds (see
/// `collect_all_bytes`). During that window a fresh scrape observes
/// `ScrapeError::Busy` and returns `503`. A real Prometheus simply scrapes again on
/// the next interval, so the recovery assertion models that instead of demanding the
/// gate be free on the very first immediate scrape (which races the permit release
/// and made this test flaky). A genuine regression - a gate that never releases or a
/// collector that never recovers - still fails the test via the deadline, with the
/// last observed status and body attached for diagnosis.
async fn wait_for_metrics_recovery(client: &reqwest::Client, metrics_url: &str) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut last_status = None;
    let mut last_body = String::new();
    while Instant::now() < deadline {
        let response = client.get(metrics_url).send().await?;
        let status = response.status();
        if status == reqwest::StatusCode::OK {
            return Ok(());
        }
        last_status = Some(status);
        last_body = response.text().await.unwrap_or_default();
        sleep(Duration::from_millis(50)).await;
    }
    bail!(
        "exporter never recovered to 200 OK after the lock cleared \
         (last status: {last_status:?}, body: {})",
        last_body.trim()
    )
}

async fn prepare_exporter_role_and_databases(
    admin: &mut PgConnection,
    admin_options: &PgConnectOptions,
) -> Result<()> {
    admin
        .execute(
            "CREATE ROLE postgres_exporter_budget
             LOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS
             CONNECTION LIMIT 5",
        )
        .await?;
    admin
        .execute("GRANT pg_monitor TO postgres_exporter_budget")
        .await?;
    admin.execute("CREATE DATABASE budget_db_1").await?;
    admin.execute("CREATE DATABASE budget_db_2").await?;
    admin
        .execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        .await?;

    for (database, revoke_public, grant_exporter) in [
        (
            "postgres",
            "REVOKE CONNECT ON DATABASE postgres FROM PUBLIC",
            "GRANT CONNECT ON DATABASE postgres TO postgres_exporter_budget",
        ),
        (
            "budget_db_1",
            "REVOKE CONNECT ON DATABASE budget_db_1 FROM PUBLIC",
            "GRANT CONNECT ON DATABASE budget_db_1 TO postgres_exporter_budget",
        ),
        (
            "budget_db_2",
            "REVOKE CONNECT ON DATABASE budget_db_2 FROM PUBLIC",
            "GRANT CONNECT ON DATABASE budget_db_2 TO postgres_exporter_budget",
        ),
    ] {
        admin.execute(revoke_public).await?;
        admin.execute(grant_exporter).await?;
        let options = admin_options.clone().database(database);
        let mut setup = connect_with_retry(&options).await?;
        setup
            .execute("CREATE TABLE exporter_budget_lock_target (id bigint PRIMARY KEY)")
            .await?;
        setup.close().await?;
    }
    Ok(())
}

async fn assert_role_is_limited_monitor(admin: &mut PgConnection) -> Result<()> {
    let configured: bool = sqlx::query_scalar(
        "SELECT NOT rolsuper
                AND rolinherit
                AND NOT rolcreatedb
                AND NOT rolcreaterole
                AND NOT rolreplication
                AND NOT rolbypassrls
                AND rolconnlimit = 5
         FROM pg_roles
         WHERE rolname = $1",
    )
    .bind(EXPORTER_ROLE)
    .fetch_one(&mut *admin)
    .await?;
    assert!(
        configured,
        "exporter role attributes are broader than documented"
    );

    let has_pg_monitor: bool = sqlx::query_scalar("SELECT pg_has_role($1, 'pg_monitor', 'USAGE')")
        .bind(EXPORTER_ROLE)
        .fetch_one(&mut *admin)
        .await?;
    assert!(has_pg_monitor, "exporter role must inherit pg_monitor");

    let has_read_all_data: bool =
        sqlx::query_scalar("SELECT pg_has_role($1, 'pg_read_all_data', 'USAGE')")
            .bind(EXPORTER_ROLE)
            .fetch_one(&mut *admin)
            .await?;
    assert!(
        !has_read_all_data,
        "exporter must not read application table data"
    );

    let has_table_select: bool = sqlx::query_scalar(
        "SELECT has_table_privilege($1, 'public.exporter_budget_lock_target', 'SELECT')",
    )
    .bind(EXPORTER_ROLE)
    .fetch_one(&mut *admin)
    .await?;
    assert!(
        !has_table_select,
        "statistics collection must not require application-table SELECT"
    );

    let uses_reserved_connections: bool = sqlx::query_scalar(
        "SELECT COALESCE((
             SELECT pg_has_role($1, oid, 'USAGE')
             FROM pg_roles
             WHERE rolname = 'pg_use_reserved_connections'
         ), false)",
    )
    .bind(EXPORTER_ROLE)
    .fetch_one(&mut *admin)
    .await?;
    assert!(
        !uses_reserved_connections,
        "exporter must not consume reserved connection slots"
    );
    Ok(())
}

async fn assert_pg_stat_statements_visibility(role_options: &PgConnectOptions) -> Result<()> {
    let mut connection = connect_with_retry(role_options).await?;
    let sees_other_queries: bool = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1
             FROM pg_stat_statements
             WHERE userid <> (SELECT oid FROM pg_roles WHERE rolname = $1)
               AND queryid IS NOT NULL
               AND query IS NOT NULL
         )",
    )
    .bind(EXPORTER_ROLE)
    .fetch_one(&mut connection)
    .await?;
    assert!(
        sees_other_queries,
        "pg_monitor must expose other users' pg_stat_statements query text"
    );
    connection.close().await?;
    Ok(())
}

async fn assert_role_limit_is_immediate(role_options: &PgConnectOptions) -> Result<()> {
    let mut role_connections = Vec::new();
    for _ in 0..CONNECTION_BUDGET {
        role_connections.push(connect_with_retry(role_options).await?);
    }
    let sixth = timeout(
        Duration::from_secs(1),
        PgConnection::connect_with(role_options),
    )
    .await
    .context("sixth role connection waited instead of being rejected")?;
    let error = match sixth {
        Ok(connection) => {
            connection.close().await?;
            bail!("sixth role connection unexpectedly succeeded");
        }
        Err(error) => error,
    };
    let sqlstate = match error {
        sqlx::Error::Database(database_error) => {
            database_error.code().map(std::borrow::Cow::into_owned)
        }
        other => bail!("sixth role connection failed without a database SQLSTATE: {other}"),
    };
    assert_eq!(sqlstate.as_deref(), Some("53300"));
    for connection in role_connections {
        connection.close().await?;
    }
    Ok(())
}

async fn hold_access_exclusive_locks(
    admin_options: &PgConnectOptions,
) -> Result<Vec<PgConnection>> {
    let mut lock_holders = Vec::new();
    for database in ["postgres", "budget_db_1", "budget_db_2"] {
        let mut connection = connect_with_retry(&admin_options.clone().database(database)).await?;
        connection.execute("BEGIN").await?;
        connection
            .execute("LOCK TABLE exporter_budget_lock_target IN ACCESS EXCLUSIVE MODE")
            .await?;
        lock_holders.push(connection);
    }
    Ok(lock_holders)
}

#[tokio::test]
async fn limited_pg_monitor_role_runs_all_collectors_and_stays_within_five_connections()
-> Result<()> {
    let test_name = "limited_pg_monitor_role_runs_all_collectors_and_stays_within_five_connections";
    let Some((admin_options, _container)) = resolve_admin_server(test_name).await? else {
        return Ok(());
    };
    let mut admin = connect_with_retry(&admin_options).await?;

    // Idempotent: clear residue from any earlier aborted run on a shared server.
    cleanup_budget_state(&mut admin).await?;

    let scenario = run_budget_scenario(&mut admin, &admin_options).await;
    let cleanup = cleanup_budget_state(&mut admin).await;
    admin.close().await?;
    scenario?;
    cleanup
}

async fn run_budget_scenario(
    admin: &mut PgConnection,
    admin_options: &PgConnectOptions,
) -> Result<()> {
    prepare_exporter_role_and_databases(&mut *admin, admin_options).await?;
    assert_role_is_limited_monitor(&mut *admin).await?;

    let host = admin_options.get_host();
    let port = admin_options.get_port();
    let role_dsn = format!("postgresql://{EXPORTER_ROLE}@{host}:{port}/postgres?sslmode=disable");

    let role_options = PgConnectOptions::from_str(&role_dsn)?;
    assert_role_limit_is_immediate(&role_options).await?;
    assert_pg_stat_statements_visibility(&role_options).await?;
    let lock_holders = hold_access_exclusive_locks(admin_options).await?;

    let hardened_dsn =
        format!("{role_dsn}&options=-c%20lock_timeout%3D500ms%20-c%20statement_timeout%3D5000ms");
    let exporter_port = common::get_available_port();
    let enabled_collectors: Vec<String> = COLLECTOR_NAMES
        .iter()
        .map(|collector| (*collector).to_string())
        .collect();
    let collector_config = CollectorConfig::new(25).with_enabled(&enabled_collectors);
    let exporter = tokio::spawn(async move {
        pg_exporter::exporter::new(
            exporter_port,
            None,
            SecretString::from(hardened_dsn),
            collector_config,
        )
        .await
    });
    assert!(common::wait_for_server(exporter_port, 50).await);

    let stop_monitor = Arc::new(AtomicBool::new(false));
    let observed_peak = Arc::new(AtomicI64::new(0));
    let monitor_stop = Arc::clone(&stop_monitor);
    let monitor_peak = Arc::clone(&observed_peak);
    let monitor_options = admin_options.clone();
    let monitor = tokio::spawn(async move {
        let mut connection = connect_with_retry(&monitor_options).await?;
        while !monitor_stop.load(Ordering::SeqCst) {
            let count = exporter_connection_count(&mut connection).await?;
            monitor_peak.fetch_max(count, Ordering::SeqCst);
            sleep(Duration::from_millis(10)).await;
        }
        Ok::<(), anyhow::Error>(())
    });

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let metrics_url = format!("{}/metrics", common::get_test_url(exporter_port));
    let primary_client = client.clone();
    let primary_url = metrics_url.clone();
    let primary_scrape = tokio::spawn(async move { primary_client.get(primary_url).send().await });

    wait_for_lock_waiter(&mut *admin).await?;
    primary_scrape.abort();

    for _ in 0..4 {
        let response = client.get(&metrics_url).send().await?;
        assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    }

    wait_for_connection_baseline(&mut *admin).await?;
    for mut connection in lock_holders {
        connection.execute("ROLLBACK").await?;
        connection.close().await?;
    }

    wait_for_metrics_recovery(&client, &metrics_url).await?;

    stop_monitor.store(true, Ordering::SeqCst);
    monitor
        .await
        .context("connection monitor task panicked")??;
    let peak = observed_peak.load(Ordering::SeqCst);
    assert!(
        peak <= CONNECTION_BUDGET,
        "observed {peak} exporter connections"
    );
    assert!(
        peak >= 2,
        "test did not exercise concurrent exporter connections"
    );

    exporter.abort();
    Ok(())
}
