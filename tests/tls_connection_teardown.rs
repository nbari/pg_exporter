//! Issue #41: query completion and exporter shutdown must close TLS without server log noise.
//! A real subprocess isolates the exporter's process-wide connection configuration.

mod common;

use anyhow::{Context, Result, bail, ensure};
use pg_exporter::collectors::COLLECTOR_NAMES;
use sqlx::{Connection, Executor, PgConnection, postgres::PgConnectOptions};
use std::{collections::BTreeSet, process::Stdio, str::FromStr, time::Duration};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::{
    process::Command,
    time::{sleep, timeout},
};

const EXPORTER_ROLE: &str = "pgexporter_tls";
const OTHER_DATABASE: &str = "tls_secondary";
const COLLECTORS: [&str; 3] = ["index", "stat", "sequences"];
const VIEWS: [&str; 3] = [
    "pg_stat_user_indexes",
    "pg_stat_user_tables",
    "pg_sequences",
];

async fn start_postgres() -> Result<ContainerAsync<GenericImage>> {
    let tag = std::env::var("PG_EXPORTER_TEST_POSTGRES_TAG").unwrap_or_else(|_| "16".into());
    // Generate a fresh certificate inside the disposable container. TLS 1.3 reproduces
    // the reset on raw SQLx connection drop; a plaintext connection can hide the bug.
    let startup = r"
        openssl req -new -x509 -nodes -newkey rsa:2048 -days 1 \
            -subj /CN=localhost -keyout /tmp/server.key -out /tmp/server.crt
        chown postgres:postgres /tmp/server.key /tmp/server.crt
        chmod 600 /tmp/server.key
        exec docker-entrypoint.sh postgres \
            -c ssl=on \
            -c ssl_cert_file=/tmp/server.crt \
            -c ssl_key_file=/tmp/server.key \
            -c ssl_min_protocol_version=TLSv1.3 \
            -c ssl_max_protocol_version=TLSv1.3 \
            -c log_connections=on \
            -c log_disconnections=on \
            -c 'log_line_prefix=pg-tls:%p|%u|%d| ' \
            -c lc_messages=C
    ";
    Ok(GenericImage::new("postgres", &tag)
        .with_entrypoint("sh")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_DB", "postgres")
        .with_cmd(["-ec", startup])
        .start()
        .await?)
}

async fn connect(options: &PgConnectOptions) -> Result<PgConnection> {
    timeout(Duration::from_secs(60), async {
        loop {
            match PgConnection::connect_with(options).await {
                Ok(connection) => return connection,
                Err(_) => sleep(Duration::from_millis(100)).await,
            }
        }
    })
    .await
    .context("TLS PostgreSQL did not become ready")
}

async fn seed_database(options: &PgConnectOptions) -> Result<()> {
    let mut connection = connect(options).await?;
    let result = async {
        for statement in [
            "CREATE TABLE public.tls_items (id bigint PRIMARY KEY, bucket integer NOT NULL)",
            "INSERT INTO public.tls_items SELECT n, n % 10 FROM generate_series(1, 256) n",
            "CREATE INDEX tls_items_bucket_idx ON public.tls_items (bucket)",
            "CREATE SEQUENCE public.tls_sequence MAXVALUE 100",
            "SELECT setval('public.tls_sequence', 75)",
            "GRANT USAGE, SELECT ON SEQUENCE public.tls_sequence TO pgexporter_tls",
            "ANALYZE public.tls_items",
        ] {
            connection.execute(statement).await?;
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let closed = connection.close().await;
    result?;
    closed?;
    Ok(())
}

fn metric_value(body: &str, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
    body.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        let series = fields.next()?;
        let (metric, series_labels) = series
            .split_once('{')
            .map_or((series, ""), |(metric, labels)| {
                (metric, labels.trim_end_matches('}'))
            });
        if metric != name
            || !labels.iter().all(|(key, value)| {
                let expected = format!("{key}=\"{value}\"");
                series_labels.split(',').any(|label| label == expected)
            })
        {
            return None;
        }
        fields.next()?.parse().ok()
    })
}

fn assert_database_metrics(body: &str, database: &str) -> Result<()> {
    for (name, labels, minimum, maximum) in [
        ("pg_index_valid", vec![("datname", database)], 2.0, 2.0),
        (
            "pg_stat_user_tables_table_size_bytes",
            vec![
                ("datname", database),
                ("schemaname", "public"),
                ("relname", "tls_items"),
            ],
            1.0,
            f64::MAX,
        ),
        (
            "pg_sequence_used_ratio",
            vec![
                ("datname", database),
                ("schemaname", "public"),
                ("sequencename", "tls_sequence"),
            ],
            0.75,
            0.75,
        ),
    ] {
        let value = metric_value(body, name, &labels)
            .with_context(|| format!("missing {name} for {database}:\n{body}"))?;
        ensure!(
            (minimum..=maximum).contains(&value),
            "unexpected {name}={value} for {database}:\n{body}"
        );
    }
    Ok(())
}

async fn scrape(client: &reqwest::Client, base_url: &str) -> Result<String> {
    let response = client.get(format!("{base_url}/metrics")).send().await?;
    let status = response.status();
    let body = response.text().await?;
    ensure!(status.is_success(), "scrape returned {status}:\n{body}");
    ensure!(
        metric_value(&body, "pg_up", &[]).is_some_and(|value| value > 0.0),
        "scrape did not reach PostgreSQL:\n{body}"
    );
    for collector in COLLECTORS {
        ensure!(
            metric_value(
                &body,
                "pg_exporter_collector_last_scrape_success",
                &[("collector", collector)]
            )
            .is_some_and(|value| value > 0.0),
            "{collector} failed:\n{body}"
        );
    }
    Ok(body)
}

fn database_log_entries<'a>(
    logs: &'a str,
    database: &'a str,
) -> impl Iterator<Item = (&'a str, &'a str)> {
    logs.lines().filter_map(move |line| {
        let mut fields = line.strip_prefix("pg-tls:")?.splitn(4, '|');
        let pid = fields.next()?;
        let user = fields.next()?;
        let datname = fields.next()?;
        let message = fields.next()?;
        (user == EXPORTER_ROLE && datname == database).then_some((pid, message))
    })
}

async fn assert_closed_cleanly(
    container: &ContainerAsync<GenericImage>,
    admin: &mut PgConnection,
    database: &str,
    expected_connections: usize,
) -> Result<()> {
    // A scrape response alone races backend exit and container log delivery. Wait
    // for PostgreSQL's own disconnection records and pg_stat_activity to agree.
    let logs = timeout(Duration::from_secs(20), async {
        loop {
            let logs = String::from_utf8(container.stderr_to_vec().await?)?;
            let disconnected = database_log_entries(&logs, database)
                .filter(|(_, message)| message.contains("disconnection:"))
                .count();
            let active: i64 = sqlx::query_scalar(
                "SELECT count(*)::bigint FROM pg_stat_activity WHERE usename = $1 AND datname = $2",
            )
            .bind(EXPORTER_ROLE)
            .bind(database)
            .fetch_one(&mut *admin)
            .await?;
            if disconnected >= expected_connections && active == 0 {
                break Ok::<_, anyhow::Error>(logs);
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .context("exporter backends did not close and publish disconnection logs")??;

    let authorized: BTreeSet<_> = database_log_entries(&logs, database)
        .filter(|(_, message)| {
            message.contains("connection authorized:")
                && message.contains("SSL enabled")
                && message.contains("TLSv1.3")
        })
        .map(|(pid, _)| pid)
        .collect();
    let disconnected: BTreeSet<_> = database_log_entries(&logs, database)
        .filter(|(_, message)| message.contains("disconnection:"))
        .map(|(pid, _)| pid)
        .collect();
    ensure!(
        authorized.len() == expected_connections && authorized == disconnected,
        "expected {expected_connections} real TLS sessions, all closed:\n{logs}"
    );
    for (_, message) in database_log_entries(&logs, database) {
        let lower = message.to_ascii_lowercase();
        ensure!(
            !lower.contains("connection reset by peer") && !lower.contains("unexpected eof"),
            "exporter connections to {database} produced TLS teardown noise:\n{logs}"
        );
    }
    Ok(())
}

async fn run_scrapes(
    container: &ContainerAsync<GenericImage>,
    admin: &mut PgConnection,
    secondary: &mut PgConnection,
    base_url: &str,
) -> Result<()> {
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(30))
        .build()?;
    timeout(Duration::from_secs(30), async {
        loop {
            if let Ok(response) = client.get(format!("{base_url}/health")).send().await
                && response.status().is_success()
            {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .context("exporter did not become healthy")?;

    let healthy = scrape(&client, base_url).await?;
    assert_database_metrics(&healthy, "postgres")?;
    assert_database_metrics(&healthy, OTHER_DATABASE)?;
    assert_closed_cleanly(container, admin, OTHER_DATABASE, COLLECTORS.len()).await?;

    // Permission failures are real server-side errors, not cancelled futures. The
    // default database remains readable, so these collectors publish partial results.
    for statement in [
        "REVOKE SELECT ON pg_catalog.pg_stat_user_indexes FROM PUBLIC",
        "REVOKE SELECT ON pg_catalog.pg_stat_user_tables FROM PUBLIC",
        "REVOKE SELECT ON pg_catalog.pg_sequences FROM PUBLIC",
    ] {
        secondary.execute(statement).await?;
    }
    let partial = scrape(&client, base_url).await?;
    assert_database_metrics(&partial, "postgres")?;
    for name in [
        "pg_index_valid",
        "pg_stat_user_tables_table_size_bytes",
        "pg_sequence_used_ratio",
    ] {
        ensure!(
            metric_value(&partial, name, &[("datname", OTHER_DATABASE)]).is_none(),
            "failed query left misleading {name} in partial snapshot:\n{partial}"
        );
    }
    assert_closed_cleanly(container, admin, OTHER_DATABASE, 2 * COLLECTORS.len()).await?;
    Ok(())
}

#[cfg(unix)]
async fn assert_graceful_shutdown(
    child: &mut tokio::process::Child,
    container: &ContainerAsync<GenericImage>,
    admin: &mut PgConnection,
) -> Result<()> {
    let live_connections: i64 = sqlx::query_scalar(
        "SELECT count(*)::bigint FROM pg_stat_activity WHERE usename = $1 AND datname = 'postgres'",
    )
    .bind(EXPORTER_ROLE)
    .fetch_one(&mut *admin)
    .await?;
    ensure!(
        live_connections > 0,
        "shutdown fixture has no live pooled connections"
    );
    let logs = String::from_utf8(container.stderr_to_vec().await?)?;
    let connections = database_log_entries(&logs, "postgres")
        .filter(|(_, message)| message.contains("connection authorized:"))
        .count();

    let pid = child.id().context("exporter exited before SIGTERM")?;
    let signal = Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .output()
        .await?;
    ensure!(
        signal.status.success(),
        "could not send SIGTERM: {signal:?}"
    );
    let status = timeout(Duration::from_secs(10), child.wait())
        .await
        .context("exporter did not exit within its shutdown budget")??;
    ensure!(status.success(), "exporter failed on SIGTERM: {status}");

    assert_closed_cleanly(container, admin, "postgres", connections).await
}

async fn run_exporter(
    container: &ContainerAsync<GenericImage>,
    admin: &mut PgConnection,
    secondary: &mut PgConnection,
    dsn: &str,
) -> Result<()> {
    let port = common::get_available_port();
    let output = tempfile::NamedTempFile::new()?;
    let mut command = Command::new(env!("CARGO_BIN_EXE_pg_exporter"));
    // Only the child environment changes; no process-global test configuration races.
    for (key, _) in std::env::vars_os() {
        if key
            .to_str()
            .is_some_and(|key| key.starts_with("PG_EXPORTER_") || key.starts_with("OTEL_"))
        {
            command.env_remove(key);
        }
    }
    command.args(["--listen", "127.0.0.1", "--port", &port.to_string()]);
    for name in COLLECTOR_NAMES {
        command.arg(format!("--no-collector.{name}"));
    }
    for name in COLLECTORS.into_iter().chain(["exporter"]) {
        command.arg(format!("--collector.{name}"));
    }
    let mut child = command
        .env("PG_EXPORTER_DSN", dsn)
        .env("RUST_LOG", "warn")
        .stdout(Stdio::from(output.as_file().try_clone()?))
        .stderr(Stdio::from(output.as_file().try_clone()?))
        .kill_on_drop(true)
        .spawn()?;
    let scenario = timeout(
        Duration::from_secs(150),
        run_scrapes(
            container,
            admin,
            secondary,
            &format!("http://127.0.0.1:{port}"),
        ),
    )
    .await
    .context("TLS teardown scenario timed out")
    .and_then(std::convert::identity);

    #[cfg(unix)]
    let shutdown = assert_graceful_shutdown(&mut child, container, admin).await;
    // Reap even if SIGTERM or an earlier scenario failed; other platforms have no SIGTERM.
    let stopped = if child.id().is_some() {
        child.kill().await
    } else {
        Ok(())
    };
    let reaped = child.wait().await;
    let exporter_logs = std::fs::read_to_string(output.path())?;
    let postgres_logs = String::from_utf8(container.stderr_to_vec().await?)?;
    scenario.with_context(|| {
        format!("exporter logs:\n{exporter_logs}\nPostgreSQL logs:\n{postgres_logs}")
    })?;
    #[cfg(unix)]
    shutdown.with_context(|| {
        format!("exporter logs:\n{exporter_logs}\nPostgreSQL logs:\n{postgres_logs}")
    })?;
    stopped?;
    reaped?;
    for view in VIEWS {
        ensure!(
            exporter_logs.contains(&format!("permission denied for view {view}")),
            "original query error was lost during teardown ({view}):\n{exporter_logs}"
        );
    }
    Ok(())
}

async fn scenario(container: &ContainerAsync<GenericImage>) -> Result<()> {
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432.tcp()).await?;
    let dsn = format!("postgresql://postgres:postgres@{host}:{port}/postgres?sslmode=require");
    let options = PgConnectOptions::from_str(&dsn)?;
    let mut admin = connect(&options).await?;
    let result = async {
        admin
            .execute("CREATE ROLE pgexporter_tls LOGIN PASSWORD 'exporter'")
            .await?;
        admin.execute("GRANT pg_monitor TO pgexporter_tls").await?;
        admin.execute("CREATE DATABASE tls_secondary").await?;
        seed_database(&options).await?;
        let secondary_options = options.clone().database(OTHER_DATABASE);
        seed_database(&secondary_options).await?;
        let mut secondary = connect(&secondary_options).await?;
        let exporter_dsn =
            format!("postgresql://pgexporter_tls:exporter@{host}:{port}/postgres?sslmode=require");
        let result = run_exporter(container, &mut admin, &mut secondary, &exporter_dsn).await;
        let closed = secondary.close().await;
        result?;
        closed?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let closed = admin.close().await;
    result?;
    closed?;
    Ok(())
}

#[tokio::test]
async fn tls_connections_close_cleanly_after_queries_and_shutdown() -> Result<()> {
    if !common::ensure_container_runtime_for_test(
        "tls_connections_close_cleanly_after_queries_and_shutdown",
    )? {
        return Ok(());
    }
    // Once a runtime is available, fixture/startup failures must not silently skip.
    let container = start_postgres().await?;
    let result = scenario(&container).await;
    if let Err(error) = result {
        let logs = String::from_utf8_lossy(&container.stderr_to_vec().await?).into_owned();
        bail!("{error:#}\nPostgreSQL logs:\n{logs}");
    }
    Ok(())
}
