//! End-to-end regressions for issues #37 and #38. Separate process so the connection
//! configuration cannot race other integration tests' process-wide `OnceCells`.
use anyhow::{Result, anyhow, ensure};
use pg_exporter::collectors::{Collector, config::CollectorConfig, index::IndexCollector};
use prometheus::Registry;
use sqlx::{PgPool, Row};
use std::time::Duration;
use tokio::{sync::Mutex, task::JoinHandle};

mod common;

static SERIAL: Mutex<()> = Mutex::const_new(());

fn configure_concurrency() -> Result<()> {
    let limit = std::env::var("PG_EXPORTER_TEST_DB_CONCURRENCY")
        .unwrap_or_else(|_| "2".into())
        .parse::<usize>()?;
    ensure!(matches!(limit, 1 | 2), "test concurrency must be 1 or 2");
    pg_exporter::collectors::util::set_max_db_concurrency(limit);
    Ok(())
}

async fn index_query_calls(admin: &PgPool, datname: &str) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT COALESCE(sum(calls), 0)::bigint FROM pg_stat_statements
        WHERE dbid = (SELECT oid FROM pg_database WHERE datname = $1)
          AND query LIKE '%pg_stat_user_indexes%'",
    )
    .bind(datname)
    .fetch_one(admin)
    .await?)
}

struct Server(JoinHandle<Result<()>>);

const REFERENCE: &str = include_str!("collectors/index/reference.sql");
const METRICS: [(&str, &str); 10] = [
    ("pg_index_scans_total", "total_scans"),
    ("pg_index_tuples_read_total", "total_tup_read"),
    ("pg_index_tuples_fetched_total", "total_tup_fetch"),
    ("pg_index_size_bytes", "total_size_bytes"),
    ("pg_index_valid", "valid_count"),
    ("pg_index_idx_blks_read_total", "total_idx_blks_read"),
    ("pg_index_idx_blks_hit_total", "total_idx_blks_hit"),
    ("pg_index_unused_count", "unused_count"),
    ("pg_index_unused_size_bytes", "unused_size_bytes"),
    ("pg_index_invalid_count", "invalid_count"),
];

fn metric(registry: &Registry, name: &str, datname: &str) -> Option<i64> {
    registry
        .gather()
        .iter()
        .find(|f| f.name() == name)?
        .get_metric()
        .iter()
        .find(|m| {
            m.get_label()
                .iter()
                .any(|l| l.name() == "datname" && l.value() == datname)
        })
        .map(|m| common::metric_value_to_i64(m.get_gauge().value()))
}

async fn seed_indexes(pool: &PgPool) -> Result<()> {
    for statement in [
        "CREATE SCHEMA example",
        "CREATE TABLE example.items (id bigint PRIMARY KEY, unused bigint, scanned bigint, unique_val bigint UNIQUE)",
        "INSERT INTO example.items SELECT g, g % 10, g, g FROM generate_series(1, 2000) g",
        "CREATE INDEX unused_idx ON example.items (unused)",
        "CREATE INDEX scanned_idx ON example.items (scanned)",
        "CREATE INDEX expression_idx ON example.items ((unused + 1)) WHERE unused > 0",
        "CREATE MATERIALIZED VIEW example.materialized AS SELECT id FROM example.items",
        "CREATE INDEX materialized_idx ON example.materialized (id)",
        "CREATE TABLE example.partitioned (id integer) PARTITION BY RANGE (id)",
        "CREATE TABLE example.partition_child PARTITION OF example.partitioned FOR VALUES FROM (0) TO (10)",
        "CREATE INDEX partition_parent_idx ON ONLY example.partitioned (id)",
    ] {
        sqlx::query(statement).execute(pool).await?;
    }
    let invalid =
        sqlx::query("CREATE UNIQUE INDEX CONCURRENTLY invalid_idx ON example.items (unused)")
            .execute(pool)
            .await;
    ensure!(
        invalid
            .as_ref()
            .err()
            .and_then(sqlx::Error::as_database_error)
            .and_then(sqlx::error::DatabaseError::code)
            .as_deref()
            == Some("23505"),
        "fixture must leave a physical invalid index after a uniqueness violation"
    );
    // Pin the session: SET and the workload must use the same connection.
    let mut workload = pool.acquire().await?;
    sqlx::query("SET enable_seqscan = off")
        .execute(&mut *workload)
        .await?;
    sqlx::query("SELECT scanned FROM example.items WHERE scanned = 42")
        .fetch_all(&mut *workload)
        .await?;
    sqlx::query("RESET enable_seqscan")
        .execute(&mut *workload)
        .await?;
    Ok(())
}

#[tokio::test]
async fn merged_index_metrics_match_original_sql_and_retire_stale_databases() -> Result<()> {
    let _guard = SERIAL.lock().await;
    configure_concurrency()?;
    let admin = common::create_test_pool().await?;
    let rich = common::IsolatedTestDatabase::new("index_equivalence").await?;
    let empty = common::IsolatedTestDatabase::new("index_empty").await?;
    seed_indexes(rich.pool()).await?;
    // Closing workload sessions publishes their statistics, on PostgreSQL 14 too.
    rich.pool().close().await;
    let mut options = admin.connect_options().as_ref().clone();
    options = options.database(rich.database_name());
    let reference_pool = PgPool::connect_with(options).await?;
    let mut reference = sqlx::query(REFERENCE).fetch_one(&reference_pool).await?;
    for _ in 0..50 {
        if reference.try_get::<i64, _>("total_scans")? > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        reference = sqlx::query(REFERENCE).fetch_one(&reference_pool).await?;
    }
    ensure!(
        reference.try_get::<i64, _>("total_scans")? > 0,
        "fixture must exercise index scans"
    );
    ensure!(
        reference.try_get::<i64, _>("invalid_count")? == 2,
        "physical invalid index and partition parent must both count"
    );
    ensure!(
        reference.try_get::<i64, _>("unused_count")? == 3,
        "only the three unused non-unique indexes count"
    );
    let collector = IndexCollector::default();
    let registry = Registry::new();
    collector.register_metrics(&registry)?;
    collector.collect(&admin).await?;
    for (name, column) in METRICS {
        assert_eq!(
            metric(&registry, name, rich.database_name()),
            Some(reference.try_get(column)?),
            "{name} must match the original SQL"
        );
        assert_eq!(
            metric(&registry, name, empty.database_name()),
            Some(0),
            "empty database {name}"
        );
    }
    assert!(metric(&registry, "pg_index_size_bytes", "postgres").is_some());
    assert!(metric(&registry, "pg_index_size_bytes", "template1").is_none());
    let retired = rich.database_name().to_owned();
    reference_pool.close().await;
    rich.cleanup().await?;
    collector.collect(&admin).await?;
    for (name, _) in METRICS {
        assert!(metric(&registry, name, &retired).is_none(), "stale {name}");
    }
    empty.cleanup().await?;
    admin.close().await;
    // Discovery failure preserves the previous snapshot and returns an error.
    let before = registry.gather();
    assert!(collector.collect(&admin).await.is_err());
    assert_eq!(registry.gather(), before);
    Ok(())
}

#[tokio::test]
async fn index_preserves_readable_groups_and_recovers_after_permission_errors() -> Result<()> {
    let _guard = SERIAL.lock().await;
    configure_concurrency()?;
    let admin = common::create_test_pool().await?;
    let db = common::IsolatedTestDatabase::new("index_permissions").await?;
    let mut setup = db.pool().acquire().await?;
    sqlx::query("CREATE TABLE items (id integer PRIMARY KEY)")
        .execute(&mut *setup)
        .await?;
    // Only newly opened fixture connections use pg_monitor. The pinned session stays
    // postgres so it can restore privileges; no global role is created or modified.
    // Identifiers cannot be bound; quote and escape the generated fixture name.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER DATABASE \"{}\" SET role TO 'pg_monitor'",
        db.database_name().replace('"', "\"\"")
    )))
    .execute(&admin)
    .await?;
    let collector = IndexCollector::new();
    let registry = Registry::new();
    collector.register_metrics(&registry)?;
    collector.collect(&admin).await?;
    assert_eq!(
        metric(&registry, "pg_index_valid", db.database_name()),
        Some(1)
    );

    sqlx::query("REVOKE SELECT ON pg_statio_user_indexes FROM PUBLIC")
        .execute(&mut *setup)
        .await?;
    collector.collect(&admin).await?;
    assert!(metric(&registry, "pg_index_valid", db.database_name()).is_none());
    assert_eq!(
        metric(&registry, "pg_index_unused_count", db.database_name()),
        Some(0)
    );
    sqlx::query("GRANT SELECT ON pg_statio_user_indexes TO PUBLIC")
        .execute(&mut *setup)
        .await?;

    // An explicitly ordered search_path can expose a restricted catalog wrapper.
    // Stored statistics views keep referencing the real pg_class by OID; the
    // standalone invalid-index subquery resolves the unreadable wrapper instead.
    sqlx::query("CREATE VIEW public.pg_class AS SELECT * FROM pg_catalog.pg_class")
        .execute(&mut *setup)
        .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER DATABASE \"{}\" SET search_path TO public, pg_catalog",
        db.database_name().replace('"', "\"\"")
    )))
    .execute(&admin)
    .await?;
    collector.collect(&admin).await?;
    assert_eq!(
        metric(&registry, "pg_index_valid", db.database_name()),
        Some(1)
    );
    assert!(metric(&registry, "pg_index_unused_count", db.database_name()).is_none());
    sqlx::query("GRANT SELECT ON public.pg_class TO PUBLIC")
        .execute(&mut *setup)
        .await?;

    collector.collect(&admin).await?;
    assert_eq!(
        metric(&registry, "pg_index_valid", db.database_name()),
        Some(1)
    );
    assert_eq!(
        metric(&registry, "pg_index_invalid_count", db.database_name()),
        Some(0)
    );
    drop(setup);
    db.cleanup().await?;
    admin.close().await;
    Ok(())
}

impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[tokio::test]
async fn index_opens_one_session_per_nondefault_database_per_scrape() -> Result<()> {
    let _guard = SERIAL.lock().await;
    configure_concurrency()?;
    let admin = common::create_test_pool().await?;
    let db = common::IsolatedTestDatabase::new("index_sessions").await?;
    db.pool().close().await;
    let collector = IndexCollector::new();
    let registry = Registry::new();
    collector.register_metrics(&registry)?;

    // Warm the shared pool and wait for setup/warm-up sessions to finish reporting.
    collector.collect(&admin).await?;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let before: i64 =
        sqlx::query_scalar("SELECT sessions FROM pg_stat_database WHERE datname = $1")
            .bind(db.database_name())
            .fetch_one(&admin)
            .await?;
    let queries_before = index_query_calls(&admin, db.database_name()).await?;
    collector.collect(&admin).await?;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let after: i64 = sqlx::query_scalar("SELECT sessions FROM pg_stat_database WHERE datname = $1")
        .bind(db.database_name())
        .fetch_one(&admin)
        .await?;
    let backends: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE datname = $1")
            .bind(db.database_name())
            .fetch_one(&admin)
            .await?;
    let queries_after = index_query_calls(&admin, db.database_name()).await?;

    db.cleanup().await?;
    admin.close().await;
    assert_eq!(
        after - before,
        1,
        "one healthy index scrape must open one ephemeral session per non-default database"
    );
    assert_eq!(backends, 0, "index connections must not survive a scrape");
    assert_eq!(
        queries_after - queries_before,
        1,
        "one healthy index query per database"
    );
    Ok(())
}

#[tokio::test]
async fn permit_timings_are_exported_for_top_level_collectors() -> Result<()> {
    let _guard = SERIAL.lock().await;
    configure_concurrency()?;
    let db = common::IsolatedTestDatabase::new("permit_metrics").await?;
    let port = common::get_available_port();
    let dsn = common::get_test_dsn_secret();
    let names = ["index", "stat", "sequences", "exporter"].map(str::to_string);
    let config = CollectorConfig::new(25).with_enabled(&names);
    let server = Server(tokio::spawn(async move {
        pg_exporter::exporter::new(port, Some("127.0.0.1".into()), dsn, config).await
    }));
    ensure!(
        common::wait_for_server(port, 50).await,
        "exporter did not start"
    );
    let body = reqwest::get(format!("http://127.0.0.1:{port}/metrics"))
        .await?
        .error_for_status()?
        .text()
        .await?;
    drop(server);
    db.cleanup().await?;

    for collector in ["index", "stat", "sequences"] {
        for phase in ["wait", "hold"] {
            let prefix = format!(
                "pg_exporter_collector_permit_{phase}_seconds_count{{collector=\"{collector}\"}} "
            );
            let count = body
                .lines()
                .find_map(|line| line.strip_prefix(&prefix))
                .ok_or_else(|| anyhow!("missing {prefix}"))?
                .parse::<u64>()?;
            ensure!(
                count > 0,
                "{collector} must record its spawned database tasks"
            );
        }
    }
    ensure!(
        !body.contains("collector=\"index_stats\""),
        "labels must identify the top-level collector"
    );
    ensure!(
        !body.contains("collector=\"stat_user_tables\""),
        "labels must identify the top-level collector"
    );
    Ok(())
}
