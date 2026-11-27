use super::super::common;
use anyhow::{Result, anyhow};
use pg_exporter::collectors::{Collector, vacuum::progress::VacuumProgressCollector};
use prometheus::Registry;
use sqlx::{PgPool, postgres::PgQueryResult};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

#[tokio::test]
async fn test_vacuum_progress_collector_registers_without_error() -> Result<()> {
    let collector = VacuumProgressCollector::new();
    let registry = Registry::new();

    // Should not error when registering
    collector.register_metrics(&registry)?;

    Ok(())
}

#[tokio::test]
async fn test_vacuum_progress_collector_has_all_metrics_after_collection() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = VacuumProgressCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // The global active flag should always exist. Per-table metric families are only
    // exported when a vacuum is active.
    assert!(
        metric_families
            .iter()
            .any(|m| m.name() == "pg_vacuum_active"),
        "pg_vacuum_active should exist. Found: {:?}",
        metric_families
            .iter()
            .map(prometheus::proto::MetricFamily::name)
            .collect::<Vec<_>>()
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_vacuum_progress_collector_handles_no_active_vacuums() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = VacuumProgressCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // When no vacuums are running, pg_vacuum_active should be 0
    let active_metric = metric_families
        .iter()
        .find(|m| m.name() == "pg_vacuum_active")
        .expect("pg_vacuum_active metric should exist");

    let value = common::metric_value_to_i64(active_metric.get_metric()[0].get_gauge().value());

    // Since we just started the test database, likely no vacuums are running
    // Value should be 0 or 1 (if autovacuum happened to start)
    assert!(
        value == 0 || value == 1,
        "pg_vacuum_active should be 0 or 1, got {value}"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_vacuum_progress_collector_metrics_have_database_and_table_labels() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = VacuumProgressCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Check that table-level metrics have both "database" and "table" labels
    let table_metrics = vec![
        "pg_vacuum_in_progress",
        "pg_vacuum_heap_progress",
        "pg_vacuum_heap_vacuumed",
        "pg_vacuum_index_vacuum_count",
    ];

    for metric_name in table_metrics {
        if let Some(metric_family) = metric_families.iter().find(|m| m.name() == metric_name) {
            // If there are metrics, they should have "database" and "table" labels
            for metric in metric_family.get_metric() {
                let has_database_label = metric.get_label().iter().any(|l| l.name() == "database");
                let has_table_label = metric.get_label().iter().any(|l| l.name() == "table");
                assert!(
                    has_database_label,
                    "Metric {metric_name} should have 'database' label"
                );
                assert!(
                    has_table_label,
                    "Metric {metric_name} should have 'table' label"
                );
            }
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_vacuum_progress_collector_name() {
    let collector = VacuumProgressCollector::new();
    assert_eq!(collector.name(), "vacuum_progress");
}

#[tokio::test]
async fn test_vacuum_progress_collector_progress_ratio_is_valid() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = VacuumProgressCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // pg_vacuum_heap_progress is a 0.0-1.0 ratio (percentunit), not a 0-100 percentage.
    if let Some(progress_metric) = metric_families
        .iter()
        .find(|m| m.name() == "pg_vacuum_heap_progress")
    {
        for metric in progress_metric.get_metric() {
            let value = metric.get_gauge().value();
            assert!(
                (0.0..=1.0).contains(&value),
                "Progress ratio should be within 0.0-1.0, got {value}"
            );
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_vacuum_progress_collector_counts_are_non_negative() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = VacuumProgressCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // All vacuum progress counts should be non-negative
    for family in &metric_families {
        if family.name().starts_with("pg_vacuum_") {
            for metric in family.get_metric() {
                let value = metric.get_gauge().value();
                assert!(
                    value >= 0.0,
                    "Metric {} should be non-negative, got: {}",
                    family.name(),
                    value
                );
            }
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_vacuum_progress_collector_handles_concurrent_collection() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = VacuumProgressCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;

    // Collect concurrently
    let (r1, r2, r3) = tokio::join!(
        collector.collect(&pool),
        collector.collect(&pool),
        collector.collect(&pool)
    );

    // All should succeed
    r1?;
    r2?;
    r3?;

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_vacuum_progress_collector_captures_actual_vacuum() -> Result<()> {
    let pool = common::create_test_pool().await?;

    // Create a test table with data (split into separate statements)
    sqlx::query("DROP TABLE IF EXISTS test_vacuum_progress_table")
        .execute(&pool)
        .await?;

    sqlx::query(
        "CREATE TABLE test_vacuum_progress_table (
            id SERIAL PRIMARY KEY,
            data TEXT
        )",
    )
    .execute(&pool)
    .await?;

    sqlx::query(
        "INSERT INTO test_vacuum_progress_table (data)
        SELECT 'test_data_' || generate_series(1, 1000)",
    )
    .execute(&pool)
    .await?;

    let collector = VacuumProgressCollector::new();
    let registry = Registry::new();
    collector.register_metrics(&registry)?;

    // Spawn a vacuum in a separate task (it will run for a moment)
    let pool_clone = pool.clone();
    let vacuum_task = tokio::spawn(async move {
        // VACUUM VERBOSE to make it take longer
        let _ = sqlx::query("VACUUM (VERBOSE) test_vacuum_progress_table")
            .execute(&pool_clone)
            .await;
    });

    // Give vacuum a moment to start
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Collect metrics while vacuum might be running
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Check that we have the database label with value "postgres"
    let in_progress_metric = metric_families
        .iter()
        .find(|m| m.name() == "pg_vacuum_in_progress");

    if let Some(metric_family) = in_progress_metric {
        for metric in metric_family.get_metric() {
            let database_label = metric
                .get_label()
                .iter()
                .find(|l| l.name() == "database")
                .map(prometheus::proto::LabelPair::value);

            // If we caught a vacuum in progress, verify database label exists
            if let Some(db) = database_label {
                if db == "none" || db == "unknown" {
                    continue;
                }

                let database_exists = sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)",
                )
                .bind(db)
                .fetch_one(&pool)
                .await?;

                // Other integration tests create and drop isolated `test_*` databases in
                // parallel. `pg_stat_progress_vacuum` can report a vacuum from one of those
                // databases, then the database may be dropped before this assertion checks
                // `pg_database`. That is test-suite interference, not an invalid label.
                if !database_exists && db.starts_with("test_") {
                    continue;
                }

                assert!(
                    database_exists,
                    "Database label should resolve to an existing database, got: {db}"
                );
            }
        }
    }

    // Wait for vacuum to complete
    let _ = vacuum_task.await;

    // Clean up
    sqlx::query("DROP TABLE IF EXISTS test_vacuum_progress_table")
        .execute(&pool)
        .await?;

    pool.close().await;
    Ok(())
}

/// Runs the `VACUUM` statement `vacuum_sql` slowed down enough to be observed, and
/// returns its backend PID so the caller can cancel it.
///
/// The throttle is a session setting, so it and the `VACUUM` must share one connection.
/// Run through the pool, each statement may land on a different backend: the `VACUUM`
/// is then unthrottled, and a test only passes when it catches a short vacuum in flight.
async fn start_throttled_vacuum(
    pool: &PgPool,
    vacuum_sql: &'static str,
) -> Result<(i32, JoinHandle<Result<PgQueryResult, sqlx::Error>>)> {
    let mut conn = pool.acquire().await?;
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *conn)
        .await?;
    sqlx::query("SET vacuum_cost_delay = '20ms'")
        .execute(&mut *conn)
        .await?;
    sqlx::query("SET vacuum_cost_limit = 1")
        .execute(&mut *conn)
        .await?;
    // A backstop: if a test dies before it cancels the VACUUM, the server ends it.
    sqlx::query("SET statement_timeout = '60s'")
        .execute(&mut *conn)
        .await?;
    let task = tokio::spawn(async move { sqlx::query(vacuum_sql).execute(&mut *conn).await });
    Ok((pid, task))
}

/// What the collector reported while a `VACUUM` of one table was running.
struct VacuumObservation {
    /// The `database:table` label pair was exported.
    resolved: bool,
    /// Every `database:table` pair seen, for the failure message.
    labels: Vec<String>,
    /// A table label in the watched database that is a bare relation OID.
    numeric_label: Option<String>,
}

/// Scrapes until the collector reports `table_label` in `dbname`, the `VACUUM` ends, or
/// 20 seconds pass. It returns instead of asserting, so the caller can always stop the
/// `VACUUM` and drop the database first.
async fn observe_vacuum<T>(
    pool: &PgPool,
    dbname: &str,
    table_label: &str,
    vacuum_task: &JoinHandle<T>,
) -> Result<VacuumObservation> {
    let collector = VacuumProgressCollector::new();
    let registry = Registry::new();
    collector.register_metrics(&registry)?;

    let mut observation = VacuumObservation {
        resolved: false,
        labels: Vec::new(),
        numeric_label: None,
    };
    let deadline = Instant::now() + Duration::from_secs(20);

    while Instant::now() < deadline {
        collector.collect(pool).await?;

        for family in registry.gather() {
            if family.name() != "pg_vacuum_in_progress" {
                continue;
            }
            for metric in family.get_metric() {
                let label = |name: &str| {
                    metric
                        .get_label()
                        .iter()
                        .find(|label| label.name() == name)
                        .map(|label| label.value().to_owned())
                };
                let (Some(database), Some(table)) = (label("database"), label("table")) else {
                    continue;
                };
                observation.labels.push(format!("{database}:{table}"));

                if database == dbname && table == table_label {
                    observation.resolved = true;
                } else if database == dbname && table.chars().all(|ch| ch.is_ascii_digit()) {
                    observation.numeric_label = Some(table);
                }
            }
        }

        if observation.resolved || observation.numeric_label.is_some() || vacuum_task.is_finished()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Ok(observation)
}

#[tokio::test]
async fn test_vacuum_progress_resolves_table_name_in_other_database() -> Result<()> {
    // The pool that observes and cancels comes first: once the database exists and the
    // VACUUM runs, nothing that can fail may stand between them and the cleanup.
    let pool = common::create_test_pool().await?;
    let test_db = common::IsolatedTestDatabase::new("vacuum_progress").await?;
    let dbname = test_db.database_name().to_string();
    let table_name = "test_vacuum_progress_cross_db";
    let table_label = format!("public.{table_name}");

    sqlx::query(
        "CREATE TABLE test_vacuum_progress_cross_db (
            id bigint PRIMARY KEY,
            data text
        )",
    )
    .execute(test_db.pool())
    .await?;
    sqlx::query(
        "INSERT INTO test_vacuum_progress_cross_db (id, data)
         SELECT g, repeat('x', 200)
         FROM generate_series(1, 250000) g",
    )
    .execute(test_db.pool())
    .await?;
    sqlx::query("DELETE FROM test_vacuum_progress_cross_db WHERE id % 3 = 0")
        .execute(test_db.pool())
        .await?;

    // Throttled, this VACUUM would run for minutes, so it is cancelled once the collector
    // has seen it. Everything from here to the cleanup runs whatever the observation
    // returned: an early return would leave the backend vacuuming and the database behind.
    let started = start_throttled_vacuum(
        test_db.pool(),
        "VACUUM (VERBOSE) public.test_vacuum_progress_cross_db",
    )
    .await;
    let (vacuum_pid, vacuum_task) = match started {
        Ok(started) => started,
        Err(error) => {
            pool.close().await;
            test_db.cleanup().await?;
            return Err(error);
        }
    };

    let observation = observe_vacuum(&pool, &dbname, &table_label, &vacuum_task).await;

    let cancel = sqlx::query("SELECT pg_cancel_backend($1)")
        .bind(vacuum_pid)
        .execute(&pool)
        .await;
    let vacuum_outcome = tokio::time::timeout(Duration::from_secs(30), vacuum_task).await;
    pool.close().await;
    test_db.cleanup().await?;

    cancel?;
    let vacuum_outcome = vacuum_outcome
        .map_err(|_| anyhow!("the VACUUM did not stop within 30 s of being cancelled"))?
        .map_err(|error| anyhow!("vacuum task failed to join: {error}"))?;
    // Cancelled (57014) is the usual outcome. A VACUUM that finished by itself after it
    // was seen is fine too; any other failure is not.
    let vacuum_ended_cleanly = match &vacuum_outcome {
        Ok(_) => true,
        Err(sqlx::Error::Database(error)) => error.code().as_deref() == Some("57014"),
        Err(_) => false,
    };

    let observation = observation?;
    assert!(
        observation.numeric_label.is_none(),
        "vacuum_progress should resolve the table label in {dbname}, got numeric relid label {:?}",
        observation.numeric_label
    );
    assert!(
        observation.resolved,
        "expected active vacuum metric for {dbname}:{table_label}; observed labels: {:?}",
        observation.labels
    );
    assert!(
        vacuum_ended_cleanly,
        "the VACUUM should have been cancelled or have finished, got: {vacuum_outcome:?}"
    );

    Ok(())
}

#[tokio::test]
async fn test_vacuum_progress_collector_database_label_format() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = VacuumProgressCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let metric_families = registry.gather();

    // Verify that all vacuum metrics have properly formatted labels
    let vacuum_metrics = vec![
        "pg_vacuum_in_progress",
        "pg_vacuum_heap_progress",
        "pg_vacuum_heap_vacuumed",
        "pg_vacuum_index_vacuum_count",
        "pg_vacuum_is_autovacuum",
        "pg_vacuum_duration_seconds",
    ];

    for metric_name in vacuum_metrics {
        if let Some(metric_family) = metric_families.iter().find(|m| m.name() == metric_name) {
            for metric in metric_family.get_metric() {
                let labels: Vec<_> = metric
                    .get_label()
                    .iter()
                    .map(prometheus::proto::LabelPair::name)
                    .collect();

                // Should have exactly database and table labels (in that order)
                assert_eq!(
                    labels.len(),
                    2,
                    "Metric {metric_name} should have exactly 2 labels, got: {labels:?}"
                );

                assert_eq!(
                    labels[0], "database",
                    "First label should be 'database' for metric {metric_name}"
                );

                assert_eq!(
                    labels[1], "table",
                    "Second label should be 'table' for metric {metric_name}"
                );
            }
        }
    }

    pool.close().await;
    Ok(())
}
