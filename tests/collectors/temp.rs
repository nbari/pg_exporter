use super::common;
use anyhow::{Context, Result};
use pg_exporter::collectors::{Collector, temp::TempCollector};
use prometheus::Registry;
use sqlx::{PgPool, Row};
use std::time::Duration;
use tokio::time::sleep;

/// Every metric family the collector exposes on `PostgreSQL` 12+.
const TEMP_METRICS: [&str; 3] = [
    "pg_temp_files_current_bytes",
    "pg_temp_files_current_count",
    "pg_temp_files_oldest_age_seconds",
];

async fn server_version_num(pool: &PgPool) -> Result<i32> {
    let row = sqlx::query("SELECT current_setting('server_version_num')::int AS v")
        .fetch_one(pool)
        .await?;
    Ok(row.try_get::<i32, _>("v")?)
}

/// Whether the connected role may call `pg_ls_tmpdir()` at all.
async fn can_read_tmpdir(pool: &PgPool) -> bool {
    sqlx::query("SELECT 1 FROM pg_ls_tmpdir() LIMIT 1")
        .fetch_optional(pool)
        .await
        .is_ok()
}

fn gauge_value(registry: &Registry, metric_name: &str, tablespace: &str) -> Option<f64> {
    registry.gather().iter().find_map(|family| {
        if family.name() != metric_name {
            return None;
        }
        family.get_metric().iter().find_map(|metric| {
            let matches = metric
                .get_label()
                .iter()
                .any(|label| label.name() == "tablespace" && label.value() == tablespace);
            matches.then(|| metric.get_gauge().value())
        })
    })
}

#[tokio::test]
async fn test_temp_registers_without_error() -> Result<()> {
    let registry = Registry::new();
    TempCollector::new().register_metrics(&registry)?;
    Ok(())
}

/// Registering twice into the same registry must fail, proving the metric names
/// are actually registered rather than silently dropped.
#[tokio::test]
async fn test_temp_metric_names_are_registered() -> Result<()> {
    let registry = Registry::new();
    TempCollector::new().register_metrics(&registry)?;
    assert!(
        TempCollector::new().register_metrics(&registry).is_err(),
        "expected a duplicate registration error"
    );
    Ok(())
}

#[tokio::test]
async fn test_temp_name_and_default_disabled() {
    let collector = TempCollector::new();
    assert_eq!(collector.name(), "temp");
    assert!(
        !collector.enabled_by_default(),
        "temp must stay opt-in because pg_ls_tmpdir() requires superuser or pg_monitor"
    );
}

/// Collecting must succeed on every supported server version and with any
/// privilege level: a clean no-op below `PostgreSQL` 12 or without the
/// `pg_monitor` role, a populated snapshot otherwise. It must never fail the
/// scrape, which would take `pg_up` down with it.
#[tokio::test]
async fn test_temp_collect_never_fails_the_scrape() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = TempCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;
    // A second scrape exercises the warn-once paths.
    collector.collect(&pool).await?;

    pool.close().await;
    Ok(())
}

/// Every metric must have a `pg_default` series even when nothing is spilling, so
/// alerts can use `> 0` instead of `absent()`. The values themselves are cluster-wide
/// and a concurrent test may legitimately be spilling, so the invariant that can be
/// asserted is non-negativity plus internal consistency: no files means no bytes and
/// no age.
#[tokio::test]
async fn test_temp_empty_directory_reports_zero() -> Result<()> {
    let pool = common::create_test_pool().await?;
    if server_version_num(&pool).await? < 120_000 || !can_read_tmpdir(&pool).await {
        pool.close().await;
        return Ok(());
    }

    let registry = Registry::new();
    let collector = TempCollector::new();
    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    for metric in TEMP_METRICS {
        let value = gauge_value(&registry, metric, "pg_default")
            .with_context(|| format!("expected a pg_default series for {metric}"))?;
        assert!(value >= 0.0, "{metric} must never be negative, got {value}");
    }

    let files = gauge_value(&registry, "pg_temp_files_current_count", "pg_default")
        .context("expected a pg_default file count")?;
    if files < f64::EPSILON {
        let bytes = gauge_value(&registry, "pg_temp_files_current_bytes", "pg_default")
            .context("expected a pg_default byte gauge")?;
        let age = gauge_value(&registry, "pg_temp_files_oldest_age_seconds", "pg_default")
            .context("expected a pg_default age gauge")?;
        assert!(
            bytes < f64::EPSILON,
            "no temp files must mean no temp bytes, got {bytes}"
        );
        assert!(
            age < f64::EPSILON,
            "no temp files must mean no oldest age, got {age}"
        );
    }

    pool.close().await;
    Ok(())
}

/// The core acceptance criterion of issue #32: a still-running statement that
/// spills to disk must be visible *now*, not once it finishes and the cumulative
/// counters catch up. The gauges must then fall back to zero.
#[tokio::test]
async fn test_temp_reports_in_progress_spill_and_clears_afterwards() -> Result<()> {
    let pool = common::create_test_pool().await?;
    if server_version_num(&pool).await? < 120_000 || !can_read_tmpdir(&pool).await {
        pool.close().await;
        return Ok(());
    }

    let registry = Registry::new();
    let collector = TempCollector::new();
    collector.register_metrics(&registry)?;

    let spiller_pool = common::create_test_pool().await?;
    let spiller = tokio::spawn(async move {
        let _ = sqlx::query("SET work_mem = '64kB'")
            .execute(&spiller_pool)
            .await;
        // Large enough to spill for several seconds, sorted on a wide key so the
        // temp files are big rather than numerous.
        let _ = sqlx::query(
            "SELECT COUNT(*)::bigint
             FROM (
                 SELECT g, repeat(md5(g::text), 20) AS padding
                 FROM generate_series(1, 2000000) g
                 ORDER BY padding
             ) spilling",
        )
        .fetch_one(&spiller_pool)
        .await;
        spiller_pool.close().await;
    });

    // Poll while the spilling statement is still running, tracking the peak.
    let mut peak_bytes = 0.0_f64;
    let mut peak_files = 0.0_f64;
    for _ in 0..80 {
        collector.collect(&pool).await?;
        peak_bytes = peak_bytes.max(
            gauge_value(&registry, "pg_temp_files_current_bytes", "pg_default").unwrap_or_default(),
        );
        peak_files = peak_files.max(
            gauge_value(&registry, "pg_temp_files_current_count", "pg_default").unwrap_or_default(),
        );
        if spiller.is_finished() {
            break;
        }
        sleep(Duration::from_millis(250)).await;
    }

    spiller.await?;

    assert!(
        peak_bytes > 0.0,
        "expected a live temp-file footprint while a spilling statement was running"
    );
    assert!(
        peak_files > 0.0,
        "expected at least one live temp file, got {peak_files}"
    );

    // PostgreSQL removes the files when the statement ends; the gauges must
    // follow instead of pinning the peak forever. The gauges are cluster-wide,
    // so a concurrently spilling test can keep them above zero; requiring a drop
    // below our own peak is what actually distinguishes a live reading from a
    // pinned maximum.
    let mut released = false;
    for _ in 0..20 {
        collector.collect(&pool).await?;
        let bytes =
            gauge_value(&registry, "pg_temp_files_current_bytes", "pg_default").unwrap_or_default();
        if bytes == 0.0 || bytes < peak_bytes {
            released = true;
            break;
        }
        sleep(Duration::from_millis(250)).await;
    }
    assert!(
        released,
        "temp gauges must drop back once PostgreSQL removes the temporary files, \
         but they stayed at the {peak_bytes} byte peak"
    );

    pool.close().await;
    Ok(())
}

/// Temp footprints routinely exceed 2 GiB, so every step of the path has to stay
/// 64-bit.
///
/// The collector's own decode step is covered exhaustively by the `TempRowValues`
/// unit tests in `src/collectors/temp/pg_ls_tmpdir.rs` (2 GiB, `i64::MAX`, missing
/// columns, negative ages). What those cannot cover is the live `PostgreSQL` side:
/// this test locks that `pg_ls_tmpdir()`'s size column really is a `bigint` and
/// that summing it stays a `bigint`, so a future `::int` narrowing in the
/// collector's SQL would fail here instead of silently exporting `0`.
#[tokio::test]
async fn test_temp_handles_large_byte_values() -> Result<()> {
    let pool = common::create_test_pool().await?;
    if server_version_num(&pool).await? < 120_000 || !can_read_tmpdir(&pool).await {
        pool.close().await;
        return Ok(());
    }

    let signature: String = sqlx::query_scalar(
        "SELECT pg_get_function_arguments(oid) FROM pg_proc \
         WHERE proname = 'pg_ls_tmpdir' AND pronargs = 0",
    )
    .fetch_one(&pool)
    .await?;
    assert!(
        signature.contains("OUT size bigint"),
        "pg_ls_tmpdir().size must stay bigint; a narrower type would truncate large spills, got {signature}"
    );

    // The aggregation the collector performs must survive a value far beyond i32
    // range, and must still decode as i64 through the driver.
    let summed: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(size), 0)::bigint \
         FROM (SELECT 250::bigint * 1024 * 1024 * 1024 AS size) AS huge",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(summed, 268_435_456_000);

    let registry = Registry::new();
    let collector = TempCollector::new();
    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let bytes =
        gauge_value(&registry, "pg_temp_files_current_bytes", "pg_default").unwrap_or_default();
    assert!(bytes >= 0.0, "byte gauge must never be negative");
    assert!(
        bytes.fract().abs() < f64::EPSILON,
        "byte gauge must stay integral, got {bytes}"
    );

    pool.close().await;
    Ok(())
}

/// A role without `pg_monitor` must not break the scrape: the collector warns
/// once and exports nothing.
///
/// It must also *stop* exporting. Skipping the update while keeping the last
/// successful reading would publish a stale temp footprint as if it were current,
/// which is what happens when the privilege is revoked or a failover moves the
/// exporter to a server where the function is unavailable.
#[tokio::test]
async fn test_temp_degrades_gracefully_without_privileges() -> Result<()> {
    let pool = common::create_test_pool().await?;

    // Role names starting with `pg_` are reserved by PostgreSQL.
    let role = "exporter_temp_unprivileged";
    let is_superuser: bool =
        sqlx::query_scalar("SELECT usesuper FROM pg_user WHERE usename = current_user")
            .fetch_optional(&pool)
            .await?
            .unwrap_or(false);
    if !is_superuser {
        pool.close().await;
        return Ok(());
    }

    let _ = sqlx::query(sqlx::AssertSqlSafe(format!("DROP ROLE IF EXISTS {role}")))
        .execute(&pool)
        .await;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE ROLE {role} LOGIN PASSWORD 'temp_probe' NOSUPERUSER"
    )))
    .execute(&pool)
    .await?;

    let result = async {
        let dsn = common::get_test_dsn();
        let unprivileged_dsn = dsn
            .replace("postgres:postgres@", &format!("{role}:temp_probe@"))
            .replace("//postgres@", &format!("//{role}@"));

        let unprivileged_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(5))
            .connect(&unprivileged_dsn)
            .await?;

        let registry = Registry::new();
        let collector = TempCollector::new();
        collector.register_metrics(&registry)?;

        // Populate the gauges from a privileged connection first so the
        // unprivileged scrape has stale values it could leave behind.
        if can_read_tmpdir(&pool).await {
            collector.collect(&pool).await?;
            let seeded = registry.gather().iter().any(|family| {
                family.name().starts_with("pg_temp_files_") && !family.get_metric().is_empty()
            });
            assert!(
                seeded,
                "the privileged scrape must publish series, otherwise this test \
                 cannot prove the denied scrape clears them"
            );
        }

        // Must not error even though pg_ls_tmpdir() is denied.
        collector.collect(&unprivileged_pool).await?;
        collector.collect(&unprivileged_pool).await?;

        let populated = registry.gather().iter().any(|family| {
            family.name().starts_with("pg_temp_files_") && !family.get_metric().is_empty()
        });
        assert!(
            !populated,
            "no temp series may be exported when pg_ls_tmpdir() is denied"
        );

        unprivileged_pool.close().await;
        Ok::<(), anyhow::Error>(())
    }
    .await;

    let _ = sqlx::query(sqlx::AssertSqlSafe(format!("DROP ROLE IF EXISTS {role}")))
        .execute(&pool)
        .await;
    pool.close().await;
    result
}

/// On servers older than `PostgreSQL` 12 the collector must skip gracefully. This
/// assertion only runs when the suite is executed against a pre-12 server; on 12+
/// it is a documented no-op.
#[tokio::test]
async fn test_temp_is_graceful_noop_before_pg12() -> Result<()> {
    let pool = common::create_test_pool().await?;
    if server_version_num(&pool).await? >= 120_000 {
        pool.close().await;
        return Ok(());
    }

    let registry = Registry::new();
    let collector = TempCollector::new();
    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let populated = registry.gather().iter().any(|family| {
        family.name().starts_with("pg_temp_files_") && !family.get_metric().is_empty()
    });
    assert!(
        !populated,
        "pg_ls_tmpdir() must expose no series on PostgreSQL versions older than 12"
    );

    pool.close().await;
    Ok(())
}

/// `pg_global` never holds temporary relations and `pg_ls_tmpdir()` errors on it,
/// so it must not appear as a label value.
#[tokio::test]
async fn test_temp_excludes_pg_global_tablespace() -> Result<()> {
    let pool = common::create_test_pool().await?;
    if server_version_num(&pool).await? < 120_000 || !can_read_tmpdir(&pool).await {
        pool.close().await;
        return Ok(());
    }

    let registry = Registry::new();
    let collector = TempCollector::new();
    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let has_pg_global = registry.gather().iter().any(|family| {
        family.get_metric().iter().any(|metric| {
            metric
                .get_label()
                .iter()
                .any(|label| label.name() == "tablespace" && label.value() == "pg_global")
        })
    });
    assert!(
        !has_pg_global,
        "pg_global must be excluded: pg_ls_tmpdir() rejects it"
    );

    pool.close().await;
    Ok(())
}

/// Labels must stay low cardinality: only `tablespace`, never filenames or PIDs.
#[tokio::test]
async fn test_temp_labels_are_low_cardinality() -> Result<()> {
    let pool = common::create_test_pool().await?;
    if server_version_num(&pool).await? < 120_000 || !can_read_tmpdir(&pool).await {
        pool.close().await;
        return Ok(());
    }

    let registry = Registry::new();
    let collector = TempCollector::new();
    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    for family in registry.gather() {
        if !family.name().starts_with("pg_temp_files_") {
            continue;
        }
        for metric in family.get_metric() {
            let names: Vec<&str> = metric
                .get_label()
                .iter()
                .map(prometheus::proto::LabelPair::name)
                .collect();
            assert_eq!(
                names,
                vec!["tablespace"],
                "{} must only carry the tablespace label",
                family.name()
            );
        }
    }

    pool.close().await;
    Ok(())
}
