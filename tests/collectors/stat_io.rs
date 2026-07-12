use super::common;
use anyhow::Result;
use pg_exporter::collectors::{Collector, stat_io::StatIoCollector};
use prometheus::Registry;
use sqlx::{PgPool, Row};

/// Every metric family the collector exposes on `PostgreSQL` 16+.
const STAT_IO_METRICS: [&str; 16] = [
    "pg_stat_io_reads_total",
    "pg_stat_io_writes_total",
    "pg_stat_io_writebacks_total",
    "pg_stat_io_extends_total",
    "pg_stat_io_hits_total",
    "pg_stat_io_evictions_total",
    "pg_stat_io_reuses_total",
    "pg_stat_io_fsyncs_total",
    "pg_stat_io_read_bytes_total",
    "pg_stat_io_write_bytes_total",
    "pg_stat_io_extend_bytes_total",
    "pg_stat_io_read_time_seconds_total",
    "pg_stat_io_write_time_seconds_total",
    "pg_stat_io_writeback_time_seconds_total",
    "pg_stat_io_extend_time_seconds_total",
    "pg_stat_io_fsync_time_seconds_total",
];

async fn server_version_num(pool: &PgPool) -> Result<i32> {
    let row = sqlx::query("SELECT current_setting('server_version_num')::int AS v")
        .fetch_one(pool)
        .await?;
    Ok(row.try_get::<i32, _>("v")?)
}

#[tokio::test]
async fn test_stat_io_registers_without_error() -> Result<()> {
    let registry = Registry::new();
    StatIoCollector::new().register_metrics(&registry)?;
    Ok(())
}

#[tokio::test]
async fn test_stat_io_name_and_default_disabled() {
    let collector = StatIoCollector::new();
    assert_eq!(collector.name(), "stat_io");
    assert!(
        !collector.enabled_by_default(),
        "stat_io must stay opt-in to keep label cardinality off default deployments"
    );
}

/// Collecting must succeed on every supported server version: a clean no-op
/// (with a single "requires `PostgreSQL` 16+" warning) below `PostgreSQL` 16, and
/// a populated snapshot on 16+.
#[tokio::test]
async fn test_stat_io_collect_succeeds_on_any_version() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = StatIoCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    pool.close().await;
    Ok(())
}

/// On servers older than `PostgreSQL` 16 the collector must skip gracefully: it
/// returns `Ok` (never a "relation `pg_stat_io` does not exist" error or a panic)
/// and exposes no `pg_stat_io_*` series. This assertion only runs when the suite
/// is executed against a pre-16 server; on 16+ it is a documented no-op.
#[tokio::test]
async fn test_stat_io_is_graceful_noop_before_pg16() -> Result<()> {
    let pool = common::create_test_pool().await?;
    if server_version_num(&pool).await? >= 160_000 {
        pool.close().await;
        return Ok(());
    }

    let registry = Registry::new();
    let collector = StatIoCollector::new();
    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let populated = registry
        .gather()
        .iter()
        .any(|fam| fam.name().starts_with("pg_stat_io_") && !fam.get_metric().is_empty());
    assert!(
        !populated,
        "pg_stat_io must expose no series on PostgreSQL versions older than 16"
    );

    pool.close().await;
    Ok(())
}

/// Repeated collection must stay idempotent and NULL-safe. `pg_stat_io` returns
/// many NULL cells (for example `reads` for the background writer), so a second
/// pass over the same rows must not error or accumulate.
#[tokio::test]
async fn test_stat_io_collect_is_idempotent() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = StatIoCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;
    collector.collect(&pool).await?;

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_stat_io_exposes_all_metrics_on_pg16plus() -> Result<()> {
    let pool = common::create_test_pool().await?;
    if server_version_num(&pool).await? < 160_000 {
        pool.close().await;
        return Ok(());
    }

    let registry = Registry::new();
    let collector = StatIoCollector::new();
    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();
    for metric_name in STAT_IO_METRICS {
        assert!(
            families.iter().any(|m| m.name() == metric_name),
            "Metric {metric_name} should exist on PostgreSQL 16+. Found: {:?}",
            families
                .iter()
                .map(prometheus::proto::MetricFamily::name)
                .collect::<Vec<_>>()
        );
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_stat_io_series_are_labeled_and_finite() -> Result<()> {
    let pool = common::create_test_pool().await?;
    if server_version_num(&pool).await? < 160_000 {
        pool.close().await;
        return Ok(());
    }

    let registry = Registry::new();
    let collector = StatIoCollector::new();
    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let mut saw_series = false;
    for fam in registry.gather() {
        if !fam.name().starts_with("pg_stat_io_") {
            continue;
        }
        for m in fam.get_metric() {
            saw_series = true;

            let label_names: Vec<&str> = m
                .get_label()
                .iter()
                .map(prometheus::proto::LabelPair::name)
                .collect();
            for expected in ["backend_type", "object", "context"] {
                assert!(
                    label_names.contains(&expected),
                    "{} is missing the {expected} label",
                    fam.name()
                );
            }

            let value = m.get_gauge().value();
            assert!(
                value.is_finite() && value >= 0.0,
                "{} has an invalid value {value}",
                fam.name()
            );
        }
    }

    assert!(
        saw_series,
        "pg_stat_io should report at least one backend_type/object/context row on PostgreSQL 16+"
    );

    pool.close().await;
    Ok(())
}

/// A realistic workload should register as I/O somewhere in `pg_stat_io`.
/// Values are cluster-wide and cumulative, so this asserts only that some
/// activity is visible rather than exact counts.
#[tokio::test]
async fn test_stat_io_reflects_workload() -> Result<()> {
    let pool = common::create_test_pool().await?;
    if server_version_num(&pool).await? < 160_000 {
        pool.close().await;
        return Ok(());
    }

    // Run the workload on a single dedicated connection so the session-scoped
    // TEMP TABLE stays valid across statements. pg_stat_io is cluster-wide, so
    // the resulting I/O is still visible when the collector reads via the pool.
    let mut conn = pool.acquire().await?;
    sqlx::query("CREATE TEMP TABLE stat_io_probe AS SELECT g FROM generate_series(1, 5000) g")
        .execute(&mut *conn)
        .await?;
    sqlx::query("SELECT count(*) FROM stat_io_probe")
        .fetch_one(&mut *conn)
        .await?;
    drop(conn);

    let registry = Registry::new();
    let collector = StatIoCollector::new();
    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let mut total_hits = 0.0;
    for fam in registry.gather() {
        if fam.name() == "pg_stat_io_hits_total" {
            for m in fam.get_metric() {
                total_hits += m.get_gauge().value();
            }
        }
    }

    assert!(
        total_hits > 0.0,
        "expected some buffer hits to be visible in pg_stat_io after a workload"
    );

    pool.close().await;
    Ok(())
}
