use super::super::common;
use anyhow::Result;
use pg_exporter::collectors::{
    Collector, replication::stat_replication_slots::StatReplicationSlotsCollector,
};
use prometheus::Registry;
use sqlx::{PgPool, Row};

const MIN_STAT_REPLICATION_SLOTS_VERSION: i32 = 140_000;

const STAT_REPLICATION_SLOTS_METRICS: [&str; 8] = [
    "pg_stat_replication_slots_spill_txns_total",
    "pg_stat_replication_slots_spill_count_total",
    "pg_stat_replication_slots_spill_bytes_total",
    "pg_stat_replication_slots_stream_txns_total",
    "pg_stat_replication_slots_stream_count_total",
    "pg_stat_replication_slots_stream_bytes_total",
    "pg_stat_replication_slots_total_txns_total",
    "pg_stat_replication_slots_total_bytes_total",
];

const STAT_REPLICATION_SLOTS_QUERY: &str = r"
SELECT
    slot_name,
    spill_txns::bigint AS spill_txns,
    spill_count::bigint AS spill_count,
    spill_bytes::bigint AS spill_bytes,
    stream_txns::bigint AS stream_txns,
    stream_count::bigint AS stream_count,
    stream_bytes::bigint AS stream_bytes,
    total_txns::bigint AS total_txns,
    total_bytes::bigint AS total_bytes
FROM pg_stat_replication_slots
WHERE slot_name IS NOT NULL
";

async fn server_version_num(pool: &PgPool) -> Result<i32> {
    let row = sqlx::query("SELECT current_setting('server_version_num')::int AS v")
        .fetch_one(pool)
        .await?;
    Ok(row.try_get::<i32, _>("v")?)
}

async fn server_is_primary(pool: &PgPool) -> Result<bool> {
    let row = sqlx::query("SELECT NOT pg_is_in_recovery() AS is_primary")
        .fetch_one(pool)
        .await?;
    Ok(row.try_get::<bool, _>("is_primary")?)
}

async fn logical_slot_stats_rows(pool: &PgPool) -> Result<i64> {
    let row = sqlx::query(
        "SELECT count(*)::bigint AS slot_count
         FROM pg_stat_replication_slots
         WHERE slot_name IS NOT NULL",
    )
    .fetch_one(pool)
    .await?;
    Ok(row.try_get::<i64, _>("slot_count")?)
}

#[must_use]
fn stat_replication_slots_series_populated(families: &[prometheus::proto::MetricFamily]) -> bool {
    families.iter().any(|family| {
        STAT_REPLICATION_SLOTS_METRICS.contains(&family.name()) && !family.get_metric().is_empty()
    })
}

#[tokio::test]
async fn test_stat_replication_slots_collector_name() {
    let collector = StatReplicationSlotsCollector::new();
    assert_eq!(collector.name(), "stat_replication_slots");
}

#[tokio::test]
async fn test_stat_replication_slots_registers_without_error() -> Result<()> {
    let registry = Registry::new();
    let collector = StatReplicationSlotsCollector::new();

    collector.register_metrics(&registry)?;
    Ok(())
}

#[tokio::test]
async fn test_stat_replication_slots_collect_succeeds_with_no_logical_slots() -> Result<()> {
    let pool = common::create_test_pool().await?;
    if server_version_num(&pool).await? < MIN_STAT_REPLICATION_SLOTS_VERSION
        || !server_is_primary(&pool).await?
        || logical_slot_stats_rows(&pool).await? != 0
    {
        pool.close().await;
        return Ok(());
    }

    let registry = Registry::new();
    let collector = StatReplicationSlotsCollector::new();
    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();
    assert!(
        !stat_replication_slots_series_populated(&families),
        "`pg_stat_replication_slots` must expose no series when there are no logical slots"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_stat_replication_slots_collect_succeeds_on_any_version() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = StatReplicationSlotsCollector::new();
    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_stat_replication_slots_type_conversion_is_bigint_safe() -> Result<()> {
    let pool = common::create_test_pool().await?;
    if server_version_num(&pool).await? < MIN_STAT_REPLICATION_SLOTS_VERSION {
        pool.close().await;
        return Ok(());
    }

    let rows = sqlx::query(STAT_REPLICATION_SLOTS_QUERY)
        .fetch_all(&pool)
        .await?;

    for row in &rows {
        let slot_name: String = row.try_get("slot_name")?;
        assert!(!slot_name.is_empty(), "`slot_name` should be populated");

        for value in [
            row.try_get::<i64, _>("spill_txns")?,
            row.try_get::<i64, _>("spill_count")?,
            row.try_get::<i64, _>("spill_bytes")?,
            row.try_get::<i64, _>("stream_txns")?,
            row.try_get::<i64, _>("stream_count")?,
            row.try_get::<i64, _>("stream_bytes")?,
            row.try_get::<i64, _>("total_txns")?,
            row.try_get::<i64, _>("total_bytes")?,
        ] {
            assert!(value >= 0, "logical slot stats should be non-negative");
        }
    }

    pool.close().await;
    Ok(())
}
