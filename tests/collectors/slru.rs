use super::common;
use anyhow::Result;
use pg_exporter::collectors::{Collector, slru::SlruCollector};
use prometheus::Registry;
use sqlx::{PgPool, Row};
use std::collections::BTreeSet;

/// Every metric family the collector exposes on `PostgreSQL` 13+.
const SLRU_METRICS: [&str; 7] = [
    "pg_stat_slru_blks_zeroed_total",
    "pg_stat_slru_blks_hit_total",
    "pg_stat_slru_blks_read_total",
    "pg_stat_slru_blks_written_total",
    "pg_stat_slru_blks_exists_total",
    "pg_stat_slru_flushes_total",
    "pg_stat_slru_truncates_total",
];

const SLRU_COUNTER_COLUMNS: [&str; 7] = [
    "blks_zeroed",
    "blks_hit",
    "blks_read",
    "blks_written",
    "blks_exists",
    "flushes",
    "truncates",
];

async fn server_version_num(pool: &PgPool) -> Result<i32> {
    let row = sqlx::query("SELECT current_setting('server_version_num')::int AS v")
        .fetch_one(pool)
        .await?;
    Ok(row.try_get::<i32, _>("v")?)
}

async fn pg_stat_slru_names(pool: &PgPool) -> Result<BTreeSet<String>> {
    let rows = sqlx::query("SELECT name FROM pg_stat_slru ORDER BY name")
        .fetch_all(pool)
        .await?;

    let mut names = BTreeSet::new();
    for row in rows {
        names.insert(row.try_get::<String, _>("name")?);
    }
    Ok(names)
}

fn metric_label_values(registry: &Registry, metric_name: &str) -> BTreeSet<String> {
    let mut values = BTreeSet::new();
    for fam in registry.gather() {
        if fam.name() != metric_name {
            continue;
        }
        for metric in fam.get_metric() {
            for label in metric.get_label() {
                if label.name() == "name" {
                    values.insert(label.value().to_string());
                }
            }
        }
    }
    values
}

#[tokio::test]
async fn test_slru_registers_without_error() -> Result<()> {
    let registry = Registry::new();
    SlruCollector::new().register_metrics(&registry)?;
    Ok(())
}

#[tokio::test]
async fn test_slru_name_and_default_disabled() {
    let collector = SlruCollector::new();
    assert_eq!(collector.name(), "slru");
    assert!(
        !collector.enabled_by_default(),
        "slru must stay opt-in because SLRU pressure metrics are targeted diagnostics"
    );
}

/// Collecting must succeed on every supported server version: a clean no-op
/// (with a single "requires `PostgreSQL` 13+" warning) below `PostgreSQL` 13,
/// and a populated snapshot on 13+.
#[tokio::test]
async fn test_slru_collect_succeeds_on_any_version() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = SlruCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    pool.close().await;
    Ok(())
}

/// On servers older than `PostgreSQL` 13 the collector must skip gracefully: it
/// returns `Ok` (never a "relation `pg_stat_slru` does not exist" error or a
/// panic) and exposes no `pg_stat_slru_*` series. This assertion only runs when
/// the suite is executed against a pre-13 server; on 13+ it is a documented
/// no-op.
#[tokio::test]
async fn test_slru_is_graceful_noop_before_pg13() -> Result<()> {
    let pool = common::create_test_pool().await?;
    if server_version_num(&pool).await? >= 130_000 {
        pool.close().await;
        return Ok(());
    }

    let registry = Registry::new();
    let collector = SlruCollector::new();
    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let populated = registry
        .gather()
        .iter()
        .any(|fam| fam.name().starts_with("pg_stat_slru_") && !fam.get_metric().is_empty());
    assert!(
        !populated,
        "`pg_stat_slru` must expose no series on `PostgreSQL` versions older than 13"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_slru_exposes_all_metrics_on_pg13plus() -> Result<()> {
    let pool = common::create_test_pool().await?;
    if server_version_num(&pool).await? < 130_000 {
        pool.close().await;
        return Ok(());
    }

    let registry = Registry::new();
    let collector = SlruCollector::new();
    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();
    for metric_name in SLRU_METRICS {
        assert!(
            families.iter().any(|m| m.name() == metric_name),
            "Metric {metric_name} should exist on `PostgreSQL` 13+. Found: {:?}",
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
async fn test_slru_populates_fixed_rows_on_pg13plus() -> Result<()> {
    let pool = common::create_test_pool().await?;
    if server_version_num(&pool).await? < 130_000 {
        pool.close().await;
        return Ok(());
    }

    let expected_names = pg_stat_slru_names(&pool).await?;
    assert!(
        !expected_names.is_empty(),
        "`pg_stat_slru` should expose fixed SLRU rows on `PostgreSQL` 13+"
    );

    let registry = Registry::new();
    let collector = SlruCollector::new();
    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    for metric_name in SLRU_METRICS {
        let observed_names = metric_label_values(&registry, metric_name);
        assert_eq!(
            observed_names, expected_names,
            "{metric_name} should expose every `pg_stat_slru` row with the `name` label"
        );
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_slru_query_type_conversions_work_on_pg13plus() -> Result<()> {
    let pool = common::create_test_pool().await?;
    if server_version_num(&pool).await? < 130_000 {
        pool.close().await;
        return Ok(());
    }

    let rows = sqlx::query(
        r"
        SELECT
            name,
            blks_zeroed::bigint AS blks_zeroed,
            blks_hit::bigint AS blks_hit,
            blks_read::bigint AS blks_read,
            blks_written::bigint AS blks_written,
            blks_exists::bigint AS blks_exists,
            flushes::bigint AS flushes,
            truncates::bigint AS truncates
        FROM pg_stat_slru
        ",
    )
    .fetch_all(&pool)
    .await?;

    assert!(
        !rows.is_empty(),
        "`pg_stat_slru` should expose SLRU rows on `PostgreSQL` 13+"
    );

    for row in rows {
        let name: String = row.try_get("name")?;
        assert!(!name.is_empty(), "`pg_stat_slru.name` must not be empty");

        for column in SLRU_COUNTER_COLUMNS {
            let value: i64 = row.try_get(column)?;
            assert!(
                value >= 0,
                "`pg_stat_slru.{column}` should be a non-negative `bigint`, got {value}"
            );
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_slru_series_are_labeled_and_finite() -> Result<()> {
    let pool = common::create_test_pool().await?;
    if server_version_num(&pool).await? < 130_000 {
        pool.close().await;
        return Ok(());
    }

    let registry = Registry::new();
    let collector = SlruCollector::new();
    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let mut saw_series = false;
    for fam in registry.gather() {
        if !SLRU_METRICS.contains(&fam.name()) {
            continue;
        }
        for metric in fam.get_metric() {
            saw_series = true;

            let label_names: Vec<&str> = metric
                .get_label()
                .iter()
                .map(prometheus::proto::LabelPair::name)
                .collect();
            assert!(
                label_names.contains(&"name"),
                "{} is missing the `name` label",
                fam.name()
            );

            let value = metric.get_gauge().value();
            let integer_value = common::metric_value_to_i64(value);
            assert!(
                integer_value >= 0,
                "{} has an invalid value {value}",
                fam.name()
            );
        }
    }

    assert!(
        saw_series,
        "`pg_stat_slru` should report at least one SLRU row on `PostgreSQL` 13+"
    );

    pool.close().await;
    Ok(())
}
