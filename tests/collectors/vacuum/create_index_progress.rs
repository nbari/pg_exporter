use super::super::common;
use anyhow::Result;
use pg_exporter::collectors::{
    Collector, vacuum::create_index_progress::CreateIndexProgressCollector,
};
use prometheus::{Registry, proto::MetricFamily};
use sqlx::{PgPool, Row};

const CREATE_INDEX_PROGRESS_METRICS: [&str; 6] = [
    "pg_stat_progress_create_index_blocks_done",
    "pg_stat_progress_create_index_blocks_total",
    "pg_stat_progress_create_index_tuples_done",
    "pg_stat_progress_create_index_tuples_total",
    "pg_stat_progress_create_index_lockers_done",
    "pg_stat_progress_create_index_lockers_total",
];

async fn server_version_num(pool: &PgPool) -> Result<i32> {
    let row = sqlx::query("SELECT current_setting('server_version_num')::int AS v")
        .fetch_one(pool)
        .await?;
    Ok(row.try_get::<i32, _>("v")?)
}

fn metric_family<'a>(families: &'a [MetricFamily], name: &str) -> Option<&'a MetricFamily> {
    families.iter().find(|family| family.name() == name)
}

#[tokio::test]
async fn test_create_index_progress_registers_without_error() -> Result<()> {
    let collector = CreateIndexProgressCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    Ok(())
}

#[tokio::test]
async fn test_create_index_progress_collector_name() {
    let collector = CreateIndexProgressCollector::new();
    assert_eq!(collector.name(), "create_index_progress");
}

#[tokio::test]
async fn test_create_index_progress_collect_succeeds_on_idle_database() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let collector = CreateIndexProgressCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_create_index_progress_no_progress_leaves_no_series() -> Result<()> {
    let pool = common::create_test_pool().await?;
    if server_version_num(&pool).await? < 120_000 {
        pool.close().await;
        return Ok(());
    }

    let before_count: i64 =
        sqlx::query_scalar("SELECT count(*)::bigint FROM pg_stat_progress_create_index")
            .fetch_one(&pool)
            .await?;

    let collector = CreateIndexProgressCollector::new();
    let registry = Registry::new();
    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let after_count: i64 =
        sqlx::query_scalar("SELECT count(*)::bigint FROM pg_stat_progress_create_index")
            .fetch_one(&pool)
            .await?;

    if before_count == 0 && after_count == 0 {
        let families = registry.gather();
        for metric_name in CREATE_INDEX_PROGRESS_METRICS {
            let sample_count =
                metric_family(&families, metric_name).map_or(0, |family| family.get_metric().len());
            assert_eq!(
                sample_count, 0,
                "idle pg_stat_progress_create_index should not leave stale {metric_name} series"
            );
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_create_index_progress_series_have_expected_labels_and_values() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let collector = CreateIndexProgressCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();
    for metric_name in CREATE_INDEX_PROGRESS_METRICS {
        if let Some(family) = metric_family(&families, metric_name) {
            for metric in family.get_metric() {
                let labels: Vec<&str> = metric
                    .get_label()
                    .iter()
                    .map(prometheus::proto::LabelPair::name)
                    .collect();
                for expected in ["database_name", "table_name", "phase"] {
                    assert!(
                        labels.contains(&expected),
                        "{metric_name} should include label {expected}"
                    );
                }
                let value = metric.get_gauge().value();
                assert!(value.is_finite() && value >= 0.0);
            }
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_create_index_progress_type_conversions() -> Result<()> {
    let pool = common::create_test_pool().await?;
    if server_version_num(&pool).await? < 120_000 {
        pool.close().await;
        return Ok(());
    }

    let row = sqlx::query(
        "SELECT
            count(*)::bigint AS rows_seen,
            COALESCE(max(blocks_total), 0)::bigint AS blocks_total,
            COALESCE(max(blocks_done), 0)::bigint AS blocks_done,
            COALESCE(max(tuples_total), 0)::bigint AS tuples_total,
            COALESCE(max(tuples_done), 0)::bigint AS tuples_done,
            COALESCE(max(lockers_total), 0)::bigint AS lockers_total,
            COALESCE(max(lockers_done), 0)::bigint AS lockers_done
         FROM pg_stat_progress_create_index",
    )
    .fetch_one(&pool)
    .await?;

    for column in [
        "rows_seen",
        "blocks_total",
        "blocks_done",
        "tuples_total",
        "tuples_done",
        "lockers_total",
        "lockers_done",
    ] {
        let value: i64 = row.try_get(column)?;
        assert!(value >= 0, "{column} should be non-negative");
    }

    pool.close().await;
    Ok(())
}
