use super::super::common;
use anyhow::Result;
use pg_exporter::collectors::{Collector, vacuum::analyze_progress::AnalyzeProgressCollector};
use prometheus::{Registry, proto::MetricFamily};
use sqlx::{PgPool, Row};

const ANALYZE_PROGRESS_METRICS: [&str; 2] = [
    "pg_stat_progress_analyze_sample_blks_scanned",
    "pg_stat_progress_analyze_sample_blks_total",
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
async fn test_analyze_progress_registers_without_error() -> Result<()> {
    let collector = AnalyzeProgressCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    Ok(())
}

#[tokio::test]
async fn test_analyze_progress_collector_name() {
    let collector = AnalyzeProgressCollector::new();
    assert_eq!(collector.name(), "analyze_progress");
}

#[tokio::test]
async fn test_analyze_progress_collect_succeeds_on_idle_database() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let collector = AnalyzeProgressCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_analyze_progress_no_progress_leaves_no_series() -> Result<()> {
    let pool = common::create_test_pool().await?;
    if server_version_num(&pool).await? < 130_000 {
        pool.close().await;
        return Ok(());
    }

    let before_count: i64 =
        sqlx::query_scalar("SELECT count(*)::bigint FROM pg_stat_progress_analyze")
            .fetch_one(&pool)
            .await?;

    let collector = AnalyzeProgressCollector::new();
    let registry = Registry::new();
    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let after_count: i64 =
        sqlx::query_scalar("SELECT count(*)::bigint FROM pg_stat_progress_analyze")
            .fetch_one(&pool)
            .await?;

    if before_count == 0 && after_count == 0 {
        let families = registry.gather();
        for metric_name in ANALYZE_PROGRESS_METRICS {
            let sample_count =
                metric_family(&families, metric_name).map_or(0, |family| family.get_metric().len());
            assert_eq!(
                sample_count, 0,
                "idle pg_stat_progress_analyze should not leave stale {metric_name} series"
            );
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_analyze_progress_series_have_expected_labels_and_values() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let collector = AnalyzeProgressCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();
    for metric_name in ANALYZE_PROGRESS_METRICS {
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
async fn test_analyze_progress_type_conversions() -> Result<()> {
    let pool = common::create_test_pool().await?;
    if server_version_num(&pool).await? < 130_000 {
        pool.close().await;
        return Ok(());
    }

    let row = sqlx::query(
        "SELECT
            count(*)::bigint AS rows_seen,
            COALESCE(max(sample_blks_total), 0)::bigint AS sample_blks_total,
            COALESCE(max(sample_blks_scanned), 0)::bigint AS sample_blks_scanned
         FROM pg_stat_progress_analyze",
    )
    .fetch_one(&pool)
    .await?;

    for column in ["rows_seen", "sample_blks_total", "sample_blks_scanned"] {
        let value: i64 = row.try_get(column)?;
        assert!(value >= 0, "{column} should be non-negative");
    }

    pool.close().await;
    Ok(())
}
