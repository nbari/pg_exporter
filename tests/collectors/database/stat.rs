use super::super::common;
use anyhow::Result;
use pg_exporter::collectors::{
    Collector,
    database::{DatabaseCollector, stats::DatabaseStatCollector},
};
use prometheus::Registry;

#[tokio::test]
async fn test_database_stats_registers_without_error() -> Result<()> {
    let collector = DatabaseStatCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    Ok(())
}

#[tokio::test]
async fn test_database_stats_has_all_metrics_after_collection() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = DatabaseStatCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();

    let expected = vec![
        "pg_stat_database_numbackends",
        "pg_stat_database_xact_commit",
        "pg_stat_database_xact_rollback",
        "pg_stat_database_blks_read",
        "pg_stat_database_blks_hit",
        "pg_stat_database_blks_hit_ratio", // NEW: cache hit ratio
        "pg_stat_database_tup_returned",
        "pg_stat_database_tup_fetched",
        "pg_stat_database_tup_inserted",
        "pg_stat_database_tup_updated",
        "pg_stat_database_tup_deleted",
        "pg_stat_database_conflicts",
        "pg_stat_database_temp_files",
        "pg_stat_database_temp_bytes",
        "pg_stat_database_deadlocks",
        "pg_stat_database_blk_read_time",
        "pg_stat_database_blk_write_time",
        "pg_stat_database_stats_reset",
        // active_time_seconds_total may not exist on PG < 14; handled later
    ];

    for name in expected {
        assert!(
            families.iter().any(|m| m.name() == name),
            "Metric {} should exist. Found: {:?}",
            name,
            families
                .iter()
                .map(prometheus::proto::MetricFamily::name)
                .collect::<Vec<_>>()
        );
    }

    // Active time metric (present on PG >= 14)
    let _maybe_active = families
        .iter()
        .any(|m| m.name() == "pg_stat_database_active_time_seconds_total");
    // It's okay if absent (older PG), but if present it should have metrics
    if let Some(fam) = families
        .iter()
        .find(|m| m.name() == "pg_stat_database_active_time_seconds_total")
    {
        assert!(
            !fam.get_metric().is_empty(),
            "active_time_seconds_total should have values when present"
        );
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_database_stats_labels_present() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = DatabaseStatCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();

    for fam in &families {
        if fam.name().starts_with("pg_stat_database_") {
            for m in fam.get_metric() {
                let labels: Vec<_> = m
                    .get_label()
                    .iter()
                    .map(|l| (l.name(), l.value()))
                    .collect();
                let has_datid = labels.iter().any(|(n, _)| *n == "datid");
                let has_datname = labels.iter().any(|(n, _)| *n == "datname");
                assert!(has_datid, "Metric {} should have 'datid' label", fam.name());
                assert!(
                    has_datname,
                    "Metric {} should have 'datname' label",
                    fam.name()
                );
            }
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_database_stats_values_non_negative() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = DatabaseStatCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    for fam in registry.gather() {
        if fam.name().starts_with("pg_stat_database_") {
            for m in fam.get_metric() {
                let v = m.get_gauge().value();
                assert!(
                    v.is_finite() && v >= 0.0,
                    "Metric {} should be non-negative, got {}",
                    fam.name(),
                    v
                );
            }
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_database_collector_name_and_enabled() {
    let coll = DatabaseCollector::new();
    assert_eq!(coll.name(), "database");
    assert!(
        !coll.enabled_by_default(),
        "database collector is disabled by default"
    );
}

#[tokio::test]
async fn test_database_stats_handles_concurrent_collection() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = DatabaseStatCollector::new();
    let registry = Registry::new();
    collector.register_metrics(&registry)?;

    let (r1, r2) = tokio::join!(collector.collect(&pool), collector.collect(&pool));
    r1?;
    r2?;

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_database_stats_cache_hit_ratio_exists() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = DatabaseStatCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();

    // Cache hit ratio metric must exist
    let cache_hit = families
        .iter()
        .find(|m| m.name() == "pg_stat_database_blks_hit_ratio")
        .expect("pg_stat_database_blks_hit_ratio metric should exist");

    assert!(
        !cache_hit.get_metric().is_empty(),
        "Cache hit ratio should have at least one value"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_database_stats_cache_hit_ratio_range() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = DatabaseStatCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();

    // Cache hit ratio should be between 0.0 and 1.0
    let cache_hit = families
        .iter()
        .find(|m| m.name() == "pg_stat_database_blks_hit_ratio")
        .expect("pg_stat_database_blks_hit_ratio metric should exist");

    for metric in cache_hit.get_metric() {
        let value = metric.get_gauge().value();
        assert!(
            (0.0..=1.0).contains(&value),
            "Cache hit ratio should be between 0.0 and 1.0, got: {value}"
        );
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_database_stats_cache_hit_ratio_calculation() -> Result<()> {
    let pool = common::create_test_pool().await?;

    // Generate some activity to ensure we have actual values
    let _ = sqlx::query("SELECT 1").execute(&pool).await?;

    let collector = DatabaseStatCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();

    // Get blks_read, blks_hit, and cache_hit_ratio
    let blks_read_family = families
        .iter()
        .find(|m| m.name() == "pg_stat_database_blks_read")
        .expect("blks_read should exist");

    let blks_hit_family = families
        .iter()
        .find(|m| m.name() == "pg_stat_database_blks_hit")
        .expect("blks_hit should exist");

    let cache_hit_family = families
        .iter()
        .find(|m| m.name() == "pg_stat_database_blks_hit_ratio")
        .expect("cache_hit_ratio should exist");

    // For each database, verify the calculation
    for metric in cache_hit_family.get_metric() {
        let labels: Vec<_> = metric
            .get_label()
            .iter()
            .map(|l| (l.name(), l.value()))
            .collect();

        let datid = labels.iter().find(|(n, _)| *n == "datid").map(|(_, v)| *v);
        let datname = labels
            .iter()
            .find(|(n, _)| *n == "datname")
            .map(|(_, v)| *v);

        let cache_ratio = metric.get_gauge().value();

        // Find corresponding blks_read and blks_hit
        let blks_read = blks_read_family
            .get_metric()
            .iter()
            .find(|m| {
                let m_labels: Vec<_> = m
                    .get_label()
                    .iter()
                    .map(|l| (l.name(), l.value()))
                    .collect();
                m_labels
                    .iter()
                    .any(|(n, v)| *n == "datid" && datid == Some(*v))
            })
            .map_or(0.0, |m| m.get_gauge().value());

        let blks_hit = blks_hit_family
            .get_metric()
            .iter()
            .find(|m| {
                let m_labels: Vec<_> = m
                    .get_label()
                    .iter()
                    .map(|l| (l.name(), l.value()))
                    .collect();
                m_labels
                    .iter()
                    .any(|(n, v)| *n == "datid" && datid == Some(*v))
            })
            .map_or(0.0, |m| m.get_gauge().value());

        // Verify calculation: cache_hit_ratio = blks_hit / (blks_hit + blks_read)
        let total = blks_hit + blks_read;
        if total > 0.0 {
            let expected_ratio = blks_hit / total;
            let diff = (cache_ratio - expected_ratio).abs();
            assert!(
                diff < 0.0001,
                "Database {datname:?}: cache hit ratio mismatch. Expected {expected_ratio}, got {cache_ratio}. (blks_hit={blks_hit}, blks_read={blks_read})"
            );
        } else {
            // If no blocks accessed yet, ratio should be 0.0
            assert!(
                (cache_ratio - 0.0).abs() < f64::EPSILON,
                "Database {datname:?}: cache hit ratio should be 0.0 when no blocks accessed"
            );
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_database_stats_cache_hit_ratio_labels() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = DatabaseStatCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();

    let cache_hit = families
        .iter()
        .find(|m| m.name() == "pg_stat_database_blks_hit_ratio")
        .expect("pg_stat_database_blks_hit_ratio metric should exist");

    // Verify all metrics have correct labels
    for metric in cache_hit.get_metric() {
        let labels: Vec<_> = metric
            .get_label()
            .iter()
            .map(prometheus::proto::LabelPair::name)
            .collect();

        assert!(
            labels.contains(&"datid"),
            "Cache hit ratio should have 'datid' label"
        );
        assert!(
            labels.contains(&"datname"),
            "Cache hit ratio should have 'datname' label"
        );
    }

    pool.close().await;
    Ok(())
}
