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
            families.iter().map(|m| m.name()).collect::<Vec<_>>()
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
