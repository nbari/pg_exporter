use super::super::common;
use anyhow::Result;
use pg_exporter::collectors::{
    Collector,
    database::{DatabaseCollector, catalog::DatabaseSubCollector},
};
use prometheus::Registry;

#[tokio::test]
async fn test_database_catalog_registers_without_error() -> Result<()> {
    let collector = DatabaseSubCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    Ok(())
}

#[tokio::test]
async fn test_database_catalog_has_metrics_after_collection() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = DatabaseSubCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();

    let expected = vec!["pg_database_size_bytes", "pg_database_connection_limit"];

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

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_database_catalog_labels_present() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = DatabaseSubCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();

    for fam in &families {
        if fam.name().starts_with("pg_database_") {
            for m in fam.get_metric() {
                let labels: Vec<_> = m
                    .get_label()
                    .iter()
                    .map(|l| (l.name(), l.value()))
                    .collect();
                let has_datname = labels.iter().any(|(n, _)| *n == "datname");
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
async fn test_database_size_and_connection_limit_values_reasonable() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = DatabaseSubCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();

    // Size: non-negative; typically >0 for 'postgres'
    if let Some(size_fam) = families
        .iter()
        .find(|m| m.name() == "pg_database_size_bytes")
    {
        for m in size_fam.get_metric() {
            let v = common::metric_value_to_i64(m.get_gauge().value());
            assert!(v >= 0, "pg_database_size_bytes should be >= 0, got {v}");
        }
        // If we find 'postgres', assert > 0
        if let Some(m) = size_fam.get_metric().iter().find(|m| {
            m.get_label()
                .iter()
                .any(|l| l.name() == "datname" && l.value() == "postgres")
        }) {
            assert!(
                m.get_gauge().value() > 0.0,
                "postgres database size should be > 0"
            );
        }
    }

    // Connection limit: -1 (unlimited) or >= 1 (non-negative ok for safety)
    if let Some(limit_fam) = families
        .iter()
        .find(|m| m.name() == "pg_database_connection_limit")
    {
        for m in limit_fam.get_metric() {
            let v = common::metric_value_to_i64(m.get_gauge().value());
            assert!(
                v == -1 || v >= 0,
                "connection limit should be -1 or >= 0, got {v}"
            );
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_database_collector_runs_both_subcollectors() -> Result<()> {
    let pool = common::create_test_pool().await?;

    let collector = DatabaseCollector::new();
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();
    let names: Vec<_> = families
        .iter()
        .map(prometheus::proto::MetricFamily::name)
        .collect();

    // From catalog sub-collector
    assert!(
        names.contains(&"pg_database_size_bytes"),
        "should have pg_database_size_bytes"
    );
    assert!(
        names.contains(&"pg_database_connection_limit"),
        "should have pg_database_connection_limit"
    );

    // From stats sub-collector
    assert!(
        names.contains(&"pg_stat_database_numbackends"),
        "should have pg_stat_database_numbackends"
    );

    pool.close().await;
    Ok(())
}
