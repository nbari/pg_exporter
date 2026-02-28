use super::super::common;
use anyhow::Result;
use pg_exporter::collectors::{Collector, citus::CitusCollector};
use prometheus::Registry;

#[tokio::test]
async fn test_citus_collector_skips_without_extension() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = CitusCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    // On a plain PostgreSQL instance, GaugeVec metrics should have no samples
    // (IntGauge metrics like *_total will have their default 0 value, which is expected)
    let families = registry.gather();
    let citus_vec_families: Vec<_> = families
        .iter()
        .filter(|f| {
            let name = f.name();
            name.starts_with("citus_") && !name.ends_with("_total")
        })
        .collect();

    for family in &citus_vec_families {
        assert!(
            family.get_metric().is_empty(),
            "No citus vec metrics should have samples when extension is not installed, found samples in {}",
            family.name()
        );
    }

    pool.close().await;
    Ok(())
}
