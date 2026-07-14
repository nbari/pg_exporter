use super::common;
use anyhow::Result;
use pg_exporter::collectors::{Collector, system::SystemCollector};
use prometheus::Registry;

/// Metric families the system collector exposes (host-wide, per-core CPU).
const SYSTEM_METRICS: [&str; 10] = [
    "pg_system_cpu_seconds_total",
    "pg_system_cpu_cores",
    "pg_system_cpu_cores_physical",
    "pg_system_load1",
    "pg_system_load5",
    "pg_system_load15",
    "pg_system_memory_total_bytes",
    "pg_system_memory_available_bytes",
    "pg_system_memory_used_bytes",
    "pg_system_swap_total_bytes",
];

fn family_names(registry: &Registry) -> Vec<String> {
    registry
        .gather()
        .iter()
        .map(|fam| fam.name().to_string())
        .collect()
}

fn family_is_populated(registry: &Registry, metric_name: &str) -> bool {
    registry
        .gather()
        .iter()
        .any(|fam| fam.name() == metric_name && !fam.get_metric().is_empty())
}

#[tokio::test]
async fn test_system_registers_without_error() -> Result<()> {
    let registry = Registry::new();
    SystemCollector::new().register_metrics(&registry)?;
    Ok(())
}

#[tokio::test]
async fn test_system_name_and_default_disabled() {
    let collector = SystemCollector::new();
    assert_eq!(collector.name(), "system");
    assert!(
        !collector.enabled_by_default(),
        "system must stay opt-in: host metrics only make sense when co-located with PostgreSQL"
    );
}

/// The collector ignores the database entirely, but `collect` still takes a pool.
/// It must succeed and populate the host metrics.
#[tokio::test]
async fn test_system_collect_populates_host_metrics() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = SystemCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    for metric_name in SYSTEM_METRICS {
        assert!(
            family_names(&registry)
                .iter()
                .any(|name| name == metric_name),
            "expected metric {metric_name} to be registered, got {:?}",
            family_names(&registry)
        );
    }

    assert!(
        family_is_populated(&registry, "pg_system_memory_total_bytes"),
        "total memory should report a series"
    );
    assert!(
        family_is_populated(&registry, "pg_system_cpu_cores"),
        "cpu cores should report a series"
    );
    assert!(
        family_is_populated(&registry, "pg_system_cpu_seconds_total"),
        "cpu seconds should report at least one core/mode series"
    );

    pool.close().await;
    Ok(())
}

/// CPU counters are node_exporter-style per-core series: `pg_system_cpu_seconds_total`
/// must always carry both a `cpu` and a `mode` label (Linux/FreeBSD; on unsupported
/// platforms CPU counters are skipped and only memory/load are exported).
#[tokio::test]
async fn test_system_cpu_seconds_is_per_core() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = SystemCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    #[cfg(any(target_os = "linux", target_os = "freebsd"))]
    {
        let mut saw_cpu_label = false;
        let mut saw_mode_label = false;
        for fam in registry.gather() {
            if fam.name() != "pg_system_cpu_seconds_total" {
                continue;
            }
            assert_eq!(
                fam.get_field_type(),
                prometheus::proto::MetricType::COUNTER,
                "CPU seconds must be exposed as a counter"
            );
            for metric in fam.get_metric() {
                let labels = metric.get_label();
                saw_cpu_label |= labels.iter().any(|l| l.name() == "cpu");
                saw_mode_label |= labels.iter().any(|l| l.name() == "mode");
            }
        }
        assert!(
            saw_cpu_label && saw_mode_label,
            "per-core CPU series must carry both `cpu` and `mode` labels on Linux/FreeBSD"
        );
    }

    pool.close().await;
    Ok(())
}

/// The process-group sub-collector must register its metrics and, on supported
/// platforms, populate the always-set `count`/`memory` series with the
/// low-cardinality `group="postgres"` label (never per-PID labels).
#[tokio::test]
async fn test_system_process_group_metrics() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = SystemCollector::new();

    collector.register_metrics(&registry)?;
    // Two scrapes: the CPU counter accumulates per-PID deltas across scrapes.
    collector.collect(&pool).await?;
    collector.collect(&pool).await?;

    #[cfg(any(target_os = "linux", target_os = "freebsd"))]
    {
        assert!(
            family_is_populated(&registry, "pg_system_process_group_count"),
            "process count series should always be set on Linux/FreeBSD"
        );
        assert!(
            family_is_populated(&registry, "pg_system_process_group_memory_bytes"),
            "process-group memory series should always be set on Linux/FreeBSD"
        );

        for fam in registry.gather() {
            if !fam.name().starts_with("pg_system_process_group_") {
                continue;
            }
            for metric in fam.get_metric() {
                let has_group = metric
                    .get_label()
                    .iter()
                    .any(|label| label.name() == "group" && label.value() == "postgres");
                assert!(
                    has_group,
                    "{} series must carry a group=\"postgres\" label",
                    fam.name()
                );
                assert!(
                    metric.get_label().iter().all(|label| label.name() != "pid"),
                    "{} must aggregate the group, never carry a per-PID label",
                    fam.name()
                );
            }
        }
    }

    pool.close().await;
    Ok(())
}

/// Host metrics must be global: no `datname`/`database` label should ever appear,
/// keeping cardinality independent of the number of databases.
#[tokio::test]
async fn test_system_metrics_have_no_database_label() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = SystemCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    for fam in registry.gather() {
        if !fam.name().starts_with("pg_system_") {
            continue;
        }
        for metric in fam.get_metric() {
            for label in metric.get_label() {
                assert_ne!(
                    label.name(),
                    "datname",
                    "{} must not carry a database label",
                    fam.name()
                );
                assert_ne!(
                    label.name(),
                    "database",
                    "{} must not carry a database label",
                    fam.name()
                );
            }
        }
    }

    pool.close().await;
    Ok(())
}
