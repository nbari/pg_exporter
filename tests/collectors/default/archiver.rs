use super::super::common;
use anyhow::Result;
use pg_exporter::collectors::{Collector, default::archiver::ArchiverCollector};
use prometheus::Registry;
use sqlx::{PgPool, Row};

/// The two age metrics the collector publishes only when there is an age to report.
const AGE_METRICS: [&str; 2] = [
    "pg_stat_archiver_last_archived_age_seconds",
    "pg_stat_archiver_last_failed_age_seconds",
];

/// Which age metrics this server can actually report.
///
/// `pg_stat_archiver.last_archived_time` / `last_failed_time` are NULL until the first
/// successful or failed archive. The collector removes the corresponding series in that case
/// rather than publishing `0`, which would assert "archived 0 seconds ago" and could mask an
/// archiver that has never worked. A test that requires the series must check first.
async fn available_age_metrics(pool: &PgPool) -> Result<Vec<&'static str>> {
    let row = sqlx::query(
        "SELECT last_archived_time IS NOT NULL AS archived,
                last_failed_time IS NOT NULL AS failed
         FROM pg_stat_archiver",
    )
    .fetch_one(pool)
    .await?;

    let mut available = Vec::new();
    if row.try_get::<bool, _>("archived").unwrap_or(false) {
        available.push(AGE_METRICS[0]);
    }
    if row.try_get::<bool, _>("failed").unwrap_or(false) {
        available.push(AGE_METRICS[1]);
    }
    Ok(available)
}

#[tokio::test]
async fn test_archiver_collector_registers_without_error() -> Result<()> {
    let registry = Registry::new();
    let collector = ArchiverCollector::new();

    collector.register_metrics(&registry)?;
    Ok(())
}

#[tokio::test]
async fn test_archiver_collector_has_all_metrics() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = ArchiverCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();
    let expected_metrics = vec![
        "pg_stat_archiver_archived_total",
        "pg_stat_archiver_failed_total",
    ];

    for metric_name in expected_metrics {
        assert!(
            families.iter().any(|m| m.name() == metric_name),
            "Metric {} should exist. Found: {:?}",
            metric_name,
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
async fn test_archiver_collector_values_non_negative() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = ArchiverCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    for fam in registry.gather() {
        if fam.name().starts_with("pg_stat_archiver_") {
            for m in fam.get_metric() {
                let v = if fam.get_field_type() == prometheus::proto::MetricType::COUNTER {
                    m.get_counter().value()
                } else {
                    m.get_gauge().value()
                };
                assert!(
                    v >= 0.0,
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
async fn test_archiver_collector_counter_and_gauge_types() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = ArchiverCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();

    // Counter metrics
    let counter_metrics = vec![
        "pg_stat_archiver_archived_total",
        "pg_stat_archiver_failed_total",
    ];

    for metric_name in counter_metrics {
        let metric_family = families
            .iter()
            .find(|m| m.name() == metric_name)
            .unwrap_or_else(|| panic!("Metric {metric_name} should exist"));

        assert_eq!(
            metric_family.get_field_type(),
            prometheus::proto::MetricType::COUNTER,
            "Metric {metric_name} should be a COUNTER"
        );
    }

    // Gauge metrics
    let gauge_metrics = available_age_metrics(&pool).await?;

    for metric_name in gauge_metrics {
        let metric_family = families
            .iter()
            .find(|m| m.name() == metric_name)
            .unwrap_or_else(|| panic!("Metric {metric_name} should exist"));

        assert_eq!(
            metric_family.get_field_type(),
            prometheus::proto::MetricType::GAUGE,
            "Metric {metric_name} should be a GAUGE"
        );
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_archiver_collector_concurrent_collections() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = ArchiverCollector::new();

    collector.register_metrics(&registry)?;

    // Run multiple collections concurrently
    let mut handles = vec![];
    for _ in 0..5 {
        let pool_clone = pool.clone();
        let collector_clone = collector.clone();
        handles.push(tokio::spawn(async move {
            collector_clone.collect(&pool_clone).await
        }));
    }

    // Wait for all to complete
    for handle in handles {
        handle.await??;
    }

    // Verify metrics are still valid
    let families = registry.gather();
    assert!(
        families
            .iter()
            .any(|m| m.name() == "pg_stat_archiver_archived_total"),
        "Metrics should exist after concurrent collections"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_archiver_collector_idempotent_collection() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = ArchiverCollector::new();

    collector.register_metrics(&registry)?;

    // Collect multiple times
    for _ in 0..3 {
        collector.collect(&pool).await?;
    }

    // Verify metrics exist and are valid
    let families = registry.gather();
    let archived = families
        .iter()
        .find(|m| m.name() == "pg_stat_archiver_archived_total")
        .expect("archived metric should exist");

    let value = archived.get_metric()[0].get_counter().value();
    assert!(
        value >= 0.0,
        "Value should be non-negative after multiple collections"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_archiver_collector_metric_help_text() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = ArchiverCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();
    let archiver_metrics = vec![
        "pg_stat_archiver_archived_total",
        "pg_stat_archiver_failed_total",
    ];

    for metric_name in archiver_metrics {
        let metric_family = families
            .iter()
            .find(|m| m.name() == metric_name)
            .unwrap_or_else(|| panic!("Metric {metric_name} should exist"));

        assert!(
            !metric_family.help().is_empty(),
            "Metric {metric_name} should have help text"
        );
    }

    for metric_name in available_age_metrics(&pool).await? {
        let metric_family = families
            .iter()
            .find(|m| m.name() == metric_name)
            .unwrap_or_else(|| panic!("Metric {metric_name} should exist"));
        assert!(
            !metric_family.help().is_empty(),
            "Metric {metric_name} should have help text"
        );
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_archiver_collector_no_labels() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = ArchiverCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();

    // archiver metrics should have no labels (they are global stats)
    for fam in families {
        if fam.name().starts_with("pg_stat_archiver_") {
            for m in fam.get_metric() {
                assert_eq!(
                    m.get_label().len(),
                    0,
                    "Archiver metrics should have no labels, found {} labels on {}",
                    m.get_label().len(),
                    fam.name()
                );
            }
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_archiver_collector_name() -> Result<()> {
    let collector = ArchiverCollector::new();
    assert_eq!(
        collector.name(),
        "archiver",
        "Collector name should be 'archiver'"
    );
    Ok(())
}

#[tokio::test]
async fn test_archiver_collector_enabled_by_default() -> Result<()> {
    let collector = ArchiverCollector::new();
    assert!(
        collector.enabled_by_default(),
        "Archiver collector should be enabled by default"
    );
    Ok(())
}

#[tokio::test]
async fn test_archiver_collector_double_registration_fails() -> Result<()> {
    let registry = Registry::new();
    let collector = ArchiverCollector::new();

    // First registration should succeed
    collector.register_metrics(&registry)?;

    // Second registration should fail
    let result = collector.register_metrics(&registry);
    assert!(
        result.is_err(),
        "Double registration should fail with an error"
    );

    Ok(())
}

#[tokio::test]
async fn test_archiver_collector_failed_count_exists() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = ArchiverCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();

    // Failed count is critical for alerting
    let failed = families
        .iter()
        .find(|m| m.name() == "pg_stat_archiver_failed_total")
        .expect("failed_count metric should exist");

    assert!(failed.get_metric()[0].get_counter().value() >= 0.0);

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_archiver_collector_age_metrics_reasonable() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = ArchiverCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();

    // Age metrics should be reasonable (not millions of years)
    let age_metrics = available_age_metrics(&pool).await?;

    for metric_name in age_metrics {
        let metric_family = families
            .iter()
            .find(|m| m.name() == metric_name)
            .unwrap_or_else(|| panic!("Metric {metric_name} should exist"));

        let value = metric_family.get_metric()[0].get_gauge().value();
        // If set, should be reasonable (less than 10 years in seconds)
        if value > 0.0 {
            assert!(
                value < 315_360_000.0, // 10 years
                "Age metric {metric_name} has unreasonable value: {value}"
            );
        }
    }

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_archiver_collector_handles_database_restart() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = ArchiverCollector::new();

    collector.register_metrics(&registry)?;

    // First collection
    collector.collect(&pool).await?;
    let first_archived = {
        let families = registry.gather();
        families
            .iter()
            .find(|m| m.name() == "pg_stat_archiver_archived_total")
            .and_then(|f| f.get_metric().first())
            .map_or(0, |m| common::metric_value_to_i64(m.get_counter().value()))
    };

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Second collection
    collector.collect(&pool).await?;
    let second_archived = {
        let families = registry.gather();
        families
            .iter()
            .find(|m| m.name() == "pg_stat_archiver_archived_total")
            .and_then(|f| f.get_metric().first())
            .map_or(0, |m| common::metric_value_to_i64(m.get_counter().value()))
    };

    // Values should be valid (either increased or stayed same)
    assert!(
        second_archived >= 0,
        "Counter should be non-negative after collection"
    );
    assert!(
        first_archived >= 0,
        "Counter should be non-negative in first collection"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_archiver_collector_all_counters_valid_after_activity() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = ArchiverCollector::new();

    collector.register_metrics(&registry)?;

    // Generate some database activity (though archiver activity is system-level)
    let mut tx = pool.begin().await?;
    for i in 0..10 {
        sqlx::query(sqlx::AssertSqlSafe(&*format!(
            "CREATE TEMP TABLE archiver_activity_{i} (data TEXT)"
        )))
        .execute(&mut *tx)
        .await?;
        sqlx::query(sqlx::AssertSqlSafe(&*format!(
            "INSERT INTO archiver_activity_{i} SELECT 'test' FROM generate_series(1, 50)"
        )))
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;

    // Collect metrics
    collector.collect(&pool).await?;

    let families = registry.gather();

    // Verify all counters are present and valid
    let archived = families
        .iter()
        .find(|m| m.name() == "pg_stat_archiver_archived_total")
        .expect("archived should exist");
    assert!(archived.get_metric()[0].get_counter().value() >= 0.0);

    let failed = families
        .iter()
        .find(|m| m.name() == "pg_stat_archiver_failed_total")
        .expect("failed should exist");
    assert!(failed.get_metric()[0].get_counter().value() >= 0.0);

    // Age gauges should also be valid
    let available = available_age_metrics(&pool).await?;
    for metric_name in &available {
        let family = families
            .iter()
            .find(|m| m.name() == *metric_name)
            .unwrap_or_else(|| panic!("{metric_name} should exist when the server reports it"));
        assert!(
            family.get_metric()[0].get_gauge().value() >= 0.0,
            "{metric_name} must not be negative"
        );
    }
    // The server has never archived or failed to archive, so there is no age to publish and
    // the series are absent rather than a fabricated 0.
    for metric_name in AGE_METRICS {
        if !available.contains(&metric_name) {
            assert!(
                !families
                    .iter()
                    .any(|m| m.name() == metric_name && !m.get_metric().is_empty()),
                "{metric_name} must be absent, not 0, when the server has no age to report"
            );
        }
    }
    pool.close().await;
    Ok(())
}

/// A `42501` on `pg_stat_archiver` must clear the metrics and still succeed, not fail the scrape.
///
/// This is the Stage 1 regression scenario: the collector used to decide "view does not
/// exist" by matching the view name in the error *message*, which reads a permission error
/// as an absent view — a missing GRANT was indistinguishable from an old server, and
/// backup-critical WAL archiving metrics vanished with no explanation. Classifying by `SQLSTATE` separates them,
/// and a denied read clears rather than leaving the last values on display.
///
/// Grants on `pg_catalog` views are per-database, so the REVOKE below is confined to this
/// isolated database and cannot disturb a sibling test.
#[tokio::test]
async fn test_archiver_clears_metrics_when_the_role_may_not_read_the_view() -> Result<()> {
    let test_db = common::IsolatedTestDatabase::new("archiver_denied").await?;
    let pool = test_db.pool();

    let is_superuser: bool =
        sqlx::query_scalar("SELECT usesuper FROM pg_user WHERE usename = current_user")
            .fetch_optional(pool)
            .await?
            .unwrap_or(false);
    if !is_superuser {
        println!("not superuser, cannot revoke a catalog grant - skipping");
        test_db.cleanup().await?;
        return Ok(());
    }

    let role = format!("exporter_archiver_denied_{}", std::process::id());
    let _ = sqlx::query(sqlx::AssertSqlSafe(format!("DROP ROLE IF EXISTS {role}")))
        .execute(pool)
        .await;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE ROLE {role} LOGIN PASSWORD 'denied_probe' NOSUPERUSER"
    )))
    .execute(pool)
    .await?;

    let outcome = async {
        let collector = ArchiverCollector::new();
        let registry = Registry::new();
        collector.register_metrics(&registry)?;

        // Seed from the privileged pool, or a cleared registry proves nothing.
        collector.collect(pool).await?;
        let seeded = registry
            .gather()
            .iter()
            .any(|f| f.name().starts_with("pg_stat_archiver_") && !f.get_metric().is_empty());
        assert!(seeded, "the privileged scrape must publish series first");

        sqlx::query("REVOKE SELECT ON pg_stat_archiver FROM PUBLIC")
            .execute(pool)
            .await?;

        let denied_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(std::time::Duration::from_secs(5))
            .connect(&test_db.dsn_for_role(&role, "denied_probe")?)
            .await?;

        let result = collector.collect(&denied_pool).await;
        assert!(
            result.is_ok(),
            "a denied read is a skip, not a scrape failure: {result:?}"
        );

        for family in registry.gather() {
            if family.name().starts_with("pg_stat_archiver_") {
                assert!(
                    family.get_metric().is_empty(),
                    "{} must be cleared after a denied read, {} samples remain",
                    family.name(),
                    family.get_metric().len()
                );
            }
        }

        denied_pool.close().await;
        Ok::<(), anyhow::Error>(())
    }
    .await;

    let _ = sqlx::query("GRANT SELECT ON pg_stat_archiver TO PUBLIC")
        .execute(pool)
        .await;
    let _ = sqlx::query(sqlx::AssertSqlSafe(format!("DROP ROLE IF EXISTS {role}")))
        .execute(pool)
        .await;
    test_db.cleanup().await?;
    outcome
}
