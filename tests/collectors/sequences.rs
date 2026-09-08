use super::common;
use anyhow::Result;
use pg_exporter::collectors::{Collector, sequences::SequencesCollector};
use prometheus::{Registry, proto::Metric};
use sqlx::Row;
use std::sync::atomic::{AtomicU64, Ordering};

static SEQUENCE_COUNTER: AtomicU64 = AtomicU64::new(1);

// Sequence fixtures live in per-test databases because other collector tests create and
// drop `SERIAL`-backed tables in the shared `postgres` database. `pg_sequences` resolves a
// sequence's value after its catalog scan; concurrent implicit-sequence DDL can therefore
// make that database task fail with a vanished OID. Keeping the assertion target in a
// database owned for the whole test makes the result independent of unrelated parallel DDL.

fn next_sequence_name(prefix: &str) -> String {
    let counter = SEQUENCE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        "pg_exporter_sequences_{prefix}_{}_{}",
        std::process::id(),
        counter
    )
}

fn quoted_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn qualified_sequence_name(sequence_name: &str) -> String {
    format!("public.{}", quoted_identifier(sequence_name))
}

async fn create_sequence(pool: &sqlx::PgPool, sequence_name: &str, max_value: i64) -> Result<()> {
    let qualified = qualified_sequence_name(sequence_name);
    sqlx::query(sqlx::AssertSqlSafe(&*format!(
        "CREATE SEQUENCE {qualified} AS bigint MINVALUE 1 MAXVALUE {max_value} START WITH 1 CACHE 1"
    )))
    .execute(pool)
    .await?;
    Ok(())
}

async fn advance_sequence(pool: &sqlx::PgPool, sequence_name: &str) -> Result<i64> {
    let qualified = qualified_sequence_name(sequence_name);
    let row = sqlx::query("SELECT nextval($1::regclass)::bigint AS value")
        .bind(&qualified)
        .fetch_one(pool)
        .await?;
    Ok(row.try_get("value").unwrap_or(0))
}

fn metric_has_label(metric: &Metric, name: &str, value: &str) -> bool {
    metric
        .get_label()
        .iter()
        .any(|label| label.name() == name && label.value() == value)
}

fn sequence_metric_value(
    registry: &Registry,
    database_name: &str,
    sequence_name: &str,
) -> Option<f64> {
    for family in registry.gather() {
        if family.name() != "pg_sequence_used_ratio" {
            continue;
        }

        for metric in family.get_metric() {
            if metric_has_label(metric, "sequencename", sequence_name)
                && metric_has_label(metric, "schemaname", "public")
                && metric_has_label(metric, "datname", database_name)
            {
                return Some(metric.get_gauge().value());
            }
        }
    }

    None
}

#[tokio::test]
async fn test_sequences_registers_without_error() -> Result<()> {
    let registry = Registry::new();
    SequencesCollector::new().register_metrics(&registry)?;
    Ok(())
}

#[tokio::test]
async fn test_sequences_name_and_default_disabled() {
    let collector = SequencesCollector::new();
    assert_eq!(collector.name(), "sequences");
    assert!(
        !collector.enabled_by_default(),
        "sequences must stay opt-in because it fans out across databases"
    );
}

#[tokio::test]
async fn test_sequences_collect_returns_ok_without_panicking() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let test_db = common::IsolatedTestDatabase::new("sequences_empty").await?;
    let registry = Registry::new();
    let collector = SequencesCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    test_db.cleanup().await?;
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_sequences_low_min_ratio_exports_advanced_sequence() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let test_db = common::IsolatedTestDatabase::new("sequences_advanced").await?;
    let sequence_name = next_sequence_name("advanced");
    create_sequence(test_db.pool(), &sequence_name, 10).await?;
    advance_sequence(test_db.pool(), &sequence_name).await?;

    let registry = Registry::new();
    let collector = SequencesCollector::with_min_ratio(0.0);
    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let value = sequence_metric_value(&registry, test_db.database_name(), &sequence_name);
    assert!(
        value.is_some(),
        "pg_sequence_used_ratio should include the advanced test sequence"
    );

    test_db.cleanup().await?;
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_sequences_default_threshold_suppresses_fresh_low_usage_sequence() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let test_db = common::IsolatedTestDatabase::new("sequences_fresh").await?;
    let sequence_name = next_sequence_name("fresh");
    let sentinel_name = next_sequence_name("fresh_sentinel");
    create_sequence(test_db.pool(), &sequence_name, 1_000_000).await?;
    create_sequence(test_db.pool(), &sentinel_name, 4).await?;
    advance_sequence(test_db.pool(), &sentinel_name).await?;
    advance_sequence(test_db.pool(), &sentinel_name).await?;

    let registry = Registry::new();
    let collector = SequencesCollector::new();
    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    assert!(
        sequence_metric_value(&registry, test_db.database_name(), &sequence_name).is_none(),
        "fresh low-usage sequence should be filtered by the default 0.5 threshold"
    );
    assert_eq!(
        sequence_metric_value(&registry, test_db.database_name(), &sentinel_name),
        Some(0.5),
        "sentinel sequence must prove the isolated database was sampled successfully"
    );

    test_db.cleanup().await?;
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_sequences_used_ratio_type_conversion_is_finite() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let test_db = common::IsolatedTestDatabase::new("sequences_ratio").await?;
    let sequence_name = next_sequence_name("ratio");
    create_sequence(test_db.pool(), &sequence_name, 4).await?;
    advance_sequence(test_db.pool(), &sequence_name).await?;
    advance_sequence(test_db.pool(), &sequence_name).await?;

    let registry = Registry::new();
    let collector = SequencesCollector::with_min_ratio(0.0);
    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let value =
        sequence_metric_value(&registry, test_db.database_name(), &sequence_name).unwrap_or(-1.0);
    assert!(
        value.is_finite() && (value - 0.5).abs() < 0.000_001,
        "expected pg_sequence_used_ratio to convert bigint values to 0.5, got {value}"
    );

    test_db.cleanup().await?;
    pool.close().await;
    Ok(())
}
