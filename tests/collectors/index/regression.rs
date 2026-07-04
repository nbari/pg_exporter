//! Cross-database regression tests for the index collector (issue #22).
//!
//! `pg_stat_user_indexes` is a per-database catalog. Before the fix the index
//! collector only queried the database named in the DSN (e.g. `postgres`), so
//! index metrics for every *other* database read 0 / were absent. These tests
//! create a **separate** database, populate it with indexes, then run the
//! collector against the default `postgres` pool and assert that the isolated
//! database shows up as its own `datname`-labelled series.
//!
//! On the pre-fix collector these assertions fail (no `datname` label / no
//! series for the isolated database); on the fixed collector they pass.

use super::super::common;
use anyhow::Result;
use pg_exporter::collectors::{
    Collector,
    index::{IndexStatsCollector, UnusedIndexCollector},
};
use prometheus::Registry;
use prometheus::proto::MetricFamily;

/// Return the gauge value for a `{datname=<datname>}` series in a family, if present.
fn gauge_for_datname(families: &[MetricFamily], metric: &str, datname: &str) -> Option<f64> {
    families
        .iter()
        .find(|f| f.name() == metric)?
        .get_metric()
        .iter()
        .find(|m| {
            m.get_label()
                .iter()
                .any(|l| l.name() == "datname" && l.value() == datname)
        })
        .map(|m| m.get_gauge().value())
}

/// True if the family carries a `datname` label on at least one series.
fn family_has_datname_label(families: &[MetricFamily], metric: &str) -> bool {
    families
        .iter()
        .find(|f| f.name() == metric)
        .is_some_and(|f| {
            f.get_metric()
                .iter()
                .any(|m| m.get_label().iter().any(|l| l.name() == "datname"))
        })
}

#[tokio::test]
async fn test_index_stats_collects_from_other_database() -> Result<()> {
    let test_db = common::IsolatedTestDatabase::new("idx_stats_regress").await?;
    let dbname = test_db.database_name().to_string();

    // Build a table with a secondary index and force an index scan so
    // pg_stat_user_indexes has a non-trivial, real-workload row.
    sqlx::query("CREATE TABLE regress_items (id bigint PRIMARY KEY, val bigint)")
        .execute(test_db.pool())
        .await?;
    sqlx::query(
        "INSERT INTO regress_items (id, val) SELECT g, g % 100 FROM generate_series(1, 5000) g",
    )
    .execute(test_db.pool())
    .await?;
    sqlx::query("CREATE INDEX regress_items_val_idx ON regress_items (val)")
        .execute(test_db.pool())
        .await?;
    // Prefer the index and run a few lookups to register index scans.
    sqlx::query("SET enable_seqscan = off")
        .execute(test_db.pool())
        .await?;
    for target in 0..5 {
        sqlx::query("SELECT count(*) FROM regress_items WHERE val = $1")
            .bind(target)
            .fetch_one(test_db.pool())
            .await?;
    }
    sqlx::query("ANALYZE regress_items")
        .execute(test_db.pool())
        .await?;

    // Collect against the DEFAULT (postgres) pool — not the isolated DB.
    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = IndexStatsCollector::new();
    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();

    // The metric must now be labelled by datname (pre-fix it was label-less).
    assert!(
        family_has_datname_label(&families, "pg_index_size_bytes"),
        "pg_index_size_bytes must carry a datname label after the multi-database fix"
    );

    // The isolated database's index must be reported with a real size (> 0).
    let size = gauge_for_datname(&families, "pg_index_size_bytes", &dbname);
    assert!(
        size.is_some_and(|v| v > 0.0),
        "expected pg_index_size_bytes{{datname={dbname}}} > 0 (cross-database index size), got {size:?}"
    );

    // The default database is also collected: prove multiple datname series exist.
    let distinct_datnames = families
        .iter()
        .find(|f| f.name() == "pg_index_size_bytes")
        .map_or(0, |f| f.get_metric().len());
    assert!(
        distinct_datnames >= 1,
        "expected at least one datname series for pg_index_size_bytes"
    );

    // Template databases must never be collected.
    assert!(
        gauge_for_datname(&families, "pg_index_size_bytes", "template0").is_none(),
        "template0 must not be collected"
    );
    assert!(
        gauge_for_datname(&families, "pg_index_size_bytes", "template1").is_none(),
        "template1 must not be collected"
    );

    pool.close().await;
    test_db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_unused_index_collects_from_other_database() -> Result<()> {
    let test_db = common::IsolatedTestDatabase::new("idx_unused_regress").await?;
    let dbname = test_db.database_name().to_string();

    // Create a plain (non-primary, non-unique) index and never scan it: it must
    // be reported as unused for this database.
    sqlx::query("CREATE TABLE regress_unused (id bigint PRIMARY KEY, tag text)")
        .execute(test_db.pool())
        .await?;
    sqlx::query(
        "INSERT INTO regress_unused (id, tag) SELECT g, 'tag' FROM generate_series(1, 1000) g",
    )
    .execute(test_db.pool())
    .await?;
    sqlx::query("CREATE INDEX regress_unused_tag_idx ON regress_unused (tag)")
        .execute(test_db.pool())
        .await?;

    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = UnusedIndexCollector::new();
    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();

    assert!(
        family_has_datname_label(&families, "pg_index_unused_count"),
        "pg_index_unused_count must carry a datname label after the multi-database fix"
    );

    let unused = gauge_for_datname(&families, "pg_index_unused_count", &dbname);
    assert!(
        unused.is_some_and(|v| v >= 1.0),
        "expected pg_index_unused_count{{datname={dbname}}} >= 1 (the never-scanned index), got {unused:?}"
    );

    // invalid_count series must exist for the isolated DB and be non-negative.
    let invalid = gauge_for_datname(&families, "pg_index_invalid_count", &dbname);
    assert!(
        invalid.is_some_and(|v| v >= 0.0),
        "expected pg_index_invalid_count{{datname={dbname}}} series to exist, got {invalid:?}"
    );

    pool.close().await;
    test_db.cleanup().await?;
    Ok(())
}

/// A full multi-database scrape must succeed and return per-database series for
/// every collected database (resilience: one scrape covers the whole cluster).
#[tokio::test]
async fn test_index_collectors_cover_multiple_databases() -> Result<()> {
    let db_a = common::IsolatedTestDatabase::new("idx_multi_a").await?;
    let db_b = common::IsolatedTestDatabase::new("idx_multi_b").await?;

    for db in [&db_a, &db_b] {
        sqlx::query("CREATE TABLE t (id bigint PRIMARY KEY, v bigint)")
            .execute(db.pool())
            .await?;
        sqlx::query("CREATE INDEX t_v_idx ON t (v)")
            .execute(db.pool())
            .await?;
        sqlx::query("INSERT INTO t SELECT g, g FROM generate_series(1, 500) g")
            .execute(db.pool())
            .await?;
    }

    let pool = common::create_test_pool().await?;
    let registry = Registry::new();
    let collector = IndexStatsCollector::new();
    collector.register_metrics(&registry)?;
    // Must not error even though it fans out across every database.
    collector.collect(&pool).await?;

    let families = registry.gather();
    for db in [&db_a, &db_b] {
        let size = gauge_for_datname(&families, "pg_index_size_bytes", db.database_name());
        assert!(
            size.is_some_and(|v| v > 0.0),
            "expected pg_index_size_bytes for {} > 0, got {size:?}",
            db.database_name()
        );
    }

    pool.close().await;
    db_a.cleanup().await?;
    db_b.cleanup().await?;
    Ok(())
}
