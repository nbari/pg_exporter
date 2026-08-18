use super::super::common;
use anyhow::{Context, Result};
use pg_exporter::collectors::Collector;
use pg_exporter::collectors::statements::pg_statements::PgStatementsCollector;
use prometheus::Registry;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration as StdDuration;
use tokio::time::{Duration, sleep};

/// Marker embedded in the collector's own SQL, used to recognise its statements.
const SELF_QUERY_MARKER: &str = "pg_exporter:statements";

async fn setup_pg_statements_test_db() -> Result<Option<common::IsolatedTestDatabase>> {
    common::create_pg_statements_test_database("pg_statements").await
}

/// Counts completed query-text lookups recorded for this database.
///
/// `pg_stat_statements` records the lookup only after it finishes, so callers can use
/// this to wait for a detached task before cleaning up its isolated database.
async fn background_text_lookup_calls(pool: &sqlx::PgPool) -> Result<i64> {
    Ok(sqlx::query_scalar::<_, i64>(
        "SELECT COALESCE(SUM(calls), 0)::bigint
             FROM pg_stat_statements
             WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
               AND query LIKE 'SELECT /* ' || $1 || ' */%'",
    )
    .bind(SELF_QUERY_MARKER)
    .fetch_one(pool)
    .await?)
}

/// Waits until `PostgreSQL` has recorded the detached query-text lookup. The statement
/// only becomes visible after it finishes, so this also prevents a test database from
/// being dropped while the background task still owns a connection to it.
async fn wait_for_background_text_lookup(pool: &sqlx::PgPool) -> Result<()> {
    for _ in 0..100 {
        if background_text_lookup_calls(pool).await? > 0 {
            // Let the detached task apply its returned rows to the in-memory cache.
            sleep(Duration::from_millis(25)).await;
            return Ok(());
        }

        sleep(Duration::from_millis(25)).await;
    }

    Err(anyhow::anyhow!(
        "background pg_stat_statements query-text lookup did not finish"
    ))
}

/// Returns the first resolved `query_short` label containing `needle`, if any.
fn resolved_query_short(registry: &Registry, needle: &str) -> Option<String> {
    registry
        .gather()
        .iter()
        .find(|family| family.name() == "postgres_pg_stat_statements_calls_total")
        .and_then(|family| {
            family.get_metric().iter().find_map(|metric| {
                metric.get_label().iter().find_map(|label| {
                    (label.name() == "query_short"
                        && label.value() != "<unknown>"
                        && label.value().contains(needle))
                    .then(|| label.value().to_string())
                })
            })
        })
}

/// Scrapes until the detached lookup has published a matching `query_short`.
///
/// Polling the observable effect avoids depending on a fixed settle delay, which is the
/// kind of assumption that turns into a flake on a loaded runner.
async fn collect_until_query_short(
    collector: &PgStatementsCollector,
    pool: &sqlx::PgPool,
    registry: &Registry,
    needle: &str,
) -> Result<String> {
    for _ in 0..100 {
        collector.collect(pool).await?;
        if let Some(value) = resolved_query_short(registry, needle) {
            return Ok(value);
        }
        sleep(Duration::from_millis(25)).await;
    }

    Err(anyhow::anyhow!(
        "no query_short containing {needle:?} was ever resolved"
    ))
}

#[tokio::test]
async fn test_pg_statements_collector_registers_without_error() -> Result<()> {
    let collector = PgStatementsCollector::with_top_n(25);
    let registry = Registry::new();

    // Should not error when registering
    collector.register_metrics(&registry)?;

    Ok(())
}

#[tokio::test]
async fn test_pg_statements_collector_has_all_metrics_after_collection() -> Result<()> {
    let Some(test_db) = setup_pg_statements_test_db().await? else {
        println!("pg_stat_statements extension not installed, skipping test");
        return Ok(());
    };
    let pool = test_db.pool();

    let collector = PgStatementsCollector::with_config(25, None);
    let registry = Registry::new();

    collector.register_metrics(&registry)?;

    for _ in 0..5 {
        let _ = sqlx::query("SELECT 1").execute(pool).await;
    }

    collector.collect(pool).await?;

    let metric_families = registry.gather();

    // Should have all pg_stat_statements metrics
    let expected_metrics = vec![
        "postgres_pg_stat_statements_total_exec_time_seconds",
        "postgres_pg_stat_statements_mean_exec_time_seconds",
        "postgres_pg_stat_statements_max_exec_time_seconds",
        "postgres_pg_stat_statements_stddev_exec_time_seconds",
        "postgres_pg_stat_statements_calls_total",
        "postgres_pg_stat_statements_rows_total",
        "postgres_pg_stat_statements_shared_blks_hit_total",
        "postgres_pg_stat_statements_shared_blks_read_total",
        "postgres_pg_stat_statements_cache_hit_ratio",
    ];

    for metric_name in expected_metrics {
        let found = metric_families.iter().any(|m| m.name() == metric_name);
        assert!(
            found,
            "Metric {} should exist. Found: {:?}",
            metric_name,
            metric_families
                .iter()
                .map(prometheus::proto::MetricFamily::name)
                .collect::<Vec<_>>()
        );
    }

    test_db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_pg_statements_collector_gracefully_handles_missing_extension() -> Result<()> {
    let test_db = common::IsolatedTestDatabase::new("pg_statements_missing").await?;
    let pool = test_db.pool();

    let collector = PgStatementsCollector::with_top_n(25);
    let registry = Registry::new();

    collector.register_metrics(&registry)?;

    // Should not panic even if extension is missing
    // The collector should just log a warning and continue
    let result = collector.collect(pool).await;
    assert!(
        result.is_ok(),
        "Collector should handle missing extension gracefully"
    );

    test_db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_pg_statements_collector_with_top_n_configuration() -> Result<()> {
    let Some(test_db) = setup_pg_statements_test_db().await? else {
        println!("pg_stat_statements extension not installed, skipping test");
        return Ok(());
    };
    let pool = test_db.pool();

    // Test with custom top_n value
    let collector = PgStatementsCollector::with_config(50, None);
    let registry = Registry::new();

    collector.register_metrics(&registry)?;

    // Should not error with custom configuration
    let result = collector.collect(pool).await;
    assert!(result.is_ok());

    test_db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_pg_statements_collector_excludes_own_query() -> Result<()> {
    let Some(test_db) = setup_pg_statements_test_db().await? else {
        println!("pg_stat_statements extension not installed, skipping test");
        return Ok(());
    };
    let pool = test_db.pool();

    common::reset_pg_stat_statements_current_database(pool).await?;

    let collector = PgStatementsCollector::with_top_n(100_000);
    let registry = Registry::new();
    collector.register_metrics(&registry)?;

    // First scrape registers the collector's own statements and schedules the lookup.
    collector.collect(pool).await?;
    wait_for_background_text_lookup(pool).await?;

    // The following scrape must filter them out using the background-populated cache.
    collector.collect(pool).await?;

    assert_no_self_queryid_exposed(pool, &registry).await?;

    test_db.cleanup().await?;
    Ok(())
}

/// Collects the queryids `PostgreSQL` recorded for the exporter's own statements in the
/// current database.
async fn self_queryids(pool: &sqlx::PgPool) -> Result<Vec<i64>> {
    let ids = sqlx::query_scalar::<_, i64>(
        "SELECT queryid
         FROM pg_stat_statements
         WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
           AND queryid IS NOT NULL
           AND (query LIKE 'WITH /* ' || $1 || ' */%' OR query LIKE 'SELECT /* ' || $1 || ' */%')",
    )
    .bind(SELF_QUERY_MARKER)
    .fetch_all(pool)
    .await?;

    Ok(ids)
}

/// Asserts self-exclusion by **queryid** rather than by `query_short`.
///
/// Matching on the label value cannot detect a leak: self queries are deliberately
/// evicted from the text cache, so a leaked series would be labelled `<unknown>` and a
/// `query_short`-based assertion would pass either way.
async fn assert_no_self_queryid_exposed(pool: &sqlx::PgPool, registry: &Registry) -> Result<()> {
    let self_ids = self_queryids(pool).await?;
    assert!(
        !self_ids.is_empty(),
        "expected PostgreSQL to keep the collector's self marker in the stored query text"
    );
    let self_ids: Vec<String> = self_ids.iter().map(i64::to_string).collect();

    let metric_families = registry.gather();
    let calls_family = metric_families
        .iter()
        .find(|family| family.name() == "postgres_pg_stat_statements_calls_total")
        .context("expected pg_stat_statements calls metrics after collection")?;

    let leaked: Vec<&str> = calls_family
        .get_metric()
        .iter()
        .filter_map(|metric| {
            metric
                .get_label()
                .iter()
                .find(|label| label.name() == "queryid")
                .map(prometheus::proto::LabelPair::value)
        })
        .filter(|queryid| self_ids.iter().any(|id| id == queryid))
        .collect();

    assert!(
        leaked.is_empty(),
        "collector must exclude its own pg_stat_statements query, leaked queryids: {leaked:?}"
    );

    Ok(())
}

/// Self-exclusion must not depend on the query-text lookup.
///
/// With `--statements.query-text-refresh=0` no lookup ever runs, so the marker-based
/// learning that populates `self_queryids` never happens. On `PostgreSQL` 14+ the scrape
/// query excludes itself through `pg_stat_activity.query_id` instead, which needs no
/// query text at all. Without that, the exporter's own statement ranks into the top-N by
/// execution time on a quiet database and is exported as `query_short="<unknown>"`.
#[tokio::test]
async fn test_pg_statements_excludes_own_query_without_text_lookups() -> Result<()> {
    let Some(test_db) = setup_pg_statements_test_db().await? else {
        println!("pg_stat_statements extension not installed, skipping test");
        return Ok(());
    };
    let pool = test_db.pool();

    common::reset_pg_stat_statements_current_database(pool).await?;

    // query_text_refresh = None disables the text lookup entirely.
    let collector = PgStatementsCollector::with_config(100_000, None);
    let registry = Registry::new();
    collector.register_metrics(&registry)?;

    // The first scrape registers the collector's own statement; from the second scrape
    // on it has a non-zero total_exec_time and becomes a top-N candidate.
    collector.collect(pool).await?;
    collector.collect(pool).await?;
    collector.collect(pool).await?;

    assert_no_self_queryid_exposed(pool, &registry).await?;

    test_db.cleanup().await?;
    Ok(())
}

/// Regression test for the scrape-time temp-file spill (issue #33).
///
/// Reading `pg_stat_statements` with `showtext = true` makes `PostgreSQL` materialize
/// every row, query text included, into a `work_mem`-bounded tuplestore, so each scrape
/// wrote a multi-megabyte file into `base/pgsql_tmp`. The scrape query must therefore
/// never write temporary blocks, no matter how much query text is stored.
#[tokio::test]
async fn test_pg_statements_scrape_writes_no_temp_files() -> Result<()> {
    let Some(test_db) = setup_pg_statements_test_db().await? else {
        println!("pg_stat_statements extension not installed, skipping test");
        return Ok(());
    };
    let pool = test_db.pool();

    common::reset_pg_stat_statements_current_database(pool).await?;
    // Query texts of dropped test databases live on in the cluster-wide view; left
    // alone they grow the corpus every run until even a raised work_mem cannot hold it.
    common::prune_orphaned_pg_stat_statements(pool).await?;

    // Fill pg_stat_statements with enough distinct texts that a full materialization
    // cannot stay inside work_mem. The statements must differ in *shape*: constants,
    // comments, and aliases do not reliably change the queryid across supported
    // PostgreSQL versions. The seed helper uses real relation and attribute identities.
    let seeded = seed_distinct_query_texts(pool, 1_500, 4_096).await?;
    assert!(
        seeded >= 1_000,
        "seeding must create distinct pg_stat_statements entries, got {seeded}"
    );

    let collector = PgStatementsCollector::with_top_n(25);
    let registry = Registry::new();
    collector.register_metrics(&registry)?;
    for _ in 0..3 {
        collector.collect(pool).await?;
    }
    wait_for_background_text_lookup(pool).await?;

    // pg_stat_statements accounts temp blocks per statement, so the collector's own
    // entries answer the question in issue #33 directly and without interference from
    // other sessions. Scope it to this database: entries of dropped test databases
    // survive in the cluster-wide view and would otherwise leak in from earlier runs.
    let scrape_temp_blks = collector_temp_blks(pool, "WITH").await?;
    assert_eq!(
        scrape_temp_blks, 0,
        "the per-scrape statements query must not write temporary blocks"
    );

    // The query-text lookup is the one statement that still reads showtext = true. It is
    // rate-limited and off the scrape path, and runs with a raised work_mem so it keeps
    // the materialized texts in memory. That only holds while the corpus fits; a shared
    // development instance can carry far more text than any work_mem, and that is an
    // environment property rather than a regression.
    let text_bytes = common::pg_stat_statements_text_bytes(pool).await?;
    if text_bytes < LOOKUP_FITS_IN_MEMORY_BYTES {
        let lookup_temp_blks = collector_temp_blks(pool, "SELECT").await?;
        assert_eq!(
            lookup_temp_blks, 0,
            "the query text lookup must not spill while the {text_bytes} byte corpus \
             fits in its work_mem"
        );
    } else {
        println!(
            "skipping the lookup spill assertion: {text_bytes} bytes of query text \
             exceed what the lookup work_mem can hold"
        );
    }

    test_db.cleanup().await?;
    Ok(())
}

/// Query-text corpus below which the lookup is expected to stay in memory. Deliberately
/// well under `QUERY_TEXT_LOOKUP_WORK_MEM`, since the tuplestore also holds ~40 counter
/// columns per row on top of the text.
const LOOKUP_FITS_IN_MEMORY_BYTES: i64 = 16 * 1024 * 1024;

/// Temp blocks written by the collector's own statement starting with `prefix`
/// (`WITH` for the scrape query, `SELECT` for the query-text lookup).
async fn collector_temp_blks(pool: &sqlx::PgPool, prefix: &str) -> Result<i64> {
    let blocks = sqlx::query_scalar::<_, i64>(
        "SELECT COALESCE(SUM(temp_blks_written), 0)::bigint
         FROM pg_stat_statements
         WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
           AND query LIKE $1 || ' /* ' || $2 || ' */%'",
    )
    .bind(prefix)
    .bind(SELF_QUERY_MARKER)
    .fetch_one(pool)
    .await?;

    Ok(blocks)
}

/// Seeds `count` statements that each occupy their own `pg_stat_statements` entry.
///
/// Returns how many distinct entries were actually created, so a caller can fail loudly
/// if `PostgreSQL` normalized them together instead of silently testing nothing.
async fn seed_distinct_query_texts(
    pool: &sqlx::PgPool,
    count: usize,
    padding: usize,
) -> Result<i64> {
    const COLUMNS_PER_SEED_TABLE: usize = 500;
    const SEED_MARKER: &str = "pg_exporter:statements-spill-seed";

    let filler = "x".repeat(padding);
    for batch_start in (0..count).step_by(COLUMNS_PER_SEED_TABLE) {
        let batch = batch_start / COLUMNS_PER_SEED_TABLE;
        let batch_len = (count - batch_start).min(COLUMNS_PER_SEED_TABLE);
        let columns = (0..batch_len)
            .map(|offset| format!("seed_{offset} integer"))
            .collect::<Vec<_>>()
            .join(", ");
        let table = format!("query_text_seed_{batch}");
        let create = format!("CREATE TABLE {table} ({columns})");
        sqlx::query(sqlx::AssertSqlSafe(create))
            .execute(pool)
            .await?;

        for offset in 0..batch_len {
            // Relation OIDs and attribute numbers are part of PostgreSQL's query jumble,
            // unlike aliases. Each statement therefore has a distinct queryid on every
            // supported PostgreSQL version while a handful of empty tables keeps setup
            // substantially cheaper than creating one relation per statement.
            let statement =
                format!("SELECT seed_{offset} FROM {table} /* {SEED_MARKER} {filler} */");
            sqlx::query(sqlx::AssertSqlSafe(statement))
                .execute(pool)
                .await?;
        }
    }

    let seeded = sqlx::query_scalar::<_, i64>(
        "SELECT count(*)::bigint
         FROM pg_stat_statements
         WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
           AND query LIKE $1",
    )
    .bind(format!("%{SEED_MARKER}%"))
    .fetch_one(pool)
    .await?;

    Ok(seeded)
}

/// A statement that spills gigabytes of temporary files must be exported even when it is
/// not among the slowest ones (issue #32).
#[tokio::test]
async fn test_pg_statements_exports_top_temp_writers_outside_time_top_n() -> Result<()> {
    let Some(test_db) = setup_pg_statements_test_db().await? else {
        println!("pg_stat_statements extension not installed, skipping test");
        return Ok(());
    };
    let pool = test_db.pool();

    common::reset_pg_stat_statements_current_database(pool).await?;

    // A dedicated connection so work_mem really applies to the spilling statement:
    // SET is session-scoped and the pool hands out arbitrary connections. SET LOCAL
    // keeps it inside the transaction so the connection cannot go back to the pool with
    // a 64kB work_mem and make an unrelated collector scrape spill.
    let mut spiller = pool.begin().await?;
    sqlx::query("SET LOCAL work_mem = '64kB'")
        .execute(&mut *spiller)
        .await?;
    let _ = sqlx::query(
        "SELECT COUNT(*)::bigint
         FROM (SELECT g FROM generate_series(1, 500000) g ORDER BY g DESC) spiller",
    )
    .fetch_one(&mut *spiller)
    .await?;
    spiller.commit().await?;

    // ...and one much slower statement that will win the execution-time ranking.
    for _ in 0..40 {
        let _ = sqlx::query("SELECT pg_sleep(0.05)").execute(pool).await;
    }

    let collector = PgStatementsCollector::with_config(1, None);
    let registry = Registry::new();
    collector.register_metrics(&registry)?;
    collector.collect(pool).await?;

    let metric_families = registry.gather();
    let temp_written = metric_families
        .iter()
        .find(|family| family.name() == "postgres_pg_stat_statements_temp_blks_written_total")
        .context("expected temp_blks_written metrics after collection")?;

    let spilling_series = temp_written
        .get_metric()
        .iter()
        .filter(|metric| metric.get_gauge().value() > 0.0)
        .count();
    assert!(
        spilling_series > 0,
        "a statement writing temporary blocks must be exported even with --statements.top-n=1"
    );

    // top_n = 1 means at most one series per ranking, so at most two in total.
    assert!(
        temp_written.get_metric().len() <= 2,
        "expected at most 2 * top_n series, got {}",
        temp_written.get_metric().len()
    );

    test_db.cleanup().await?;
    Ok(())
}

/// Every `queryid` label the collector currently exports.
fn exported_queryids(registry: &Registry) -> Vec<String> {
    registry
        .gather()
        .iter()
        .find(|family| family.name() == "postgres_pg_stat_statements_calls_total")
        .map(|family| {
            family
                .get_metric()
                .iter()
                .filter_map(|metric| {
                    metric
                        .get_label()
                        .iter()
                        .find(|label| label.name() == "queryid")
                        .map(|label| label.value().to_string())
                })
                .collect()
        })
        .unwrap_or_default()
}

/// `pg_stat_statements` is keyed by `(userid, dbid, queryid, toplevel)`, so with
/// `track = all` a statement run both directly and from inside a function has two
/// entries sharing `(userid, dbid, queryid)`. The exported label set has no `toplevel`
/// component, so both would collapse onto one series and one would silently overwrite
/// the other — and each duplicate would also consume a slot of the `2 * top_n` budget.
/// The scrape query therefore restricts itself to top-level entries.
#[tokio::test]
async fn test_pg_statements_excludes_nested_statements() -> Result<()> {
    let Some(test_db) = setup_pg_statements_test_db().await? else {
        println!("pg_stat_statements extension not installed, skipping test");
        return Ok(());
    };
    let pool = test_db.pool();

    let track: String = sqlx::query_scalar("SHOW pg_stat_statements.track")
        .fetch_one(pool)
        .await?;
    if track != "all" {
        println!("pg_stat_statements.track is {track}, not all - skipping test");
        test_db.cleanup().await?;
        return Ok(());
    }

    common::reset_pg_stat_statements_current_database(pool).await?;

    // The marker comment does not change the queryid but is stored verbatim in the
    // query text, which is what lets the nested entry be found again below.
    sqlx::query(
        "CREATE FUNCTION nested_probe() RETURNS bigint LANGUAGE plpgsql AS $$
         DECLARE result bigint;
         BEGIN
             SELECT COUNT(*)::bigint /* nested-toplevel-probe */
             INTO result FROM generate_series(1, 5000) g;
             RETURN result;
         END $$",
    )
    .execute(pool)
    .await?;

    for _ in 0..5 {
        let _ = sqlx::query("SELECT nested_probe()").fetch_one(pool).await?;
    }

    // The nested entry must exist *and* have accrued execution time, otherwise the
    // `total_exec_time > 0` pre-filter would exclude it anyway and the assertion below
    // would pass without proving anything about the toplevel filter.
    let nested: Vec<i64> = sqlx::query_scalar(
        "SELECT queryid
         FROM pg_stat_statements
         WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
           AND NOT toplevel
           AND total_exec_time > 0
           AND query LIKE '%nested-toplevel-probe%'",
    )
    .fetch_all(pool)
    .await?;
    assert!(
        !nested.is_empty(),
        "expected a nested pg_stat_statements entry with accrued execution time"
    );

    let collector = PgStatementsCollector::with_config(25, None);
    let registry = Registry::new();
    collector.register_metrics(&registry)?;
    collector.collect(pool).await?;

    let exported = exported_queryids(&registry);
    assert!(
        !exported.is_empty(),
        "expected top-level statements to still be exported"
    );
    for queryid in nested {
        assert!(
            !exported.contains(&queryid.to_string()),
            "nested statement {queryid} must not be exported; it shares its label set \
             with the top-level entry and would overwrite it"
        );
    }

    sqlx::query("DROP FUNCTION nested_probe()")
        .execute(pool)
        .await?;
    test_db.cleanup().await?;
    Ok(())
}

/// With text lookups disabled the collector still exports metrics, just without SQL text.
#[tokio::test]
async fn test_pg_statements_without_query_text_lookups() -> Result<()> {
    let Some(test_db) = setup_pg_statements_test_db().await? else {
        println!("pg_stat_statements extension not installed, skipping test");
        return Ok(());
    };
    let pool = test_db.pool();

    for _ in 0..5 {
        let _ = sqlx::query("SELECT 1").execute(pool).await;
    }

    let collector = PgStatementsCollector::with_config(25, None);
    let registry = Registry::new();
    collector.register_metrics(&registry)?;
    collector.collect(pool).await?;

    let metric_families = registry.gather();
    let calls_family = metric_families
        .iter()
        .find(|family| family.name() == "postgres_pg_stat_statements_calls_total")
        .context("expected pg_stat_statements calls metrics after collection")?;
    assert!(
        !calls_family.get_metric().is_empty(),
        "metrics must still be exported without query texts"
    );

    let all_unresolved = calls_family.get_metric().iter().all(|metric| {
        metric
            .get_label()
            .iter()
            .any(|label| label.name() == "query_short" && label.value() == "<unknown>")
    });
    assert!(
        all_unresolved,
        "query_short must stay unresolved when text lookups are disabled"
    );

    test_db.cleanup().await?;
    Ok(())
}

/// Query texts are cached, so repeated scrapes resolve labels without new lookups.
#[tokio::test]
async fn test_pg_statements_resolves_query_text_from_cache() -> Result<()> {
    let Some(test_db) = setup_pg_statements_test_db().await? else {
        println!("pg_stat_statements extension not installed, skipping test");
        return Ok(());
    };
    let pool = test_db.pool();

    for _ in 0..5 {
        let _ = sqlx::query("SELECT 424242").execute(pool).await;
    }

    let collector = PgStatementsCollector::with_top_n(100);
    let registry = Registry::new();
    collector.register_metrics(&registry)?;
    collector.collect(pool).await?;

    let resolved_labels = |registry: &Registry| -> usize {
        registry
            .gather()
            .iter()
            .find(|family| family.name() == "postgres_pg_stat_statements_calls_total")
            .map_or(0, |family| {
                family
                    .get_metric()
                    .iter()
                    .filter(|metric| {
                        metric.get_label().iter().any(|label| {
                            label.name() == "query_short" && label.value() != "<unknown>"
                        })
                    })
                    .count()
            })
    };

    assert_eq!(
        resolved_labels(&registry),
        0,
        "the scrape that schedules a lookup must publish <unknown>, not wait for the text"
    );

    // The rate limit blocks a second lookup, so this can only come from the cache
    // populated by the detached task. Poll rather than assume a settle delay.
    let resolved = collect_until_query_short(&collector, pool, &registry, "").await?;
    assert!(
        !resolved.is_empty(),
        "cached query texts must survive the next scrape"
    );

    test_db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_pg_statements_preserves_last_good_snapshot_on_query_failure() -> Result<()> {
    let Some(test_db) = setup_pg_statements_test_db().await? else {
        println!("pg_stat_statements extension not installed, skipping test");
        return Ok(());
    };
    let pool = test_db.pool();

    for _ in 0..5 {
        let _ = sqlx::query("SELECT 1").execute(pool).await;
    }

    let collector = PgStatementsCollector::with_config(25, None);
    let registry = Registry::new();
    collector.register_metrics(&registry)?;

    collector.collect(pool).await?;

    let sample_count_before = registry
        .gather()
        .iter()
        .find(|family| family.name() == "postgres_pg_stat_statements_calls_total")
        .map_or(0, |family| family.get_metric().len());

    assert!(
        sample_count_before > 0,
        "expected initial statement samples"
    );

    let broken_pool = PgPoolOptions::new()
        .acquire_timeout(StdDuration::from_millis(100))
        .connect_lazy("postgresql://postgres:postgres@localhost:54321/postgres")?;

    let failed = collector.collect(&broken_pool).await;
    assert!(
        failed.is_err(),
        "expected collection against broken pool to fail"
    );

    let sample_count_after = registry
        .gather()
        .iter()
        .find(|family| family.name() == "postgres_pg_stat_statements_calls_total")
        .map_or(0, |family| family.get_metric().len());

    assert_eq!(
        sample_count_after, sample_count_before,
        "failed collection should preserve last good snapshot"
    );

    test_db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_pg_statements_collector_metrics_have_proper_labels() -> Result<()> {
    let Some(test_db) = setup_pg_statements_test_db().await? else {
        println!("pg_stat_statements extension not installed, skipping test");
        return Ok(());
    };
    let pool = test_db.pool();

    // Generate some test queries to ensure we have data
    let _ = sqlx::query("SELECT 1").execute(pool).await;
    let _ = sqlx::query("SELECT current_timestamp").execute(pool).await;

    let collector = PgStatementsCollector::with_config(25, None);
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(pool).await?;

    let metric_families = registry.gather();

    // Find a metric with labels
    let total_time_metric = metric_families
        .iter()
        .find(|m| m.name() == "postgres_pg_stat_statements_total_exec_time_seconds");

    if let Some(metric) = total_time_metric
        && !metric.get_metric().is_empty()
    {
        let labels = metric.get_metric()[0].get_label();

        // Should have expected label names
        let label_names: Vec<&str> = labels
            .iter()
            .map(prometheus::proto::LabelPair::name)
            .collect();

        assert!(
            label_names.contains(&"queryid"),
            "Should have queryid label"
        );
        assert!(
            label_names.contains(&"datname"),
            "Should have datname label"
        );
        assert!(
            label_names.contains(&"usename"),
            "Should have usename label"
        );
        assert!(
            label_names.contains(&"query_short"),
            "Should have query_short label"
        );
    }

    test_db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_pg_statements_collector_cache_hit_ratio_is_valid() -> Result<()> {
    let Some(test_db) = setup_pg_statements_test_db().await? else {
        println!("pg_stat_statements extension not installed, skipping test");
        return Ok(());
    };
    let pool = test_db.pool();

    let collector = PgStatementsCollector::with_config(25, None);
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(pool).await?;

    let metric_families = registry.gather();

    // Cache hit ratio should be between 0.0 and 1.0
    let cache_hit_ratio = metric_families
        .iter()
        .find(|m| m.name() == "postgres_pg_stat_statements_cache_hit_ratio");

    if let Some(metric) = cache_hit_ratio {
        for m in metric.get_metric() {
            let value = m.get_gauge().value();
            assert!(
                (0.0..=1.0).contains(&value),
                "Cache hit ratio should be between 0.0 and 1.0, got {value}"
            );
        }
    }

    test_db.cleanup().await?;
    Ok(())
}

/// Test that utility statements (VACUUM, ANALYZE, etc.) with NULL query text are handled properly
#[tokio::test]
async fn test_pg_statements_handles_utility_statements() -> Result<()> {
    let Some(test_db) = setup_pg_statements_test_db().await? else {
        println!("pg_stat_statements extension not installed, skipping test");
        return Ok(());
    };
    let pool = test_db.pool();

    // Generate utility statements that may have NULL query text
    let _ = sqlx::query("VACUUM").execute(pool).await;
    let _ = sqlx::query("ANALYZE").execute(pool).await;

    let collector = PgStatementsCollector::with_config(25, None);
    let registry = Registry::new();

    collector.register_metrics(&registry)?;

    // Should not panic with utility statements
    let result = collector.collect(pool).await;
    assert!(
        result.is_ok(),
        "Should handle utility statements without panicking"
    );

    test_db.cleanup().await?;
    Ok(())
}

/// Reproduces issue #15:
/// query text with multibyte UTF-8 where byte index 80 is not a char boundary.
#[tokio::test]
async fn test_pg_statements_handles_multibyte_utf8_query_boundary() -> Result<()> {
    let Some(test_db) = setup_pg_statements_test_db().await? else {
        println!("pg_stat_statements extension not installed, skipping test");
        return Ok(());
    };
    let pool = test_db.pool();

    // Scoped to this test's database on purpose: a cluster-wide reset would wipe
    // the entries other statements tests are asserting on when the suite runs in
    // parallel.
    common::reset_pg_stat_statements_current_database(pool).await?;

    // Referencing a table created in this isolated database gives the probe a unique
    // structural queryid. An output alias would collide with ordinary `SELECT 1`
    // statements because aliases do not contribute to PostgreSQL's queryid.
    sqlx::query("CREATE TABLE utf8_query_boundary_probe (value bigint)")
        .execute(pool)
        .await?;

    // `SELECT /* ` is 10 ASCII bytes. With 69 ASCII marker bytes, byte index 80
    // falls inside the first two-byte Cyrillic character.
    let marker = "a".repeat(69);
    let sql = format!(
        "SELECT /* {marker}сначала выбираем строки с которыми будем работать */ \
         count(*) FROM utf8_query_boundary_probe"
    );
    sqlx::query(sqlx::AssertSqlSafe(&*sql))
        .execute(pool)
        .await?;

    let pattern = "%сначала выбираем строки с которыми будем работать%";
    let mut query_short: Option<String> = None;
    for _ in 0..20 {
        // Scoped to this database: earlier test databases leave identical entries
        // behind, and matching one of those would let the test continue before the
        // statement it actually needs is visible.
        query_short = sqlx::query_scalar::<_, String>(
            "SELECT LEFT(query, 80)
             FROM pg_stat_statements
             WHERE query LIKE $1
               AND dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
             ORDER BY calls DESC
             LIMIT 1",
        )
        .bind(pattern)
        .fetch_optional(pool)
        .await?;

        if query_short.is_some() {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }

    let query_short = query_short
        .ok_or_else(|| anyhow::anyhow!("failed to find UTF-8 query in pg_stat_statements"))?;
    assert!(
        query_short.len() > 80,
        "LEFT(query, 80) should exceed 80 bytes with multibyte UTF-8, got {}",
        query_short.len()
    );
    assert!(
        !query_short.is_char_boundary(80),
        "expected byte index 80 to be inside a UTF-8 character"
    );

    let collector = PgStatementsCollector::with_top_n(100_000);
    let registry = Registry::new();
    collector.register_metrics(&registry)?;

    let result = collector.collect(pool).await;
    assert!(
        result.is_ok(),
        "Collector should handle multibyte UTF-8 query truncation without panicking: {:?}",
        result.err()
    );

    // Disabling text lookups here would leave every label `<unknown>`, so the collector
    // would never receive the multibyte text and this test would prove nothing.
    let label = collect_until_query_short(&collector, pool, &registry, &marker).await?;

    assert!(
        label.ends_with("..."),
        "an over-long label must be truncated, got {label:?}"
    );
    assert!(
        label.len() <= 83,
        "the label must respect the 80-byte budget plus the ellipsis, got {} bytes",
        label.len()
    );

    let body = label.strip_suffix("...").unwrap_or(&label);
    assert!(
        query_short.starts_with(body),
        "the truncated label must be a prefix of the original text: {body:?}"
    );
    assert!(
        !body.contains('\u{fffd}'),
        "truncation must land on a character boundary, not split a code point: {body:?}"
    );

    test_db.cleanup().await?;
    Ok(())
}

/// Test that the collector handles queries with various types correctly
/// This specifically tests for the NUMERIC vs BIGINT type mismatch issue
#[tokio::test]
async fn test_pg_statements_handles_numeric_types_correctly() -> Result<()> {
    let Some(test_db) = setup_pg_statements_test_db().await? else {
        println!("pg_stat_statements extension not installed, skipping test");
        return Ok(());
    };
    let pool = test_db.pool();

    // Generate diverse queries to ensure pg_stat_statements has data with various numeric types
    for _ in 0..10 {
        let _ = sqlx::query("SELECT 1").execute(pool).await;
        let _ = sqlx::query("SELECT COUNT(*) FROM pg_stat_statements")
            .execute(pool)
            .await;
        let _ = sqlx::query("SELECT * FROM pg_stat_statements WHERE queryid IS NOT NULL LIMIT 1")
            .execute(pool)
            .await;
    }

    let collector = PgStatementsCollector::with_config(25, None);
    let registry = Registry::new();

    collector.register_metrics(&registry)?;

    // Should not panic on type conversions
    let result = collector.collect(pool).await;
    assert!(
        result.is_ok(),
        "Should handle NUMERIC type conversions without panicking: {:?}",
        result.err()
    );

    // Verify metrics were actually collected
    let metric_families = registry.gather();
    let has_data = metric_families.iter().any(|m| {
        m.name().starts_with("postgres_pg_stat_statements_") && !m.get_metric().is_empty()
    });

    // It's okay if there's no data, but if there is data, it should be valid
    if has_data {
        println!("Successfully collected pg_stat_statements metrics with numeric types");
    }

    test_db.cleanup().await?;
    Ok(())
}

/// Test that all metrics handle zero/NULL values gracefully
#[tokio::test]
async fn test_pg_statements_handles_edge_case_values() -> Result<()> {
    let Some(test_db) = setup_pg_statements_test_db().await? else {
        println!("pg_stat_statements extension not installed, skipping test");
        return Ok(());
    };
    let pool = test_db.pool();

    // Generate a minimal query
    let _ = sqlx::query("SELECT 1").execute(pool).await;

    let collector = PgStatementsCollector::with_config(25, None);
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(pool).await?;

    let metric_families = registry.gather();

    // Verify all numeric metrics handle zero/small values correctly
    for family in &metric_families {
        if family.name().starts_with("postgres_pg_stat_statements_") {
            for metric in family.get_metric() {
                // Check that we don't have NaN or Inf values
                let value = metric.get_gauge().value();
                assert!(
                    value.is_finite(),
                    "Metric {} should not have NaN/Inf values, got {}",
                    family.name(),
                    value
                );
            }
        }
    }

    test_db.cleanup().await?;
    Ok(())
}

/// Test that the collector works correctly with a realistic workload
#[tokio::test]
async fn test_pg_statements_with_realistic_workload() -> Result<()> {
    let Some(test_db) = setup_pg_statements_test_db().await? else {
        println!("pg_stat_statements extension not installed, skipping test");
        return Ok(());
    };
    let pool = test_db.pool();

    // Create a test table
    let _ = sqlx::query("CREATE TEMP TABLE test_table (id SERIAL PRIMARY KEY, data TEXT)")
        .execute(pool)
        .await;

    // Generate a realistic workload with different query types
    for i in 0..20 {
        let _ = sqlx::query("INSERT INTO test_table (data) VALUES ($1)")
            .bind(format!("data_{i}"))
            .execute(pool)
            .await;
    }

    for _ in 0..30 {
        let _ = sqlx::query("SELECT * FROM test_table WHERE id > $1")
            .bind(5)
            .execute(pool)
            .await;
    }

    for _ in 0..15 {
        let _ = sqlx::query("UPDATE test_table SET data = $1 WHERE id = $2")
            .bind("updated")
            .bind(1)
            .execute(pool)
            .await;
    }

    let collector = PgStatementsCollector::with_config(25, None);
    let registry = Registry::new();

    collector.register_metrics(&registry)?;
    collector.collect(pool).await?;

    let metric_families = registry.gather();

    // Verify we collected metrics
    let calls_metric = metric_families
        .iter()
        .find(|m| m.name() == "postgres_pg_stat_statements_calls_total");

    assert!(calls_metric.is_some(), "Should have calls_total metric");

    if let Some(metric) = calls_metric {
        let total_calls: i64 = metric
            .get_metric()
            .iter()
            .map(|m| common::metric_value_to_i64(m.get_gauge().value()))
            .sum();

        assert!(
            total_calls > 0,
            "Should have recorded some calls, got {total_calls}"
        );
    }

    test_db.cleanup().await?;
    Ok(())
}
