use super::super::common;
use anyhow::{Context, Result};
use pg_exporter::collectors::Collector;
use pg_exporter::collectors::statements::pg_statements::PgStatementsCollector;
use prometheus::Registry;
use sqlx::{PgConnection, postgres::PgPoolOptions};
use std::{
    env,
    time::{Duration as StdDuration, Instant},
};
use tokio::time::{Duration, sleep};

const SELF_QUERY_PREFIX: &str = "SELECT queryid::text, d.datname,";
const BASELINE_FILTER_BENCHMARK_QUERY: &str = "
    SELECT COUNT(*)::bigint
    FROM pg_statements_filter_benchmark
    WHERE BTRIM(REGEXP_REPLACE(query, '[[:space:]]+', ' ', 'g'))
          NOT LIKE 'SELECT queryid::text, d.datname,%'
";
const PREFIX_FILTER_BENCHMARK_QUERY: &str = "
    SELECT COUNT(*)::bigint
    FROM pg_statements_filter_benchmark
    WHERE query NOT LIKE 'SELECT queryid::text, d.datname,%'
";
const BENCHMARK_RUNS: usize = 5;
const DEFAULT_BENCHMARK_ROWS: i32 = 5_000;
const DEFAULT_BENCHMARK_QUERY_BYTES: i32 = 4_096;

async fn setup_pg_statements_test_db() -> Result<Option<common::IsolatedTestDatabase>> {
    common::create_pg_statements_test_database("pg_statements").await
}

fn positive_benchmark_setting(name: &str, default: i32) -> Result<i32> {
    let value = match env::var(name) {
        Ok(value) => value,
        Err(env::VarError::NotPresent) => return Ok(default),
        Err(error) => return Err(error.into()),
    };
    let parsed = value
        .parse::<i32>()
        .with_context(|| format!("{name} must be a positive integer"))?;
    anyhow::ensure!(parsed > 0, "{name} must be a positive integer");
    Ok(parsed)
}

async fn timed_filter_count(
    connection: &mut PgConnection,
    query: &'static str,
) -> Result<(i64, StdDuration)> {
    let started = Instant::now();
    let count = sqlx::query_scalar::<_, i64>(query)
        .fetch_one(connection)
        .await?;
    Ok((count, started.elapsed()))
}

fn median_duration(samples: &mut [StdDuration]) -> StdDuration {
    samples.sort_unstable();
    samples.get(samples.len() / 2).copied().unwrap_or_default()
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

    let collector = PgStatementsCollector::with_top_n(25);
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
    let collector = PgStatementsCollector::with_top_n(50);
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
    collector.collect(pool).await?;

    let self_query_recorded = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(
            SELECT 1
            FROM pg_stat_statements
            WHERE dbid = (
                SELECT oid
                FROM pg_database
                WHERE datname = current_database()
            )
              AND query LIKE 'SELECT queryid::text, d.datname,%'
        )",
    )
    .fetch_one(pool)
    .await?;
    assert!(
        self_query_recorded,
        "expected PostgreSQL to record the collector query with its stable prefix"
    );

    collector.collect(pool).await?;

    let metric_families = registry.gather();
    let calls_family = metric_families
        .iter()
        .find(|family| family.name() == "postgres_pg_stat_statements_calls_total")
        .context("expected pg_stat_statements calls metrics after collection")?;
    let self_query_exposed = calls_family.get_metric().iter().any(|metric| {
        metric.get_label().iter().any(|label| {
            label.name() == "query_short" && label.value().starts_with(SELF_QUERY_PREFIX)
        })
    });
    assert!(
        !self_query_exposed,
        "collector must exclude its own pg_stat_statements query"
    );

    test_db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn benchmark_pg_statements_self_filter() -> Result<()> {
    let row_count =
        positive_benchmark_setting("PG_EXPORTER_STATEMENTS_BENCH_ROWS", DEFAULT_BENCHMARK_ROWS)?;
    let query_bytes = positive_benchmark_setting(
        "PG_EXPORTER_STATEMENTS_BENCH_QUERY_BYTES",
        DEFAULT_BENCHMARK_QUERY_BYTES,
    )?;
    let filler_repeats = query_bytes.saturating_add(31) / 32;

    let test_db = common::IsolatedTestDatabase::new("pg_statements_benchmark").await?;
    let mut connection = test_db.pool().acquire().await?;

    sqlx::query("SET max_parallel_workers_per_gather = 0")
        .execute(&mut *connection)
        .await?;
    sqlx::query("SET jit = off")
        .execute(&mut *connection)
        .await?;
    sqlx::query(
        "CREATE TEMP TABLE pg_statements_filter_benchmark (
            query text NOT NULL
        )",
    )
    .execute(&mut *connection)
    .await?;
    sqlx::query(
        "INSERT INTO pg_statements_filter_benchmark (query)
         SELECT CASE
             WHEN benchmark_id = 1 THEN
                 $1 || repeat('x', GREATEST($2 - length($1), 0))
             ELSE
                 'SELECT benchmark_' || benchmark_id::text || ' ' ||
                 repeat(md5(benchmark_id::text), $3)
             END
         FROM generate_series(1, $4) AS benchmark_id",
    )
    .bind(SELF_QUERY_PREFIX)
    .bind(query_bytes)
    .bind(filler_repeats)
    .bind(row_count)
    .execute(&mut *connection)
    .await?;
    sqlx::query("ANALYZE pg_statements_filter_benchmark")
        .execute(&mut *connection)
        .await?;

    let (baseline_warmup_count, _) =
        timed_filter_count(&mut connection, BASELINE_FILTER_BENCHMARK_QUERY).await?;
    let (prefix_warmup_count, _) =
        timed_filter_count(&mut connection, PREFIX_FILTER_BENCHMARK_QUERY).await?;
    let expected_count = i64::from(row_count) - 1;
    assert_eq!(baseline_warmup_count, expected_count);
    assert_eq!(prefix_warmup_count, expected_count);

    let mut baseline_samples = Vec::with_capacity(BENCHMARK_RUNS);
    let mut prefix_samples = Vec::with_capacity(BENCHMARK_RUNS);
    for iteration in 0..BENCHMARK_RUNS {
        let (baseline, prefix) = if iteration.is_multiple_of(2) {
            (
                timed_filter_count(&mut connection, BASELINE_FILTER_BENCHMARK_QUERY).await?,
                timed_filter_count(&mut connection, PREFIX_FILTER_BENCHMARK_QUERY).await?,
            )
        } else {
            let prefix = timed_filter_count(&mut connection, PREFIX_FILTER_BENCHMARK_QUERY).await?;
            let baseline =
                timed_filter_count(&mut connection, BASELINE_FILTER_BENCHMARK_QUERY).await?;
            (baseline, prefix)
        };

        assert_eq!(baseline.0, prefix.0);
        baseline_samples.push(baseline.1);
        prefix_samples.push(prefix.1);
    }

    let baseline_median = median_duration(&mut baseline_samples);
    let prefix_median = median_duration(&mut prefix_samples);
    let speedup = if prefix_median.is_zero() {
        f64::INFINITY
    } else {
        baseline_median.as_secs_f64() / prefix_median.as_secs_f64()
    };
    println!(
        "pg_statements self-filter benchmark: rows={row_count}, query_bytes={query_bytes}, \
         regex_median_ms={:.3}, prefix_median_ms={:.3}, speedup={speedup:.2}x",
        baseline_median.as_secs_f64() * 1_000.0,
        prefix_median.as_secs_f64() * 1_000.0,
    );

    drop(connection);
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

    let collector = PgStatementsCollector::with_top_n(25);
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

    let collector = PgStatementsCollector::with_top_n(25);
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

    let collector = PgStatementsCollector::with_top_n(25);
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

    let collector = PgStatementsCollector::with_top_n(25);
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

    let _ = sqlx::query("SELECT pg_stat_statements_reset()")
        .execute(pool)
        .await;

    // `SELECT $1 AS "` is 14 ASCII chars in normalized query text.
    // 65 ASCII chars + one Cyrillic char makes byte index 80 fall inside UTF-8.
    let identifier = format!(
        "{}сначала выбираем строки с которыми будем работать",
        "a".repeat(65)
    );
    let sql = format!("SELECT 1 AS \"{identifier}\"");
    let _ = sqlx::query(sqlx::AssertSqlSafe(&*sql)).execute(pool).await;
    let _ = sqlx::query("SELECT pg_stat_force_next_flush()")
        .execute(pool)
        .await;

    let pattern = "%сначала выбираем строки с которыми будем работать%";
    let mut query_short: Option<String> = None;
    for _ in 0..20 {
        query_short = sqlx::query_scalar::<_, String>(
            "SELECT LEFT(query, 80)
             FROM pg_stat_statements
             WHERE query LIKE $1
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

    let collector = PgStatementsCollector::with_top_n(25);
    let registry = Registry::new();
    collector.register_metrics(&registry)?;

    let result = collector.collect(pool).await;
    assert!(
        result.is_ok(),
        "Collector should handle multibyte UTF-8 query truncation without panicking: {:?}",
        result.err()
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

    let collector = PgStatementsCollector::with_top_n(25);
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

    let collector = PgStatementsCollector::with_top_n(25);
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

    let collector = PgStatementsCollector::with_top_n(25);
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
