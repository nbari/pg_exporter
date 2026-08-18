//! Contract tests between `grafana/dashboard.json` and the exporter.
//!
//! A dashboard panel is only as good as the metric it queries. A renamed metric,
//! a typo, or a collector that silently stops emitting all leave the JSON valid
//! and the panel empty, so these tests tie every panel query back to a metric the
//! exporter actually produces.
#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::panic)]
#![allow(clippy::indexing_slicing)]

use anyhow::{Context, Result};
use pg_exporter::collectors::{
    COLLECTOR_NAMES, config::CollectorConfig, registry::CollectorRegistry,
};
use regex::Regex;
use serde_json::Value;
use std::collections::BTreeSet;

mod common;

const DASHBOARD: &str = include_str!("../grafana/dashboard.json");

/// Metrics whose `PostgreSQL` source only produces rows under conditions a local
/// single-node test instance cannot create on demand. They are still required to
/// be *declared* in the source (see `dashboard_metrics_are_declared_in_source`);
/// only the live export check skips them.
///
/// Every entry needs a reason. If a metric can be observed locally, it does not
/// belong here.
const CONDITIONAL_METRICS: &[(&str, &str)] = &[
    (
        "pg_blocked_sessions",
        "requires a session blocked on a lock",
    ),
    (
        "pg_blocking_sessions",
        "requires a session blocked on a lock",
    ),
    ("pg_lock_waits", "requires a session blocked on a lock"),
    (
        "pg_longest_blocked_seconds",
        "requires a session blocked on a lock",
    ),
    (
        "pg_ssl_connections_by_cipher",
        "requires TLS client connections",
    ),
    (
        "pg_ssl_connections_by_version",
        "requires TLS client connections",
    ),
    (
        "pg_stat_activity_max_query_duration_seconds",
        "requires a query running at scrape time",
    ),
    (
        "pg_stat_progress_analyze_sample_blks_scanned",
        "requires an ANALYZE running at scrape time",
    ),
    (
        "pg_stat_progress_analyze_sample_blks_total",
        "requires an ANALYZE running at scrape time",
    ),
    (
        "pg_stat_progress_create_index_blocks_done",
        "requires a CREATE INDEX running at scrape time",
    ),
    (
        "pg_stat_progress_create_index_blocks_total",
        "requires a CREATE INDEX running at scrape time",
    ),
    (
        "pg_stat_progress_create_index_tuples_done",
        "requires a CREATE INDEX running at scrape time",
    ),
    (
        "pg_stat_progress_create_index_tuples_total",
        "requires a CREATE INDEX running at scrape time",
    ),
    (
        "pg_stat_replication_pg_wal_lsn_diff",
        "requires a connected streaming replica",
    ),
    (
        "pg_stat_replication_slots_spill_bytes_total",
        "requires an active logical replication slot",
    ),
    (
        "pg_stat_replication_slots_spill_txns_total",
        "requires an active logical replication slot",
    ),
    (
        "pg_stat_replication_slots_stream_bytes_total",
        "requires an active logical replication slot",
    ),
    (
        "pg_stat_replication_slots_stream_txns_total",
        "requires an active logical replication slot",
    ),
    (
        "pg_stat_user_tables_last_autoanalyze_seconds_ago",
        "requires autovacuum to have analyzed a user table already",
    ),
    (
        "pg_stat_user_tables_last_autovacuum_seconds_ago",
        "requires autovacuum to have vacuumed a user table already",
    ),
    (
        "pg_system_process_group_cpu_seconds_total",
        "requires the exporter to be co-located with PostgreSQL",
    ),
    (
        "pg_vacuum_heap_progress",
        "requires a VACUUM running at scrape time",
    ),
    // These four are zero-label vectors, so with no certificate configured the collector
    // publishes nothing at all rather than a zeroed snapshot. A zeroed
    // pg_ssl_certificate_valid would read as "certificate invalid", which is why absence is
    // the intended behaviour here and not a gap to be papered over.
    (
        "pg_ssl_certificate_expiry_seconds",
        "requires a TLS certificate configured on the server (ssl_cert_file)",
    ),
    (
        "pg_ssl_certificate_valid",
        "requires a TLS certificate configured on the server (ssl_cert_file)",
    ),
    (
        "pg_ssl_certificate_not_before_timestamp",
        "requires a TLS certificate configured on the server (ssl_cert_file)",
    ),
    (
        "pg_ssl_certificate_not_after_timestamp",
        "requires a TLS certificate configured on the server (ssl_cert_file)",
    ),
];

/// Metric name prefixes that only exist from a given `server_version_num` onward.
///
/// `CONDITIONAL_METRICS` is a flat list and cannot express "absent on 14/15, required on
/// 16+": putting a version-gated metric there would stop the live check from ever
/// verifying it, including on the versions that do support it. Entries here are skipped
/// only on servers below their minimum and are fully required above it, so the CI matrix
/// still exercises them on every version that has them.
const VERSION_GATED_PREFIXES: &[(&str, i32, &str)] = &[
    (
        "pg_stat_io_",
        160_000,
        "pg_stat_io was introduced in PostgreSQL 16",
    ),
    (
        "pg_stat_checkpointer_",
        170_000,
        "pg_stat_checkpointer was introduced in PostgreSQL 17",
    ),
];

/// Fixtures the live scrape needs before it can observe a metric.
///
/// Several collectors are correctly silent on an idle database: `pg_stat_user_tables`
/// has no rows without a user table, and the `sequences` collector deliberately exports
/// only sequences at or above `--sequences.min-ratio`. A scrape of a lived-in
/// development database therefore passes while a fresh CI container fails, which is
/// exactly the false green this test exists to prevent. Seeding makes the check
/// hermetic.
///
/// Only the *presence* of a series matters here, not its value, so a single table with a
/// little activity is enough to give all 22 `pg_stat_user_tables_*` families a row.
const SEED_TABLE: &str = "dashboard_contract_seed";
const SEED_SEQUENCE: &str = "dashboard_contract_seed_seq";

async fn seed_scrape_fixtures(pool: &sqlx::PgPool) -> Result<()> {
    for statement in [
        format!("DROP TABLE IF EXISTS {SEED_TABLE}"),
        format!("DROP SEQUENCE IF EXISTS {SEED_SEQUENCE}"),
        format!("CREATE TABLE {SEED_TABLE} (id integer PRIMARY KEY, payload text)"),
        format!(
            "INSERT INTO {SEED_TABLE} SELECT g, repeat('x', 64) FROM generate_series(1, 500) g"
        ),
        // An UPDATE and a DELETE give n_tup_upd / n_tup_hot_upd / n_tup_del and the dead
        // tuples that back the bloat and autovacuum-threshold ratios.
        format!("UPDATE {SEED_TABLE} SET payload = repeat('y', 64) WHERE id % 3 = 0"),
        format!("DELETE FROM {SEED_TABLE} WHERE id % 7 = 0"),
        // A primary-key lookup populates idx_scan / idx_tup_fetch and the idx_blks_*
        // counters; a sequential scan populates seq_scan / seq_tup_read.
        format!("SELECT payload FROM {SEED_TABLE} WHERE id = 42"),
        format!("SELECT count(*) FROM {SEED_TABLE}"),
        format!("ANALYZE {SEED_TABLE}"),
        // max_value is deliberately small so one setval pushes the used ratio past the
        // 0.5 default of --sequences.min-ratio.
        format!("CREATE SEQUENCE {SEED_SEQUENCE} MAXVALUE 100"),
        format!("SELECT setval('{SEED_SEQUENCE}', 75)"),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(statement))
            .execute(pool)
            .await?;
    }

    // Table statistics are accumulated per backend and flushed on a timer, so the seeded
    // activity is not necessarily visible to the very next scrape. pg_stat_force_next_flush()
    // makes it immediate, but it only exists from PostgreSQL 15 (the shared-memory stats
    // rework), so it is best-effort and the poll below is what actually guarantees
    // visibility on every supported version.
    let _ = sqlx::query("SELECT pg_stat_force_next_flush()")
        .execute(pool)
        .await;

    for _ in 0..100 {
        let visible: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_stat_user_tables WHERE relname = $1)",
        )
        .bind(SEED_TABLE)
        .fetch_one(pool)
        .await?;

        if visible {
            return Ok(());
        }

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    anyhow::bail!("seeded table {SEED_TABLE} never became visible in pg_stat_user_tables")
}

async fn drop_scrape_fixtures(pool: &sqlx::PgPool) -> Result<()> {
    // Left in place these would leak into every other test sharing this database, and a
    // stale sequence would keep pg_sequence_used_ratio green even if seeding broke.
    for statement in [
        format!("DROP TABLE IF EXISTS {SEED_TABLE}"),
        format!("DROP SEQUENCE IF EXISTS {SEED_SEQUENCE}"),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(statement))
            .execute(pool)
            .await?;
    }

    Ok(())
}

/// Panel queries reference metrics by name; label matchers are stripped first so
/// that regex label *values* are never mistaken for metric names.
fn dashboard_metric_names() -> Result<BTreeSet<String>> {
    let dashboard: Value =
        serde_json::from_str(DASHBOARD).context("grafana/dashboard.json is not valid JSON")?;
    let labels = Regex::new(r"\{[^}]*\}")?;
    let metric = Regex::new(r"\b(?:pg|postgres)_[a-z0-9_]+")?;

    let mut names = BTreeSet::new();
    let mut stack: Vec<&Value> = dashboard["panels"]
        .as_array()
        .context("dashboard has no panels array")?
        .iter()
        .collect();

    while let Some(panel) = stack.pop() {
        if let Some(nested) = panel["panels"].as_array() {
            stack.extend(nested.iter());
        }
        let Some(targets) = panel["targets"].as_array() else {
            continue;
        };
        for expr in targets.iter().filter_map(|t| t["expr"].as_str()) {
            for found in metric.find_iter(&labels.replace_all(expr, "")) {
                names.insert(found.as_str().to_owned());
            }
        }
    }

    Ok(names)
}

/// Metric names present in a rendered `/metrics` payload.
fn exposed_metric_names(payload: &str) -> BTreeSet<String> {
    payload
        .lines()
        .filter(|line| !line.starts_with('#') && !line.trim().is_empty())
        .filter_map(|line| line.split(['{', ' ']).next())
        .map(str::to_owned)
        .collect()
}

async fn scrape_with(collectors: &[String]) -> Result<String> {
    let pool = common::create_test_pool().await?;
    let config = CollectorConfig::new(25).with_enabled(collectors);
    let payload = CollectorRegistry::new(&config).collect_all(&pool).await?;
    pool.close().await;
    Ok(payload)
}

/// Scrapes with every collector enabled, having first seeded the fixtures the
/// otherwise-silent collectors need. Returns the exported names and the server version
/// so version-gated metrics can be excluded.
async fn scrape_all_with_fixtures() -> Result<(BTreeSet<String>, i32)> {
    let pool = common::create_test_pool().await?;
    let version: i32 = sqlx::query_scalar("SELECT current_setting('server_version_num')::int")
        .fetch_one(&pool)
        .await?;

    seed_scrape_fixtures(&pool).await?;

    let all: Vec<String> = COLLECTOR_NAMES.iter().map(|n| (*n).to_string()).collect();
    let config = CollectorConfig::new(25).with_enabled(&all);
    let scraped = CollectorRegistry::new(&config).collect_all(&pool).await;

    // Drop the fixtures even if the scrape failed, so a failure here cannot leave the
    // shared database polluted for every other test.
    let cleanup = drop_scrape_fixtures(&pool).await;
    pool.close().await;
    cleanup?;

    Ok((exposed_metric_names(&scraped?), version))
}

/// Cheap, no-database guard: a panel must not reference a metric name that does
/// not exist anywhere in the source. Catches typos and renames immediately.
#[test]
fn dashboard_metrics_are_declared_in_source() -> Result<()> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut sources = String::new();
    let mut stack = vec![root];
    while let Some(path) = stack.pop() {
        for entry in std::fs::read_dir(&path)? {
            let entry = entry?.path();
            if entry.is_dir() {
                stack.push(entry);
            } else if entry.extension().is_some_and(|ext| ext == "rs") {
                sources.push_str(&std::fs::read_to_string(&entry)?);
            }
        }
    }

    // `Opts::new("pg_stat_statements_calls").namespace("postgres")` declares the
    // name without its namespace, and Prometheus derives `_sum`/`_count`/`_bucket`
    // from a histogram's base name, so accept those forms too.
    let undeclared: Vec<String> = dashboard_metric_names()?
        .into_iter()
        .filter(|name| {
            let unprefixed = name.strip_prefix("postgres_").unwrap_or(name);
            let histogram_base = ["_sum", "_count", "_bucket"]
                .iter()
                .find_map(|suffix| unprefixed.strip_suffix(suffix))
                .unwrap_or(unprefixed);
            !sources.contains(name.as_str())
                && !sources.contains(unprefixed)
                && !sources.contains(histogram_base)
        })
        .collect();

    assert!(
        undeclared.is_empty(),
        "dashboard queries reference metrics that no collector declares: {undeclared:#?}"
    );
    Ok(())
}

/// The strong check: every dashboard metric must actually come out of a scrape
/// with all collectors enabled, not merely exist as a string in the source.
#[tokio::test]
async fn dashboard_metrics_are_exported_by_collectors() -> Result<()> {
    let (exported, version) = scrape_all_with_fixtures().await?;
    let conditional: BTreeSet<&str> = CONDITIONAL_METRICS.iter().map(|(m, _)| *m).collect();

    let unsupported_here = |name: &str| {
        VERSION_GATED_PREFIXES
            .iter()
            .any(|(prefix, min_version, _)| name.starts_with(prefix) && version < *min_version)
    };

    let missing: Vec<String> = dashboard_metric_names()?
        .into_iter()
        .filter(|name| {
            !exported.contains(name)
                && !conditional.contains(name.as_str())
                && !unsupported_here(name)
        })
        .collect();

    assert!(
        missing.is_empty(),
        "dashboard queries reference metrics that a full scrape does not export \
         (server_version_num {version}): {missing:#?}\n\
         If the metric needs workload state, seed it in seed_scrape_fixtures. If it only \
         exists from a newer PostgreSQL, add it to VERSION_GATED_PREFIXES. If it genuinely \
         needs conditions this test cannot create, add it to CONDITIONAL_METRICS with a reason."
    );

    // Keep VERSION_GATED_PREFIXES honest in the other direction: on a server that *does*
    // support a gated metric it must really be exported, otherwise the gate would mask a
    // broken collector on every version. Asserted from the same scrape rather than a
    // second test, because both would seed the same fixtures concurrently.
    for (prefix, min_version, reason) in VERSION_GATED_PREFIXES {
        if version < *min_version {
            println!("skipping {prefix}* on server_version_num {version}: {reason}");
            continue;
        }

        assert!(
            exported.iter().any(|name| name.starts_with(prefix)),
            "server_version_num {version} supports {prefix}* ({reason}) but the scrape \
             exported none"
        );
    }

    Ok(())
}

/// Keeps `CONDITIONAL_METRICS` honest: an entry that no panel queries any more is
/// dead weight that would hide a real regression later.
#[test]
fn conditional_metrics_are_still_referenced_by_the_dashboard() -> Result<()> {
    let referenced = dashboard_metric_names()?;
    let stale: Vec<&str> = CONDITIONAL_METRICS
        .iter()
        .map(|(metric, _)| *metric)
        .filter(|metric| !referenced.contains(*metric))
        .collect();

    assert!(
        stale.is_empty(),
        "CONDITIONAL_METRICS lists metrics no dashboard panel uses; remove them: {stale:#?}"
    );
    Ok(())
}

/// The Temp Disk Pressure row spans four collectors. Enabling only
/// `--collector.temp` leaves most of the row empty, so the row's real
/// requirements are pinned here.
#[tokio::test]
async fn temp_disk_pressure_row_metrics_are_exported() -> Result<()> {
    let required = [
        "pg_temp_files_current_bytes",
        "pg_temp_files_current_count",
        "pg_temp_files_oldest_age_seconds",
        "pg_settings_temp_file_limit_bytes",
        "pg_settings_log_temp_files_bytes",
        "pg_settings_block_size_bytes",
        "pg_stat_database_temp_bytes",
        "postgres_pg_stat_statements_temp_blks_written_total",
    ];

    let collectors: Vec<String> = ["temp", "default", "database", "statements"]
        .iter()
        .map(|n| (*n).to_string())
        .collect();
    let exported = exposed_metric_names(&scrape_with(&collectors).await?);

    let missing: Vec<&&str> = required
        .iter()
        .filter(|metric| !exported.contains(**metric))
        .collect();
    assert!(
        missing.is_empty(),
        "Temp Disk Pressure panels would render empty, missing: {missing:#?}"
    );

    // Guard the panel list itself: every metric the row queries must be covered
    // above, so a new panel cannot quietly escape this test.
    let dashboard: Value = serde_json::from_str(DASHBOARD)?;
    let row = dashboard["panels"]
        .as_array()
        .context("dashboard has no panels array")?
        .iter()
        .find(|panel| {
            panel["title"]
                .as_str()
                .is_some_and(|title| title.starts_with("Temp Disk Pressure"))
        })
        .context("Temp Disk Pressure row is missing from the dashboard")?;

    let labels = Regex::new(r"\{[^}]*\}")?;
    let metric = Regex::new(r"\b(?:pg|postgres)_[a-z0-9_]+")?;
    for panel in row["panels"].as_array().unwrap_or(&Vec::new()) {
        for expr in panel["targets"]
            .as_array()
            .unwrap_or(&Vec::new())
            .iter()
            .filter_map(|t| t["expr"].as_str())
        {
            for found in metric.find_iter(&labels.replace_all(expr, "")) {
                assert!(
                    required.contains(&found.as_str()),
                    "panel {:?} queries {} which this test does not cover; add it to `required`",
                    panel["title"].as_str().unwrap_or_default(),
                    found.as_str()
                );
            }
        }
    }

    Ok(())
}

/// `temp_file_limit` and `log_temp_files` use `-1` as a sentinel ("unlimited" /
/// "disabled"). Scaling those kB settings to bytes must not turn `-1` into
/// `-1024`, which would break every dashboard threshold comparison.
#[tokio::test]
async fn temp_safeguard_settings_keep_their_sentinels() -> Result<()> {
    let payload = scrape_with(&["default".to_string()]).await?;

    for metric in [
        "pg_settings_temp_file_limit_bytes",
        "pg_settings_log_temp_files_bytes",
    ] {
        let line = payload
            .lines()
            .find(|line| line.starts_with(metric) && !line.starts_with('#'))
            .with_context(|| format!("{metric} was not exported"))?;
        let value: f64 = line
            .rsplit(' ')
            .next()
            .context("metric line has no value")?
            .parse()?;

        assert!(
            (value + 1.0).abs() < f64::EPSILON || value >= 0.0,
            "{metric} = {value}; the -1 sentinel must survive the kB to bytes conversion"
        );
    }

    Ok(())
}
