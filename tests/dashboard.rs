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
];

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
    let all: Vec<String> = COLLECTOR_NAMES.iter().map(|n| (*n).to_string()).collect();
    let exported = exposed_metric_names(&scrape_with(&all).await?);
    let conditional: BTreeSet<&str> = CONDITIONAL_METRICS.iter().map(|(m, _)| *m).collect();

    let missing: Vec<String> = dashboard_metric_names()?
        .into_iter()
        .filter(|name| !exported.contains(name) && !conditional.contains(name.as_str()))
        .collect();

    assert!(
        missing.is_empty(),
        "dashboard queries reference metrics that a full scrape does not export: {missing:#?}\n\
         If the metric genuinely needs conditions this test cannot create, add it to \
         CONDITIONAL_METRICS with a reason."
    );
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
