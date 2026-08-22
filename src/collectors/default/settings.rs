use crate::collectors::{Collected, Collector};
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{IntGauge, Opts, Registry};
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use std::sync::RwLock;
use tracing::{debug, info_span, instrument};
use tracing_futures::Instrument as _;

/// Handles selected `PostgreSQL` server settings metrics
#[derive(Clone)]
pub struct SettingsCollector {
    pub gauges: std::sync::Arc<RwLock<HashMap<String, IntGauge>>>,
}

impl Default for SettingsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl SettingsCollector {
    #[must_use]
    pub fn new() -> Self {
        Self {
            gauges: std::sync::Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn insert_gauge(&self, name: &str, gauge: IntGauge) -> Result<()> {
        self.gauges
            .write()
            .map_err(|e| anyhow::anyhow!("Failed to acquire write lock: {e}"))?
            .insert(name.to_string(), gauge);
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(
            otel.kind = "client",
            db.system = "postgresql",
            db.operation = "SELECT",
            db.statement = "SELECT name, setting, unit FROM pg_settings WHERE name IN (...)",
            db.sql.table = "pg_settings"
        )
    )]
    async fn fetch_settings(&self, pool: &PgPool) -> Result<Vec<(String, i64)>> {
        // DB query span (captures duration and errors)
        let query_span = info_span!(
            "db.query",
            otel.kind = "client",
            db.system = "postgresql",
            db.operation = "SELECT",
            db.statement = "SELECT name, setting, unit FROM pg_settings WHERE name IN (...)",
            db.sql.table = "pg_settings"
        );

        let rows = sqlx::query(
            r"
            SELECT
                name,
                setting,
                unit
            FROM pg_settings
            WHERE name IN (
                'autovacuum',
                'autovacuum_max_workers',
                'autovacuum_naptime',
                'autovacuum_analyze_threshold',
                'autovacuum_vacuum_threshold',
                'block_size',
                'checkpoint_timeout',
                'data_checksums',
                'fsync',
                'log_min_duration_statement',
                'log_temp_files',
                'maintenance_work_mem',
                'max_connections',
                'max_locks_per_transaction',
                'max_wal_size',
                'min_wal_size',
                'shared_buffers',
                'synchronous_commit',
                'temp_file_limit',
                'wal_buffers',
                'work_mem'
            )
            ORDER BY name
            ",
        )
        .fetch_all(pool)
        .instrument(query_span)
        .await?;

        // Parse/normalize the settings under a lightweight span
        let parse_span = info_span!("settings.parse_rows");
        let _g = parse_span.enter();

        let mut metrics = Vec::with_capacity(rows.len());
        for row in rows {
            let name: String = row.try_get("name")?;
            let setting: String = row.try_get("setting")?;
            let unit: Option<String> = row.try_get("unit").ok();

            let mut value: i64 = setting
                .parse::<i64>()
                .unwrap_or(i64::from(setting == "on"));

            // Convert memory settings to bytes based on their units.
            //
            // Negative values are sentinels, not sizes: `temp_file_limit = -1` means
            // unlimited and `log_temp_files = -1` means disabled. Scaling them by the
            // unit would turn `-1` into `-1024` and silently break every dashboard and
            // alert that compares against `-1`.
            if matches!(
                name.as_str(),
                "shared_buffers"
                    | "maintenance_work_mem"
                    | "work_mem"
                    | "wal_buffers"
                    | "max_wal_size"
                    | "min_wal_size"
                    | "temp_file_limit"
                    | "log_temp_files"
            ) && value >= 0
                && let Some(ref u) = unit
            {
                value = value.saturating_mul(match u.as_str() {
                    "8kB" => 8192,
                    "kB" => 1024,
                    "MB" => 1024 * 1024,
                    "GB" => 1024 * 1024 * 1024,
                    _ => 1,
                });
            }

            metrics.push((name, value));
        }

        Ok(metrics)
    }
}

/// `pg_settings` names exported by this collector, paired with their metric name
/// and help text. Kept next to the `SELECT` above: both lists must stay in sync.
const SETTINGS_METRICS: &[(&str, &str, &str)] = &[
        (
            "autovacuum",
            "pg_settings_autovacuum",
            "PostgreSQL setting: autovacuum",
        ),
        (
            "autovacuum_max_workers",
            "pg_settings_autovacuum_max_workers",
            "PostgreSQL setting: autovacuum_max_workers",
        ),
        (
            "autovacuum_naptime",
            "pg_settings_autovacuum_naptime_seconds",
            "PostgreSQL setting: autovacuum_naptime in seconds",
        ),
        (
            "autovacuum_analyze_threshold",
            "pg_settings_autovacuum_analyze_threshold",
            "PostgreSQL setting: autovacuum_analyze_threshold",
        ),
        (
            "autovacuum_vacuum_threshold",
            "pg_settings_autovacuum_vacuum_threshold",
            "PostgreSQL setting: autovacuum_vacuum_threshold",
        ),
        (
            "block_size",
            "pg_settings_block_size_bytes",
            "PostgreSQL setting: block_size in bytes; multiply pg_stat_statements temp_blks_* by this to get bytes",
        ),
        (
            "checkpoint_timeout",
            "pg_settings_checkpoint_timeout_seconds",
            "PostgreSQL setting: checkpoint_timeout in seconds",
        ),
        ("data_checksums", "pg_settings_data_checksums", "PostgreSQL setting: data_checksums"),
        ("fsync", "pg_settings_fsync", "PostgreSQL setting: fsync"),
        (
            "log_min_duration_statement",
            "pg_settings_log_min_duration_statement_milliseconds",
            "PostgreSQL setting: log_min_duration_statement in milliseconds",
        ),
        (
            "log_temp_files",
            "pg_settings_log_temp_files_bytes",
            "PostgreSQL setting: log_temp_files in bytes; -1 disables temp-file logging, 0 logs every temporary file",
        ),
        (
            "maintenance_work_mem",
            "pg_settings_maintenance_work_mem_bytes",
            "PostgreSQL setting: maintenance_work_mem in bytes",
        ),
        (
            "max_connections",
            "pg_settings_max_connections",
            "PostgreSQL setting: max_connections",
        ),
        (
            "max_locks_per_transaction",
            "pg_settings_max_locks_per_transaction",
            "PostgreSQL setting: max_locks_per_transaction",
        ),
        (
            "max_wal_size",
            "pg_settings_max_wal_size_bytes",
            "PostgreSQL setting: max_wal_size in bytes",
        ),
        (
            "min_wal_size",
            "pg_settings_min_wal_size_bytes",
            "PostgreSQL setting: min_wal_size in bytes",
        ),
        (
            "shared_buffers",
            "pg_settings_shared_buffers_bytes",
            "PostgreSQL setting: shared_buffers in bytes",
        ),
        (
            "synchronous_commit",
            "pg_settings_synchronous_commit",
            "PostgreSQL setting: synchronous_commit",
        ),
        (
            "temp_file_limit",
            "pg_settings_temp_file_limit_bytes",
            "PostgreSQL setting: temp_file_limit in bytes; -1 means unlimited. Applies per process, so parallel workers and concurrent sessions can each consume this much. Reflects the exporter connection's effective value; database, role and session overrides may differ",
        ),
        (
            "wal_buffers",
            "pg_settings_wal_buffers_bytes",
            "PostgreSQL setting: wal_buffers in bytes",
        ),
        (
            "work_mem",
            "pg_settings_work_mem_bytes",
            "PostgreSQL setting: work_mem in bytes",
        ),
];

impl Collector for SettingsCollector {
    fn name(&self) -> &'static str {
        "settings"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "settings")
    )]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        for &(name, metric_name, help) in SETTINGS_METRICS {
            let gauge = IntGauge::with_opts(Opts::new(metric_name, help))?;
            registry.register(Box::new(gauge.clone()))?;
            self.insert_gauge(name, gauge)?;
            debug!(metric = %metric_name, "registered settings gauge");
        }

        Ok(())
    }

    #[instrument(skip(self, pool), level = "info", err, fields(collector = "settings", otel.kind = "internal"))]
    fn collect_once<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<Collected>> {
        Box::pin(async move {
            // Fetch settings (child span inside fetch_settings)
            let settings = self.fetch_settings(pool).await?;

            // Apply metrics under its own span for clarity
            let apply_span = info_span!("settings.apply_metrics", items = settings.len());
            let _g = apply_span.enter();

            let gauges = self
                .gauges
                .read()
                .map_err(|e| anyhow::anyhow!("Failed to acquire read lock: {e}"))?;
            for (name, value) in settings {
                if let Some(gauge) = gauges.get(&name) {
                    gauge.set(value);
                    debug!(metric = %name, value, "updated settings gauge");
                }
            }

            Ok(Collected::Fresh)
        })
    }

    /// No-op: this collector has no skip path, so it is never settled, and its
    /// metrics are scalars that cannot be removed while registered.
    fn reset_metrics(&self) {
        // Nothing to remove.
    }

    fn enabled_by_default(&self) -> bool {
        true
    }
}
