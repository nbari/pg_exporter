use crate::collectors::Collector;
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
                'checkpoint_timeout',
                'fsync',
                'log_min_duration_statement',
                'maintenance_work_mem',
                'max_connections',
                'max_locks_per_transaction',
                'shared_buffers',
                'synchronous_commit',
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

            let mut value: i64 = match setting.parse::<i64>() {
                Ok(v) => v,
                Err(_) => match setting.as_str() {
                    "on" => 1,
                    _ => 0,
                },
            };

            // Convert memory settings to bytes based on their units
            if matches!(name.as_str(), "shared_buffers" | "maintenance_work_mem" | "work_mem" | "wal_buffers")
                && let Some(ref u) = unit
            {
                value *= match u.as_str() {
                    "8kB" => 8192,
                    "kB" => 1024,
                    "MB" => 1024 * 1024,
                    "GB" => 1024 * 1024 * 1024,
                    _ => 1,
                };
            }

            metrics.push((name, value));
        }

        Ok(metrics)
    }
}

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
        let metric_configs = vec![
            ("autovacuum", "pg_settings_autovacuum", "PostgreSQL setting: autovacuum"),
            ("autovacuum_max_workers", "pg_settings_autovacuum_max_workers", "PostgreSQL setting: autovacuum_max_workers"),
            ("autovacuum_naptime", "pg_settings_autovacuum_naptime_seconds", "PostgreSQL setting: autovacuum_naptime in seconds"),
            ("autovacuum_analyze_threshold", "pg_settings_autovacuum_analyze_threshold", "PostgreSQL setting: autovacuum_analyze_threshold"),
            ("autovacuum_vacuum_threshold", "pg_settings_autovacuum_vacuum_threshold", "PostgreSQL setting: autovacuum_vacuum_threshold"),
            ("checkpoint_timeout", "pg_settings_checkpoint_timeout_seconds", "PostgreSQL setting: checkpoint_timeout in seconds"),
            ("fsync", "pg_settings_fsync", "PostgreSQL setting: fsync"),
            ("log_min_duration_statement", "pg_settings_log_min_duration_statement_milliseconds", "PostgreSQL setting: log_min_duration_statement in milliseconds"),
            ("maintenance_work_mem", "pg_settings_maintenance_work_mem_bytes", "PostgreSQL setting: maintenance_work_mem in bytes"),
            ("max_connections", "pg_settings_max_connections", "PostgreSQL setting: max_connections"),
            ("max_locks_per_transaction", "pg_settings_max_locks_per_transaction", "PostgreSQL setting: max_locks_per_transaction"),
            ("shared_buffers", "pg_settings_shared_buffers_bytes", "PostgreSQL setting: shared_buffers in bytes"),
            ("synchronous_commit", "pg_settings_synchronous_commit", "PostgreSQL setting: synchronous_commit"),
            ("wal_buffers", "pg_settings_wal_buffers_bytes", "PostgreSQL setting: wal_buffers in bytes"),
            ("work_mem", "pg_settings_work_mem_bytes", "PostgreSQL setting: work_mem in bytes"),
        ];

        {
            let mut gauges = self.gauges.write().map_err(|e| anyhow::anyhow!("Failed to acquire write lock: {e}"))?;

            for (name, metric_name, help) in metric_configs {
                let gauge = IntGauge::with_opts(Opts::new(metric_name, help))?;
                registry.register(Box::new(gauge.clone()))?;
                gauges.insert(name.to_string(), gauge);
                debug!(metric = %metric_name, "registered settings gauge");
            }
        }

        Ok(())
    }

    #[instrument(skip(self, pool), level = "info", err, fields(collector = "settings", otel.kind = "internal"))]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            // Fetch settings (child span inside fetch_settings)
            let settings = self.fetch_settings(pool).await?;

            // Apply metrics under its own span for clarity
            let apply_span = info_span!("settings.apply_metrics", items = settings.len());
            let _g = apply_span.enter();

            let gauges = self.gauges.read().map_err(|e| anyhow::anyhow!("Failed to acquire read lock: {e}"))?;
            for (name, value) in settings {
                if let Some(gauge) = gauges.get(&name) {
                    gauge.set(value);
                    debug!(metric = %name, value, "updated settings gauge");
                }
            }

            Ok(())
        })
    }

    fn enabled_by_default(&self) -> bool {
        true
    }
}
