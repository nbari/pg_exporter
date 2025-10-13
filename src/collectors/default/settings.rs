use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{IntGauge, Opts, Registry};
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use std::sync::RwLock;
use tracing::{debug, info_span, instrument};
use tracing_futures::Instrument as _;

/// Handles selected PostgreSQL server settings metrics
#[derive(Clone)]
pub struct SettingsCollector {
    pub gauges: std::sync::Arc<RwLock<HashMap<String, IntGauge>>>,
}

impl SettingsCollector {
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
            db.statement = "SELECT name, setting FROM pg_settings WHERE name IN (...)",
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
            db.statement = "SELECT name, setting FROM pg_settings WHERE name IN (...)",
            db.sql.table = "pg_settings"
        );

        let rows = sqlx::query(
            r#"
            SELECT
                name,
                setting
            FROM pg_settings
            WHERE name IN (
                'autovacuum',
                'autovacuum_max_workers',
                'autovacuum_naptime',
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
            "#,
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

            let value: i64 = match setting.parse::<i64>() {
                Ok(v) => v,
                Err(_) => match setting.as_str() {
                    "on" => 1,
                    "off" => 0,
                    _ => 0,
                },
            };

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
        let metric_names = vec![
            "autovacuum",
            "autovacuum_max_workers",
            "autovacuum_naptime",
            "checkpoint_timeout",
            "fsync",
            "log_min_duration_statement",
            "maintenance_work_mem",
            "max_connections",
            "max_locks_per_transaction",
            "shared_buffers",
            "synchronous_commit",
            "wal_buffers",
            "work_mem",
        ];

        let mut gauges = self.gauges.write().unwrap();

        for name in metric_names {
            let metric_name = format!("pg_settings_{}", name);
            let gauge = IntGauge::with_opts(Opts::new(
                &metric_name,
                format!("PostgreSQL setting: {}", name),
            ))?;
            registry.register(Box::new(gauge.clone()))?;
            gauges.insert(name.to_string(), gauge);
            debug!(metric = %metric_name, "registered settings gauge");
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

            let gauges = self.gauges.read().unwrap();
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
