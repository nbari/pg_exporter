use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{IntGauge, Opts, Registry};
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use std::sync::RwLock;

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

    async fn fetch_settings(&self, pool: &PgPool) -> Result<Vec<(String, i64)>> {
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
                'shared_buffers',
                'synchronous_commit',
                'wal_buffers',
                'work_mem'
            )
            ORDER BY name
            "#,
        )
        .fetch_all(pool)
        .await?;

        let mut metrics = Vec::new();
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
        }

        Ok(())
    }

    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let settings = self.fetch_settings(pool).await?;
            let gauges = self.gauges.read().unwrap();
            for (name, value) in settings {
                if let Some(gauge) = gauges.get(&name) {
                    gauge.set(value);
                }
            }
            Ok(())
        })
    }

    fn enabled_by_default(&self) -> bool {
        true
    }
}
