use crate::collectors::Collector;
use anyhow::{Result, anyhow};
use prometheus::{IntGaugeVec, Opts, Registry};
use regex::Regex;
use sqlx::PgPool;

/// DefaultCollector (version-only)
///
/// Exports:
/// - `pg_version_info{version,short_version} 1` - PostgreSQL version information
/// - `pg_settings_server_version_num{server} N` - Numeric server version
#[derive(Clone)]
pub struct DefaultCollector {
    pg_version_info: IntGaugeVec,
    pg_settings_server_version_num: IntGaugeVec,
    version_regex: Regex,
    server_version_regex: Regex,
}

impl DefaultCollector {
    /// Zero-argument constructor so it matches the macro's expectation.
    pub fn new() -> Self {
        let pg_version_info = IntGaugeVec::new(
            Opts::new(
                "pg_version_info",
                "PostgreSQL version information with labels for version details.",
            ),
            &["version", "short_version"],
        )
        .expect("valid pg_version_info metric opts");

        let pg_settings_server_version_num = IntGaugeVec::new(
            Opts::new(
                "pg_settings_server_version_num",
                "Server Parameter: server_version_num",
            ),
            &["server"],
        )
        .expect("valid pg_settings_server_version_num metric opts");

        // Regex to extract version from "PostgreSQL 17.6 on x86_64-pc-linux-gnu..."
        let version_regex =
            Regex::new(r"^\w+ ((\d+)(\.\d+)?(\.\d+)?)").expect("valid version regex");

        // Regex to extract version from server_version like "17.6 (Debian 17.6-1.pgdg120+1)"
        let server_version_regex =
            Regex::new(r"^((\d+)(\.\d+)?(\.\d+)?)").expect("valid server version regex");

        Self {
            pg_version_info,
            pg_settings_server_version_num,
            version_regex,
            server_version_regex,
        }
    }

    /// Get server connection info from the database itself
    async fn get_server_info(&self, pool: &PgPool) -> Result<String> {
        // Option 1: Use environment variable if set
        if let Ok(server_label) = std::env::var("PG_EXPORTER_SERVER_LABEL") {
            return Ok(server_label);
        }

        // Option 2: Try to get connection info from PostgreSQL system functions
        // This query gets host, port, and database name
        let server_info = sqlx::query_as::<_, (Option<String>, Option<i32>, String)>(
            "SELECT
                CASE
                    WHEN inet_server_addr() IS NOT NULL THEN inet_server_addr()::text
                    ELSE 'localhost'
                END as host,
                inet_server_port() as port,
                current_database() as database",
        )
        .fetch_one(pool)
        .await;

        match server_info {
            Ok((host, port, database)) => {
                let host = host.unwrap_or_else(|| "localhost".to_string());
                let port = port.unwrap_or(5432);
                Ok(format!("{}:{}:{}", host, port, database))
            }
            Err(_) => {
                // Fallback: Try to get just the database name
                match sqlx::query_scalar::<_, String>("SELECT current_database()")
                    .fetch_one(pool)
                    .await
                {
                    Ok(database) => Ok(format!("localhost:5432:{}", database)),
                    Err(_) => Ok("unknown".to_string()),
                }
            }
        }
    }

    /// Parse PostgreSQL version string into semver format.
    /// Tries SELECT version() first, then SHOW server_version as fallback.
    async fn query_version(&self, pool: &PgPool) -> Result<String> {
        // Try to get version from SELECT version() first
        if let Ok(version_str) = sqlx::query_scalar::<_, String>("SELECT version()")
            .fetch_one(pool)
            .await
            && let Some(captures) = self.version_regex.captures(&version_str)
            && let Some(version_match) = captures.get(1)
        {
            return Ok(self.normalize_version(version_match.as_str()));
        }

        // Fallback to SHOW server_version
        let server_version = sqlx::query_scalar::<_, String>("SHOW server_version")
            .fetch_one(pool)
            .await?;

        if let Some(captures) = self.server_version_regex.captures(&server_version)
            && let Some(version_match) = captures.get(1)
        {
            return Ok(self.normalize_version(version_match.as_str()));
        }

        Err(anyhow!("could not parse version from server response"))
    }

    /// Normalize version to semver format.
    /// PostgreSQL often omits patch version, so "17.6" becomes "17.6.0"
    fn normalize_version(&self, version: &str) -> String {
        let parts: Vec<&str> = version.split('.').collect();

        match parts.len() {
            1 => format!("{}.0.0", parts[0]),            // "17" -> "17.0.0"
            2 => format!("{}.{}.0", parts[0], parts[1]), // "17.6" -> "17.6.0"
            _ => version.to_string(),                    // "17.6.1" stays as is
        }
    }
}

impl Default for DefaultCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl Collector for DefaultCollector {
    fn name(&self) -> &'static str {
        "default"
    }

    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.pg_version_info.clone()))?;
        registry.register(Box::new(self.pg_settings_server_version_num.clone()))?;
        Ok(())
    }

    async fn collect(&self, pool: &PgPool) -> Result<()> {
        // Get the full version string for the label
        let full_version = sqlx::query_scalar::<_, String>("SELECT version()")
            .fetch_one(pool)
            .await?;

        // Parse and normalize the version
        let short_version = self.query_version(pool).await?;

        // Get the numeric server version as string, then parse to i64
        let server_version_num_str = sqlx::query_scalar::<_, String>("SHOW server_version_num")
            .fetch_one(pool)
            .await?;

        let server_version_num: i64 = server_version_num_str.parse().map_err(|e| {
            anyhow!(
                "Failed to parse server_version_num '{}': {}",
                server_version_num_str,
                e
            )
        })?;

        // Get dynamic server info from the database connection
        let server_label = self.get_server_info(pool).await?;

        // Set the version info gauge
        let version_gauge = self
            .pg_version_info
            .with_label_values(&[&full_version, &short_version]);
        version_gauge.set(1);

        // Set the numeric version gauge
        let version_num_gauge = self
            .pg_settings_server_version_num
            .with_label_values(&[&server_label]);
        version_num_gauge.set(server_version_num);

        Ok(())
    }

    fn enabled_by_default(&self) -> bool {
        true
    }
}
