use crate::collectors::Collector;
use anyhow::{Result, anyhow};
use futures::future::BoxFuture;
use prometheus::{IntGaugeVec, Opts, Registry};
use regex::Regex;
use sqlx::PgPool;
use tracing::{debug, error, info_span, instrument};
use tracing_futures::Instrument as _;

/// Handles `PostgreSQL` version metrics
#[derive(Clone)]
pub struct VersionCollector {
    pub pg_version_info: IntGaugeVec,
    pub pg_settings_server_version_num: IntGaugeVec,
    version_regex: Regex,
    server_version_regex: Regex,
}

impl Default for VersionCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl VersionCollector {
    /// Creates a new `VersionCollector`
    ///
    /// # Panics
    ///
    /// Panics if metric creation fails (should never happen with valid metric names)
    #[must_use]
    #[allow(clippy::expect_used)]
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

        let version_regex =
            Regex::new(r"^\w+ ((\d+)(\.\d+)?(\.\d+)?)").expect("valid version regex");
        let server_version_regex =
            Regex::new(r"^((\d+)(\.\d+)?(\.\d+)?)").expect("valid server version regex");

        Self {
            pg_version_info,
            pg_settings_server_version_num,
            version_regex,
            server_version_regex,
        }
    }

    #[instrument(skip(self, pool), level = "info", err, fields(db.system = "postgresql", otel.kind = "client"))]
    async fn get_server_info(&self, pool: &PgPool) -> Result<String> {
        if let Ok(server_label) = std::env::var("PG_EXPORTER_SERVER_LABEL") {
            return Ok(server_label);
        }

        // First attempt: SELECT host/port/db
        let span = info_span!(
            "db.query",
            db.operation = "SELECT",
            db.statement = "SELECT inet_server_addr(), inet_server_port(), current_database()"
        );
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
        .instrument(span)
        .await;

        match server_info {
            Ok((host, port, database)) => {
                let host = host.unwrap_or_else(|| "localhost".to_string());
                let port = port.unwrap_or(5432);
                Ok(format!("{host}:{port}:{database}"))
            }
            Err(e) => {
                debug!(error = %e, "failed to fetch inet_server_*; falling back to current_database()");
                // Fallback: SELECT current_database()
                let span = info_span!(
                    "db.query",
                    db.operation = "SELECT",
                    db.statement = "SELECT current_database()"
                );
                match sqlx::query_scalar::<_, String>("SELECT current_database()")
                    .fetch_one(pool)
                    .instrument(span)
                    .await
                {
                    Ok(database) => Ok(format!("localhost:5432:{database}")),
                    Err(e2) => {
                        debug!(error = %e2, "failed to fetch current_database(); returning 'unknown'");
                        Ok("unknown".to_string())
                    }
                }
            }
        }
    }

    #[instrument(skip(self, pool), level = "info", err, fields(db.system = "postgresql", otel.kind = "client"))]
    async fn query_version(&self, pool: &PgPool) -> Result<String> {
        // Try: SELECT version()
        let span = info_span!(
            "db.query",
            db.operation = "SELECT",
            db.statement = "SELECT version()"
        );
        if let Ok(version_str) = sqlx::query_scalar::<_, String>("SELECT version()")
            .fetch_one(pool)
            .instrument(span)
            .await
            && let Some(captures) = self.version_regex.captures(&version_str)
            && let Some(version_match) = captures.get(1)
        {
            return Ok(Self::normalize_version(version_match.as_str()));
        }

        // Fallback: SHOW server_version
        let span = info_span!(
            "db.query",
            db.operation = "SHOW",
            db.statement = "SHOW server_version"
        );
        let server_version = sqlx::query_scalar::<_, String>("SHOW server_version")
            .fetch_one(pool)
            .instrument(span)
            .await?;

        if let Some(captures) = self.server_version_regex.captures(&server_version)
            && let Some(version_match) = captures.get(1)
        {
            return Ok(Self::normalize_version(version_match.as_str()));
        }

        Err(anyhow!("could not parse version from server response"))
    }

    fn normalize_version(version: &str) -> String {
        let parts: Vec<&str> = version.split('.').collect();
        match parts.len() {
            1 => format!("{}.0.0", parts.first().copied().unwrap_or("0")),
            2 => format!("{}.{}.0", parts.first().copied().unwrap_or("0"), parts.get(1).copied().unwrap_or("0")),
            _ => version.to_string(),
        }
    }
}

impl Collector for VersionCollector {
    fn name(&self) -> &'static str {
        "version"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "version")
    )]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.pg_version_info.clone()))?;
        registry.register(Box::new(self.pg_settings_server_version_num.clone()))?;
        Ok(())
    }

    #[instrument(skip(self, pool), level = "info", err, fields(collector = "version", otel.kind = "internal"))]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            // version()
            let span = info_span!(
                "db.query",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT version()",
                otel.kind = "client"
            );
            let full_version = sqlx::query_scalar::<_, String>("SELECT version()")
                .fetch_one(pool)
                .instrument(span)
                .await?;

            // Normalized short version
            let short_version = self.query_version(pool).await?;

            // SHOW server_version_num
            let span = info_span!(
                "db.query",
                db.system = "postgresql",
                db.operation = "SHOW",
                db.statement = "SHOW server_version_num",
                otel.kind = "client"
            );
            let server_version_num_str = sqlx::query_scalar::<_, String>("SHOW server_version_num")
                .fetch_one(pool)
                .instrument(span)
                .await?;

            let server_version_num: i64 = server_version_num_str.parse().map_err(|e| {
                error!(%e, server_version_num = %server_version_num_str, "Failed to parse server_version_num");
                anyhow!(
                    "Failed to parse server_version_num '{server_version_num_str}': {e}"
                )
            })?;

            // Server label lookup
            let server_label = self.get_server_info(pool).await?;

            // Set metrics (synchronous)
            self.pg_version_info
                .with_label_values(&[&full_version, &short_version])
                .set(1);
            self.pg_settings_server_version_num
                .with_label_values(&[&server_label])
                .set(server_version_num);

            Ok(())
        })
    }

    fn enabled_by_default(&self) -> bool {
        true
    }
}
