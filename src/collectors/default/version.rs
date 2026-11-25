use crate::collectors::{Collected, Collector};
use anyhow::{Result, anyhow};
use futures::future::BoxFuture;
use prometheus::{IntGaugeVec, Opts, Registry};
use sqlx::PgPool;
use tracing::{debug, error, info_span, instrument};
use tracing_futures::Instrument as _;

/// Handles `PostgreSQL` version metrics
#[derive(Clone)]
pub struct VersionCollector {
    pub pg_version_info: IntGaugeVec,
    pub pg_settings_server_version_num: IntGaugeVec,
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

        Self {
            pg_version_info,
            pg_settings_server_version_num,
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

    /// Formats `server_version_num` as the `major.minor` version `PostgreSQL` has used
    /// since release 10, for example `180006` as `18.6`.
    ///
    /// The number is the one source every build agrees on: `version()` and
    /// `server_version` carry packaging suffixes and pre-release names (`19beta1`),
    /// which the full `version` label keeps. A pre-release reports minor `0`.
    fn short_version(server_version_num: i64) -> String {
        let major = server_version_num / 10_000;
        let minor = server_version_num % 10_000;
        format!("{major}.{minor}")
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
    fn collect_once<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<Collected>> {
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

            let short_version = Self::short_version(server_version_num);

            // Server label lookup
            let server_label = self.get_server_info(pool).await?;

            // Reset metrics to clear any stale label combinations (e.g., after version upgrade)
            self.pg_version_info.reset();
            self.pg_settings_server_version_num.reset();

            // Set metrics (synchronous)
            self.pg_version_info
                .with_label_values(&[&full_version, &short_version])
                .set(1);
            self.pg_settings_server_version_num
                .with_label_values(&[&server_label])
                .set(server_version_num);

            Ok(Collected::Fresh)
        })
    }

    /// Removes every labeled series this collector owns.
    fn reset_metrics(&self) {
        self.pg_version_info.reset();
        self.pg_settings_server_version_num.reset();
    }

    fn enabled_by_default(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::VersionCollector;

    #[test]
    fn short_version_is_major_and_minor() {
        for (server_version_num, expected) in [
            (140_000, "14.0"),
            (140_013, "14.13"),
            (160_004, "16.4"),
            (170_010, "17.10"),
            (180_000, "18.0"),
            (180_006, "18.6"),
            // 19devel, 19beta1 and 19rc1 all report 190000.
            (190_000, "19.0"),
        ] {
            assert_eq!(
                VersionCollector::short_version(server_version_num),
                expected,
                "server_version_num {server_version_num}"
            );
        }
    }

    /// Issue 42: the label was padded to three parts (`18.6.0`), a version
    /// `PostgreSQL` has not used since release 10.
    #[test]
    fn short_version_never_has_a_third_part() {
        for server_version_num in [140_000, 150_017, 180_006, 190_000, 200_001] {
            let short = VersionCollector::short_version(server_version_num);
            assert_eq!(
                short.split('.').count(),
                2,
                "{short} from server_version_num {server_version_num}"
            );
        }
    }
}
