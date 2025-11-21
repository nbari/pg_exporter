use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{IntGauge, Opts, Registry};
use sqlx::PgPool;
use tracing::{info_span, warn};
use tracing_futures::Instrument;

/// Collector for `PostgreSQL` SSL/TLS server configuration
#[derive(Clone)]
pub struct ServerTlsConfigCollector {
    pg_ssl_enabled: IntGauge,
}

impl ServerTlsConfigCollector {
    /// Creates a new `ServerTlsConfigCollector` with all required metrics.
    ///
    /// # Panics
    ///
    /// Panics if metric registration fails (e.g., duplicate metric names).
    /// This should only happen during development if metric names conflict.
    #[must_use]
    #[allow(clippy::new_without_default)]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        let pg_ssl_enabled = IntGauge::with_opts(Opts::new(
            "pg_ssl_enabled",
            "Whether SSL/TLS is enabled on the server (1 = enabled, 0 = disabled)",
        ))
        .expect("Failed to create pg_ssl_enabled metric");

        Self { pg_ssl_enabled }
    }
}

impl Collector for ServerTlsConfigCollector {
    fn name(&self) -> &'static str {
        "tls.server_config"
    }

    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.pg_ssl_enabled.clone()))?;
        Ok(())
    }

    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let span = info_span!(
                "db.query",
                db.system = "postgresql",
                db.operation = "SHOW",
                db.statement = "SHOW ssl",
                otel.kind = "client"
            );

            // Query SSL enabled status
            match sqlx::query_scalar::<_, String>("SHOW ssl")
                .fetch_one(pool)
                .instrument(span)
                .await
            {
                Ok(ssl_status) => {
                    let enabled =
                        i64::from(ssl_status.eq_ignore_ascii_case("on"));
                    self.pg_ssl_enabled.set(enabled);
                }
                Err(e) => {
                    warn!("Failed to query SSL status: {e}");
                    // Set to 0 (disabled) if we can't determine status
                    self.pg_ssl_enabled.set(0);
                }
            }

            Ok(())
        })
    }

    fn enabled_by_default(&self) -> bool {
        false
    }
}
