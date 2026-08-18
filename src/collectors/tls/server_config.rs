use crate::collectors::util::{QueryFailure, classify_query_error};
use crate::collectors::{Collected, Collector, NO_LABELS};
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{IntGaugeVec, Opts, Registry};
use sqlx::PgPool;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tracing::{debug, info_span, warn};
use tracing_futures::Instrument;

/// Collector for `PostgreSQL` SSL/TLS server configuration
#[derive(Clone)]
pub struct ServerTlsConfigCollector {
    pg_ssl_enabled: IntGaugeVec,
    /// Ensures the missing-privilege warning is logged at most once per process.
    denied_warned: Arc<AtomicBool>,
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
        let pg_ssl_enabled = IntGaugeVec::new(Opts::new(
            "pg_ssl_enabled",
            "Whether SSL/TLS is enabled on the server (1 = enabled, 0 = disabled)",
        ), &[])
        .expect("Failed to create pg_ssl_enabled metric");

        Self {
            pg_ssl_enabled,
            denied_warned: Arc::new(AtomicBool::new(false)),
        }
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

    fn collect_once<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<Collected>> {
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
                    self.pg_ssl_enabled.with_label_values(&NO_LABELS).set(enabled);
                }
                Err(error) => {
                    // Never publish 0 here: `pg_ssl_enabled = 0` asserts "TLS is disabled",
                    // when the truth is that the server could not be asked — a
                    // security-relevant false negative.
                    //
                    // Which of the two non-publishing outcomes is right depends on *why*.
                    // A skip clears the series, which is correct when the setting genuinely
                    // is not there to read. A fault must propagate instead, so the last good
                    // reading survives for the next scrape rather than being destroyed by a
                    // transient error.
                    match classify_query_error(&error) {
                        QueryFailure::Absent => {
                            debug!("Skipping pg_ssl_enabled (ssl setting not available): {error}");
                            return Ok(Collected::Skipped);
                        }
                        QueryFailure::Denied => {
                            if !self.denied_warned.swap(true, Ordering::Relaxed) {
                                warn!(
                                    "the exporter role may not read the ssl setting; \
                                     pg_ssl_enabled will not be published"
                                );
                            }
                            return Ok(Collected::Skipped);
                        }
                        QueryFailure::Fault => return Err(error.into()),
                    }
                }
            }

            Ok(Collected::Fresh)
        })
    }

    /// Removes `pg_ssl_enabled`.
    ///
    /// A zero here would claim TLS is disabled when the truth is that the server could
    /// not be asked. As a zero-label vector the series can be removed instead.
    fn reset_metrics(&self) {
        self.pg_ssl_enabled.reset();
    }

    fn enabled_by_default(&self) -> bool {
        false
    }
}
