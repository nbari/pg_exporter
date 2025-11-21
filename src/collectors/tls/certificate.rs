use crate::collectors::{Collector, i64_to_f64};
use anyhow::Result;
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use prometheus::{Gauge, IntGauge, Opts, Registry};
use sqlx::PgPool;
use std::fs;
use std::path::Path;
use tracing::{debug, info_span, warn};
use tracing_futures::Instrument;
use x509_parser::prelude::*;

/// Collector for SSL/TLS certificate expiration and validity
///
/// This collector reads certificate files from the filesystem to check expiration dates.
/// It requires filesystem access to the certificate files specified in `ssl_cert_file`.
///
/// **Remote Installations:** When the exporter runs on a different machine than `PostgreSQL`,
/// certificate files will not be accessible, and no certificate metrics will be exported.
/// This is expected behavior - the collector will log a debug message and continue gracefully.
#[derive(Clone)]
#[allow(clippy::struct_field_names)]
pub struct CertificateCollector {
    pg_ssl_certificate_expiry_seconds: Gauge,
    pg_ssl_certificate_valid: IntGauge,
    pg_ssl_certificate_not_before_timestamp: Gauge,
    pg_ssl_certificate_not_after_timestamp: Gauge,
}

impl CertificateCollector {
    /// Creates a new `CertificateCollector` with all required metrics.
    ///
    /// # Panics
    ///
    /// Panics if metric registration fails (e.g., duplicate metric names).
    /// This should only happen during development if metric names conflict.
    #[must_use]
    #[allow(clippy::new_without_default)]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        let pg_ssl_certificate_expiry_seconds = Gauge::with_opts(Opts::new(
            "pg_ssl_certificate_expiry_seconds",
            "Seconds until SSL/TLS certificate expires (negative if expired)",
        ))
        .expect("Failed to create pg_ssl_certificate_expiry_seconds metric");

        let pg_ssl_certificate_valid = IntGauge::with_opts(Opts::new(
            "pg_ssl_certificate_valid",
            "Whether SSL/TLS certificate is currently valid (1 = valid, 0 = invalid/expired)",
        ))
        .expect("Failed to create pg_ssl_certificate_valid metric");

        let pg_ssl_certificate_not_before_timestamp = Gauge::with_opts(Opts::new(
            "pg_ssl_certificate_not_before_timestamp",
            "Unix timestamp when SSL/TLS certificate becomes valid",
        ))
        .expect("Failed to create pg_ssl_certificate_not_before_timestamp metric");

        let pg_ssl_certificate_not_after_timestamp = Gauge::with_opts(Opts::new(
            "pg_ssl_certificate_not_after_timestamp",
            "Unix timestamp when SSL/TLS certificate expires",
        ))
        .expect("Failed to create pg_ssl_certificate_not_after_timestamp metric");

        Self {
            pg_ssl_certificate_expiry_seconds,
            pg_ssl_certificate_valid,
            pg_ssl_certificate_not_before_timestamp,
            pg_ssl_certificate_not_after_timestamp,
        }
    }

    /// Parse certificate file and extract validity information
    ///
    /// Returns Ok(()) if the file doesn't exist or can't be read (expected for remote installations)
    fn parse_certificate_file(&self, cert_path: &str) -> Result<()> {
        let path = Path::new(cert_path);
        if !path.exists() {
            debug!(
                "Certificate file not accessible: {cert_path} (this is expected when running remotely)"
            );
            return Ok(());
        }

        // Read certificate file
        let cert_data = match fs::read(cert_path) {
            Ok(data) => data,
            Err(e) => {
                debug!(
                    "Cannot read certificate file {cert_path}: {e} (this is expected when running remotely)"
                );
                return Ok(());
            }
        };

        // Try to parse as PEM first
        let der_data = if cert_data.starts_with(b"-----BEGIN") {
            // Parse PEM format
            let pem = parse_x509_pem(&cert_data)
                .map_err(|e| anyhow::anyhow!("Failed to parse PEM certificate: {e:?}"))?
                .1;
            pem.contents
        } else {
            // Assume DER format
            cert_data
        };

        // Parse the certificate
        let (_, cert) = X509Certificate::from_der(&der_data)
            .map_err(|e| anyhow::anyhow!("Failed to parse X.509 certificate: {e:?}"))?;

        // Extract validity information
        let not_before = cert.validity().not_before.timestamp();
        let not_after = cert.validity().not_after.timestamp();
        let now = Utc::now().timestamp();

        // Calculate seconds until expiry
        let seconds_until_expiry = not_after - now;

        // Check if currently valid
        let is_valid = now >= not_before && now <= not_after;

        // Set metrics
        self.pg_ssl_certificate_expiry_seconds
            .set(i64_to_f64(seconds_until_expiry));
        self.pg_ssl_certificate_not_before_timestamp
            .set(i64_to_f64(not_before));
        self.pg_ssl_certificate_not_after_timestamp
            .set(i64_to_f64(not_after));
        self.pg_ssl_certificate_valid
            .set(i64::from(is_valid));

        // Log certificate info
        debug!(
            "Certificate: not_before={}, not_after={}, valid={is_valid}, expires_in={seconds_until_expiry}s",
            DateTime::from_timestamp(not_before, 0)
                .map_or_else(|| "invalid".to_string(), |dt| dt.to_rfc3339()),
            DateTime::from_timestamp(not_after, 0)
                .map_or_else(|| "invalid".to_string(), |dt| dt.to_rfc3339()),
        );

        Ok(())
    }
}

impl Collector for CertificateCollector {
    fn name(&self) -> &'static str {
        "tls.certificate"
    }

    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.pg_ssl_certificate_expiry_seconds.clone()))?;
        registry.register(Box::new(self.pg_ssl_certificate_valid.clone()))?;
        registry.register(Box::new(
            self.pg_ssl_certificate_not_before_timestamp.clone(),
        ))?;
        registry.register(Box::new(
            self.pg_ssl_certificate_not_after_timestamp.clone(),
        ))?;
        Ok(())
    }

    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let span = info_span!(
                "db.query",
                db.system = "postgresql",
                db.operation = "SHOW",
                db.statement = "SHOW ssl_cert_file",
                otel.kind = "client"
            );

            // Query the certificate file path from PostgreSQL
            match sqlx::query_scalar::<_, String>("SHOW ssl_cert_file")
                .fetch_one(pool)
                .instrument(span)
                .await
            {
                Ok(cert_path) => {
                    if cert_path.is_empty() {
                        debug!("ssl_cert_file is not configured");
                        return Ok(());
                    }

                    // Parse the certificate file
                    if let Err(e) = self.parse_certificate_file(&cert_path) {
                        warn!("Failed to parse certificate file '{cert_path}': {e}");
                    }
                }
                Err(e) => {
                    warn!("Failed to query ssl_cert_file: {}", e);
                }
            }

            Ok(())
        })
    }

    fn enabled_by_default(&self) -> bool {
        false
    }
}
