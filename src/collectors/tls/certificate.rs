use crate::collectors::util::{QueryFailure, classify_query_error};
use crate::collectors::{NO_LABELS, Collected, Collector, blocking, i64_to_f64};
use anyhow::Result;
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use prometheus::{GaugeVec, IntGaugeVec, Opts, Registry};
use sqlx::PgPool;
use std::fs;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
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
    pg_ssl_certificate_expiry_seconds: GaugeVec,
    pg_ssl_certificate_valid: IntGaugeVec,
    pg_ssl_certificate_not_before_timestamp: GaugeVec,
    pg_ssl_certificate_not_after_timestamp: GaugeVec,
    /// Ensures the missing-privilege warning is logged at most once per process.
    denied_warned: Arc<AtomicBool>,
    /// Coalesces certificate reads to one in flight: a started `spawn_blocking` read
    /// cannot be cancelled, so without this a hung `ssl_cert_file` (e.g. a dead network
    /// mount) would leak one blocking-pool thread per scrape, without bound. A
    /// `tokio::sync::Mutex`, not a `Semaphore`: collector semaphores are reserved for the
    /// per-database query budget (enforced in `tests/collector_safety.rs`).
    cert_read_slot: Arc<tokio::sync::Mutex<()>>,
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
        let pg_ssl_certificate_expiry_seconds = GaugeVec::new(Opts::new(
            "pg_ssl_certificate_expiry_seconds",
            "Seconds until SSL/TLS certificate expires (negative if expired)",
        ), &[])
        .expect("Failed to create pg_ssl_certificate_expiry_seconds metric");

        let pg_ssl_certificate_valid = IntGaugeVec::new(Opts::new(
            "pg_ssl_certificate_valid",
            "Whether SSL/TLS certificate is currently valid (1 = valid, 0 = invalid/expired)",
        ), &[])
        .expect("Failed to create pg_ssl_certificate_valid metric");

        let pg_ssl_certificate_not_before_timestamp = GaugeVec::new(Opts::new(
            "pg_ssl_certificate_not_before_timestamp",
            "Unix timestamp when SSL/TLS certificate becomes valid",
        ), &[])
        .expect("Failed to create pg_ssl_certificate_not_before_timestamp metric");

        let pg_ssl_certificate_not_after_timestamp = GaugeVec::new(Opts::new(
            "pg_ssl_certificate_not_after_timestamp",
            "Unix timestamp when SSL/TLS certificate expires",
        ), &[])
        .expect("Failed to create pg_ssl_certificate_not_after_timestamp metric");

        Self {
            denied_warned: Arc::new(AtomicBool::new(false)),
            cert_read_slot: Arc::new(tokio::sync::Mutex::new(())),
            pg_ssl_certificate_expiry_seconds,
            pg_ssl_certificate_valid,
            pg_ssl_certificate_not_before_timestamp,
            pg_ssl_certificate_not_after_timestamp,
        }
    }

    /// Parses the certificate file and publishes its validity information.
    ///
    /// Returns [`Collected::Skipped`] when the file is absent or unreadable, which is
    /// expected when the exporter runs remotely from the server. A **malformed**
    /// certificate is an `Err`: that means TLS is misconfigured rather than unobservable.
    fn parse_certificate_file(&self, cert_path: &str) -> Result<Collected> {
        let path = Path::new(cert_path);
        if !path.exists() {
            debug!(
                "Certificate file not accessible: {cert_path} (this is expected when running remotely)"
            );
            return Ok(Collected::Skipped);
        }

        // Read certificate file
        let cert_data = match fs::read(cert_path) {
            Ok(data) => data,
            Err(e) => {
                debug!(
                    "Cannot read certificate file {cert_path}: {e} (this is expected when running remotely)"
                );
                return Ok(Collected::Skipped);
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
            .with_label_values(&NO_LABELS).set(i64_to_f64(seconds_until_expiry));
        self.pg_ssl_certificate_not_before_timestamp
            .with_label_values(&NO_LABELS).set(i64_to_f64(not_before));
        self.pg_ssl_certificate_not_after_timestamp
            .with_label_values(&NO_LABELS).set(i64_to_f64(not_after));
        self.pg_ssl_certificate_valid
            .with_label_values(&NO_LABELS).set(i64::from(is_valid));

        // Log certificate info
        debug!(
            "Certificate: not_before={}, not_after={}, valid={is_valid}, expires_in={seconds_until_expiry}s",
            DateTime::from_timestamp(not_before, 0)
                .map_or_else(|| "invalid".to_string(), |dt| dt.to_rfc3339()),
            DateTime::from_timestamp(not_after, 0)
                .map_or_else(|| "invalid".to_string(), |dt| dt.to_rfc3339()),
        );

        Ok(Collected::Fresh)
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

    fn collect_once<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<Collected>> {
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
                        return Ok(Collected::Skipped);
                    }

                    // A malformed certificate propagates: it means TLS is misconfigured,
                    // which is worth failing loudly for, unlike a file this process simply
                    // cannot see.
                    //
                    // The read and the X.509 parse are blocking, so they go to the blocking
                    // pool rather than the runtime worker (issue #35): `ssl_cert_file` can
                    // point anywhere, including a network mount that hangs. The coalescing
                    // slot caps that case at one in-flight read — a hung read already holds
                    // a blocking thread it cannot be cancelled out of, so queueing one more
                    // per scrape would grow the pool without bound. While a read is stuck,
                    // scrapes keep the last published series.
                    let collector = self.clone();
                    let outcome = blocking::offload_coalesced(
                        "tls.certificate",
                        &self.cert_read_slot,
                        move || collector.parse_certificate_file(&cert_path),
                    )
                    .await?;
                    match outcome {
                        Some(result) => result,
                        None => Ok(Collected::Fresh),
                    }
                }
                Err(error) => {
                    // An absent or unreadable setting is a skip, which clears. Anything else
                    // is a fault and propagates, so the previous certificate reading survives
                    // for the next scrape instead of being destroyed by a transient error.
                    match classify_query_error(&error) {
                        QueryFailure::Absent => {
                            debug!("Skipping certificate metrics (ssl_cert_file absent): {error}");
                            Ok(Collected::Skipped)
                        }
                        QueryFailure::Denied => {
                            if !self.denied_warned.swap(true, Ordering::Relaxed) {
                                warn!(
                                    "the exporter role may not read ssl_cert_file; certificate \
                                     metrics will not be published"
                                );
                            }
                            Ok(Collected::Skipped)
                        }
                        QueryFailure::Fault => Err(error.into()),
                    }
                }
            }
        })
    }

    /// Removes the certificate series.
    ///
    /// These are state and threshold gauges, so zeroing them would be actively wrong: a
    /// zeroed `pg_ssl_certificate_valid` reads as "certificate invalid" and a zeroed
    /// `pg_ssl_certificate_expiry_seconds` as "expires now". As zero-label vectors they
    /// can be removed instead, so a missing certificate file reads as unknown.
    fn reset_metrics(&self) {
        self.pg_ssl_certificate_expiry_seconds.reset();
        self.pg_ssl_certificate_valid.reset();
        self.pg_ssl_certificate_not_before_timestamp.reset();
        self.pg_ssl_certificate_not_after_timestamp.reset();
    }

    fn enabled_by_default(&self) -> bool {
        false
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::{CertificateCollector, Collected, NO_LABELS};
    use std::io::Write;

    /// Seeds a snapshot so "the previous values survived" is a real assertion rather than a
    /// vacuous one.
    fn seeded() -> CertificateCollector {
        let collector = CertificateCollector::new();
        collector
            .pg_ssl_certificate_valid
            .with_label_values(&NO_LABELS)
            .set(1);
        collector
            .pg_ssl_certificate_expiry_seconds
            .with_label_values(&NO_LABELS)
            .set(86_400.0);
        assert_eq!(
            snapshot(&collector),
            (1, 86_400.0),
            "the seed must take, or the preservation assertions below are vacuous"
        );
        collector
    }

    fn snapshot(collector: &CertificateCollector) -> (i64, f64) {
        (
            collector
                .pg_ssl_certificate_valid
                .with_label_values(&NO_LABELS)
                .get(),
            collector
                .pg_ssl_certificate_expiry_seconds
                .with_label_values(&NO_LABELS)
                .get(),
        )
    }

    fn write_temp(contents: &[u8]) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().expect("temp file");
        file.write_all(contents).expect("write");
        file.flush().expect("flush");
        file
    }

    /// A malformed certificate is a **fault**, not a skip.
    ///
    /// "The file is not readable from here" means the data is unavailable, so it clears. A file
    /// that is present but unparsable means TLS is misconfigured, which is worth failing loudly
    /// for — and the error must leave the previous snapshot intact rather than clearing it,
    /// because `Collector::collect` only settles a skip.
    #[test]
    fn malformed_pem_is_an_error_and_keeps_the_previous_snapshot() {
        let collector = seeded();
        let before = snapshot(&collector);
        let file = write_temp(
            b"-----BEGIN CERTIFICATE-----\nthis is not base64 at all\n-----END CERTIFICATE-----\n",
        );

        let result = collector.parse_certificate_file(file.path().to_str().expect("path"));

        assert!(result.is_err(), "a malformed PEM must be an error, got {result:?}");
        assert_eq!(
            snapshot(&collector),
            before,
            "an error must not disturb the previous snapshot"
        );
    }

    /// Anything not starting with `-----BEGIN` is treated as DER, so garbage bytes exercise the
    /// X.509 path rather than the PEM path.
    #[test]
    fn malformed_der_is_an_error_and_keeps_the_previous_snapshot() {
        let collector = seeded();
        let before = snapshot(&collector);
        let file = write_temp(&[0x01, 0x02, 0x03, 0x04, 0xff, 0xfe]);

        let result = collector.parse_certificate_file(file.path().to_str().expect("path"));

        assert!(result.is_err(), "a malformed DER must be an error, got {result:?}");
        assert_eq!(snapshot(&collector), before);
    }

    /// The contrasting case: a file this process cannot see is unavailable data, so it is a
    /// skip. `Collector::collect` clears on that, which is why the distinction matters.
    #[test]
    fn a_missing_file_is_a_skip_not_an_error() {
        let collector = seeded();

        let outcome = collector
            .parse_certificate_file("/nonexistent/pg_exporter/definitely-not-here.crt")
            .expect("a missing file must not be an error");

        assert_eq!(outcome, Collected::Skipped);
    }

    /// An unreadable file is also unavailable rather than corrupt.
    #[cfg(unix)]
    #[test]
    fn an_unreadable_file_is_a_skip_not_an_error() {
        use std::os::unix::fs::PermissionsExt;

        let file = write_temp(b"irrelevant");
        std::fs::set_permissions(file.path(), std::fs::Permissions::from_mode(0o000))
            .expect("chmod");

        let collector = seeded();
        let outcome = collector.parse_certificate_file(file.path().to_str().expect("path"));

        // Running as root defeats the permission bits, so only assert when it actually applies.
        if std::fs::read(file.path()).is_err() {
            assert_eq!(
                outcome.expect("an unreadable file must not be an error"),
                Collected::Skipped
            );
        }
    }
}
