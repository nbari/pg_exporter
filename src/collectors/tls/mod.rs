pub mod certificate;
pub mod connection_stats;
pub mod server_config;

use crate::collectors::Collector;
use anyhow::Result;
use certificate::CertificateCollector;
use connection_stats::ConnectionTlsCollector;
use futures::{future::BoxFuture, stream::FuturesUnordered, StreamExt};
use prometheus::Registry;
use server_config::ServerTlsConfigCollector;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{info_span, warn};
use tracing_futures::Instrument;

/// Main TLS collector that orchestrates all TLS-related sub-collectors
///
/// This collector provides comprehensive SSL/TLS monitoring for `PostgreSQL`:
/// - Server SSL/TLS configuration (works remotely)
/// - Certificate expiration and validity monitoring (requires local access to cert files)
/// - Per-connection SSL/TLS statistics (`PostgreSQL` 9.5+, works remotely)
///
/// **Note:** The certificate collector requires filesystem access to the certificate files.
/// When running the exporter remotely, certificate metrics will not be available unless
/// the certificate files are accessible via a mounted filesystem.
///
/// This collector is disabled by default and must be explicitly enabled with `--collector.tls`
#[derive(Clone, Default)]
pub struct TlsCollector {
    subs: Vec<Arc<dyn Collector + Send + Sync>>,
}

impl TlsCollector {
    #[must_use]
    pub fn new() -> Self {
        Self {
            subs: vec![
                Arc::new(ServerTlsConfigCollector::new()),
                Arc::new(CertificateCollector::new()),
                Arc::new(ConnectionTlsCollector::new()),
            ],
        }
    }
}

impl Collector for TlsCollector {
    fn name(&self) -> &'static str {
        "tls"
    }

    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        for sub in &self.subs {
            let span = info_span!("collector.register_metrics", sub_collector = %sub.name());
            let _guard = span.enter();

            if let Err(e) = sub.register_metrics(registry) {
                warn!(
                    "Failed to register metrics for sub-collector '{}': {}",
                    sub.name(),
                    e
                );
                return Err(e);
            }
        }
        Ok(())
    }

    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let mut tasks = FuturesUnordered::new();

            for sub in &self.subs {
                let span = info_span!(
                    "collector.collect",
                    sub_collector = %sub.name(),
                    otel.kind = "internal"
                );
                tasks.push(sub.collect(pool).instrument(span));
            }

            while let Some(res) = tasks.next().await {
                if let Err(e) = res {
                    warn!("Sub-collector failed: {}", e);
                    return Err(e);
                }
            }

            Ok(())
        })
    }

    fn enabled_by_default(&self) -> bool {
        false
    }
}
