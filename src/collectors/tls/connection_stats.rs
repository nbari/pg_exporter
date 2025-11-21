use crate::collectors::{util::is_pg_version_at_least, Collector, i64_to_f64};
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{Gauge, GaugeVec, Opts, Registry};
use sqlx::PgPool;
use std::collections::HashMap;
use tracing::{info_span, warn};
use tracing_futures::Instrument;

/// Collector for active `PostgreSQL` connection SSL/TLS statistics
/// Requires `PostgreSQL` 9.5+ for `pg_stat_ssl` view
#[derive(Clone)]
#[allow(clippy::struct_field_names)]
pub struct ConnectionTlsCollector {
    pg_ssl_connections_total: Gauge,
    pg_ssl_connections_by_version: GaugeVec,
    pg_ssl_connections_by_cipher: GaugeVec,
    pg_ssl_connection_bits_avg: Gauge,
}

impl ConnectionTlsCollector {
    /// Creates a new `ConnectionTlsCollector` with all required metrics.
    ///
    /// # Panics
    ///
    /// Panics if metric registration fails (e.g., duplicate metric names).
    /// This should only happen during development if metric names conflict.
    #[must_use]
    #[allow(clippy::new_without_default)]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        let pg_ssl_connections_total = Gauge::with_opts(Opts::new(
            "pg_ssl_connections_total",
            "Total number of connections using SSL/TLS",
        ))
        .expect("Failed to create pg_ssl_connections_total metric");

        let pg_ssl_connections_by_version = GaugeVec::new(
            Opts::new(
                "pg_ssl_connections_by_version",
                "Number of SSL/TLS connections by TLS version",
            ),
            &["version"],
        )
        .expect("Failed to create pg_ssl_connections_by_version metric");

        let pg_ssl_connections_by_cipher = GaugeVec::new(
            Opts::new(
                "pg_ssl_connections_by_cipher",
                "Number of SSL/TLS connections by cipher suite",
            ),
            &["cipher"],
        )
        .expect("Failed to create pg_ssl_connections_by_cipher metric");

        let pg_ssl_connection_bits_avg = Gauge::with_opts(Opts::new(
            "pg_ssl_connection_bits_avg",
            "Average number of bits in SSL/TLS connections",
        ))
        .expect("Failed to create pg_ssl_connection_bits_avg metric");

        Self {
            pg_ssl_connections_total,
            pg_ssl_connections_by_version,
            pg_ssl_connections_by_cipher,
            pg_ssl_connection_bits_avg,
        }
    }
}

impl Collector for ConnectionTlsCollector {
    fn name(&self) -> &'static str {
        "tls.connection_stats"
    }

    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.pg_ssl_connections_total.clone()))?;
        registry.register(Box::new(self.pg_ssl_connections_by_version.clone()))?;
        registry.register(Box::new(self.pg_ssl_connections_by_cipher.clone()))?;
        registry.register(Box::new(self.pg_ssl_connection_bits_avg.clone()))?;
        Ok(())
    }

    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            // pg_stat_ssl is available since PostgreSQL 9.5 (version 90500)
            if !is_pg_version_at_least(90_500) {
                warn!(
                    "pg_stat_ssl view requires PostgreSQL 9.5+, skipping connection TLS stats"
                );
                return Ok(());
            }

            let span = info_span!(
                "db.query",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT FROM pg_stat_ssl",
                otel.kind = "client"
            );

            let query = r"
                SELECT
                    ssl,
                    version,
                    cipher,
                    bits
                FROM pg_stat_ssl
                WHERE ssl = true
            ";

            match sqlx::query_as::<_, (bool, Option<String>, Option<String>, Option<i32>)>(query)
                .fetch_all(pool)
                .instrument(span)
                .await
            {
                Ok(rows) => {
                    // Reset metrics
                    self.pg_ssl_connections_total.set(0.0);
                    self.pg_ssl_connections_by_version.reset();
                    self.pg_ssl_connections_by_cipher.reset();
                    self.pg_ssl_connection_bits_avg.set(0.0);

                    if rows.is_empty() {
                        // No SSL connections
                        return Ok(());
                    }

                    #[allow(clippy::cast_precision_loss)]
                    {
                        let total = rows.len() as f64;
                        self.pg_ssl_connections_total.set(total);
                    }

                    // Aggregate by version
                    let mut version_counts: HashMap<String, f64> = HashMap::new();
                    // Aggregate by cipher
                    let mut cipher_counts: HashMap<String, f64> = HashMap::new();
                    // Calculate average bits
                    let mut total_bits = 0i64;
                    let mut bits_count = 0;

                    for (_ssl, version, cipher, bits) in rows {
                        if let Some(v) = version {
                            *version_counts.entry(v).or_insert(0.0) += 1.0;
                        }
                        if let Some(c) = cipher {
                            *cipher_counts.entry(c).or_insert(0.0) += 1.0;
                        }
                        if let Some(b) = bits {
                            total_bits += i64::from(b);
                            bits_count += 1;
                        }
                    }

                    // Set version metrics
                    for (version, count) in version_counts {
                        self.pg_ssl_connections_by_version
                            .with_label_values(&[&version])
                            .set(count);
                    }

                    // Set cipher metrics
                    for (cipher, count) in cipher_counts {
                        self.pg_ssl_connections_by_cipher
                            .with_label_values(&[&cipher])
                            .set(count);
                    }

                    // Set average bits
                    if bits_count > 0 {
                        let avg = i64_to_f64(total_bits) / f64::from(bits_count);
                        self.pg_ssl_connection_bits_avg.set(avg);
                    }
                }
                Err(e) => {
                    warn!("Failed to query pg_stat_ssl: {e}");
                }
            }

            Ok(())
        })
    }

    fn enabled_by_default(&self) -> bool {
        false
    }
}
