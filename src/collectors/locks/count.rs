use crate::collectors::{util::get_excluded_databases, Collector};
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{IntGaugeVec, Opts, Registry};
use sqlx::{PgPool, Row};
use tracing::{debug, info, info_span, instrument};
use tracing_futures::Instrument as _;

/// Tracks `PostgreSQL` lock contention
#[derive(Clone)]
pub struct LocksSubCollector {
    locks_count: IntGaugeVec,
}

impl Default for LocksSubCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl LocksSubCollector {
    #[must_use]
    /// Creates a new `LocksSubCollector`
    ///
    /// # Panics
    ///
    /// Panics if metric creation fails (should never happen with valid metric names)
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        let locks_count = IntGaugeVec::new(
            Opts::new("pg_locks_count", "Number of locks per database and mode"),
            &["datname", "mode"],
        )
        .expect("Failed to create pg_locks_count metric");

        Self { locks_count }
    }
}

impl Collector for LocksSubCollector {
    fn name(&self) -> &'static str {
        "locks"
    }

    #[instrument(skip(self, registry), level = "info", err, fields(collector = "locks"))]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.locks_count.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector="locks", otel.kind="internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            // Build exclusion list from global OnceCell (set at startup via Clap/env).
            let excluded: Vec<String> = get_excluded_databases().to_vec();

            // Client span for querying lock statistics
            let query_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT datname, mode, count FROM pg_locks + pg_database join (filtered)",
                db.sql.table = "pg_locks"
            );

            let rows = sqlx::query(
                r"
                SELECT
                    COALESCE(d.datname, '') AS datname,
                    l.mode,
                    COUNT(*) AS count
                FROM pg_locks l
                LEFT JOIN pg_database d ON l.database = d.oid
                WHERE NOT (COALESCE(d.datname, '') = ANY($1))
                GROUP BY d.datname, l.mode
                ORDER BY datname, mode
                ",
            )
            .bind(&excluded)
            .fetch_all(pool)
            .instrument(query_span)
            .await?;

            // Span for applying metrics
            let apply_span = info_span!("locks.apply_metrics", locks = rows.len());
            let _g = apply_span.enter();

            // Reset all metrics before setting new values
            self.locks_count.reset();

            for row in &rows {
                let datname: String = row.try_get("datname")?;
                let mode: String = row.try_get("mode")?;
                let count: i64 = row.try_get("count").unwrap_or(0);

                self.locks_count
                    .with_label_values(&[&datname, &mode])
                    .set(count);

                debug!(
                    datname = %datname,
                    mode = %mode,
                    count,
                    "updated lock metrics"
                );
            }

            info!("Collected lock metrics for {} database/mode combinations", rows.len());

            Ok(())
        })
    }
}
