use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{GaugeVec, Opts, Registry};
use sqlx::{PgPool, Row};
use tracing::{debug, info, info_span, instrument};
use tracing_futures::Instrument as _;

/// Tracks PostgreSQL lock contention
#[derive(Clone)]
pub struct LocksCollector {
    locks_waiting: GaugeVec,
    locks_granted: GaugeVec,
}

impl Default for LocksCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl LocksCollector {
    pub fn new() -> Self {
        let locks_waiting = GaugeVec::new(
            Opts::new("pg_locks_waiting", "Number of locks waiting per relation"),
            &["relation"],
        )
        .expect("Failed to create locks_waiting metric");

        let locks_granted = GaugeVec::new(
            Opts::new("pg_locks_granted", "Number of locks granted per relation"),
            &["relation"],
        )
        .expect("Failed to create locks_granted metric");

        Self {
            locks_waiting,
            locks_granted,
        }
    }
}

impl Collector for LocksCollector {
    fn name(&self) -> &'static str {
        "locks"
    }

    #[instrument(skip(self, registry), level = "info", err, fields(collector = "locks"))]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.locks_waiting.clone()))?;
        registry.register(Box::new(self.locks_granted.clone()))?;
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
            // Client span for querying lock statistics
            let query_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT relation, waiting, granted FROM pg_locks + pg_class join",
                db.sql.table = "pg_locks"
            );

            let rows = sqlx::query(
                r#"
                SELECT
                    COALESCE(c.relname, 'none') AS relation,
                    COUNT(*) FILTER (WHERE NOT granted) AS waiting,
                    COUNT(*) FILTER (WHERE granted) AS granted
                FROM pg_locks l
                LEFT JOIN pg_class c ON l.relation = c.oid
                GROUP BY c.relname
                ORDER BY relation
                "#,
            )
            .fetch_all(pool)
            .instrument(query_span)
            .await?;

            // Span for applying metrics
            let apply_span = info_span!("locks.apply_metrics", relations = rows.len());
            let _g = apply_span.enter();

            for row in &rows {
                let relation: String = row.try_get("relation")?;
                let waiting: i64 = row.try_get("waiting").unwrap_or(0);
                let granted: i64 = row.try_get("granted").unwrap_or(0);

                self.locks_waiting
                    .with_label_values(&[&relation])
                    .set(waiting as f64);
                self.locks_granted
                    .with_label_values(&[&relation])
                    .set(granted as f64);

                debug!(
                    relation = %relation,
                    waiting,
                    granted,
                    "updated lock metrics"
                );
            }

            info!("Collected lock metrics for {} relations", rows.len());

            Ok(())
        })
    }

    fn enabled_by_default(&self) -> bool {
        false
    }
}
