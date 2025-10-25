use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{IntCounter, Opts, Registry};
use sqlx::{PgPool, Row};
use tracing::{debug, info_span, instrument};
use tracing_futures::Instrument as _;

/// Exposes PostgreSQL background writer statistics from pg_stat_bgwriter:
/// - pg_stat_bgwriter_buffers_clean_total (Counter)
/// - pg_stat_bgwriter_maxwritten_clean_total (Counter)
/// - pg_stat_bgwriter_buffers_alloc_total (Counter)
#[derive(Clone)]
pub struct BgwriterCollector {
    buffers_clean: IntCounter,     // pg_stat_bgwriter_buffers_clean_total
    maxwritten_clean: IntCounter,  // pg_stat_bgwriter_maxwritten_clean_total
    buffers_alloc: IntCounter,     // pg_stat_bgwriter_buffers_alloc_total
}

impl Default for BgwriterCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl BgwriterCollector {
    pub fn new() -> Self {
        let buffers_clean = IntCounter::with_opts(Opts::new(
            "pg_stat_bgwriter_buffers_clean_total",
            "Number of buffers written by the background writer",
        ))
        .expect("Failed to create pg_stat_bgwriter_buffers_clean_total");

        let maxwritten_clean = IntCounter::with_opts(Opts::new(
            "pg_stat_bgwriter_maxwritten_clean_total",
            "Number of times the background writer stopped a cleaning scan because it had written too many buffers",
        ))
        .expect("Failed to create pg_stat_bgwriter_maxwritten_clean_total");

        let buffers_alloc = IntCounter::with_opts(Opts::new(
            "pg_stat_bgwriter_buffers_alloc_total",
            "Number of buffers allocated",
        ))
        .expect("Failed to create pg_stat_bgwriter_buffers_alloc_total");

        Self {
            buffers_clean,
            maxwritten_clean,
            buffers_alloc,
        }
    }
}

impl Collector for BgwriterCollector {
    fn name(&self) -> &'static str {
        "bgwriter"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "bgwriter")
    )]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.buffers_clean.clone()))?;
        registry.register(Box::new(self.maxwritten_clean.clone()))?;
        registry.register(Box::new(self.buffers_alloc.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector="bgwriter", otel.kind="internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let query_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT * FROM pg_stat_bgwriter",
                db.sql.table = "pg_stat_bgwriter"
            );

            let row = sqlx::query(
                r#"
                SELECT
                    buffers_clean,
                    maxwritten_clean,
                    buffers_alloc
                FROM pg_stat_bgwriter
                "#,
            )
            .fetch_one(pool)
            .instrument(query_span)
            .await?;

            let buffers_clean: i64 = row.try_get("buffers_clean")?;
            let maxwritten_clean: i64 = row.try_get("maxwritten_clean")?;
            let buffers_alloc: i64 = row.try_get("buffers_alloc")?;

            // Reset and set the counter values
            // Since these are cumulative counters, we reset them first to avoid accumulation
            self.buffers_clean.reset();
            self.maxwritten_clean.reset();
            self.buffers_alloc.reset();

            self.buffers_clean.inc_by(buffers_clean as u64);
            self.maxwritten_clean.inc_by(maxwritten_clean as u64);
            self.buffers_alloc.inc_by(buffers_alloc as u64);

            debug!(
                buffers_clean,
                maxwritten_clean,
                buffers_alloc,
                "updated bgwriter metrics"
            );

            Ok(())
        })
    }

    fn enabled_by_default(&self) -> bool {
        true
    }
}
