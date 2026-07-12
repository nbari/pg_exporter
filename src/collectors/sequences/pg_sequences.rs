use crate::collectors::util::{
    acquire_db_query_permit, get_default_database, get_excluded_databases, open_db_connection,
};
use crate::collectors::{Collector, all_databases_failed};
use anyhow::{Result, anyhow};
use futures::future::BoxFuture;
use prometheus::{GaugeVec, Opts, Registry};
use sqlx::{PgPool, Row, postgres::PgRow};
use tokio::task::JoinSet;
use tracing::{debug, error, info_span, instrument};
use tracing_futures::Instrument as _;

const DEFAULT_MIN_RATIO: f64 = 0.5;
const SEQUENCE_LABELS: [&str; 3] = ["schemaname", "sequencename", "datname"];

/// Per-database `pg_sequences` query.
///
/// `last_value` is NULL until a sequence is first used, and `max_value` is
/// guarded before division so malformed or unexpected sequence metadata cannot
/// produce a divide-by-zero error.
const PG_SEQUENCES_QUERY: &str = r"
    SELECT
        current_database() AS datname,
        schemaname,
        sequencename,
        CASE WHEN max_value > 0
             THEN COALESCE(last_value, 0)::double precision / max_value::double precision
             ELSE 0.0
        END AS used_ratio
    FROM pg_sequences
    ";

#[derive(Clone, Debug)]
struct SequenceSample {
    datname: String,
    schemaname: String,
    sequencename: String,
    used_ratio: f64,
}

/// Collector for sequence exhaustion from `pg_sequences`.
///
/// Emits `pg_sequence_used_ratio{schemaname,sequencename,datname}` only when a
/// sequence has reached the configured minimum used ratio. The collector fans
/// out across all connectable, non-excluded `PostgreSQL` databases because
/// `pg_sequences` is a per-database view.
#[derive(Clone)]
pub struct PgSequencesCollector {
    used_ratio: GaugeVec,
    min_ratio: f64,
}

impl Default for PgSequencesCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl PgSequencesCollector {
    /// Creates a new `PgSequencesCollector` using the default minimum ratio.
    ///
    /// # Panics
    ///
    /// Panics if metric creation fails, which only happens with an invalid
    /// metric name or label set and therefore never at runtime.
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        Self {
            used_ratio: GaugeVec::new(
                Opts::new(
                    "pg_sequence_used_ratio",
                    "Current sequence last_value divided by max_value, by schema, sequence, and database",
                ),
                &SEQUENCE_LABELS,
            )
            .expect("Failed to create pg_sequence_used_ratio"),
            min_ratio: DEFAULT_MIN_RATIO,
        }
    }

    /// Creates a new `PgSequencesCollector` with a custom minimum used ratio.
    #[must_use]
    pub fn with_min_ratio(min_ratio: f64) -> Self {
        let mut collector = Self::new();
        collector.min_ratio = min_ratio;
        collector
    }

    fn reset_metrics(&self) {
        self.used_ratio.reset();
    }

    fn sample_from_row(row: &PgRow) -> SequenceSample {
        SequenceSample {
            datname: row.try_get("datname").unwrap_or_default(),
            schemaname: row.try_get("schemaname").unwrap_or_default(),
            sequencename: row.try_get("sequencename").unwrap_or_default(),
            used_ratio: row.try_get("used_ratio").unwrap_or(0.0),
        }
    }
}

impl Collector for PgSequencesCollector {
    fn name(&self) -> &'static str {
        "pg_sequences"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "pg_sequences")
    )]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.used_ratio.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector = "pg_sequences", otel.kind = "internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let excluded = get_excluded_databases().to_vec();
            let db_list_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT datname FROM pg_database WHERE datallowconn ...",
                db.sql.table = "pg_database"
            );
            let dbs: Vec<String> = sqlx::query_scalar(
                r"
                SELECT datname
                FROM pg_database
                WHERE datallowconn
                  AND NOT datistemplate
                  AND NOT (datname = ANY($1))
                ORDER BY datname
                ",
            )
            .bind(&excluded)
            .fetch_all(pool)
            .instrument(db_list_span)
            .await?;

            let shared_pool = pool.clone();
            let default_db = get_default_database().map(std::string::ToString::to_string);
            let mut tasks: JoinSet<Result<Vec<SequenceSample>>> = JoinSet::new();

            let num_dbs = dbs.len();
            for datname in dbs {
                let shared_pool = shared_pool.clone();
                let default_db = default_db.clone();

                tasks.spawn(async move {
                    let use_shared = default_db.as_deref() == Some(datname.as_str());

                    let query_span = info_span!(
                        "db.query",
                        otel.kind = "client",
                        db.system = "postgresql",
                        db.operation = "SELECT",
                        db.statement = "SELECT ... FROM pg_sequences",
                        db.sql.table = "pg_sequences",
                        datname = %datname,
                        reuse_pool = use_shared
                    );

                    let db_query_permit = if use_shared {
                        None
                    } else {
                        Some(acquire_db_query_permit().await.map_err(|e| {
                            anyhow!("pg_sequences: failed to acquire database query permit: {e}")
                        })?)
                    };

                    let rows_res: anyhow::Result<Vec<PgRow>> = if use_shared {
                        sqlx::query(PG_SEQUENCES_QUERY)
                            .fetch_all(&shared_pool)
                            .instrument(query_span)
                            .await
                            .map_err(Into::into)
                    } else {
                        let Some(permit) = db_query_permit.as_ref() else {
                            return Err(anyhow!("pg_sequences: missing database query permit"));
                        };
                        match open_db_connection(&datname, permit).await {
                            Ok(mut conn) => sqlx::query(PG_SEQUENCES_QUERY)
                                .fetch_all(&mut conn)
                                .instrument(query_span)
                                .await
                                .map_err(Into::into),
                            Err(e) => Err(e),
                        }
                    };

                    Ok(rows_res?
                        .iter()
                        .map(Self::sample_from_row)
                        .collect::<Vec<_>>())
                });
            }

            let mut all_samples = Vec::new();
            let mut failures = Vec::new();
            let mut failed_db_count = 0;
            while let Some(joined) = tasks.join_next().await {
                match joined {
                    Ok(Ok(samples)) => all_samples.extend(samples),
                    Ok(Err(e)) => {
                        error!(error=?e, "pg_sequences: task returned error");
                        failures.push(e.to_string());
                        failed_db_count += 1;
                    }
                    Err(e) => {
                        error!(error=?e, "pg_sequences: task join error");
                        failures.push(e.to_string());
                        failed_db_count += 1;
                    }
                }
            }

            if all_databases_failed(num_dbs, failed_db_count) {
                return Err(anyhow!(
                    "pg_sequences collection failed for ALL {failed_db_count} database task(s): {}",
                    failures.join("; ")
                ));
            }

            if !failures.is_empty() {
                error!(
                    failed_databases = failed_db_count,
                    errors = %failures.join("; "),
                    "pg_sequences: continuing with partial snapshot after per-database failures"
                );
            }

            self.reset_metrics();

            for sample in &all_samples {
                if sample.used_ratio >= self.min_ratio {
                    let labels = [
                        sample.schemaname.as_str(),
                        sample.sequencename.as_str(),
                        sample.datname.as_str(),
                    ];
                    self.used_ratio
                        .with_label_values(&labels)
                        .set(sample.used_ratio);

                    debug!(
                        datname = %sample.datname,
                        schemaname = %sample.schemaname,
                        sequencename = %sample.sequencename,
                        used_ratio = sample.used_ratio,
                        "updated pg_sequence_used_ratio metric"
                    );
                }
            }

            Ok(())
        })
    }

    fn enabled_by_default(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collector_name_is_pg_sequences() {
        assert_eq!(PgSequencesCollector::new().name(), "pg_sequences");
    }

    #[test]
    fn collector_is_disabled_by_default() {
        assert!(!PgSequencesCollector::new().enabled_by_default());
    }

    #[test]
    fn query_reads_pg_sequences_with_safe_ratio() {
        assert!(PG_SEQUENCES_QUERY.contains("FROM pg_sequences"));
        assert!(PG_SEQUENCES_QUERY.contains("current_database() AS datname"));
        assert!(PG_SEQUENCES_QUERY.contains("COALESCE(last_value, 0)::double precision"));
        assert!(PG_SEQUENCES_QUERY.contains("max_value::double precision"));
        assert!(PG_SEQUENCES_QUERY.contains("CASE WHEN max_value > 0"));
    }

    #[test]
    fn register_metrics_succeeds() {
        let registry = Registry::new();
        assert!(PgSequencesCollector::new().register_metrics(&registry).is_ok());
    }
}
