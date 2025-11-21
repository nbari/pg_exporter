use crate::collectors::{Collector, util::get_excluded_databases};
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{IntGauge, IntGaugeVec, Opts, Registry};
use sqlx::{PgPool, Row};
use std::collections::{HashMap, HashSet};
use tracing::{debug, info_span, instrument};
use tracing_futures::Instrument as _;

/// Minimal vacuum stats (lightweight, single-connection):
/// - `pg_vacuum_database_freeze_age_xids`{`datname`}
/// - `pg_vacuum_freeze_max_age_xids`
/// - `pg_vacuum_database_freeze_age_pct_of_max`{`datname`}
/// - `pg_vacuum_autovacuum_workers`{`datname`}
#[derive(Clone)]
pub struct VacuumStatsCollector {
    // Per-database freeze age (age(datfrozenxid) in xids)
    db_freeze_age_xids: IntGaugeVec, // pg_vacuum_database_freeze_age_xids{datname}
    // Global autovacuum_freeze_max_age (xids)
    freeze_max_age_xids: IntGauge, // pg_vacuum_freeze_max_age_xids
    // Per-database % of max (rounded integer 0..100+, capped to 100 for display)
    db_freeze_age_pct_of_max: IntGaugeVec, // pg_vacuum_database_freeze_age_pct_of_max{datname}
    // Per-database autovacuum workers currently running
    autovac_workers: IntGaugeVec, // pg_vacuum_autovacuum_workers{datname}
}

impl Default for VacuumStatsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl VacuumStatsCollector {
    /// Creates a new `VacuumStatsCollector`
    ///
    /// # Panics
    ///
    /// Panics if metric creation fails (should never happen with valid metric names)
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        let db_freeze_age_xids = IntGaugeVec::new(
            Opts::new(
                "pg_vacuum_database_freeze_age_xids",
                "Age in transactions (xids) since database freeze (age(datfrozenxid)).",
            ),
            &["datname"],
        )
        .expect("create pg_vacuum_database_freeze_age_xids");

        let freeze_max_age_xids = IntGauge::with_opts(Opts::new(
            "pg_vacuum_freeze_max_age_xids",
            "Configured autovacuum_freeze_max_age (xids).",
        ))
        .expect("create pg_vacuum_freeze_max_age_xids");

        let db_freeze_age_pct_of_max = IntGaugeVec::new(
            Opts::new(
                "pg_vacuum_database_freeze_age_pct_of_max",
                "Freeze age as percent of autovacuum_freeze_max_age (0..100).",
            ),
            &["datname"],
        )
        .expect("create pg_vacuum_database_freeze_age_pct_of_max");

        let autovac_workers = IntGaugeVec::new(
            Opts::new(
                "pg_vacuum_autovacuum_workers",
                "Number of autovacuum workers currently running per database.",
            ),
            &["datname"],
        )
        .expect("create pg_vacuum_autovacuum_workers");

        Self {
            db_freeze_age_xids,
            freeze_max_age_xids,
            db_freeze_age_pct_of_max,
            autovac_workers,
        }
    }
}

impl Collector for VacuumStatsCollector {
    fn name(&self) -> &'static str {
        "vacuum_stats"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "vacuum_stats")
    )]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.db_freeze_age_xids.clone()))?;
        registry.register(Box::new(self.freeze_max_age_xids.clone()))?;
        registry.register(Box::new(self.db_freeze_age_pct_of_max.clone()))?;
        registry.register(Box::new(self.autovac_workers.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector="vacuum_stats", otel.kind="internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let excluded: Vec<String> = get_excluded_databases().to_vec();

            // Query 1: global autovacuum_freeze_max_age (xids)
            let q_freeze_max = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT current_setting('autovacuum_freeze_max_age')",
            );
            let freeze_max_age_xids: i64 = sqlx::query_scalar(
                r"SELECT current_setting('autovacuum_freeze_max_age')::bigint",
            )
            .fetch_one(pool)
            .instrument(q_freeze_max)
            .await
            .unwrap_or(200_000_000); // default if missing, very unlikely
            self.freeze_max_age_xids.set(freeze_max_age_xids);

            // Query 2: per-database freeze age (xids) from pg_database
            let q_db_freeze_age = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT datname, age(datfrozenxid) FROM pg_database",
                db.sql.table = "pg_database"
            );
            let rows = sqlx::query(
                r"
                SELECT
                    datname,
                    age(datfrozenxid)::bigint AS freeze_age
                FROM pg_database
                WHERE datallowconn
                  AND NOT datistemplate
                  AND NOT (datname = ANY($1))
                ORDER BY datname
                ",
            )
            .bind(&excluded)
            .fetch_all(pool)
            .instrument(q_db_freeze_age)
            .await?;

            let mut seen_dbs: HashSet<String> = HashSet::new();

            for row in &rows {
                let datname: String = row
                    .try_get::<Option<String>, _>("datname")?
                    .unwrap_or_else(|| "[unknown]".to_string());
                let age_xids: i64 = row.try_get::<i64, _>("freeze_age").unwrap_or(0);

                seen_dbs.insert(datname.clone());

                self.db_freeze_age_xids
                    .with_label_values(&[&datname])
                    .set(age_xids);

                // integer percent; cap to 100 (can exceed in theory; cap keeps dashboards sane)
                let pct = if freeze_max_age_xids > 0 {
                    let numerator = i128::from(age_xids).saturating_mul(100);
                    let denominator = i128::from(freeze_max_age_xids);
                    if denominator > 0 {
                        let rounded = numerator.saturating_add(denominator / 2) / denominator;
                        i64::try_from(rounded.clamp(0, 100)).unwrap_or(0)
                    } else {
                        0
                    }
                } else {
                    0
                };
                self.db_freeze_age_pct_of_max
                    .with_label_values(&[&datname])
                    .set(pct);

                debug!(
                    datname = %datname,
                    age_xids,
                    freeze_max_age_xids,
                    pct_of_max = pct,
                    "updated freeze age metrics"
                );
            }

            // Query 3: autovacuum workers per database (from pg_stat_activity)
            let q_workers = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement =
                    "SELECT count(*) FROM pg_stat_activity WHERE backend_type='autovacuum worker'",
                db.sql.table = "pg_stat_activity"
            );
            let worker_rows = sqlx::query(
                r"
                SELECT
                    datname,
                    COUNT(*)::bigint AS workers
                FROM pg_stat_activity
                WHERE backend_type = 'autovacuum worker'
                  AND NOT (COALESCE(datname,'') = ANY($1))
                GROUP BY datname
                ORDER BY datname
                ",
            )
            .bind(&excluded)
            .fetch_all(pool)
            .instrument(q_workers)
            .await?;

            let mut worker_map: HashMap<String, i64> = HashMap::new();
            for row in &worker_rows {
                let datname: String = row
                    .try_get::<Option<String>, _>("datname")?
                    .unwrap_or_else(|| "[unknown]".to_string());
                let workers: i64 = row.try_get::<i64, _>("workers").unwrap_or(0);
                worker_map.insert(datname.clone(), workers);

                self.autovac_workers
                    .with_label_values(&[&datname])
                    .set(workers);
            }

            // Ensure we emit zeros for DBs with no workers visible this scrape
            for db in seen_dbs {
                if !worker_map.contains_key(&db) {
                    self.autovac_workers.with_label_values(&[&db]).set(0);
                }
            }

            Ok(())
        })
    }
}
