use crate::collectors::{Collector, util::get_excluded_databases};
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{GaugeVec, Opts, Registry};
use sqlx::{PgPool, Row};
use tracing::{debug, info_span, instrument};
use tracing_futures::Instrument as _;

/// Exposes pg_stat_database metrics with the same names/labels as postgres_exporter.
/// Metrics:
/// - pg_stat_database_numbackends               {datid,datname} (current)
/// - pg_stat_database_xact_commit               {datid,datname}
/// - pg_stat_database_xact_rollback             {datid,datname}
/// - pg_stat_database_blks_read                 {datid,datname}
/// - pg_stat_database_blks_hit                  {datid,datname}
/// - pg_stat_database_tup_returned              {datid,datname}
/// - pg_stat_database_tup_fetched               {datid,datname}
/// - pg_stat_database_tup_inserted              {datid,datname}
/// - pg_stat_database_tup_updated               {datid,datname}
/// - pg_stat_database_tup_deleted               {datid,datname}
/// - pg_stat_database_conflicts                 {datid,datname}
/// - pg_stat_database_temp_files                {datid,datname}
/// - pg_stat_database_temp_bytes                {datid,datname}
/// - pg_stat_database_deadlocks                 {datid,datname}
/// - pg_stat_database_blk_read_time             {datid,datname} (ms)
/// - pg_stat_database_blk_write_time            {datid,datname} (ms)
/// - pg_stat_database_stats_reset               {datid,datname} (epoch seconds)
/// - pg_stat_database_active_time_seconds_total {datid,datname} (only PG >= 14; seconds)
///
/// Notes:
/// - We export absolute values as Gauges; use rate()/increase() in PromQL for cumulative series.
/// - Database exclusions are applied server-side using the global list set via CLI/env.
#[derive(Clone)]
pub struct DatabaseStatCollector {
    numbackends: GaugeVec,

    xact_commit: GaugeVec,
    xact_rollback: GaugeVec,
    blks_read: GaugeVec,
    blks_hit: GaugeVec,
    tup_returned: GaugeVec,
    tup_fetched: GaugeVec,
    tup_inserted: GaugeVec,
    tup_updated: GaugeVec,
    tup_deleted: GaugeVec,
    conflicts: GaugeVec,
    temp_files: GaugeVec,
    temp_bytes: GaugeVec,
    deadlocks: GaugeVec,

    blk_read_time: GaugeVec,
    blk_write_time: GaugeVec,

    stats_reset: GaugeVec,

    active_time_seconds_total: GaugeVec, // PG >= 14
}

impl Default for DatabaseStatCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl DatabaseStatCollector {
    pub fn new() -> Self {
        let labels = &["datid", "datname"];

        let numbackends = GaugeVec::new(
            Opts::new(
                "pg_stat_database_numbackends",
                "Number of backends currently connected to this database.",
            ),
            labels,
        )
        .expect("register pg_stat_database_numbackends");

        let xact_commit = GaugeVec::new(
            Opts::new(
                "pg_stat_database_xact_commit",
                "Number of transactions committed.",
            ),
            labels,
        )
        .expect("register pg_stat_database_xact_commit");

        let xact_rollback = GaugeVec::new(
            Opts::new(
                "pg_stat_database_xact_rollback",
                "Number of transactions rolled back.",
            ),
            labels,
        )
        .expect("register pg_stat_database_xact_rollback");

        let blks_read = GaugeVec::new(
            Opts::new("pg_stat_database_blks_read", "Number of disk blocks read."),
            labels,
        )
        .expect("register pg_stat_database_blks_read");

        let blks_hit = GaugeVec::new(
            Opts::new(
                "pg_stat_database_blks_hit",
                "Number of buffer cache hits (PostgreSQL buffer cache).",
            ),
            labels,
        )
        .expect("register pg_stat_database_blks_hit");

        let tup_returned = GaugeVec::new(
            Opts::new("pg_stat_database_tup_returned", "Rows returned by queries."),
            labels,
        )
        .expect("register pg_stat_database_tup_returned");

        let tup_fetched = GaugeVec::new(
            Opts::new("pg_stat_database_tup_fetched", "Rows fetched by queries."),
            labels,
        )
        .expect("register pg_stat_database_tup_fetched");

        let tup_inserted = GaugeVec::new(
            Opts::new("pg_stat_database_tup_inserted", "Rows inserted by queries."),
            labels,
        )
        .expect("register pg_stat_database_tup_inserted");

        let tup_updated = GaugeVec::new(
            Opts::new("pg_stat_database_tup_updated", "Rows updated by queries."),
            labels,
        )
        .expect("register pg_stat_database_tup_updated");

        let tup_deleted = GaugeVec::new(
            Opts::new("pg_stat_database_tup_deleted", "Rows deleted by queries."),
            labels,
        )
        .expect("register pg_stat_database_tup_deleted");

        let conflicts = GaugeVec::new(
            Opts::new(
                "pg_stat_database_conflicts",
                "Queries canceled due to conflicts with recovery.",
            ),
            labels,
        )
        .expect("register pg_stat_database_conflicts");

        let temp_files = GaugeVec::new(
            Opts::new(
                "pg_stat_database_temp_files",
                "Number of temporary files created by queries.",
            ),
            labels,
        )
        .expect("register pg_stat_database_temp_files");

        let temp_bytes = GaugeVec::new(
            Opts::new(
                "pg_stat_database_temp_bytes",
                "Total data written to temporary files by queries.",
            ),
            labels,
        )
        .expect("register pg_stat_database_temp_bytes");

        let deadlocks = GaugeVec::new(
            Opts::new(
                "pg_stat_database_deadlocks",
                "Number of deadlocks detected in this database.",
            ),
            labels,
        )
        .expect("register pg_stat_database_deadlocks");

        let blk_read_time = GaugeVec::new(
            Opts::new(
                "pg_stat_database_blk_read_time",
                "Time spent reading data file blocks (milliseconds).",
            ),
            labels,
        )
        .expect("register pg_stat_database_blk_read_time");

        let blk_write_time = GaugeVec::new(
            Opts::new(
                "pg_stat_database_blk_write_time",
                "Time spent writing data file blocks (milliseconds).",
            ),
            labels,
        )
        .expect("register pg_stat_database_blk_write_time");

        let stats_reset = GaugeVec::new(
            Opts::new(
                "pg_stat_database_stats_reset",
                "Time at which these statistics were last reset (epoch seconds).",
            ),
            labels,
        )
        .expect("register pg_stat_database_stats_reset");

        let active_time_seconds_total = GaugeVec::new(
            Opts::new(
                "pg_stat_database_active_time_seconds_total",
                "Time spent executing SQL statements (seconds, PG >= 14).",
            ),
            labels,
        )
        .expect("register pg_stat_database_active_time_seconds_total");

        Self {
            numbackends,
            xact_commit,
            xact_rollback,
            blks_read,
            blks_hit,
            tup_returned,
            tup_fetched,
            tup_inserted,
            tup_updated,
            tup_deleted,
            conflicts,
            temp_files,
            temp_bytes,
            deadlocks,
            blk_read_time,
            blk_write_time,
            stats_reset,
            active_time_seconds_total,
        }
    }
}

impl Collector for DatabaseStatCollector {
    fn name(&self) -> &'static str {
        "database_stats"
    }

    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.numbackends.clone()))?;
        registry.register(Box::new(self.xact_commit.clone()))?;
        registry.register(Box::new(self.xact_rollback.clone()))?;
        registry.register(Box::new(self.blks_read.clone()))?;
        registry.register(Box::new(self.blks_hit.clone()))?;
        registry.register(Box::new(self.tup_returned.clone()))?;
        registry.register(Box::new(self.tup_fetched.clone()))?;
        registry.register(Box::new(self.tup_inserted.clone()))?;
        registry.register(Box::new(self.tup_updated.clone()))?;
        registry.register(Box::new(self.tup_deleted.clone()))?;
        registry.register(Box::new(self.conflicts.clone()))?;
        registry.register(Box::new(self.temp_files.clone()))?;
        registry.register(Box::new(self.temp_bytes.clone()))?;
        registry.register(Box::new(self.deadlocks.clone()))?;
        registry.register(Box::new(self.blk_read_time.clone()))?;
        registry.register(Box::new(self.blk_write_time.clone()))?;
        registry.register(Box::new(self.stats_reset.clone()))?;
        registry.register(Box::new(self.active_time_seconds_total.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector="database_stats", otel.kind="internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            // Version check for active_time (PG >= 14)
            let vrow = sqlx::query(r#"SELECT current_setting('server_version_num')::int AS v"#)
                .fetch_one(pool)
                .await?;
            let version_num: i32 = vrow.try_get("v")?;
            let has_active_time = version_num >= 140000;

            // Columns per postgres_exporter
            let mut cols = vec![
                "datid::text AS datid",
                "datname",
                "numbackends::bigint AS numbackends",
                "xact_commit::bigint AS xact_commit",
                "xact_rollback::bigint AS xact_rollback",
                "blks_read::bigint AS blks_read",
                "blks_hit::bigint AS blks_hit",
                "tup_returned::bigint AS tup_returned",
                "tup_fetched::bigint AS tup_fetched",
                "tup_inserted::bigint AS tup_inserted",
                "tup_updated::bigint AS tup_updated",
                "tup_deleted::bigint AS tup_deleted",
                "conflicts::bigint AS conflicts",
                "temp_files::bigint AS temp_files",
                "temp_bytes::bigint AS temp_bytes",
                "deadlocks::bigint AS deadlocks",
                "blk_read_time::double precision AS blk_read_time",
                "blk_write_time::double precision AS blk_write_time",
                "EXTRACT(EPOCH FROM stats_reset)::double precision AS stats_reset_epoch",
            ];
            if has_active_time {
                // Convert ms to seconds to match *_seconds_total naming in Go
                cols.push("(active_time / 1000.0)::double precision AS active_time_seconds");
            }

            // Apply exclusions server-side. If the list is empty, this is a no-op.
            let excluded_list: Vec<String> = get_excluded_databases().to_vec();

            let sql = format!(
                "SELECT {} FROM pg_stat_database WHERE NOT (datname = ANY($1)) ORDER BY datname",
                cols.join(", ")
            );

            let span_q = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "pg_stat_database",
                db.sql.table = "pg_stat_database"
            );

            let rows = sqlx::query(&sql)
                .bind(&excluded_list)
                .fetch_all(pool)
                .instrument(span_q)
                .await?;

            let apply_span = info_span!("database_stats.apply_metrics", databases = rows.len());
            let _g = apply_span.enter();

            for row in &rows {
                let datid: String = row.try_get::<String, _>("datid")?;
                let datname: String = row
                    .try_get::<Option<String>, _>("datname")?
                    .unwrap_or_else(|| "[unknown]".to_string());

                let labels = [&datid, &datname];

                self.numbackends
                    .with_label_values(&labels)
                    .set(row.try_get::<i64, _>("numbackends").unwrap_or(0) as f64);

                self.xact_commit
                    .with_label_values(&labels)
                    .set(row.try_get::<i64, _>("xact_commit").unwrap_or(0) as f64);
                self.xact_rollback
                    .with_label_values(&labels)
                    .set(row.try_get::<i64, _>("xact_rollback").unwrap_or(0) as f64);
                self.blks_read
                    .with_label_values(&labels)
                    .set(row.try_get::<i64, _>("blks_read").unwrap_or(0) as f64);
                self.blks_hit
                    .with_label_values(&labels)
                    .set(row.try_get::<i64, _>("blks_hit").unwrap_or(0) as f64);
                self.tup_returned
                    .with_label_values(&labels)
                    .set(row.try_get::<i64, _>("tup_returned").unwrap_or(0) as f64);
                self.tup_fetched
                    .with_label_values(&labels)
                    .set(row.try_get::<i64, _>("tup_fetched").unwrap_or(0) as f64);
                self.tup_inserted
                    .with_label_values(&labels)
                    .set(row.try_get::<i64, _>("tup_inserted").unwrap_or(0) as f64);
                self.tup_updated
                    .with_label_values(&labels)
                    .set(row.try_get::<i64, _>("tup_updated").unwrap_or(0) as f64);
                self.tup_deleted
                    .with_label_values(&labels)
                    .set(row.try_get::<i64, _>("tup_deleted").unwrap_or(0) as f64);
                self.conflicts
                    .with_label_values(&labels)
                    .set(row.try_get::<i64, _>("conflicts").unwrap_or(0) as f64);
                self.temp_files
                    .with_label_values(&labels)
                    .set(row.try_get::<i64, _>("temp_files").unwrap_or(0) as f64);
                self.temp_bytes
                    .with_label_values(&labels)
                    .set(row.try_get::<i64, _>("temp_bytes").unwrap_or(0) as f64);
                self.deadlocks
                    .with_label_values(&labels)
                    .set(row.try_get::<i64, _>("deadlocks").unwrap_or(0) as f64);

                self.blk_read_time
                    .with_label_values(&labels)
                    .set(row.try_get::<f64, _>("blk_read_time").unwrap_or(0.0));
                self.blk_write_time
                    .with_label_values(&labels)
                    .set(row.try_get::<f64, _>("blk_write_time").unwrap_or(0.0));

                self.stats_reset
                    .with_label_values(&labels)
                    .set(row.try_get::<f64, _>("stats_reset_epoch").unwrap_or(0.0));

                if has_active_time {
                    self.active_time_seconds_total
                        .with_label_values(&labels)
                        .set(row.try_get::<f64, _>("active_time_seconds").unwrap_or(0.0));
                }

                debug!(%datid, %datname, "updated pg_stat_database metrics");
            }

            Ok(())
        })
    }
}
