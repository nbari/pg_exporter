use crate::collectors::{Collector, util::get_excluded_databases};
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{GaugeVec, Opts, Registry};
use sqlx::{PgPool, Row};
use tracing::{debug, info_span, instrument};
use tracing_futures::Instrument as _;

/// Tracks pg_database metrics:
/// - pg_database_size_bytes{datname}
/// - pg_database_connection_limit{datname}
///
/// Exclusions:
/// - Set via CLI flag `--exclude-databases a,b,c` or env `PG_EXPORTER_EXCLUDE_DATABASES`.
/// - Exclusions are applied server-side using a single query.
#[derive(Clone)]
pub struct DatabaseSubCollector {
    size_bytes: GaugeVec,       // pg_database_size_bytes{datname}
    connection_limit: GaugeVec, // pg_database_connection_limit{datname}
}

impl Default for DatabaseSubCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl DatabaseSubCollector {
    pub fn new() -> Self {
        let size_bytes = GaugeVec::new(
            Opts::new("pg_database_size_bytes", "Disk space used by the database"),
            &["datname"],
        )
        .expect("register pg_database_size_bytes");

        let connection_limit = GaugeVec::new(
            Opts::new(
                "pg_database_connection_limit",
                "Connection limit set for the database (may be -1 for unlimited)",
            ),
            &["datname"],
        )
        .expect("register pg_database_connection_limit");

        Self {
            size_bytes,
            connection_limit,
        }
    }
}

impl Collector for DatabaseSubCollector {
    fn name(&self) -> &'static str {
        "pg_database"
    }

    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.size_bytes.clone()))?;
        registry.register(Box::new(self.connection_limit.clone()))?;
        Ok(())
    }

    #[instrument(
        skip(self, pool),
        level = "info",
        err,
        fields(collector="pg_database", otel.kind="internal")
    )]
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            // Build exclusion list from global OnceCell (set at startup via Clap/env).
            let excluded_list: Vec<String> = get_excluded_databases().to_vec();

            // Single round-trip: size + connection limit per database, with server-side exclusion.
            let q_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT datname, datconnlimit, pg_database_size(datname) FROM pg_database WHERE NOT (datname = ANY($1))",
                db.sql.table = "pg_database"
            );

            let rows = sqlx::query(
                r#"
                SELECT
                    datname,
                    datconnlimit,
                    pg_database_size(datname)::bigint AS size
                FROM pg_database
                WHERE NOT (datname = ANY($1))
                ORDER BY datname
                "#,
            )
            .bind(&excluded_list)
            .fetch_all(pool)
            .instrument(q_span)
            .await?;

            let apply_span = info_span!("pg_database.apply_metrics", databases = rows.len());
            let _g = apply_span.enter();

            for row in &rows {
                let datname: Option<String> = row.try_get::<Option<String>, _>("datname")?;
                let Some(dat) = datname.filter(|d| !d.is_empty()) else {
                    continue;
                };

                // Connection limit (may be -1 for unlimited)
                let conn_limit: Option<i32> = row.try_get::<Option<i32>, _>("datconnlimit")?;
                let limit_val = conn_limit.unwrap_or(0) as f64;
                self.connection_limit
                    .with_label_values(&[&dat])
                    .set(limit_val);

                // Size
                let size: Option<i64> = row.try_get::<Option<i64>, _>("size")?;
                let size_val = size.unwrap_or(0) as f64;
                self.size_bytes.with_label_values(&[&dat]).set(size_val);

                debug!(
                    datname = %dat,
                    connection_limit = limit_val,
                    size_bytes = size_val,
                    "updated pg_database metrics"
                );
            }

            Ok(())
        })
    }

    fn enabled_by_default(&self) -> bool {
        false
    }
}
