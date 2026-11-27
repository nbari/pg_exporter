use crate::collectors::{
    Collected, Collector, i64_to_f64,
    util::{get_excluded_databases, is_undefined_database},
};
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{GaugeVec, Opts, Registry};
use sqlx::{PgPool, Row};
use tracing::{debug, info_span, instrument};
use tracing_futures::Instrument as _;

/// How often the catalog statement is read when a database is dropped underneath it.
const DROPPED_DATABASE_ATTEMPTS: usize = 4;

/// Tracks `pg_database` metrics:
/// - `pg_database_size_bytes`{`datname`}
/// - `pg_database_connection_limit`{`datname`}
///
/// Exclusions:
/// - Set via CLI flag `--exclude-databases a,b,c` or env `PG_EXPORTER_EXCLUDE_DATABASES`.
/// - Exclusions are applied server-side using a single `query`.
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
    /// Creates a new `DatabaseSubCollector`
    ///
    /// # Panics
    ///
    /// Panics if metric creation fails (should never happen with valid metric names)
    #[must_use]
    #[allow(clippy::expect_used)]
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
    fn collect_once<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<Collected>> {
        Box::pin(async move {
            // Build exclusion list from global OnceCell (set at startup via Clap/env).
            let excluded_list: Vec<String> = get_excluded_databases().to_vec();

            // Single round-trip: size + connection limit per database, with server-side
            // exclusion. `pg_database_size` raises an error for a database that was dropped
            // after the scan listed it, which failed the whole collector whenever a scrape
            // overlapped a `DROP DATABASE`. The statement is simply read again: the dropped
            // database is no longer listed. A few attempts bound it on a server that drops
            // databases continuously.
            let mut attempt = 0;
            let rows = loop {
                attempt += 1;
                let q_span = info_span!(
                    "db.query",
                    otel.kind = "client",
                    db.system = "postgresql",
                    db.operation = "SELECT",
                    db.statement = "SELECT datname, datconnlimit, pg_database_size(datname) FROM pg_database WHERE NOT (datname = ANY($1))",
                    db.sql.table = "pg_database"
                );

                match sqlx::query(
                    r"
                    SELECT
                        datname,
                        datconnlimit,
                        pg_database_size(datname)::bigint AS size
                    FROM pg_database
                    WHERE NOT (datname = ANY($1))
                    ORDER BY datname
                    ",
                )
                .bind(&excluded_list)
                .fetch_all(pool)
                .instrument(q_span)
                .await
                {
                    Ok(rows) => break rows,
                    Err(error)
                        if attempt < DROPPED_DATABASE_ATTEMPTS && is_undefined_database(&error) =>
                    {
                        debug!(
                            attempt,
                            error = %error,
                            "a database was dropped while its size was read - reading again"
                        );
                    }
                    Err(error) => return Err(error.into()),
                }
            };

            // Only now that the read succeeded: drop the previous snapshot, so a dropped
            // database does not linger, and publish the new one.
            self.size_bytes.reset();
            self.connection_limit.reset();

            let apply_span = info_span!("pg_database.apply_metrics", databases = rows.len());
            let _g = apply_span.enter();

            for row in &rows {
                let datname: Option<String> = row.try_get::<Option<String>, _>("datname")?;
                let Some(dat) = datname.filter(|d| !d.is_empty()) else {
                    continue;
                };

                // Connection limit (may be -1 for unlimited)
                let conn_limit: Option<i32> = row.try_get::<Option<i32>, _>("datconnlimit")?;
                let limit_val = f64::from(conn_limit.unwrap_or(0));
                self.connection_limit
                    .with_label_values(&[&dat])
                    .set(limit_val);

                // Size
                let size: Option<i64> = row.try_get::<Option<i64>, _>("size")?;
                let size_val = i64_to_f64(size.unwrap_or(0));
                self.size_bytes.with_label_values(&[&dat]).set(size_val);

                debug!(
                    datname = %dat,
                    connection_limit = limit_val,
                    size_bytes = size_val,
                    "updated pg_database metrics"
                );
            }

            Ok(Collected::Fresh)
        })
    }

    /// Removes every labeled series this collector owns.
    fn reset_metrics(&self) {
        self.size_bytes.reset();
        self.connection_limit.reset();
    }
}
