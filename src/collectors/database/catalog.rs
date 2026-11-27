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

/// One row of the catalog snapshot.
struct DatabaseRow {
    name: String,
    connection_limit: i32,
    size_bytes: i64,
}

fn query_span(statement: &'static str) -> tracing::Span {
    info_span!(
        "db.query",
        otel.kind = "client",
        db.system = "postgresql",
        db.operation = "SELECT",
        db.statement = statement,
        db.sql.table = "pg_database"
    )
}

/// Size and connection limit of every database in one statement, with server-side
/// exclusion. Fails as a whole when a listed database is dropped before its size is read.
async fn read_all_at_once(pool: &PgPool, excluded: &[String]) -> Result<Vec<DatabaseRow>, sqlx::Error> {
    let rows = sqlx::query(
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
    .bind(excluded)
    .fetch_all(pool)
    .instrument(query_span(
        "SELECT datname, datconnlimit, pg_database_size(datname) FROM pg_database WHERE NOT (datname = ANY($1))",
    ))
    .await?;

    let mut databases = Vec::with_capacity(rows.len());
    for row in &rows {
        let Some(name) = row
            .try_get::<Option<String>, _>("datname")?
            .filter(|name| !name.is_empty())
        else {
            continue;
        };
        databases.push(DatabaseRow {
            name,
            connection_limit: row.try_get::<Option<i32>, _>("datconnlimit")?.unwrap_or(0),
            size_bytes: row.try_get::<Option<i64>, _>("size")?.unwrap_or(0),
        });
    }
    Ok(databases)
}

/// The same snapshot read with one statement per database, for a scrape that overlaps a
/// `DROP DATABASE`. A database that is gone by the time its size is asked for is left out;
/// every other one is reported. Costs one round trip per database, which is why it is
/// only the fallback.
async fn read_one_by_one(pool: &PgPool, excluded: &[String]) -> Result<Vec<DatabaseRow>> {
    let listed: Vec<(Option<String>, Option<i32>)> = sqlx::query_as(
        r"
        SELECT datname, datconnlimit
        FROM pg_database
        WHERE NOT (datname = ANY($1))
        ORDER BY datname
        ",
    )
    .bind(excluded)
    .fetch_all(pool)
    .instrument(query_span(
        "SELECT datname, datconnlimit FROM pg_database WHERE NOT (datname = ANY($1))",
    ))
    .await?;

    let mut databases = Vec::with_capacity(listed.len());
    for (name, connection_limit) in listed {
        let Some(name) = name.filter(|name| !name.is_empty()) else {
            continue;
        };
        let size = sqlx::query_scalar::<_, Option<i64>>("SELECT pg_database_size($1::name)::bigint")
            .bind(&name)
            .fetch_one(pool)
            .instrument(query_span("SELECT pg_database_size($1)"))
            .await;
        match size {
            Ok(size) => databases.push(DatabaseRow {
                name,
                connection_limit: connection_limit.unwrap_or(0),
                size_bytes: size.unwrap_or(0),
            }),
            Err(error) if is_undefined_database(&error) => {
                debug!(datname = %name, "database was dropped during the scrape - left out");
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(databases)
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
            let excluded: Vec<String> = get_excluded_databases().to_vec();

            // One round trip in the normal case. `pg_database_size` raises an error for a
            // database that was dropped after the scan listed it, and that fails the
            // whole statement; then the databases are read one by one instead, so only the
            // one that vanished is lost.
            let databases = match read_all_at_once(pool, &excluded).await {
                Ok(databases) => databases,
                Err(error) if is_undefined_database(&error) => {
                    debug!(
                        error = %error,
                        "a database was dropped while its size was read - reading one by one"
                    );
                    read_one_by_one(pool, &excluded).await?
                }
                Err(error) => return Err(error.into()),
            };

            // Only now that the read succeeded: drop the previous snapshot, so a dropped
            // database does not linger, and publish the new one.
            self.size_bytes.reset();
            self.connection_limit.reset();

            let apply_span = info_span!("pg_database.apply_metrics", databases = databases.len());
            let _g = apply_span.enter();

            for database in &databases {
                // Connection limit (may be -1 for unlimited)
                let limit_val = f64::from(database.connection_limit);
                self.connection_limit
                    .with_label_values(&[&database.name])
                    .set(limit_val);

                let size_val = i64_to_f64(database.size_bytes);
                self.size_bytes
                    .with_label_values(&[&database.name])
                    .set(size_val);

                debug!(
                    datname = %database.name,
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
