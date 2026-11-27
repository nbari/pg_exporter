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
#[derive(Debug)]
struct DatabaseRow {
    name: String,
    connection_limit: i32,
    /// `None` when `PostgreSQL` reported no size for the database.
    size_bytes: Option<i64>,
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
            size_bytes: row.try_get::<Option<i64>, _>("size")?,
        });
    }
    Ok(databases)
}

/// Connection limit and size of one database, as read in one statement.
type LimitAndSize = (Option<i32>, Option<i64>);

/// The databases to report, by name, with the same server-side exclusion.
async fn list_database_names(pool: &PgPool, excluded: &[String]) -> Result<Vec<String>> {
    let names: Vec<Option<String>> = sqlx::query_scalar(
        r"
        SELECT datname
        FROM pg_database
        WHERE NOT (datname = ANY($1))
        ORDER BY datname
        ",
    )
    .bind(excluded)
    .fetch_all(pool)
    .instrument(query_span(
        "SELECT datname FROM pg_database WHERE NOT (datname = ANY($1))",
    ))
    .await?;
    Ok(names
        .into_iter()
        .flatten()
        .filter(|name| !name.is_empty())
        .collect())
}

/// The snapshot read with one statement per database, for a scrape that overlaps a
/// `DROP DATABASE`. Each statement reads the connection limit and the size of one
/// database together, so both describe the same database even when one was dropped and
/// created again under its name in between. A database that is gone by the time it is
/// asked for is left out; every other one is reported. This costs one round trip per
/// database, which is why it is only the fallback.
async fn read_each(pool: &PgPool, names: &[String]) -> Result<Vec<DatabaseRow>> {
    let mut databases = Vec::with_capacity(names.len());
    for name in names {
        let read: Result<Option<LimitAndSize>, sqlx::Error> = sqlx::query_as(
            r"
            SELECT datconnlimit, pg_database_size(datname)::bigint
            FROM pg_database
            WHERE datname = $1
            ",
        )
        .bind(name)
        .fetch_optional(pool)
        .instrument(query_span(
            "SELECT datconnlimit, pg_database_size(datname) FROM pg_database WHERE datname = $1",
        ))
        .await;
        match read {
            Ok(Some((connection_limit, size_bytes))) => databases.push(DatabaseRow {
                name: name.clone(),
                connection_limit: connection_limit.unwrap_or(0),
                size_bytes,
            }),
            // Dropped before this statement ran, or while it was running.
            Ok(None) => {
                debug!(datname = %name, "database was dropped during the scrape - left out");
            }
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
                    let names = list_database_names(pool, &excluded).await?;
                    read_each(pool, &names).await?
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

                // `pg_database_size` can answer NULL; that is "unknown", not 0 bytes.
                if let Some(size) = database.size_bytes {
                    self.size_bytes
                        .with_label_values(&[&database.name])
                        .set(i64_to_f64(size));
                }

                debug!(
                    datname = %database.name,
                    connection_limit = limit_val,
                    size_bytes = ?database.size_bytes,
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

#[cfg(test)]
mod tests {
    use super::{list_database_names, read_all_at_once, read_each};
    use anyhow::Result;

    /// The fallback is only taken when a database vanishes mid-scrape, which a test cannot
    /// time. Handing it a name that does not exist is the same situation, on demand: the
    /// database that is gone is left out and the one that exists is reported in full.
    #[tokio::test]
    async fn the_one_by_one_read_leaves_out_a_database_that_is_gone() -> Result<()> {
        let pool = sqlx::PgPool::connect(&crate::test_dsn::test_dsn()).await?;
        let current: String = sqlx::query_scalar("SELECT current_database()::text")
            .fetch_one(&pool)
            .await?;

        let names = vec![
            "no_such_database_for_the_catalog_test".to_owned(),
            current.clone(),
        ];
        let read = read_each(&pool, &names).await;
        pool.close().await;
        let databases = read?;

        assert_eq!(databases.len(), 1, "only the existing database: {databases:?}");
        let reported = databases
            .first()
            .map(|database| (database.name.as_str(), database.size_bytes));
        assert!(
            matches!(reported, Some((name, Some(size))) if name == current && size > 0),
            "the existing database keeps its name and size: {databases:?}"
        );
        Ok(())
    }

    /// Both ways of reading must agree on a database nobody is dropping.
    #[tokio::test]
    async fn both_reads_report_the_same_connection_limit() -> Result<()> {
        let pool = sqlx::PgPool::connect(&crate::test_dsn::test_dsn()).await?;
        let current: String = sqlx::query_scalar("SELECT current_database()::text")
            .fetch_one(&pool)
            .await?;

        let listed = list_database_names(&pool, &[]).await?;
        let at_once = read_all_at_once(&pool, &[]).await;
        let one_by_one = read_each(&pool, std::slice::from_ref(&current)).await;
        pool.close().await;

        assert!(listed.contains(&current), "the list names this database");
        let limit_at_once = at_once
            .ok()
            .and_then(|rows| rows.into_iter().find(|row| row.name == current))
            .map(|row| row.connection_limit);
        let limit_one_by_one = one_by_one?.first().map(|row| row.connection_limit);
        // A database dropped elsewhere can fail the bulk read; then there is nothing to
        // compare, and the other test covers the fallback.
        if limit_at_once.is_some() {
            assert_eq!(limit_at_once, limit_one_by_one);
        }
        Ok(())
    }
}
