use crate::collectors::{
    Collected, Collector, i64_to_f64,
    util::{UNDEFINED_OBJECT, get_excluded_databases, is_undefined_database},
};
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{GaugeVec, Opts, Registry};
use sqlx::{PgPool, Row, postgres::types::Oid};
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

/// A database as the catalog listed it, before its size is known.
#[derive(Debug)]
struct ListedDatabase {
    oid: Oid,
    name: String,
    connection_limit: i32,
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

/// Whether a size could not be read because the database no longer exists.
///
/// Asked by OID, `PostgreSQL` answers a missing database with `undefined_object` on
/// current versions and with `invalid_catalog_name` on older ones. The first code is
/// shared with other missing objects, so the caller confirms that the database is gone
/// before it acts on it.
fn database_may_be_gone(error: &sqlx::Error) -> bool {
    is_undefined_database(error)
        || matches!(
            error,
            sqlx::Error::Database(db_error) if db_error.code().as_deref() == Some(UNDEFINED_OBJECT)
        )
}

/// Size and connection limit of every database in one statement, with server-side
/// exclusion.
///
/// The size is asked for by the OID of the row that was listed, not by its name: a name
/// is looked up a second time, and a database dropped and created again under the same
/// name in between would be reported with the old connection limit and the new size.
/// The statement fails as a whole when a listed database is dropped before its size is
/// read.
async fn read_all_at_once(
    pool: &PgPool,
    excluded: &[String],
) -> Result<Vec<DatabaseRow>, sqlx::Error> {
    let rows = sqlx::query(
        r"
        SELECT
            datname,
            datconnlimit,
            pg_database_size(oid)::bigint AS size
        FROM pg_database
        WHERE NOT (datname = ANY($1))
        ORDER BY datname
        ",
    )
    .bind(excluded)
    .fetch_all(pool)
    .instrument(query_span(
        "SELECT datname, datconnlimit, pg_database_size(oid) FROM pg_database WHERE NOT (datname = ANY($1))",
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
            connection_limit: row
                .try_get::<Option<i32>, _>("datconnlimit")?
                .unwrap_or(0),
            size_bytes: row.try_get::<Option<i64>, _>("size")?,
        });
    }
    Ok(databases)
}

/// The databases to report, with the same server-side exclusion, each with the OID that
/// identifies it.
async fn list_databases(pool: &PgPool, excluded: &[String]) -> Result<Vec<ListedDatabase>> {
    let rows: Vec<(Oid, Option<String>, Option<i32>)> = sqlx::query_as(
        r"
        SELECT oid, datname, datconnlimit
        FROM pg_database
        WHERE NOT (datname = ANY($1))
        ORDER BY datname
        ",
    )
    .bind(excluded)
    .fetch_all(pool)
    .instrument(query_span(
        "SELECT oid, datname, datconnlimit FROM pg_database WHERE NOT (datname = ANY($1))",
    ))
    .await?;
    Ok(rows
        .into_iter()
        .filter_map(|(oid, name, connection_limit)| {
            let name = name.filter(|name| !name.is_empty())?;
            Some(ListedDatabase {
                oid,
                name,
                connection_limit: connection_limit.unwrap_or(0),
            })
        })
        .collect())
}

/// Whether a database with this OID still exists, on a fresh look at the catalog.
async fn database_exists(pool: &PgPool, oid: Oid) -> Result<bool> {
    Ok(
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_database WHERE oid = $1)")
            .bind(oid)
            .fetch_one(pool)
            .instrument(query_span("SELECT EXISTS (SELECT 1 FROM pg_database WHERE oid = $1)"))
            .await?,
    )
}

/// The snapshot read with one statement per database, for a scrape that overlaps a
/// `DROP DATABASE`. Each size is asked for by the OID that was listed, so it belongs to
/// the database whose name and connection limit are reported with it. A database that
/// turns out to be gone is left out, also when another one has taken its name; every
/// other one is reported. This costs one round trip per database, which is why it is
/// only the fallback.
async fn read_each(pool: &PgPool, listed: &[ListedDatabase]) -> Result<Vec<DatabaseRow>> {
    let mut databases = Vec::with_capacity(listed.len());
    for database in listed {
        let size: Result<Option<i64>, sqlx::Error> =
            sqlx::query_scalar("SELECT pg_database_size($1::oid)::bigint")
                .bind(database.oid)
                .fetch_one(pool)
                .instrument(query_span("SELECT pg_database_size($1::oid)"))
                .await;
        let size_bytes = match size {
            Ok(Some(size)) => Some(size),
            // No size, or an error that may mean the database is gone: older servers
            // answer NULL for a missing database where newer ones raise an error. Only
            // the catalog can tell a vanished database from one without a known size.
            Ok(None) => {
                if !database_exists(pool, database.oid).await? {
                    debug!(datname = %database.name, "database was dropped during the scrape - left out");
                    continue;
                }
                None
            }
            Err(error) if database_may_be_gone(&error) => {
                if database_exists(pool, database.oid).await? {
                    return Err(error.into());
                }
                debug!(datname = %database.name, "database was dropped during the scrape - left out");
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        databases.push(DatabaseRow {
            name: database.name.clone(),
            connection_limit: database.connection_limit,
            size_bytes,
        });
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
                Err(error) if database_may_be_gone(&error) => {
                    debug!(
                        error = %error,
                        "a database may have been dropped while its size was read - \
                         reading one by one"
                    );
                    let listed = list_databases(pool, &excluded).await?;
                    read_each(pool, &listed).await?
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
    use super::{ListedDatabase, database_may_be_gone, list_databases, read_all_at_once, read_each};
    use anyhow::Result;
    use sqlx::postgres::types::Oid;

    /// The fallback is only taken when a database vanishes mid-scrape, which a test cannot
    /// time. Handing it an OID no database has is the same situation, on demand, and so
    /// is a database dropped and created again under its name: the OID that was listed
    /// is gone. It must be left out, and the database that exists reported in full.
    #[tokio::test]
    async fn the_one_by_one_read_leaves_out_a_database_that_is_gone() -> Result<()> {
        let pool = sqlx::PgPool::connect(&crate::test_dsn::test_dsn()).await?;
        let (current_oid, current): (Oid, String) = sqlx::query_as(
            "SELECT oid, datname::text FROM pg_database WHERE datname = current_database()",
        )
        .fetch_one(&pool)
        .await?;

        let listed = [
            ListedDatabase {
                oid: Oid(4_294_967_294),
                name: "replaced_under_the_same_name".to_owned(),
                connection_limit: 11,
            },
            ListedDatabase {
                oid: current_oid,
                name: current.clone(),
                connection_limit: 37,
            },
        ];
        let read = read_each(&pool, &listed).await;
        pool.close().await;
        let databases = read?;

        assert_eq!(databases.len(), 1, "only the existing database: {databases:?}");
        let reported = databases
            .first()
            .map(|database| (database.name.as_str(), database.connection_limit, database.size_bytes));
        assert!(
            matches!(reported, Some((name, 37, Some(size))) if name == current && size > 0),
            "the existing database keeps its name, limit and size: {databases:?}"
        );
        Ok(())
    }

    /// Both ways of reading must agree on a database nobody is dropping.
    #[tokio::test]
    async fn both_reads_report_the_same_database() -> Result<()> {
        let pool = sqlx::PgPool::connect(&crate::test_dsn::test_dsn()).await?;
        let current: String = sqlx::query_scalar("SELECT current_database()::text")
            .fetch_one(&pool)
            .await?;

        let listed = list_databases(&pool, &[]).await?;
        let at_once = read_all_at_once(&pool, &[]).await;
        let listed_current: Vec<ListedDatabase> = listed
            .into_iter()
            .filter(|database| database.name == current)
            .collect();
        let one_by_one = read_each(&pool, &listed_current).await;
        pool.close().await;

        let one_by_one = one_by_one?;
        let expected = one_by_one.first().map(|row| row.connection_limit);
        assert!(expected.is_some(), "the list names this database");
        match at_once {
            Ok(rows) => {
                let limit = rows
                    .into_iter()
                    .find(|row| row.name == current)
                    .map(|row| row.connection_limit);
                assert_eq!(limit, expected);
            }
            // Another test dropped a database under the bulk read; that is the case the
            // fallback exists for, and the other test covers it. Anything else is a bug.
            Err(error) => assert!(database_may_be_gone(&error), "unexpected error: {error}"),
        }
        Ok(())
    }
}
