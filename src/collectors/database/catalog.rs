use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{GaugeVec, Opts, Registry};
use sqlx::{PgPool, Row};
use std::collections::HashSet;
use std::env;
use tracing::{debug, info_span, instrument};
use tracing_futures::Instrument as _;

/// Tracks pg_database metrics:
/// - pg_database_size_bytes{datname}
/// - pg_database_connection_limit{datname}
///
/// Exclusions:
/// - Set PG_EXPORTER_EXCLUDE_DATABASES="db1,db2" to skip those databases.
#[derive(Clone)]
pub struct DatabaseSubCollector {
    size_bytes: GaugeVec,       // pg_database_size_bytes{datname}
    connection_limit: GaugeVec, // pg_database_connection_limit{datname}
    excluded: HashSet<String>,
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

        let excluded = env::var("PG_EXPORTER_EXCLUDE_DATABASES")
            .ok()
            .map(|s| {
                s.split(',')
                    .filter_map(|v| {
                        let t = v.trim();
                        if t.is_empty() {
                            None
                        } else {
                            Some(t.to_string())
                        }
                    })
                    .collect::<HashSet<_>>()
            })
            .unwrap_or_default();

        Self {
            size_bytes,
            connection_limit,
            excluded,
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
            // 1) List databases and their connection limits
            let list_span = info_span!(
                "db.query",
                otel.kind = "client",
                db.system = "postgresql",
                db.operation = "SELECT",
                db.statement = "SELECT datname, datconnlimit FROM pg_database",
                db.sql.table = "pg_database"
            );

            let rows = sqlx::query(
                r#"
                SELECT
                    datname,
                    datconnlimit
                FROM pg_database
                "#,
            )
            .fetch_all(pool)
            .instrument(list_span)
            .await?;

            let mut databases: Vec<String> = Vec::new();

            for row in &rows {
                let datname: Option<String> = row.try_get::<Option<String>, _>("datname")?;
                let conn_limit: Option<i32> = row.try_get::<Option<i32>, _>("datconnlimit")?;

                let dat = match datname {
                    Some(d) if !d.is_empty() => d,
                    _ => continue,
                };

                // Exclude if configured
                if self.excluded.contains(&dat) {
                    debug!(datname = %dat, "excluded datname");
                    continue;
                }

                // Emit connection limit (may be -1 for unlimited)
                let limit_val = conn_limit.unwrap_or(0) as f64;
                self.connection_limit
                    .with_label_values(&[&dat])
                    .set(limit_val);

                databases.push(dat);
            }

            // 2) For each database, query size individually
            for dat in databases {
                let size_span = info_span!(
                    "db.query",
                    otel.kind = "client",
                    db.system = "postgresql",
                    db.operation = "SELECT",
                    db.statement = "SELECT pg_database_size($1)",
                    db.sql.table = "pg_database",
                    datname = %dat
                );

                let size_row = sqlx::query(r#"SELECT pg_database_size($1) AS size"#)
                    .bind(&dat)
                    .fetch_one(pool)
                    .instrument(size_span)
                    .await?;

                let size: Option<i64> = size_row.try_get::<Option<i64>, _>("size")?;
                let size_val = size.unwrap_or(0) as f64;

                self.size_bytes.with_label_values(&[&dat]).set(size_val);

                debug!(datname = %dat, size_bytes = size_val, "updated pg_database_size_bytes");
            }

            Ok(())
        })
    }

    fn enabled_by_default(&self) -> bool {
        false
    }
}
