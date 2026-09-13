use super::{
    stats::{INDEX_STATS_QUERY, IndexStatsMetrics, IndexStatsSample},
    unused::{UNUSED_INDEX_QUERY, UnusedIndexMetrics, UnusedIndexSample},
};
use crate::collectors::{
    Collected, Collector, all_databases_failed, permit_metrics,
    util::{
        acquire_db_query_permit, get_default_database, get_excluded_databases, open_db_connection,
    },
};
use anyhow::{Context, Result, anyhow};
use futures::future::BoxFuture;
use prometheus::Registry;
use sqlx::{PgConnection, PgPool};
use tokio::task::JoinSet;
use tracing::{error, info_span, instrument, warn};
use tracing_futures::Instrument as _;

/// One shared input for statistics and unused indexes. MATERIALIZED prevents the
/// optimizer from evaluating `pg_relation_size` twice for an unused index.
///
/// Invalid indexes deliberately retain their broader catalog scope: partitioned
/// index parents (and TOAST indexes) are absent from `pg_stat_user_indexes`.
const INDEX_QUERY: &str = r"
    WITH index_rows AS MATERIALIZED (
        SELECT s.idx_scan, s.idx_tup_read, s.idx_tup_fetch,
               i.indisvalid, i.indisprimary, i.indisunique,
               pg_relation_size(s.indexrelid) AS index_size_bytes,
               io.idx_blks_read, io.idx_blks_hit
        FROM pg_stat_user_indexes s
        JOIN pg_index i ON s.indexrelid = i.indexrelid
        LEFT JOIN pg_statio_user_indexes io ON s.indexrelid = io.indexrelid
        WHERE s.schemaname NOT IN ('pg_catalog', 'information_schema')
    )
    SELECT current_database() AS datname,
        COALESCE(SUM(idx_scan), 0)::bigint AS total_scans,
        COALESCE(SUM(idx_tup_read), 0)::bigint AS total_tup_read,
        COALESCE(SUM(idx_tup_fetch), 0)::bigint AS total_tup_fetch,
        COALESCE(SUM(index_size_bytes), 0)::bigint AS total_size_bytes,
        COALESCE(SUM(indisvalid::int), 0)::bigint AS valid_count,
        COALESCE(SUM(idx_blks_read), 0)::bigint AS total_idx_blks_read,
        COALESCE(SUM(idx_blks_hit), 0)::bigint AS total_idx_blks_hit,
        (COUNT(*) FILTER (WHERE idx_scan = 0 AND NOT indisprimary AND NOT indisunique))::bigint AS unused_count,
        COALESCE(SUM(index_size_bytes) FILTER (
            WHERE idx_scan = 0 AND NOT indisprimary AND NOT indisunique
        ), 0)::bigint AS unused_size_bytes,
        (
            SELECT COUNT(*)::bigint
            FROM pg_index i
            JOIN pg_class c ON i.indexrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE NOT i.indisvalid
              AND n.nspname NOT IN ('pg_catalog', 'information_schema')
        ) AS invalid_count
    FROM index_rows
";

/// All index metrics share database discovery, a connection, and the healthy query.
#[derive(Clone, Default)]
pub struct PgStatUserIndexesCollector {
    stats: IndexStatsMetrics,
    unused: UnusedIndexMetrics,
}

struct DatabaseSamples {
    stats: Result<Option<IndexStatsSample>>,
    unused: Result<Option<UnusedIndexSample>>,
}

#[derive(Default)]
struct Snapshot {
    stats: Vec<IndexStatsSample>,
    unused: Vec<UnusedIndexSample>,
    stats_errors: Vec<String>,
    unused_errors: Vec<String>,
}

impl Snapshot {
    fn record(&mut self, result: Result<DatabaseSamples>) {
        match result {
            Ok(samples) => {
                match samples.stats {
                    Ok(Some(sample)) => self.stats.push(sample),
                    Ok(None) => {}
                    Err(error) => self.stats_errors.push(error.to_string()),
                }
                match samples.unused {
                    Ok(Some(sample)) => self.unused.push(sample),
                    Ok(None) => {}
                    Err(error) => self.unused_errors.push(error.to_string()),
                }
            }
            Err(error) => {
                self.stats_errors.push(error.to_string());
                self.unused_errors.push(error.to_string());
            }
        }
    }

    fn validate(&self, num_dbs: usize) -> Result<()> {
        let stats_failed = all_databases_failed(num_dbs, self.stats_errors.len());
        let unused_failed = all_databases_failed(num_dbs, self.unused_errors.len());
        if stats_failed || unused_failed {
            return Err(anyhow!(
                "index collection failed for ALL databases in a metric group: stats=[{}], unused=[{}]",
                self.stats_errors.join("; "),
                self.unused_errors.join("; ")
            ));
        }
        if !self.stats_errors.is_empty() || !self.unused_errors.is_empty() {
            error!(stats_errors = ?self.stats_errors, unused_errors = ?self.unused_errors,
                "index: continuing with partial snapshots after per-database failures");
        }
        Ok(())
    }
}

impl PgStatUserIndexesCollector {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Only readability errors permit the compatibility fallback. Retrying a
    /// timeout, lock failure or lost connection would amplify load during an incident.
    fn permits_fallback(error: &sqlx::Error) -> bool {
        matches!(
            error
                .as_database_error()
                .and_then(sqlx::error::DatabaseError::code)
                .as_deref(),
            Some("42501" | "42P01" | "42703" | "42883" | "42704")
        )
    }

    async fn query_database(conn: &mut PgConnection) -> Result<DatabaseSamples> {
        match sqlx::query(INDEX_QUERY).fetch_optional(&mut *conn).await {
            Ok(row) => Ok(DatabaseSamples {
                stats: row
                    .as_ref()
                    .map(IndexStatsMetrics::sample_from_row)
                    .transpose(),
                unused: row
                    .as_ref()
                    .map(UnusedIndexMetrics::sample_from_row)
                    .transpose(),
            }),
            Err(error) if Self::permits_fallback(&error) => {
                warn!(%error, "index: combined query unavailable; collecting readable metric groups separately");
                // Autocommit statements: a failed query must not poison the other group.
                // Keep the same connection and permit throughout the fallback.
                let stats = sqlx::query(INDEX_STATS_QUERY)
                    .fetch_optional(&mut *conn)
                    .await
                    .map_err(anyhow::Error::from)
                    .and_then(|row| {
                        row.as_ref()
                            .map(IndexStatsMetrics::sample_from_row)
                            .transpose()
                    });
                let unused = sqlx::query(UNUSED_INDEX_QUERY)
                    .fetch_optional(&mut *conn)
                    .await
                    .map_err(anyhow::Error::from)
                    .and_then(|row| {
                        row.as_ref()
                            .map(UnusedIndexMetrics::sample_from_row)
                            .transpose()
                    });
                Ok(DatabaseSamples { stats, unused })
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn collect_database(pool: &PgPool, datname: &str) -> Result<DatabaseSamples> {
        if get_default_database() == Some(datname) {
            let mut conn = pool.acquire().await?;
            Self::query_database(&mut conn).await
        } else {
            let permit = acquire_db_query_permit().await?;
            let mut conn = open_db_connection(datname, &permit).await?;
            let result = Self::query_database(&mut conn).await;
            drop(conn);
            drop(permit);
            result
        }
    }
}

impl Collector for PgStatUserIndexesCollector {
    fn name(&self) -> &'static str {
        "pg_stat_user_indexes"
    }

    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        self.stats.register_metrics(registry)?;
        self.unused.register_metrics(registry)
    }

    #[instrument(skip(self, pool), level = "info", err, fields(collector = "index"))]
    fn collect_once<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<Collected>> {
        Box::pin(async move {
            let dbs: Vec<String> = sqlx::query_scalar(
                r"
                SELECT datname FROM pg_database
                WHERE datallowconn AND NOT datistemplate AND NOT (datname = ANY($1))
                ORDER BY datname
            ",
            )
            .bind(get_excluded_databases())
            .fetch_all(pool)
            .await?;
            let num_dbs = dbs.len();
            let mut tasks = JoinSet::new();
            for datname in dbs {
                let pool = pool.clone();
                let span = info_span!("db.query", db.system = "postgresql", db.operation = "SELECT",
                    db.sql.table = "pg_stat_user_indexes", %datname);
                tasks.spawn(permit_metrics::inherit(
                    async move {
                        Self::collect_database(&pool, &datname)
                            .await
                            .with_context(|| format!("index database {datname:?}"))
                    }
                    .instrument(span),
                ));
            }
            let mut snapshot = Snapshot::default();
            while let Some(joined) = tasks.join_next().await {
                snapshot.record(
                    joined
                        .map_err(anyhow::Error::from)
                        .and_then(|result| result),
                );
            }
            // Preserve the old independent all-databases-failed policy. Do not clear
            // either group on a total failure; the registry serves no samples on error.
            snapshot.validate(num_dbs)?;
            self.stats.publish(&snapshot.stats);
            self.unused.publish(&snapshot.unused);
            Ok(Collected::Fresh)
        })
    }

    fn reset_metrics(&self) {
        self.stats.reset_all();
        self.unused.reset_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_groups_are_accounted_independently() -> Result<()> {
        let mut snapshot = Snapshot::default();
        snapshot.validate(0)?;
        snapshot.record(Ok(DatabaseSamples {
            stats: Ok(None),
            unused: Err(anyhow!("unreadable unused indexes")),
        }));
        assert!(
            snapshot.validate(1).is_err(),
            "a wholly unreadable group must fail the scrape"
        );
        snapshot.record(Ok(DatabaseSamples {
            stats: Err(anyhow!("unreadable index statistics")),
            unused: Ok(None),
        }));
        snapshot.validate(2)?;
        snapshot.record(Err(anyhow!("connection failed")));
        assert_eq!(snapshot.stats_errors.len(), 2);
        assert_eq!(snapshot.unused_errors.len(), 2);
        snapshot.validate(3)?;
        assert!(snapshot.validate(2).is_err());
        Ok(())
    }

    #[test]
    fn connection_and_protocol_errors_do_not_retry() {
        assert!(!PgStatUserIndexesCollector::permits_fallback(
            &sqlx::Error::PoolClosed
        ));
        assert!(!PgStatUserIndexesCollector::permits_fallback(
            &sqlx::Error::Protocol("lost connection".into())
        ));
    }
}
