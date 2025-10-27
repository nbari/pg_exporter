use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{GaugeVec, IntGaugeVec, Opts, Registry};
use sqlx::{PgPool, Row};
use tracing::{debug, info_span, instrument, warn};
use tracing_futures::Instrument as _;

/// PgStatementsCollector tracks pg_stat_statements metrics
///
/// Collects query performance statistics including:
/// - Execution time (total, mean, max, stddev)
/// - Call frequency and row counts
/// - I/O metrics (cache hits/misses, disk reads/writes)
/// - Temp file usage (queries spilling to disk)
/// - WAL generation per query
/// - Cache hit ratios
///
/// This collector exposes the top N queries by total execution time
/// to provide actionable insights for Database Reliability Engineers.
#[derive(Clone)]
pub struct PgStatementsCollector {
    // Execution time metrics (most important for DBREs)
    total_exec_time: GaugeVec,       // {queryid, datname, usename, query_short}
    mean_exec_time: GaugeVec,        // {queryid, datname, usename, query_short}
    max_exec_time: GaugeVec,         // {queryid, datname, usename, query_short}
    stddev_exec_time: GaugeVec,      // {queryid, datname, usename, query_short}
    
    // Call frequency metrics
    calls: IntGaugeVec,              // {queryid, datname, usename, query_short}
    rows: IntGaugeVec,               // {queryid, datname, usename, query_short}
    
    // I/O metrics (critical for performance analysis)
    shared_blks_hit: IntGaugeVec,    // {queryid, datname, usename, query_short} - cache hits
    shared_blks_read: IntGaugeVec,   // {queryid, datname, usename, query_short} - disk reads
    shared_blks_dirtied: IntGaugeVec, // {queryid, datname, usename, query_short}
    shared_blks_written: IntGaugeVec, // {queryid, datname, usename, query_short}
    
    // Local I/O (temp tables)
    local_blks_hit: IntGaugeVec,     // {queryid, datname, usename, query_short}
    local_blks_read: IntGaugeVec,    // {queryid, datname, usename, query_short}
    local_blks_dirtied: IntGaugeVec, // {queryid, datname, usename, query_short}
    local_blks_written: IntGaugeVec, // {queryid, datname, usename, query_short}
    
    // Temp file usage (queries spilling to disk - often indicates memory issues)
    temp_blks_read: IntGaugeVec,     // {queryid, datname, usename, query_short}
    temp_blks_written: IntGaugeVec,  // {queryid, datname, usename, query_short}
    
    // WAL generation (write-heavy queries)
    wal_bytes: IntGaugeVec,          // {queryid, datname, usename, query_short} - PG13+
    
    // Cache hit ratio (derived metric)
    cache_hit_ratio: GaugeVec,       // {queryid, datname, usename, query_short}
    
    // Top N tracking limit
    top_n: usize,
}

impl Default for PgStatementsCollector {
    fn default() -> Self {
        Self::with_top_n(100) // Track top 100 queries by default
    }
}

impl PgStatementsCollector {
    /// Create a new pg_statements collector with default top_n = 100
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new pg_statements collector
    ///
    /// # Arguments
    /// * `top_n` - Number of top queries to track (default: 100)
    ///   - Too low: Miss important queries
    ///   - Too high: High cardinality, expensive to scrape
    ///   - Recommended: 50-200 for production
    pub fn with_top_n(top_n: usize) -> Self {
        let labels = vec!["queryid", "datname", "usename", "query_short"];
        
        let total_exec_time = GaugeVec::new(
            Opts::new(
                "pg_stat_statements_total_exec_time_seconds",
                "Total time spent executing this query (seconds)"
            )
            .namespace("postgres"),
            &labels,
        )
        .expect("pg_stat_statements_total_exec_time_seconds");

        let mean_exec_time = GaugeVec::new(
            Opts::new(
                "pg_stat_statements_mean_exec_time_seconds",
                "Mean time per execution (seconds) - key for finding slow queries"
            )
            .namespace("postgres"),
            &labels,
        )
        .expect("pg_stat_statements_mean_exec_time_seconds");

        let max_exec_time = GaugeVec::new(
            Opts::new(
                "pg_stat_statements_max_exec_time_seconds",
                "Maximum execution time observed (seconds)"
            )
            .namespace("postgres"),
            &labels,
        )
        .expect("pg_stat_statements_max_exec_time_seconds");

        let stddev_exec_time = GaugeVec::new(
            Opts::new(
                "pg_stat_statements_stddev_exec_time_seconds",
                "Standard deviation of execution time - high value indicates inconsistent performance"
            )
            .namespace("postgres"),
            &labels,
        )
        .expect("pg_stat_statements_stddev_exec_time_seconds");

        let calls = IntGaugeVec::new(
            Opts::new(
                "pg_stat_statements_calls_total",
                "Number of times this query has been executed"
            )
            .namespace("postgres"),
            &labels,
        )
        .expect("pg_stat_statements_calls_total");

        let rows = IntGaugeVec::new(
            Opts::new(
                "pg_stat_statements_rows_total",
                "Total number of rows retrieved or affected by this query"
            )
            .namespace("postgres"),
            &labels,
        )
        .expect("pg_stat_statements_rows_total");

        let shared_blks_hit = IntGaugeVec::new(
            Opts::new(
                "pg_stat_statements_shared_blks_hit_total",
                "Shared block cache hits (found in memory)"
            )
            .namespace("postgres"),
            &labels,
        )
        .expect("pg_stat_statements_shared_blks_hit_total");

        let shared_blks_read = IntGaugeVec::new(
            Opts::new(
                "pg_stat_statements_shared_blks_read_total",
                "Shared blocks read from disk (cache miss - expensive!)"
            )
            .namespace("postgres"),
            &labels,
        )
        .expect("pg_stat_statements_shared_blks_read_total");

        let shared_blks_dirtied = IntGaugeVec::new(
            Opts::new(
                "pg_stat_statements_shared_blks_dirtied_total",
                "Shared blocks dirtied (modified)"
            )
            .namespace("postgres"),
            &labels,
        )
        .expect("pg_stat_statements_shared_blks_dirtied_total");

        let shared_blks_written = IntGaugeVec::new(
            Opts::new(
                "pg_stat_statements_shared_blks_written_total",
                "Shared blocks written to disk"
            )
            .namespace("postgres"),
            &labels,
        )
        .expect("pg_stat_statements_shared_blks_written_total");

        let local_blks_hit = IntGaugeVec::new(
            Opts::new(
                "pg_stat_statements_local_blks_hit_total",
                "Local block cache hits (temp tables)"
            )
            .namespace("postgres"),
            &labels,
        )
        .expect("pg_stat_statements_local_blks_hit_total");

        let local_blks_read = IntGaugeVec::new(
            Opts::new(
                "pg_stat_statements_local_blks_read_total",
                "Local blocks read from disk (temp tables)"
            )
            .namespace("postgres"),
            &labels,
        )
        .expect("pg_stat_statements_local_blks_read_total");

        let local_blks_dirtied = IntGaugeVec::new(
            Opts::new(
                "pg_stat_statements_local_blks_dirtied_total",
                "Local blocks dirtied (temp tables)"
            )
            .namespace("postgres"),
            &labels,
        )
        .expect("pg_stat_statements_local_blks_dirtied_total");

        let local_blks_written = IntGaugeVec::new(
            Opts::new(
                "pg_stat_statements_local_blks_written_total",
                "Local blocks written to disk (temp tables)"
            )
            .namespace("postgres"),
            &labels,
        )
        .expect("pg_stat_statements_local_blks_written_total");

        let temp_blks_read = IntGaugeVec::new(
            Opts::new(
                "pg_stat_statements_temp_blks_read_total",
                "Temp file blocks read - query spilled to disk (work_mem too small!)"
            )
            .namespace("postgres"),
            &labels,
        )
        .expect("pg_stat_statements_temp_blks_read_total");

        let temp_blks_written = IntGaugeVec::new(
            Opts::new(
                "pg_stat_statements_temp_blks_written_total",
                "Temp file blocks written - query spilled to disk (work_mem too small!)"
            )
            .namespace("postgres"),
            &labels,
        )
        .expect("pg_stat_statements_temp_blks_written_total");

        let wal_bytes = IntGaugeVec::new(
            Opts::new(
                "pg_stat_statements_wal_bytes_total",
                "WAL bytes generated by this query (PostgreSQL 13+)"
            )
            .namespace("postgres"),
            &labels,
        )
        .expect("pg_stat_statements_wal_bytes_total");

        let cache_hit_ratio = GaugeVec::new(
            Opts::new(
                "pg_stat_statements_cache_hit_ratio",
                "Cache hit ratio for this query (0.0-1.0, higher is better)"
            )
            .namespace("postgres"),
            &labels,
        )
        .expect("pg_stat_statements_cache_hit_ratio");

        Self {
            total_exec_time,
            mean_exec_time,
            max_exec_time,
            stddev_exec_time,
            calls,
            rows,
            shared_blks_hit,
            shared_blks_read,
            shared_blks_dirtied,
            shared_blks_written,
            local_blks_hit,
            local_blks_read,
            local_blks_dirtied,
            local_blks_written,
            temp_blks_read,
            temp_blks_written,
            wal_bytes,
            cache_hit_ratio,
            top_n,
        }
    }

    /// Truncate query text for labels (avoid high cardinality)
    fn truncate_query(query: &str, max_len: usize) -> String {
        let cleaned = query
            .trim()
            .lines()
            .map(|l| l.trim())
            .collect::<Vec<_>>()
            .join(" ");
        
        if cleaned.len() <= max_len {
            cleaned
        } else {
            format!("{}...", &cleaned[..max_len])
        }
    }
}

impl Collector for PgStatementsCollector {
    fn name(&self) -> &'static str {
        "pg_statements"
    }

    #[instrument(
        skip(self, registry),
        level = "info",
        err,
        fields(collector = "pg_statements")
    )]
    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.total_exec_time.clone()))?;
        registry.register(Box::new(self.mean_exec_time.clone()))?;
        registry.register(Box::new(self.max_exec_time.clone()))?;
        registry.register(Box::new(self.stddev_exec_time.clone()))?;
        registry.register(Box::new(self.calls.clone()))?;
        registry.register(Box::new(self.rows.clone()))?;
        registry.register(Box::new(self.shared_blks_hit.clone()))?;
        registry.register(Box::new(self.shared_blks_read.clone()))?;
        registry.register(Box::new(self.shared_blks_dirtied.clone()))?;
        registry.register(Box::new(self.shared_blks_written.clone()))?;
        registry.register(Box::new(self.local_blks_hit.clone()))?;
        registry.register(Box::new(self.local_blks_read.clone()))?;
        registry.register(Box::new(self.local_blks_dirtied.clone()))?;
        registry.register(Box::new(self.local_blks_written.clone()))?;
        registry.register(Box::new(self.temp_blks_read.clone()))?;
        registry.register(Box::new(self.temp_blks_written.clone()))?;
        registry.register(Box::new(self.wal_bytes.clone()))?;
        registry.register(Box::new(self.cache_hit_ratio.clone()))?;

        debug!(collector = "pg_statements", "registered metrics");
        Ok(())
    }

    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(
            async move {
                // Check if pg_stat_statements extension is available
                let ext_check = sqlx::query(
                    "SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'"
                )
                .fetch_optional(pool)
                .await?;

                if ext_check.is_none() {
                    warn!(
                        collector = "pg_statements",
                        "pg_stat_statements extension not installed - skipping collection"
                    );
                    return Ok(());
                }

                // Query top N queries by total execution time
                // This is what DBREs care about most: "What queries are consuming database time?"
                //
                // IMPORTANT: All numeric columns are explicitly cast to prevent type mismatches.
                // PostgreSQL's pg_stat_statements uses NUMERIC type for many columns, but Rust
                // expects i64/f64. Without ::bigint or ::double precision casts, we get:
                // "mismatched types; Rust type `i64` is not compatible with SQL type `NUMERIC`"
                // This caused production panics - never remove these casts!
                let query = format!(
                    r#"
                    SELECT
                        queryid::text,
                        d.datname,
                        u.usename,
                        LEFT(query, 80) as query_short,
                        calls::bigint,
                        (total_exec_time / 1000.0)::double precision as total_exec_time_sec,
                        (mean_exec_time / 1000.0)::double precision as mean_exec_time_sec,
                        (max_exec_time / 1000.0)::double precision as max_exec_time_sec,
                        (stddev_exec_time / 1000.0)::double precision as stddev_exec_time_sec,
                        rows::bigint,
                        shared_blks_hit::bigint,
                        shared_blks_read::bigint,
                        shared_blks_dirtied::bigint,
                        shared_blks_written::bigint,
                        local_blks_hit::bigint,
                        local_blks_read::bigint,
                        local_blks_dirtied::bigint,
                        local_blks_written::bigint,
                        temp_blks_read::bigint,
                        temp_blks_written::bigint,
                        COALESCE(wal_bytes, 0)::bigint as wal_bytes
                    FROM pg_stat_statements s
                    JOIN pg_database d ON d.oid = s.dbid
                    JOIN pg_user u ON u.usesysid = s.userid
                    WHERE queryid IS NOT NULL
                      AND total_exec_time > 0
                      AND d.datname NOT IN ('template0', 'template1')
                    ORDER BY total_exec_time DESC
                    LIMIT {}
                    "#,
                    self.top_n
                );

                let rows = sqlx::query(&query).fetch_all(pool).await?;
                let row_count = rows.len();

                for row in rows {
                    // SAFETY: Use try_get() instead of get() to handle NULL values gracefully.
                    // Utility statements (VACUUM, ANALYZE) can have NULL query text.
                    // Using get() would panic, causing production crashes.
                    let queryid: String = row.try_get("queryid").unwrap_or_else(|_| "unknown".to_string());
                    let datname: String = row.try_get("datname").unwrap_or_else(|_| "unknown".to_string());
                    let usename: String = row.try_get("usename").unwrap_or_else(|_| "unknown".to_string());
                    
                    // Handle NULL query text (occurs with utility statements like VACUUM)
                    let query_text: Option<String> = row.try_get("query_short").ok();
                    let query_short = match query_text {
                        Some(q) => Self::truncate_query(&q, 80),
                        None => "<utility>".to_string(), // Fallback for NULL queries
                    };
                    
                    let labels = &[
                        queryid.as_str(),
                        datname.as_str(),
                        usename.as_str(),
                        query_short.as_str(),
                    ];

                    // SAFETY: try_get() with unwrap_or() prevents panics on NULL or type mismatches.
                    // SQL query already casts to ::double precision, but we handle edge cases here.
                    let total_time: f64 = row.try_get("total_exec_time_sec").unwrap_or(0.0);
                    let mean_time: f64 = row.try_get("mean_exec_time_sec").unwrap_or(0.0);
                    let max_time: f64 = row.try_get("max_exec_time_sec").unwrap_or(0.0);
                    let stddev_time: f64 = row.try_get("stddev_exec_time_sec").unwrap_or(0.0);
                    
                    self.total_exec_time.with_label_values(labels).set(total_time);
                    self.mean_exec_time.with_label_values(labels).set(mean_time);
                    self.max_exec_time.with_label_values(labels).set(max_time);
                    self.stddev_exec_time.with_label_values(labels).set(stddev_time);

                    // SAFETY: Default to 0 for NULL values (though SQL casts should prevent this)
                    let calls: i64 = row.try_get("calls").unwrap_or(0);
                    let rows_count: i64 = row.try_get("rows").unwrap_or(0);
                    self.calls.with_label_values(labels).set(calls);
                    self.rows.with_label_values(labels).set(rows_count);

                    // I/O metrics - all use try_get() for safety
                    let shared_hit: i64 = row.try_get("shared_blks_hit").unwrap_or(0);
                    let shared_read: i64 = row.try_get("shared_blks_read").unwrap_or(0);
                    let shared_dirtied: i64 = row.try_get("shared_blks_dirtied").unwrap_or(0);
                    let shared_written: i64 = row.try_get("shared_blks_written").unwrap_or(0);
                    
                    self.shared_blks_hit.with_label_values(labels).set(shared_hit);
                    self.shared_blks_read.with_label_values(labels).set(shared_read);
                    self.shared_blks_dirtied.with_label_values(labels).set(shared_dirtied);
                    self.shared_blks_written.with_label_values(labels).set(shared_written);

                    let local_hit: i64 = row.try_get("local_blks_hit").unwrap_or(0);
                    let local_read: i64 = row.try_get("local_blks_read").unwrap_or(0);
                    let local_dirtied: i64 = row.try_get("local_blks_dirtied").unwrap_or(0);
                    let local_written: i64 = row.try_get("local_blks_written").unwrap_or(0);
                    
                    self.local_blks_hit.with_label_values(labels).set(local_hit);
                    self.local_blks_read.with_label_values(labels).set(local_read);
                    self.local_blks_dirtied.with_label_values(labels).set(local_dirtied);
                    self.local_blks_written.with_label_values(labels).set(local_written);

                    let temp_read: i64 = row.try_get("temp_blks_read").unwrap_or(0);
                    let temp_written: i64 = row.try_get("temp_blks_written").unwrap_or(0);
                    self.temp_blks_read.with_label_values(labels).set(temp_read);
                    self.temp_blks_written.with_label_values(labels).set(temp_written);

                    // WAL bytes available in PostgreSQL 13+
                    let wal: i64 = row.try_get("wal_bytes").unwrap_or(0);
                    self.wal_bytes.with_label_values(labels).set(wal);

                    // SAFETY: Check denominator before division to prevent division by zero.
                    // If no blocks were accessed, we consider it a 100% hit rate (no misses).
                    let total_blocks = shared_hit + shared_read;
                    let hit_ratio = if total_blocks > 0 {
                        shared_hit as f64 / total_blocks as f64
                    } else {
                        1.0 // No blocks accessed = 100% hit (avoid division by zero)
                    };
                    self.cache_hit_ratio.with_label_values(labels).set(hit_ratio);
                }

                debug!(
                    collector = "pg_statements",
                    queries_tracked = row_count,
                    "collected pg_stat_statements metrics"
                );

                Ok(())
            }
            .instrument(info_span!("pg_statements.collect")),
        )
    }

    fn enabled_by_default(&self) -> bool {
        false // Disabled by default - requires extension
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pg_statements_collector_name() {
        let collector = PgStatementsCollector::new();
        assert_eq!(collector.name(), "pg_statements");
    }

    #[test]
    fn test_pg_statements_collector_not_enabled_by_default() {
        let collector = PgStatementsCollector::new();
        assert!(!collector.enabled_by_default());
    }

    #[test]
    fn test_truncate_query() {
        let short = "SELECT * FROM users";
        assert_eq!(PgStatementsCollector::truncate_query(short, 80), short);

        let long = "SELECT * FROM users WHERE id = 1 AND name = 'test' AND email = 'test@example.com' AND created_at > NOW()";
        let truncated = PgStatementsCollector::truncate_query(long, 80);
        assert_eq!(truncated.len(), 83); // 80 + "..."
        assert!(truncated.ends_with("..."));
    }

    #[test]
    fn test_truncate_query_multiline() {
        let multiline = "SELECT *\n  FROM users\n  WHERE id = 1";
        let result = PgStatementsCollector::truncate_query(multiline, 80);
        assert_eq!(result, "SELECT * FROM users WHERE id = 1");
    }
}
