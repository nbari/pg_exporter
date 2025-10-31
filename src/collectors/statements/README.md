# pg_stat_statements Collector

The `statements` collector tracks query performance metrics from PostgreSQL's `pg_stat_statements` extension. It's one of the most powerful tools for identifying and optimizing slow queries in production.

## Why This Matters

- **Find slow queries during incidents** - "What query is causing high load?"
- **Detect N+1 query problems** - Before they scale and impact production
- **Identify performance regressions** - After deployments or configuration changes
- **Optimize based on real data** - Use actual production query patterns, not guesses
- **Track resource-intensive queries** - I/O, WAL generation, temp files

This collector complements other collectors:
- **`default`** - System-wide metrics (cache hit ratio, checkpoints, connections)
- **`stat.user_tables`** - Table-level metrics (bloat, vacuum, DML rates)
- **`statements`** - Query-level metrics (execution time, frequency, I/O)

Together, they provide complete visibility from system → table → query level.

## Prerequisites

### Enable the Extension

```sql
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
```

### Configure PostgreSQL

Add to `postgresql.conf`:

```ini
shared_preload_libraries = 'pg_stat_statements'
pg_stat_statements.track = all
pg_stat_statements.max = 10000
```

Restart PostgreSQL after modifying `postgresql.conf`.

## Usage

Enable the collector:

```bash
pg_exporter --dsn postgresql:///postgres?user=postgres_exporter --collector.statements
```

By default, it tracks the **top 100 queries** by total execution time.

Configure the number of queries to track:

```bash
# Track top 50 queries
pg_exporter --dsn postgresql://... --collector.statements --statements.top-n=50
```

## Key Metrics

### Execution Time
- `pg_stat_statements_total_exec_time_seconds` - Total time spent in this query
- `pg_stat_statements_mean_exec_time_seconds` - Average time per execution
- `pg_stat_statements_max_exec_time_seconds` - Slowest execution
- `pg_stat_statements_stddev_exec_time_seconds` - Execution time variance

### Frequency
- `pg_stat_statements_calls_total` - How many times the query was executed
- `pg_stat_statements_rows_total` - Total rows returned/affected

### I/O Metrics
- `pg_stat_statements_shared_blks_hit_total` - Cache hits (fast)
- `pg_stat_statements_shared_blks_read_total` - Disk reads (slow)
- `pg_stat_statements_temp_blks_written_total` - Queries spilling to disk
- `pg_stat_statements_cache_hit_ratio` - Query cache effectiveness (0.0-1.0)

### Resource Usage
- `pg_stat_statements_wal_bytes_total` - WAL generation (PostgreSQL 13+)

## Use Cases

### 1. Finding Slow Queries

PromQL query to find queries with highest total time:

```promql
topk(10, 
  pg_stat_statements_total_exec_time_seconds
)
```

### 2. Identifying N+1 Problems

Queries executed many times with low row counts:

```promql
pg_stat_statements_calls_total > 1000
and
pg_stat_statements_rows_total / pg_stat_statements_calls_total < 10
```

### 3. Cache Miss Detection

Queries with poor cache hit ratios:

```promql
pg_stat_statements_cache_hit_ratio < 0.9
and
pg_stat_statements_calls_total > 100
```

### 4. Temp File Usage

Queries writing to disk (needs more work_mem):

```promql
rate(pg_stat_statements_temp_blks_written_total[5m]) > 0
```

### 5. Performance Regression Detection

Alert on queries getting slower:

```promql
increase(pg_stat_statements_mean_exec_time_seconds[1h]) > 0.5
```

## Labels

All metrics include these labels:

- `queryid` - Unique query identifier
- `datname` - Database name
- `usename` - User/role name
- `query_short` - First 80 characters of the query (or `<utility>` for VACUUM/ANALYZE)

## Important Notes

### Query Text Normalization

PostgreSQL normalizes queries by replacing constants with placeholders:

```sql
-- These are the same query:
SELECT * FROM users WHERE id = 1;
SELECT * FROM users WHERE id = 2;

-- Tracked as:
SELECT * FROM users WHERE id = $1;
```

### Utility Statements

Utility statements (VACUUM, ANALYZE, CREATE INDEX, etc.) may appear as `<utility>` in the `query_short` label since PostgreSQL doesn't always track their full text.

### Top N Queries

The collector tracks the top N queries **by total execution time**. This means:
- Long-running infrequent queries appear at the top
- Fast but frequent queries also appear if their total time is high
- Adjust `--statements.top-n` based on your query diversity

### Performance Impact

`pg_stat_statements` has minimal overhead (typically <1% CPU). However:
- Higher `pg_stat_statements.max` values use more memory
- The collector queries `pg_stat_statements` on each scrape
- For high-traffic databases, consider longer scrape intervals

## Troubleshooting

### Extension Not Found

```
ERROR: extension "pg_stat_statements" is not available
```

**Solution**: Install the extension package:
```bash
# Debian/Ubuntu
apt-get install postgresql-contrib

# RHEL/CentOS
yum install postgresql-contrib
```

### No Metrics Appear

**Possible causes**:
1. Extension not loaded - Check `SHOW shared_preload_libraries;`
2. Extension not created - Run `CREATE EXTENSION pg_stat_statements;`
3. No queries executed yet - Run some queries to populate stats
4. Collector not enabled - Use `--collector.statements`

### Query Text Shows as NULL or `<utility>`

This is normal for:
- Utility statements (VACUUM, ANALYZE, etc.)
- Queries from other monitoring tools
- Internal PostgreSQL operations

## Best Practices

1. **Enable in production** - Query-level insights are essential for troubleshooting
2. **Monitor the top 50-100 queries** - Balance coverage vs cardinality
3. **Reset stats after major changes** - `SELECT pg_stat_statements_reset();` after schema migrations
4. **Set appropriate scrape intervals** - 30-60 seconds is usually sufficient
5. **Combine with other collectors** - Use `activity`, `stat`, and `vacuum` collectors together for complete visibility

## Example Grafana Dashboard

Track your slowest queries:

```promql
# Panel 1: Top 10 Slowest Queries (by total time)
topk(10, pg_stat_statements_total_exec_time_seconds)

# Panel 2: Most Called Queries
topk(10, rate(pg_stat_statements_calls_total[5m]))

# Panel 3: Cache Hit Ratio Heatmap
pg_stat_statements_cache_hit_ratio

# Panel 4: Queries Writing Temp Files
sum by (query_short) (
  rate(pg_stat_statements_temp_blks_written_total[5m])
)
```

## References

- [PostgreSQL pg_stat_statements documentation](https://www.postgresql.org/docs/current/pgstatstatements.html)
- [Source code](pg_statements.rs)
- [Integration tests](../../tests/collectors/statements/pg_statements.rs)
