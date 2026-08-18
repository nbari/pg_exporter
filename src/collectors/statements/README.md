# pg_stat_statements Collector

The `statements` collector tracks query performance metrics from PostgreSQL's `pg_stat_statements` extension. It's one of the most powerful tools for identifying and optimizing slow queries in production.

`pg_exporter` supports PostgreSQL 14 and newer, so all metrics documented here assume PostgreSQL 14+.

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

By default, it tracks the **top 25 queries** by total execution time.

Configure the number of queries to track:

```bash
# Track top 10 queries
pg_exporter --dsn postgresql://... --collector.statements --statements.top-n=10

# Environment variable form
PG_EXPORTER_STATEMENTS_TOP_N=50 pg_exporter --dsn postgresql://... --collector.statements
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
- `pg_stat_statements_wal_bytes_total` - WAL generation

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
- `query_short` - First 80 characters of the query (or `<unknown>` when the text has not been resolved yet, see [How `query_short` is resolved](#how-query_short-is-resolved))

`query_short` is intentionally capped at 80 characters to keep Prometheus label
cardinality and label size under control. It is meant for fast identification in
Prometheus and Grafana, not as a full SQL text export.

When you need the full normalized statement text, use the `queryid` label from
the metric and query `pg_stat_statements` directly.

#### How `query_short` is resolved

The scrape does **not wait** for query texts (see
[Scrape cost and query texts](#scrape-cost-and-query-texts)). They come from a
single-flight background lookup that runs only when a newly exported `queryid` has
no cached text, and never more often than `--statements.query-text-refresh`
(default `900` seconds, env `PG_EXPORTER_STATEMENTS_QUERY_TEXT_REFRESH`). The first
missing text starts a lookup immediately; 900 seconds is the minimum interval
between later attempts, not a startup delay. A `queryid` always maps to the same
normalized text, so cached entries never go stale and the cache is bounded by the
number of exported statements.

A statement whose text has not been resolved yet is labelled `<unknown>`; a scrape
after the background task finishes normally fills it in. A statement first seen
just after a lookup may remain unresolved until the next 900-second window. Numeric
statement metrics still refresh on every scrape. PostgreSQL sometimes has no text
at all for an entry — utility statements, or texts dropped by `pg_stat_statements`
garbage collection. Those results are never cached, so the label stays `<unknown>`
and the lookup is retried later instead of pinning an empty label forever. Setting
the flag to `0` disables text lookups entirely — `query_short` then stays `<unknown>`
and SQL must be looked up by `queryid` directly in `pg_stat_statements`.

The lookup itself is the one place that still has to call `pg_stat_statements(true)`,
which materializes every query text into a `work_mem`-bounded tuplestore. To keep it
from writing the very temp files this collector was fixed to avoid, the lookup runs
in its own transaction with `SET LOCAL work_mem` raised (64 MB) for the duration of
that one statement. Nothing else in the session is affected. If the server refuses
the `SET`, the exporter warns once and the background lookup still runs with the
session default, where it may spill. A corpus larger than 64 MB may also spill. The
lookup is not awaited by the scrape, but it still shares the PostgreSQL connection
pool and database I/O, so it can indirectly contend with scrape queries.

The collector excludes its own statements from the exported series without ever
comparing query text on the scrape path. On PostgreSQL 14+ the scrape query reads
its own `pg_stat_activity.query_id` while it is executing — that value is exactly
the `queryid` `pg_stat_statements` records for it — so self-exclusion works on every
scrape, including with `--statements.query-text-refresh=0`. On PostgreSQL 12 and 13
`query_id` does not exist, so the collector falls back to learning its own `queryid`
from the marker comment during the text lookup; with the lookup disabled on those
versions its own queries may appear in the metrics.

Example:

```sql
SELECT
    queryid::text,
    d.datname,
    r.rolname,
    s.query
FROM pg_stat_statements s
JOIN pg_database d ON d.oid = s.dbid
LEFT JOIN pg_roles r ON r.oid = s.userid
WHERE queryid::text = '<queryid-from-metric>';
```

If you want to narrow the search further, also filter by `datname`:

```sql
SELECT
    queryid::text,
    d.datname,
    r.rolname,
    s.query
FROM pg_stat_statements s
JOIN pg_database d ON d.oid = s.dbid
LEFT JOIN pg_roles r ON r.oid = s.userid
WHERE queryid::text = '<queryid-from-metric>'
  AND d.datname = '<database-from-metric>';
```

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

Utility statements (VACUUM, ANALYZE, CREATE INDEX, etc.) are tracked like any other
statement, but PostgreSQL does not always keep their text. When it does not, the
`query_short` label stays `<unknown>`.

### Statement Selection and Cardinality

The collector exports the **deduplicated union** of two rankings:

1. the top N by `total_exec_time`, and
2. the top N by `temp_blks_written`,

where N is `--statements.top-n` (default `25`).

The second ranking exists because a query can write gigabytes of temporary files
without being one of the slowest statements. Ranking by execution time alone made
such a query disappear from the temp metrics entirely, which is exactly the
statement you need during a temp-disk incident.

**Maximum series count: `2 x --statements.top-n` per metric.** A statement present
in both rankings is exported once, so the real count is usually well below the
maximum.

- Long-running infrequent queries appear via the execution-time ranking
- Fast but frequent queries also appear if their total time is high
- Heavy spillers appear via the temp ranking, regardless of their execution time
- Adjust `--statements.top-n` based on your query diversity and scrape budget

#### Only top-level statements are exported

`pg_stat_statements` is keyed by `(userid, dbid, queryid, toplevel)`. With
`pg_stat_statements.track = all`, a statement executed both directly *and* from inside a
function or procedure gets **two** entries that share `(userid, dbid, queryid)` and carry
different counters. The exported label set — `{queryid, datname, usename, query_short}` —
has no `toplevel` component, so both entries would collapse onto a single Prometheus
series where one silently overwrites the other, and each duplicate would also consume a
slot of the `2 x top-n` budget.

The scrape query therefore selects only `toplevel` entries. The trade-off: under
`track = all` a statement that is *only* ever called from inside a function is no longer
exported on its own — its cost is still counted in the top-level statement that invoked
it. Under the default `track = top` nothing changes, because nested statements are never
recorded in the first place.

The column arrived in `pg_stat_statements` 1.9 (PostgreSQL 14). On older extension
versions the filter is omitted, since `track = all` there cannot produce the duplicate in
the first place.

Convert temp blocks to bytes with `pg_settings_block_size_bytes` (from
`--collector.default`):

```promql
rate(postgres_pg_stat_statements_temp_blks_written_total[5m])
  * on(instance, job) group_left() pg_settings_block_size_bytes
```

These counters are **cumulative** and only update when a statement *finishes*. To
see temporary files that are on disk **right now**, including those of a query that
is still running, enable the [`temp` collector](../temp/README.md).

### Performance Impact

`pg_stat_statements` has minimal overhead (typically <1% CPU). However:
- Higher `pg_stat_statements.max` values use more memory
- The collector queries `pg_stat_statements` on each scrape
- For high-traffic databases, consider longer scrape intervals

#### Scrape cost and query texts

The per-scrape query reads `pg_stat_statements(false)` — that is, **without** query
texts — and resolves the `query_short` label from the cache. Missing texts schedule
a separate, rate-limited background lookup; the scrape never waits for it.

This is not a micro-optimization. `pg_stat_statements` is a set-returning function:
with `showtext = true` it loads the entire query-text file into backend memory and
materializes **every** row, text included, into a `work_mem`-bounded tuplestore
*before* any filter, join or `LIMIT` runs. On an instance with ~10k entries and tens
of megabytes of query text, every single scrape wrote a multi-megabyte file into
`base/pgsql_tmp`. Moving the ranking into a CTE does not help, because the
materialization happens below every planner node — only `showtext = false` avoids it.

Measured on a seeded instance (9002 entries, 8365 kB of text, `work_mem = 4MB`):

| Query | Temp blocks written | Duration |
| --- | ---: | ---: |
| Reading `pg_stat_statements` (`showtext = true`) | 1506 | 38.8 ms |
| Same, with the ranking moved into a CTE | 3012 | 56.9 ms |
| Reading `pg_stat_statements(false)` (current) | **0** | **7.8 ms** |

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

### Query Text Shows as `<unknown>`

`query_short` is `<unknown>` until the text lookup resolves it. That is expected:

- On the scrape where a statement first enters the top-N. The first lookup starts
  immediately in the background; later attempts are limited by
  `--statements.query-text-refresh` (see [How `query_short` is resolved](#how-query_short-is-resolved)).
- Permanently, when PostgreSQL has no text for the entry — utility statements, or
  entries whose text was dropped by `pg_stat_statements` garbage collection.

Missing texts are never cached, so a statement that gains a text later is picked up on
a subsequent lookup. Setting `--statements.query-text-refresh=0` disables the lookup
entirely, leaving every label `<unknown>`.

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
