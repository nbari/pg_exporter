# Diagnosing high CPU, blocking, and missing indexes

This guide explains how to use `pg_exporter` metrics to root-cause the three most
common "the database is oversaturated" scenarios — especially on instances that do
**not** use a connection pooler such as pgbouncer:

1. [High CPU usage](#1-high-cpu-usage)
2. [Blocking / lock contention](#2-blocking--lock-contention)
3. [Missing indexes](#3-missing-indexes)
4. [Connection saturation without pgbouncer](#4-connection-saturation-without-pgbouncer)

The matching Grafana panels ship in `grafana/dashboard.json` (rows **CPU Pressure**,
**Locks & Blocking**, and **Table Statistics**).

> All PromQL below assumes the dashboard variables `$job`, `$instance`, and
> `$database`. Drop those label filters if you query Prometheus directly.

## Drill-down quick reference (`just` recipes)

Metrics/panels alert you *that* there's a problem and *how bad*; these on-demand
queries show *which* session/query/table so you can act. Each honors `PG_HOST`/
`PG_PORT` (host or devcontainer) and its SQL lives in `scripts/`.

| Command | Shows | Pairs with panel |
| --- | --- | --- |
| `just blocking` | who is blocking whom (PID + query) | Longest Lock Wait / Blocked Sessions |
| `just on-cpu` | queries running on CPU right now | On-CPU Backends |
| `just long-running` | long-running + idle-in-transaction sessions | Long-Running Query Age / Idle in Transaction |
| `just seq-scans` | tables likely missing an index | Avg Rows per Seq Scan / Seq Scan Rate |
| `just connections` | connections by db/user/app/state | Connection Pool Utilization |
| `just bloat` | top dead-tuple / repack candidates | Vacuum & Bloat Pressure |
| `just top-queries` | top queries by total exec time | Query Performance |

---

## 1. High CPU usage

PostgreSQL does not expose per-backend CPU%, so use two complementary proxies.

### On-CPU backends (the fastest signal)

`pg_stat_activity_on_cpu_backends` counts **active** client backends that are **not
waiting on anything** (`state = 'active' AND wait_event IS NULL`) — i.e. backends
actually burning CPU right now. Compare the total to the instance vCPU count:

```promql
sum(pg_stat_activity_on_cpu_backends{job="$job", instance="$instance", datname=~"$database"})
```

* Total **< vCPUs** → CPU has headroom; a slowdown is more likely I/O or locks.
* Total **>= vCPUs**, sustained → CPU saturation: the run queue is building and every
  query gets slower. Look for the queries below.

Requires `--collector.activity` (enabled by default).

### Which queries are on CPU right now (root cause)

The metric tells you *how many* backends are on CPU; to see *which* queries, drill
into PostgreSQL — run `just on-cpu` (uses
[`scripts/on-cpu-queries.sql`](../scripts/on-cpu-queries.sql)), or directly:

```sql
SELECT pid, datname, round(extract(epoch FROM now() - query_start), 1) AS running_s,
       left(query, 80) AS query
FROM pg_stat_activity
WHERE state = 'active' AND wait_event IS NULL
  AND backend_type = 'client backend' AND pid <> pg_backend_pid()
ORDER BY query_start;
```

Then act: `SELECT pg_cancel_backend(<pid>);` (gentle) or `pg_terminate_backend(<pid>)`,
or fix the cause (a missing index causing seq scans, an unbounded query, too much
concurrency without a pooler). As with blocking, PID/query text are intentionally not
exported as metrics — the panel is for alerting/trends, this query is the drill-down.

### Which queries burn the CPU (cumulative)

`pg_stat_statements` exposes `total_exec_time` — the best per-query CPU proxy.
Top consumers by recent execution time:

```promql
topk(10, rate(pg_stat_statements_total_exec_time_seconds{job="$job", instance="$instance"}[5m]))
```

### CPU-bound vs I/O-bound

A query with high execution time **and** a high cache-hit ratio is CPU-bound (it is
not waiting on disk). A low cache-hit ratio means it is I/O-bound instead:

```promql
pg_stat_statements_shared_blks_hit_total
/
clamp_min(pg_stat_statements_shared_blks_hit_total + pg_stat_statements_shared_blks_read_total, 1)
```

CPU-bound queries are usually fixed by a better plan (often a [missing
index](#3-missing-indexes)), less data scanned, or query rewriting. Requires
`--collector.statements`.

---

## 2. Blocking / lock contention

A few queries holding locks can stall the whole database. The `locks` collector
exposes who is blocked, who is blocking, and for how long.

| Metric | Meaning |
| --- | --- |
| `pg_blocked_sessions{datname}` | Sessions currently **waiting** on a lock held by another session. |
| `pg_blocking_sessions{datname}` | Distinct sessions that are **blocking** at least one other session. |
| `pg_longest_blocked_seconds{datname}` | Age of the longest-waiting blocked session (`now() - query_start`). |
| `pg_lock_waits{datname,mode}` | **Ungranted** locks by lock mode — shows *which* lock type is contended. |

Quick triage:

```promql
# Are sessions stuck waiting on locks right now?
sum(pg_blocked_sessions{job="$job", instance="$instance", datname=~"$database"})

# How long has the worst one been waiting? (alert when this stays high)
max(pg_longest_blocked_seconds{job="$job", instance="$instance", datname=~"$database"})

# What kind of lock is contended? (e.g. AccessExclusiveLock from DDL, RowExclusiveLock from writes)
pg_lock_waits{job="$job", instance="$instance", datname=~"$database"}
```

Interpretation:

* `pg_blocked_sessions > 0` with a growing `pg_longest_blocked_seconds` → a long
  transaction (often **idle in transaction**, see §4) is holding a lock. Find it in
  `pg_stat_activity` and consider terminating it.
* A spike in `pg_lock_waits{mode="AccessExclusiveLock"}` → DDL (`ALTER TABLE`,
  `VACUUM FULL`, index builds) is blocking reads/writes. Schedule it in a window or
  use `CONCURRENTLY` variants.

### Who is blocking whom (root cause)

The metrics tell you *that* you're blocked and *how bad*; to find *who* (PID) and
*what* (query), drill into PostgreSQL — run `just blocking` (uses
[`scripts/blocking-tree.sql`](../scripts/blocking-tree.sql)), or directly:

```sql
SELECT blocked.pid  AS blocked_pid,  left(blocked.query, 50)  AS blocked_query,
       blocking.pid AS blocking_pid, left(blocking.query, 60) AS blocking_query,
       blocking.state AS blocking_state,
       round(extract(epoch FROM now() - blocked.query_start))::int AS wait_s
FROM pg_stat_activity blocked
JOIN LATERAL unnest(pg_blocking_pids(blocked.pid)) AS bp(pid) ON true
JOIN pg_stat_activity blocking ON blocking.pid = bp.pid
WHERE cardinality(pg_blocking_pids(blocked.pid)) > 0
ORDER BY wait_s DESC;
```

Then act: `SELECT pg_terminate_backend(<blocking_pid>);`, or fix the root cause
(long/idle-in-transaction sessions, slow UPDATEs from a missing index, blocking DDL).
PID and query text are intentionally **not** exported as metrics (high cardinality);
the panels are for alerting/trends, this query is the on-demand drill-down.

Requires `--collector.locks`.

---

## 3. Missing indexes

Sequential scans on large tables waste CPU and I/O. The `stat` collector exports
per-table `pg_stat_user_tables_*` gauges (labels `datname`, `schemaname`, `relname`).
The exporter publishes these cumulative PostgreSQL counters as Prometheus **gauges**,
so use instant ratios for usage and `deriv(...[5m])` (the gauge-appropriate
per-second slope) for rates. Avoid `rate()`/`increase()` here — they are counter
functions and Grafana will warn that the metric is not a counter.

Use these three signals together — none proves a missing index on its own.

### 3.1 Average rows read per sequential scan

High values mean each sequential scan reads many rows — a strong "missing index" hint:

```promql
pg_stat_user_tables_seq_tup_read
/
clamp_min(pg_stat_user_tables_seq_scan, 1)
```

### 3.2 Index-usage ratio

`idx_scan / (idx_scan + seq_scan)`. A low ratio on a large, frequently-scanned table
suggests a missing or unused index (`1.0` = always index access):

```promql
pg_stat_user_tables_idx_scan
/
clamp_min(pg_stat_user_tables_idx_scan + pg_stat_user_tables_seq_scan, 1)
```

### 3.3 Large tables with a high sequential-scan rate

```promql
deriv(pg_stat_user_tables_seq_scan[5m])
and on (datname, schemaname, relname)
  pg_stat_user_tables_table_size_bytes > 1073741824
```

### How to act

Identify the candidate table, then find the actual SQL in `pg_stat_statements` and
confirm with `EXPLAIN (ANALYZE, BUFFERS)` before creating an index. Prefer targeted
indexes matching real query predicates/joins/ordering; consider partial or composite
indexes. Be careful on **write-heavy** tables: every extra index adds
insert/update/delete cost, storage, WAL, and vacuum overhead.

Requires `--collector.stat`.

---

## 4. Connection saturation without pgbouncer

Without a pooler, applications open many direct connections. Each idle-in-transaction
connection can hold locks and amplify §2.

```promql
# Pool utilization (alert > 0.8)
pg_stat_activity_used_connections / clamp_min(pg_stat_activity_max_connections, 1)

# Idle-in-transaction connections (hold locks/snapshots; should be ~0)
sum(pg_stat_activity_idle_in_transaction{job="$job", instance="$instance", datname=~"$database"})
```

A rising utilization ratio plus idle-in-transaction connections is the classic
no-pooler failure mode: connections pile up, hold locks, block writers, and CPU climbs
as backends contend. The Grafana **Connection Analysis & Idle Age** and **Critical
Alerts** rows visualize this. Requires `--collector.activity`.
