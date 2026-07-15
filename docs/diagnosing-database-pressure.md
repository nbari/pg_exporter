# Diagnosing high CPU, blocking, and missing indexes

This guide explains how to use `pg_exporter` metrics to root-cause the three most
common "the database is oversaturated" scenarios — especially on instances that do
**not** use a connection pooler such as pgbouncer:

1. [High CPU usage](#1-high-cpu-usage)
2. [Blocking / lock contention](#2-blocking--lock-contention)
3. [Missing indexes](#3-missing-indexes)
4. [Connection saturation without pgbouncer](#4-connection-saturation-without-pgbouncer)
5. [Vacuum can't advance — who's pinning the xmin horizon](#5-vacuum-cant-advance--whos-pinning-the-xmin-horizon)
6. [Sequence exhaustion](#6-sequence-exhaustion)
7. [SLRU cache pressure](#7-slru-cache-pressure)
8. [Session churn, checksum failures, and logical slot spill](#8-session-churn-checksum-failures-and-logical-slot-spill)

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
per-second slope) for rates. For recent-window ratios, use non-negative
`delta(...[5m])` values and filter out tables with no matching activity. Avoid
`rate()`/`increase()` here — they are counter functions and Grafana will warn that
the metric is not a counter.

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

### 3.4 Buffer cache hit ratio (I/O pressure)

The `stat` collector also exports per-table block-I/O counters sourced from
`pg_statio_user_tables` (labels `datname`, `schemaname`, `relname`) as Prometheus
**gauges**. `*_blks_hit` counts blocks served from the shared-buffer cache; `*_blks_read`
counts blocks that had to be fetched from the OS/disk. A low ratio means the table is
I/O-bound and reinforces the "missing index" / cold-cache story above.

Heap (table) cache-hit ratio — `1.0` = everything served from cache:

```promql
pg_stat_user_tables_heap_blks_hit_total
/
clamp_min(pg_stat_user_tables_heap_blks_hit_total + pg_stat_user_tables_heap_blks_read_total, 1)
```

Index cache-hit ratio for the same table (low values mean the index itself is not
resident in shared buffers, so index scans require reads outside that cache). Rank
only tables with index I/O; otherwise idle tables appear as false 0% problems:

```promql
(
  clamp_min(delta(pg_stat_user_tables_idx_blks_hit_total[5m]), 0)
  /
  clamp_min(
    clamp_min(delta(pg_stat_user_tables_idx_blks_hit_total[5m]), 0)
    + clamp_min(delta(pg_stat_user_tables_idx_blks_read_total[5m]), 0),
    0.000000001
  )
)
and on (datname, schemaname, relname)
(
  clamp_min(delta(pg_stat_user_tables_idx_blks_hit_total[5m]), 0)
  + clamp_min(delta(pg_stat_user_tables_idx_blks_read_total[5m]), 0)
  > 0
)
```

The `index` collector exposes the equivalent cluster-wide index I/O via
`pg_index_idx_blks_hit_total` / `pg_index_idx_blks_read_total` (labelled by `datname`),
useful for a per-database rollup. TOAST and TOAST-index blocks are available too via
`pg_stat_user_tables_toast_blks_*` and `pg_stat_user_tables_tidx_blks_*`.

A persistently low heap or index hit ratio on a hot table points to memory pressure:
either `shared_buffers` is too small for the working set, or a missing/oversized index
is forcing extra reads. PostgreSQL block reads may still be served by the operating
system cache, so correlate the ratio with read volume and query latency. Pair this
with §3.1–§3.3 before acting.

### 3.5 HOT update ratio

HOT (heap-only tuple) updates avoid rewriting indexes when no indexed column changes
and the heap page has enough free space. Calculate the ratio only for tables with
recent updates:

```promql
(
  clamp_min(delta(pg_stat_user_tables_n_tup_hot_upd[5m]), 0)
  /
  clamp_min(delta(pg_stat_user_tables_n_tup_upd[5m]), 0.000000001)
)
and on (datname, schemaname, relname)
  clamp_min(delta(pg_stat_user_tables_n_tup_upd[5m]), 0) > 0
```

A persistently low ratio on a write-heavy table is a reason to check whether the
workload updates indexed columns and whether a lower `fillfactor` would leave enough
room for HOT updates. Some update patterns are not HOT-eligible, so a low ratio is
not automatically a configuration defect.

### 3.6 Index scan selectivity and size

Rows fetched per active index scan is a workload/selectivity signal:

```promql
(
  clamp_min(delta(pg_stat_user_tables_idx_tup_fetch[5m]), 0)
  /
  clamp_min(delta(pg_stat_user_tables_idx_scan[5m]), 0.000000001)
)
and on (datname, schemaname, relname)
  clamp_min(delta(pg_stat_user_tables_idx_scan[5m]), 0) > 0
```

High values can reveal broad range scans or low selectivity, but may be normal for
batch queries and bitmap scans. Confirm with `EXPLAIN (ANALYZE, BUFFERS)`.

Find the largest per-table index footprints with:

```promql
topk(15, pg_stat_user_tables_index_size_bytes)
```

Large indexes increase cache pressure and write/vacuum work, but size alone does not
mean bloat or justify dropping an index. Compare with table size, scan activity, and
the unused-index collector.

### 3.7 Stale planner statistics

Use the normalized autoanalyze threshold ratio instead of ranking absolute
`n_mod_since_analyze`, which unfairly favors large tables:

```promql
topk(10, pg_stat_user_tables_autoanalyze_threshold_ratio)
```

`1.0` means the effective per-table autoanalyze trigger has been reached; values above
`1.0` suggest planner statistics may be overdue. Pair it with:

```promql
topk(10, pg_stat_user_tables_last_autoanalyze_seconds_ago)
```

Tables that have never been autoanalyzed have no age series; use
`pg_stat_user_tables_never_autoanalyzed` to identify them.

### How to act

Identify the candidate table, then find the actual SQL in `pg_stat_statements` and
confirm with `EXPLAIN (ANALYZE, BUFFERS)` before creating an index. Prefer targeted
indexes matching real query predicates/joins/ordering; consider partial or composite
indexes. Be careful on **write-heavy** tables: every extra index adds
insert/update/delete cost, storage, WAL, and vacuum overhead.

Requires `--collector.stat`.

### 3.8 Cluster-wide I/O and shared-buffer pressure (`pg_stat_io`)

Per-table cache-hit ratios can stay high (98–99%) even while the instance is under real
memory pressure, because the working set churns through `shared_buffers` faster than the
ratio reveals. The `stat_io` collector exposes `pg_stat_io` (PostgreSQL 16+), which breaks
I/O down by `backend_type`, `object`, and `context` and makes that churn visible from
inside PostgreSQL — no host access required, so it works on RDS/Aurora.

**Evictions** are the most direct `shared_buffers`-pressure signal: a buffer had to be
evicted to make room for another block, meaning the working set no longer fits in cache.

```promql
sum(rate(pg_stat_io_evictions_total[5m])) by (backend_type)
```

**Storage latency from inside PostgreSQL** (requires `track_io_timing = on`; otherwise the
timing series stay at zero). Average read latency in seconds:

```promql
sum(rate(pg_stat_io_read_time_seconds_total[5m]))
/
clamp_min(sum(rate(pg_stat_io_reads_total[5m])), 1)
```

> **Note:** `track_io_timing` is **off by default**. The count, byte, and eviction metrics
> work out of the box, but every `pg_stat_io_*_time_seconds_total` series (and the
> dashboard's "Average Read Latency" / "Average Write & Fsync Latency" panels) stays flat at
> zero until you enable it: `ALTER SYSTEM SET track_io_timing = on; SELECT pg_reload_conf();`
> On managed services set the equivalent parameter (for example the RDS/Aurora parameter
> group). `pg_stat_io` itself is built-in on PostgreSQL 16+ and needs no extension.

**Throughput by backend type** separates client-backend I/O from `bgwriter`,
`checkpointer`, and `autovacuum` — a distinction `pg_stat_database.blks_read` lumps
together:

```promql
sum(rate(pg_stat_io_read_bytes_total[5m])) by (backend_type)
sum(rate(pg_stat_io_write_bytes_total[5m])) by (backend_type)
```

Sustained non-zero evictions on `client backend` / `normal` context alongside rising read
throughput is the classic "working set outgrew `shared_buffers`" pattern; correlate with
host/instance `FreeableMemory` before resizing.

Requires `--collector.stat_io` (PostgreSQL 16+). Byte totals are native on PostgreSQL 18+
and derived from `op_bytes` on 16/17; timing metrics require `track_io_timing` (and, for
WAL rows on 18+, `track_wal_io_timing`).

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

---

## 5. Vacuum can't advance — who's pinning the xmin horizon

Bloat that never shrinks and a transaction-ID age that keeps climbing usually mean
something is holding the **xmin horizon** back: VACUUM cannot remove dead tuples newer
than the oldest snapshot still considered "in use" anywhere in the cluster. The three
classic culprits are long-running/idle-in-transaction backends, orphaned prepared
(two-phase) transactions, and stale replication slots. The `vacuum` collector's
`blockers` sub-collector surfaces all three plus the single worst offender's identity, so
you know exactly what to terminate or drop.

```promql
# Oldest xmin age (in transaction IDs) per holder type — compare to autovacuum_freeze_max_age
pg_xmin_horizon_age_xids{job="$job", instance="$instance"}

# The single worst offender per holder (application_name / prepared-xact gid / slot_name)
pg_xmin_horizon_holder_age_xids{job="$job", instance="$instance"}

# Orphaned prepared transactions (each one pins the horizon until committed/rolled back)
pg_prepared_xacts_count{job="$job", instance="$instance"}
pg_prepared_xacts_oldest_age_seconds{job="$job", instance="$instance"}
```

Act on the offender the `holder` label points to: `ROLLBACK PREPARED '<gid>'` for a
stuck prepared xact, `pg_terminate_backend(pid)` for a runaway backend, or
`pg_drop_replication_slot('<slot>')` for an abandoned slot. The Grafana **Vacuum Horizon
Blockers & Progress** row visualizes this alongside live `CREATE INDEX` / `ANALYZE`
progress (`pg_stat_progress_create_index`, `pg_stat_progress_analyze`) so you can watch a
maintenance operation drain the backlog. Requires `--collector.vacuum`.

---

## 6. Sequence exhaustion

An `int4` primary key backed by a sequence overflows at ~2.1 billion; when `nextval()`
hits `max_value` every insert fails. The failure is abrupt and total, so you want to be
warned with plenty of runway. The opt-in `sequences` collector reads `pg_sequences`
across every database and exports the consumed ratio — but **only** for sequences at or
above `--sequences.min-ratio` (default `0.5`), so healthy sequences add no cardinality.

```promql
# Sequences closest to exhaustion (already filtered to >= --sequences.min-ratio)
topk(20, pg_sequence_used_ratio{job="$job", instance="$instance", datname=~"$database"})

# Anything past 90% needs a migration to bigint before it overflows
count(pg_sequence_used_ratio{job="$job", instance="$instance"} >= 0.9) or vector(0)
```

The fix is to migrate the underlying column to `bigint` (or `ALTER SEQUENCE ... AS
bigint` where the column already allows it) well before the ratio reaches `1.0`. The
Grafana **Sequence Exhaustion** row ranks the hottest sequences. Requires
`--collector.sequences`.

---

## 7. SLRU cache pressure

PostgreSQL keeps several small fixed-size caches (SLRUs) for subtransactions, multixacts,
`NOTIFY`, commit timestamps, and more. Unlike `shared_buffers`, they are tiny and not
tunable, so a workload with deeply nested subtransactions (many `SAVEPOINT`s) or heavy
multixact use (lots of `SELECT ... FOR SHARE` / foreign-key contention) can thrash them
and stall — even while `shared_buffers` looks healthy. The opt-in `slru` collector exposes
`pg_stat_slru` (PostgreSQL 13+) so this otherwise-invisible pressure becomes measurable.

```promql
# Per-cache hit ratio — sustained low values on a specific `name` pinpoint the pressure
rate(pg_stat_slru_blks_hit_total{job="$job", instance="$instance"}[5m])
/
clamp_min(
  rate(pg_stat_slru_blks_hit_total{job="$job", instance="$instance"}[5m])
  + rate(pg_stat_slru_blks_read_total{job="$job", instance="$instance"}[5m]), 1)

# Disk reads per cache — the direct thrash signal
rate(pg_stat_slru_blks_read_total{job="$job", instance="$instance"}[5m])
```

A hot `Subtransaction` cache usually means an application pattern with thousands of
subtransactions per transaction; a hot `MultiXact*` cache points to row-lock/FK
contention. The Grafana **SLRU Cache** row breaks all of this down by cache area.
Requires `--collector.slru` (PostgreSQL 13+; older servers are a graceful no-op).

---

## 8. Session churn, checksum failures, and logical slot spill

These three signals come "for free" once the relevant collector is enabled and are worth
watching as always-on canaries.

**Session churn** (`database` collector, PostgreSQL 14+) turns `pg_stat_database` session
counters into rates. A spike in abandoned/fatal/killed sessions is an early sign of client
disconnect storms, backend crashes, or operator terminations:

```promql
rate(pg_stat_database_sessions_abandoned_total{job="$job", instance="$instance", datname=~"$database"}[5m])
rate(pg_stat_database_sessions_fatal_total{job="$job", instance="$instance", datname=~"$database"}[5m])
```

**Checksum failures** (`database` collector, PostgreSQL 12+, requires data checksums) are
a corruption canary — **any** non-zero value is an incident:

```promql
pg_stat_database_checksum_failures_total{job="$job", instance="$instance", datname=~"$database"} > 0
```

> **Caveat — a green `0` is not proof of integrity.** When data checksums are *disabled*
> (`data_checksums = off`), PostgreSQL never verifies page checksums, so `checksum_failures`
> is `NULL` (exported as `0`) and this canary can never fire. Data checksums are a cluster-wide,
> `initdb`-time setting — **enabled by default only on PostgreSQL 18+**; clusters initialized on
> older versions and upgraded in place are commonly `off` and must be converted offline with
> `pg_checksums --enable`. Confirm the cluster is actually protected with the
> `pg_settings_data_checksums` metric (the **Data Checksums** tile, `1` = on / `0` = off) before
> trusting a green failure count:
>
> ```promql
> pg_settings_data_checksums{job="$job", instance="$instance"} == 0
> ```

**Logical slot spill** (`replication` collector, PostgreSQL 14+) shows when logical
decoding spills transactions to disk because `logical_decoding_work_mem` is too small —
sustained spill hurts replication throughput:

```promql
rate(pg_stat_replication_slots_spill_bytes_total{job="$job", instance="$instance"}[5m])
```

Session-churn panels appear in the Grafana **Connection Analysis & Idle Age** row and the
data-checksum panels (status, failures over time, last-failure age) in the **Critical Alerts**
row, immediately after **Active vs Idle Connections**; slot spill appears in
the **Replication** row. Requires `--collector.database` and `--collector.replication`
respectively.
