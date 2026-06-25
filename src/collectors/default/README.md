# `default` collector

The `default` collector is an umbrella for cheap, always-on signals. It registers
several sub-collectors:

- `version` – server version
- `settings` – selected `pg_settings` values (see below)
- `postmaster` – postmaster start time / uptime
- `bgwriter` – background writer stats (`pg_stat_bgwriter`)
- `checkpointer` – checkpointer stats (see below)
- `archiver` – WAL archiver stats (`pg_stat_archiver`)
- `wal` – WAL generation stats (`pg_stat_wal`, PostgreSQL 14+)

This document focuses on the checkpoint-related metrics, because they are the most
commonly misunderstood and the most useful for capacity/tuning decisions.

## Checkpoint metrics

### From `pg_stat_checkpointer` (PostgreSQL 17+)

| Metric | Type | Meaning |
|---|---|---|
| `pg_stat_checkpointer_timed_total` | counter | Checkpoints triggered by `checkpoint_timeout` (time-driven) |
| `pg_stat_checkpointer_requested_total` | counter | Checkpoints triggered by other reasons, primarily WAL volume reaching `max_wal_size` (xlog-driven), plus manual `CHECKPOINT` |
| `pg_stat_checkpointer_buffers_written_total` | counter | Buffers written during checkpoints |
| `pg_stat_checkpointer_write_time_seconds_total` | counter | Cumulative time spent writing buffers (milliseconds) |
| `pg_stat_checkpointer_sync_time_seconds_total` | counter | Cumulative time spent syncing buffers (milliseconds) |

### From `pg_control_checkpoint()` (all supported versions)

| Metric | Type | Meaning |
|---|---|---|
| `pg_last_checkpoint_age_seconds` | gauge | Seconds since the last completed checkpoint. Reflects the **achieved** checkpoint interval and checkpointer liveness; climbs unbounded if the checkpointer stalls. |
| `pg_wal_bytes_since_last_checkpoint` | gauge | WAL bytes generated since the last checkpoint's redo point. This is the WAL that must be replayed on crash recovery — a **proxy for recovery time (RTO)** and for headroom against `max_wal_size`. On standbys it is computed from the last replayed LSN. |

> These two gauges are best-effort. On older PostgreSQL versions `pg_control_checkpoint()`
> may require `pg_monitor` (or superuser). If it is not permitted, the gauges are skipped
> with a warning and the rest of the checkpointer metrics keep working.

### WAL settings (from `settings`)

| Metric | Meaning |
|---|---|
| `pg_settings_checkpoint_timeout_seconds` | `checkpoint_timeout` |
| `pg_settings_max_wal_size_bytes` | `max_wal_size` (the WAL-volume checkpoint trigger) |
| `pg_settings_min_wal_size_bytes` | `min_wal_size` |

### WAL overhead (from `wal`, PostgreSQL 14+)

| Metric | Meaning |
|---|---|
| `pg_stat_wal_bytes_total` | Total WAL bytes generated |
| `pg_stat_wal_fpi_total` | Full-page images written to WAL |
| `pg_stat_wal_records_total` | WAL records generated |

## Why tune `checkpoint_timeout` (5m vs 30m)?

A checkpoint fires on **whichever comes first**: `checkpoint_timeout` elapses
(→ `timed`) or WAL since the last checkpoint reaches the WAL-volume trigger
(→ `requested`). Because of this, `checkpoint_timeout` and `max_wal_size` must be
tuned **together**.

> **The WAL trigger is not `max_wal_size`.** Postgres triggers a requested
> checkpoint at roughly `max_wal_size / (2 + checkpoint_completion_target)`
> (about **⅓ of `max_wal_size`** with the default `0.9`), leaving room to spread
> the checkpoint writes and to bound the *total* retained WAL. So
> `pg_wal_bytes_since_last_checkpoint` peaks **well below** `max_wal_size` even
> when WAL volume is the binding trigger — a checkpoint at ~⅓ of `max_wal_size`
> is expected, not wasted capacity.

**A longer interval (e.g. 30m) reduces checkpoint I/O pressure** — fewer checkpoints
means fewer full-page images (the first write to each page after a checkpoint is a
full 8 KB page image in WAL), so less write amplification and less total WAL. This
helps when **storage write throughput is the bottleneck**.

**The cost of a longer interval is recovery time (RTO).** More WAL accumulates
between checkpoints, so crash recovery must replay more WAL — exactly the failure
mode behind long checkpoint-related outages. Do not increase `checkpoint_timeout`
without understanding the availability tradeoff.

## A metrics-driven tuning decision tree

1. **Is the interval time-driven or WAL-driven?**
   - Compare `rate(pg_stat_checkpointer_requested_total[1h])` vs
     `rate(pg_stat_checkpointer_timed_total[1h])`.
   - If `requested` dominates, checkpoints are firing because WAL reached
     `max_wal_size`. **Raising `checkpoint_timeout` alone will not lengthen the
     interval — raise `max_wal_size` first.**

2. **What interval are we actually achieving?**
   - Peak of `pg_last_checkpoint_age_seconds` vs `pg_settings_checkpoint_timeout_seconds`.
   - If the peak reaches the timeout, checkpoints are time-driven. If the peak sits
     well below it, they are WAL-driven.

3. **How much WAL headroom is there?**
   - Peak of `pg_wal_bytes_since_last_checkpoint` vs `pg_settings_max_wal_size_bytes`.
   - A peak that flatlines around `max_wal_size / (2 + checkpoint_completion_target)`
     (~⅓ of `max_wal_size`) together with `requested` checkpoints confirms WAL is the
     binding trigger — increase `max_wal_size`. A peak far below even that point with
     `timed` checkpoints means `max_wal_size` is oversized and can be reduced (leaving
     margin above the observed peak).

4. **What is the recovery cost (RTO)?**
   - `pg_wal_bytes_since_last_checkpoint` is the amount of WAL replayed after a crash.
     A larger interval increases this peak. Size it against your recovery budget.

5. **Are checkpoints keeping up (storage bottleneck)?**
   - Average work per checkpoint:
     ```promql
     ( rate(pg_stat_checkpointer_write_time_seconds_total[1h])
     + rate(pg_stat_checkpointer_sync_time_seconds_total[1h]) )
     / ( rate(pg_stat_checkpointer_timed_total[1h])
       + rate(pg_stat_checkpointer_requested_total[1h]) )
     ```
     As this approaches `checkpoint_timeout`, the checkpointer cannot keep up; a
     longer interval reduces how often this I/O happens.

6. **What is the full-page-write overhead (benefit of a longer interval)?**
   - `rate(pg_stat_wal_fpi_total[1h])` and the
     `rate(pg_stat_wal_fpi_total) / rate(pg_stat_wal_records_total)` ratio drop as the
     interval lengthens (fewer post-checkpoint full-page images), which reduces WAL
     volume and write amplification.

### Rule of thumb

- If `max_wal_size` is the binding trigger and storage write IOPS is the bottleneck,
  increase `max_wal_size` (and possibly `checkpoint_timeout`) so checkpoints are
  time-driven again — but size `pg_wal_bytes_since_last_checkpoint` against your RTO
  budget before increasing the interval.
- Keep `log_checkpoints` on, and watch `pg_last_checkpoint_age_seconds` for a stalled
  checkpointer (age climbing far past `checkpoint_timeout`).
