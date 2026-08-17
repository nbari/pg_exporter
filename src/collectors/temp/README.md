# `temp` Collector

The `temp` collector exposes the **live** temporary-file footprint of a
`PostgreSQL` cluster, read from `pg_ls_tmpdir()`.

`PostgreSQL` writes sorts, hash joins, materialized CTEs and large cursors that
exceed `work_mem` into a `pgsql_tmp` directory inside the tablespace. A single bad
plan can turn that into hundreds of gigabytes and fill the data volume, which
takes the whole cluster down.

This collector is **opt-in** and requires `PostgreSQL` 12 or newer.

## Why this exists

`pg_exporter` already exposes cumulative temp-I/O metrics:

- `pg_stat_database_temp_files` / `pg_stat_database_temp_bytes`
- `postgres_pg_stat_statements_temp_blks_read_total` /
  `postgres_pg_stat_statements_temp_blks_written_total`

All of them are **cumulative** and only updated when a statement *finishes*. A
query that has been spilling for twenty minutes and is still running contributes
nothing to any of them. By the time those counters move, the volume is already
full.

The gauges in this collector are point-in-time: they rise while a statement is
still spilling and drop back to zero once `PostgreSQL` removes the files.

## Usage

```bash
pg_exporter --dsn postgresql:///postgres?user=postgres_exporter --collector.temp
```

`pg_ls_tmpdir()` is restricted to superusers and members of `pg_monitor`:

```sql
GRANT pg_monitor TO postgres_exporter;
```

Without the privilege the collector logs a single warning and exports nothing. It
never fails the scrape and never affects `pg_up`.

## Metrics

All metrics carry a single `tablespace` label.

| Metric | Description |
| --- | --- |
| `pg_temp_files_current_bytes` | Bytes currently held by temporary files |
| `pg_temp_files_current_count` | Number of temporary files currently present |
| `pg_temp_files_oldest_age_seconds` | Age of the oldest temporary file, `0` when empty |

`pg_global` is excluded: `PostgreSQL` rejects `pg_ls_tmpdir()` for it, and
temporary relations are never placed there.

Tablespaces with no `pgsql_tmp` directory report `0` rather than disappearing, so
alerting rules can use `> 0` instead of `absent()`.

## Cardinality

Three gauges times the number of non-global tablespaces — typically three series
in total.

Filenames, `PID`s and query text are deliberately **not** exported. A spilling
query creates one file per parallel worker and per spill batch, so those labels
would be unbounded. See "Attributing a spill" below for how to find the
responsible query instead.

## Interpreting the gauges

A healthy instance shows short-lived spikes: a query spills, finishes, and the
files are removed.

- **Sustained growth** is the actionable signal. `deriv()` over
  `pg_temp_files_current_bytes` gives bytes/second; dividing the free space on the
  volume by that rate estimates the time to exhaustion.
- **A high `oldest_age_seconds` that keeps climbing** means one long-running
  statement has been spilling for that long and has not released its files.
- **Many files, small total size** usually means many parallel workers each
  spilling a little. **Few files, huge total size** means one query is writing a
  very large sort or hash.

> `PostgreSQL` cannot report free space on the volume. These gauges say how much
> temp data exists, not how much room is left. On managed services correlate them
> with the provider metric — on RDS/Aurora that is `FreeStorageSpace`.

## Example alerts

```promql
# Sustained growth: >100 MB/s of new temp files
deriv(pg_temp_files_current_bytes[5m]) > 100e6

# Large absolute footprint (tune to your volume size)
pg_temp_files_current_bytes > 100e9

# A single statement has been spilling for over 30 minutes
pg_temp_files_oldest_age_seconds > 1800
```

## Attributing a spill

Once the statement finishes, the cumulative metrics say who did it:

```promql
# Which database
rate(pg_stat_database_temp_bytes[5m])

# Which statement, converted from blocks to bytes
rate(postgres_pg_stat_statements_temp_blks_written_total[5m])
  * on(instance, job) group_left() pg_settings_block_size_bytes
```

The `statements` collector selects the deduplicated union of the top N by
`total_exec_time` and the top N by `temp_blks_written`, so a heavy spiller that is
not one of the slowest queries still appears.

For live attribution set `log_temp_files` to a few megabytes: `PostgreSQL` then
logs every temporary file above that size together with the statement that
created it.

## Related settings

The `default` collector exposes the relevant safeguards:

| Metric | Notes |
| --- | --- |
| `pg_settings_temp_file_limit_bytes` | `-1` means unlimited (the default) |
| `pg_settings_log_temp_files_bytes` | `-1` disables logging, `0` logs every file |
| `pg_settings_block_size_bytes` | Converts statement temp blocks to bytes |

> **`temp_file_limit` is per `PostgreSQL` process**, not per cluster or per query.
> Every parallel worker and every concurrent session may consume the full limit, so
> the total footprint is not bounded by it: a query with 8 parallel workers can use
> 9x `temp_file_limit`. It also applies only to temporary *files*, not temporary
> tables.
>
> The exporter reports the effective value **for its own connection**.
> `ALTER DATABASE`, `ALTER ROLE` and per-session `SET` overrides mean other
> backends may run with a different limit.

## Version handling

`pg_ls_tmpdir()` was introduced in `PostgreSQL` 12. On older servers the collector
logs a single warning and skips cleanly — no error, no series.

## Dashboard

The shipped Grafana dashboard has a **Temp Disk Pressure** row with the live
footprint, growth rate, per-database and per-statement attribution, and the
configured safeguards.

## See also

- [Diagnosing database pressure](../../../docs/diagnosing-database-pressure.md#9-temporary-file-disk-pressure)
- [`statements` collector](../statements/README.md)
