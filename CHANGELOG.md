# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.19.0] (unreleased)

### Fixed

- **`/metrics` no longer wedges at `503` forever after a scrape timeout** ([#34]).
  The single-scrape gate handed its semaphore permit to the spawned scrape task.
  On timeout the request dropped the `JoinHandle`, which in Tokio **detaches**
  rather than cancels the task, so returning the permit depended entirely on that
  detached task unwinding — and nothing bounded it: the inner collector loop has
  no deadline of its own, so a collector that never resolved held the only permit
  of a `Semaphore::new(1)` for the rest of the process lifetime. Switching to
  `abort()` would have narrowed the window but not closed it: a Tokio task is only
  cancelled at an await point, and the `system` collector (see [#35] below) ran
  synchronous `std::fs` I/O inside an `async` block, so the abort could not land
  until that walk returned — tens of seconds later on the affected host. Either
  way, `/metrics` answered `503 another /metrics scrape is already running` while
  the gate stayed shut, which on the reporting instance meant a **66+ minute**
  metrics blackout cleared only by a manual restart.

  The permit is now owned by the **request** future, so it is released by the
  ordinary `Drop` on every exit path — success, timeout, collector error, or
  client disconnect — with no dependence on the scrape task's behaviour. The
  scrape task is additionally wrapped in an abort-on-drop guard so a timed-out
  scrape stops driving its in-flight `sqlx` futures and hands the pooled
  connections back instead of leaving them parked as idle `Client:ClientRead`.

- **`--collector.system` no longer costs ~14 s per scrape, and no longer starves
  the async runtime** ([#35]). Two independent defects with one symptom:

  1. **PSS is now opt-in, RSS is the default.** The process-group memory gauge
     read `/proc/<pid>/smaps_rollup`, which forces the kernel to walk every
     page-table entry of every mapping — `O(processes × resident pages)`. On the
     affected production primary (253 `postgres` processes,
     `shared_buffers = 15939MB`) that took **13.851 s**, versus **0.016 s** for
     the equivalent `/proc/<pid>/stat` reads: **866× slower**, and 92% of the 15 s
     scrape budget on its own. The default is now RSS from `/proc/<pid>/statm`
     (one cheap read per process); PSS is available via the new
     `--system.process-memory=pss` flag for operators who have measured the cost.
     This restores the behaviour originally requested in [#26], which asked for
     RSS. **No metric was renamed**: `pg_system_process_group_memory_bytes`
     is source-agnostic, so no dashboard or alert changes are required.
  2. **Blocking OS I/O moved off the runtime worker.** All three `system`
     sub-collectors performed synchronous `std::fs` / `sysctl` / `sysinfo` reads
     directly inside their `async` blocks. That monopolised a Tokio worker for
     the entire walk and stopped every other collector's futures from being
     polled — which surfaced as the badly misleading
     `pool timed out while waiting for an open connection`, sending operators to
     investigate a connection pool that was never the problem. Sampling now runs
     on the blocking pool via `spawn_blocking`, so a slow host read degrades into
     a slow `system` collector instead of a stalled exporter.

- **Blocking OS reads moved off the runtime worker in two more collectors.** The
  `exporter` collector's own process metrics (a `sysinfo` refresh plus a
  `/proc/<pid>/fd` read) and the `tls` collector's certificate read and X.509
  parse both ran inline in `collect_once`. Both are far smaller than [#35]'s
  `/proc` walk, but they are the same defect, and `ssl_cert_file` can point at a
  path that is slow or hangs. Both now run on the blocking pool, and the
  regression guard in `tests/collector_safety.rs` covers every collector rather
  than only `--collector.system`.

- **`pg_system_process_group_cpu_seconds_total` no longer over-reports when
  scrapes overlap.** Now that the scrape gate reopens immediately on timeout
  ([#34]), a new scrape can begin while an abandoned scrape's sample is still
  running — an already-started `spawn_blocking` task cannot be cancelled. The
  process-group collector sampled `/proc` *outside* its per-PID CPU baseline
  lock, so the newer pass could publish its baseline first, the older pass would
  then count no delta and overwrite the baseline with its own lower totals, and
  the next pass re-counted the interval between them. The lock now spans the
  sample, which also stops two concurrent walks from doubling the `/proc` load.

  Measured effect of the collector being pathological on the affected host:
  `/metrics` went from `504` after 15.002 s with **0 metrics** and 59–100% CPU,
  to `200` in **0.017 s** with 694 metrics at 0.1% CPU.

  Per-collector cost was already observable via
  `pg_exporter_collector_scrape_duration_seconds{collector="system"}` — but only
  when `--collector.exporter` is also enabled, since that collector owns the
  metric. Watch it if you enable PSS, reading the magnitude from
  `_sum / _count`: the histogram's top bucket is 5 s, so a PSS walk lands in
  `+Inf` and the bucket counts alone will not tell you how expensive it was.

### Changed

- **Dependencies**: refreshed to the latest compatible versions — `tower-http` 0.7.0 -> 0.7.1
  in the manifest, plus lockfile bumps across the tree including `rustls` 0.23.43 -> 0.23.44,
  `hyper` 1.11.0 -> 1.11.1, `h2` 0.4.16 -> 0.4.19, `uuid` 1.24.1 -> 1.26.0, `mio` 1.2.2 -> 1.2.3
  and `tokio-rustls` 0.26.4 -> 0.26.5. Two transitive crates stay pinned: `matchit` is held at
  0.8.4 by an exact `=0.8.4` requirement in `axum` 0.8.9, and `crypto-common` 0.1.x is held by
  `digest` 0.10 via `sqlx`; forcing it would downgrade `generic-array`.

[#26]: https://github.com/nbari/pg_exporter/issues/26
[#34]: https://github.com/nbari/pg_exporter/issues/34
[#35]: https://github.com/nbari/pg_exporter/issues/35

## [0.18.0] - 2026-08-17

### Changed - action may be required

- **A collector that publishes nothing now removes its series instead of serving the last value.** Previously a collector could finish a scrape successfully without publishing anything — an unsupported server version, a missing view, a revoked privilege, an absent certificate file — and its previous values stayed in the registry, served as though current. Only the `temp` collector cleared them. The `Collector` trait now requires every collector to declare what a skip means for it, and the shared entry point clears the metrics after one. Exposition is **unchanged** whenever a value is present.

  **This is alerting-breaking, not just cosmetic.** A threshold alert stops firing when its series disappears, because there is no longer a value to compare. Affected series, previously `0` or stale, now absent:

  | Series | When it is now absent |
  | --- | --- |
  | `pg_stat_checkpointer_*` | PostgreSQL 14-16 (the view is 17+) |
  | `pg_ssl_certificate_*` | no `ssl_cert_file`, or the file is unreadable from the exporter |
  | `pg_ssl_enabled` | the `ssl` setting is absent, or the role may not read it |
  | `pg_ssl_connections_*` | `pg_stat_ssl` is absent, or the role may not read it |
  | `pg_stat_archiver_*`, `pg_stat_wal_*` | the view is missing or the role lacks privileges |
  | `pg_stat_archiver_last_archived_age_seconds` | **nothing has been archived yet** (`last_archived_time IS NULL`), which is the normal state when `archive_mode = off` |
  | `pg_stat_archiver_last_failed_age_seconds` | **no archive attempt has failed yet** (`last_failed_time IS NULL`) |
  | `pg_last_checkpoint_age_seconds`, `pg_wal_bytes_since_last_checkpoint` | `pg_control_checkpoint()` unavailable or denied |
  | `pg_wal_bytes_since_last_checkpoint` | a standby that has not replayed any WAL yet (`pg_last_wal_replay_lsn()` is NULL) |
  | `pg_stat_io_*`, `pg_stat_slru_*`, `pg_stat_replication_slots_*`, `pg_sequence_used_ratio`, `pg_temp_files_*`, `pg_stat_statements_*`, vacuum progress | the collector's version/extension gate is not satisfied |

  Any series listed above is also absent until the first successful collection, rather than reading `0` from registration.

  Two rows above will affect most installations rather than a minority: the archiver *age*
  series disappear unless WAL archiving has actually run, because `0` there asserted
  "archived 0 seconds ago" and could hide an archiver that had never worked. If you graph or
  alert on archive age, expect those series to be absent until the first archive, and guard
  with `absent()`.

  Only **known absence** clears a series: the object does not exist (`42P01`, `42883`,
  `42704`) or the role may not read it (`42501`). A genuine failure — anything else,
  including a connection dropping mid-scrape — is reported as an error instead, which
  **preserves registry state**: the retained values are not deleted, so the next successful
  scrape continues the series. Note the errored scrape itself publishes no metric samples — it
  returns 503 with an error-only body (`# Error collecting metrics: ...`), so that interval has
  no sample either way. The difference is whether the series resumes afterwards or has been
  cleared.

  Guard threshold alerts with `absent()` so a vanished series pages instead of going quiet:

  ```promql
  # Fires when the certificate is close to expiry OR the exporter stopped reporting it
  pg_ssl_certificate_expiry_seconds < 7 * 24 * 3600
    or absent(pg_ssl_certificate_expiry_seconds)

  # "TLS is off" and "we could not determine whether TLS is on" are different problems
  pg_ssl_enabled == 0 or absent(pg_ssl_enabled)
  ```

  Equality alerts on a value that used to be a placeholder zero (for example
  `pg_stat_checkpointer_timed_total == 0` on PostgreSQL 14-16) were reading a fabricated
  number and should be removed rather than rewritten.

- **Source-incompatible for custom `Collector` implementations.** `Collector::collect` is now a provided method; implementors write `collect_once(..) -> Result<Collected>` and a required `reset_metrics(&self)`, and `collect` carries a `where Self: Sync` bound. Callers are unaffected: `collect(..) -> Result<()>` keeps its name and signature, and calling it is what settles a skip. In-tree collectors, the registry and every existing test needed no call-site changes.

### Added
- **Live temporary-file disk pressure** ([#32](https://github.com/nbari/pg_exporter/issues/32)): New opt-in `temp` collector (`--collector.temp`) exposing `pg_temp_files_current_bytes`, `pg_temp_files_current_count`, and `pg_temp_files_oldest_age_seconds`, labeled by `tablespace`, from `pg_ls_tmpdir()` (PostgreSQL 12+). `pg_stat_database_temp_bytes` and the `pg_stat_statements` temp counters are cumulative and only update once a statement *finishes*, so a query that is still spilling is invisible to them; these gauges report what is on disk right now and return to zero once PostgreSQL removes the files. Cluster-wide, so it reads only the shared pool. A missing `pg_monitor` grant, a missing function, or a pre-12 server degrades to a warn-once no-op without failing the scrape or `pg_up`. Adds a **Temp Disk Pressure** dashboard row and a diagnostics guide section with alert examples.
- **Temp-file safeguards in the settings collector** ([#32](https://github.com/nbari/pg_exporter/issues/32)): `pg_settings_temp_file_limit_bytes`, `pg_settings_log_temp_files_bytes`, and `pg_settings_block_size_bytes`. The `-1` sentinels (unlimited / disabled) are preserved through the kB-to-bytes conversion instead of being scaled to `-1024`. `temp_file_limit` is documented as a per-process limit that parallel workers and concurrent sessions can each consume.
- **Dashboard contract tests** ([tests/dashboard.rs](tests/dashboard.rs)): every `grafana/dashboard.json` panel query is now checked against the metrics the exporter really exports, so a renamed metric fails `cargo test` instead of silently leaving a panel empty. Covers name-vs-source declaration, a full live scrape with all collectors enabled, the four collectors the Temp Disk Pressure row spans, and the `-1` sentinels of `temp_file_limit`/`log_temp_files`. Metrics that need conditions a local instance cannot create (a replica, a blocked session, an in-flight `VACUUM`) are listed in `CONDITIONAL_METRICS` with a reason, and a test fails if such an entry becomes stale.
- **`--statements.query-text-refresh`** (`PG_EXPORTER_STATEMENTS_QUERY_TEXT_REFRESH`, seconds, default `900`): minimum delay between `pg_stat_statements` query-text lookups; `0` disables them entirely. Lookups run in a detached background task, so scrapes never block on one, and a single-flight guard keeps overlapping scrapes from starting more than one at a time.

### Fixed
- **A missing `GRANT` on `pg_stat_archiver` / `pg_stat_wal` looked like an old PostgreSQL.** Both collectors decided "this view does not exist" by matching the view name in the error *message*, and the name appears in a permission error too — so an under-privileged role silently produced no WAL-archiving or WAL-generation metrics, with nothing in the log to explain it. Both now classify by `SQLSTATE`: `42P01` skips quietly, `42501` warns once naming the missing grant, and anything else is a real error.
- **`pg_ssl_enabled` reported `0` ("TLS is disabled") when the server could not be asked.** A failed `SHOW ssl` published a definite "off" for an unknown state — a security-relevant false negative on a dashboard. It now removes the series instead, and a genuine fault propagates rather than being silently absorbed.
- **The connection-TLS collector served stale counts after a failed query.** A failed `pg_stat_ssl` read logged a warning and returned success, and its reset only ran on the success path, so the previous connection counts kept being published as current. A missing or denied view now clears them; any other failure propagates.
- **Archiver ages could stay pinned forever.** `pg_stat_archiver_last_archived_age_seconds` and `..._last_failed_age_seconds` were only written when PostgreSQL reported a timestamp, so after `pg_stat_reset_shared('archiver')` they kept their last value indefinitely and age-based alerts read a stale number.
- **Two version gates never fired as intended outside normal startup.** `checkpointer` tested `is_pg_version_at_least(170_000)` and `tls`'s connection statistics tested `is_pg_version_at_least(90_500)` — PostgreSQL 9.5, on a project supporting 14+. Both read only the process-wide version cache, which reports `0` when unset, so in any process that had not run the exporter's startup path they skipped unconditionally. A normally started exporter was unaffected because the registry populates the cache first, but the test suite never exercised either collector's real path. Both now resolve the version with a live fallback.
- **`just` and the setup scripts could hang waiting for `q`.** `scripts/setup-local-test-db.sh` finished its work and then appeared to stall: psql's pager defaults to on and pages on output *width* as well as height, so the "Top 5 queries" table (~90 characters) went through `less`. The shared psql wrapper now disables the pager, which covers every script that uses it.
- **The statements collector no longer spills temporary files on every scrape** ([#33](https://github.com/nbari/pg_exporter/issues/33)): The per-scrape query read `pg_stat_statements` with `showtext = true`, which makes PostgreSQL load the entire query-text file into backend memory and materialize every row, text included, into a `work_mem`-bounded tuplestore *before* any filter, join or `LIMIT` runs. On busy instances that wrote a multi-megabyte file into `base/pgsql_tmp` on every scrape. The collector now reads `pg_stat_statements(false)` and resolves `query_short` from a separate, rate-limited, cached lookup. Measured on a seeded instance (9002 entries, 8365 kB of query text, `work_mem = 4MB`): 1506 temp blocks / 38.8 ms before, 0 temp blocks / 7.8 ms after. Moving the ranking into a CTE — the originally proposed fix — was measured at 3012 temp blocks / 56.9 ms, because the materialization happens below every planner node. The rate-limited text lookup still has to call `pg_stat_statements(true)`, so it now runs inside a transaction with `SET LOCAL work_mem`, which keeps the tuplestore in memory: measured on a 9.4 MB corpus at the server's `work_mem = 4MB`, the lookup wrote 1391 temp blocks / 29.7 ms; with the raised setting it writes 0 blocks and is ~25% faster. If the server rejects the `SET` (for example a role without permission), the lookup falls back to running without it rather than failing the scrape, and warns once so the residual is visible. Finally, the lookup no longer runs *during* a scrape at all: it is dispatched as a detached background task, so a scrape can never wait on it. Statements published before their text resolves are labeled `<unknown>` and pick up the real text on a later scrape.
- **The temp collector could publish a stale footprint indefinitely** ([#32](https://github.com/nbari/pg_exporter/issues/32)): The graceful skip paths — `insufficient_privilege`, a missing `pg_ls_tmpdir()`, and the pre-12 version gate — returned success without touching the gauges. If `pg_monitor` was revoked, or a failover moved the exporter to a server without the function, the last successful reading stayed exposed and looked current forever. All three paths now clear the gauges.
- **High-temp statements could disappear from the metrics** ([#32](https://github.com/nbari/pg_exporter/issues/32)): Statements were selected by `total_exec_time` only, so a query writing gigabytes of temporary files vanished from the temp metrics whenever it was not also one of the slowest. The collector now exports the deduplicated union of the top N by execution time and the top N by `temp_blks_written`, bounded at `2 x --statements.top-n` series per metric.

### Changed
- **`just watch` enables every collector automatically**: the recipe hand-listed collector flags and had drifted (missing `default` and `temp`); it now derives them from `register_collectors!` via a new `just collector-flags`.
- **Grafana row layout normalized**: `Exporter Self-Monitoring` is pinned to the bottom, the new `Temp Disk Pressure` row sits above it, and every collapsed row now anchors its panels at `row_y + 1` with the panel array sorted by position, matching the convention Grafana itself writes.
- **Self-exclusion in the statements collector is now text-free**: on PostgreSQL 14+ the scrape query reads its own in-flight `pg_stat_activity.query_id` — which equals the `queryid` `pg_stat_statements` records for it — instead of matching a query-text prefix, so the collector keeps excluding itself even with `--statements.query-text-refresh=0`. PostgreSQL 12 and 13 have no `query_id` and fall back to learning it from the marker comment during the text lookup. Self-identification matches the exact marker-comment prefix rather than the bare marker, so a user statement that merely mentions `pg_exporter:statements` is no longer mistaken for the exporter's own query and dropped from the metrics. The now-obsolete regex-vs-prefix self-filter benchmark was removed.
- **`query_short` reports `<unknown>` instead of `<utility>`** for statements PostgreSQL has no text for. Query texts are now resolved by a separate lookup, so the label also reads `<unknown>` in the window before a newly ranked statement is resolved. Dashboards or alerts matching `query_short="<utility>"` must be updated. Missing texts are never cached, so a statement that gains a text later is picked up by a subsequent lookup instead of being pinned to an empty label forever.
- **Only top-level statements are exported**: `pg_stat_statements` is keyed by `(userid, dbid, queryid, toplevel)`, so with `pg_stat_statements.track = all` a statement executed both directly and from inside a function has two entries sharing `(userid, dbid, queryid)` and carrying different counters. The exported label set has no `toplevel` component, so both collapsed onto one series where the row order decided which counters survived, and each duplicate also consumed a slot of the `2 x --statements.top-n` budget. The scrape query now filters to `toplevel` entries, gated on a catalog probe because the column arrived in `pg_stat_statements` 1.9 (PostgreSQL 14). Under `track = all` a statement only ever called from inside a function is no longer exported on its own; its cost is still counted in the top-level statement that invoked it. Under the default `track = top` nothing changes.
- **`<insufficient privilege>` is no longer cached as a query text**: a role without `pg_read_all_stats` receives that placeholder instead of another role's query text. Caching it pinned the label until the text cache happened to be pruned, so a later `GRANT pg_read_all_stats` did not take effect. It is now treated like a missing text and retried on the next lookup window.
- **Dependencies**: `ulid` 2 -> 3 (`Ulid::r#gen()` -> `Ulid::generate()`) and `base64` 0.22 -> 0.23. `testcontainers` 0.27 -> 0.28 (dev-only), which also moves `bollard` 0.20 -> 0.21. The `testcontainers-modules` dev-dependency is **dropped**: nothing used it, since every container test builds its image with `GenericImage::new` directly, and it was the only thing holding `testcontainers` back to 0.27.

## [0.17.2] - 2026-07-15

### Added
- **Expanded table-statistics dashboard coverage**: Added panels for estimated row counts, HOT-update ratio, total index size, active-table index cache-hit ratio, and rows fetched per active index scan. Refined stale-statistics and automatic-maintenance panels, with matching diagnostic guidance for interpreting each signal safely.
- **Unified collector exercise workflow**: `just exercise-collectors` now performs one pgbench setup followed by mixed workload/session/I/O stimuli, SLRU and sequence activity, scrape-visible ANALYZE and CREATE INDEX progress, manual VACUUM, and verified autovacuum plus autoanalyze. Restart-dependent prepared-transaction and logical-slot stimuli are attempted when supported and reported as skipped otherwise.
- **End-to-end maintenance-age regression**: Added an always-run PostgreSQL integration test that executes the real `stat` collector path and verifies both automatic-maintenance age metric families, labels, finite values, and expected ages in the Prometheus registry.
- **Richer soak comparison signals**: Extended the benchmark sampler and status report with statements collector mean/p95 duration and success, plus database-host CPU, available memory, and load averages, preserving comparison data beyond Prometheus retention.

### Fixed
- **Automatic-maintenance ages were silently missing**: Explicitly cast `EXTRACT(EPOCH ...)` results to `double precision` before SQLx decodes them as `f64`, and propagate decode failures instead of converting type mismatches into absent metrics. `pg_stat_user_tables_last_autovacuum_seconds_ago` and `pg_stat_user_tables_last_autoanalyze_seconds_ago` now emit whenever PostgreSQL has the corresponding timestamps.
- **ANALYZE progress was missed between scrapes**: The local exercise uses session-only statistics and vacuum-cost settings to keep ANALYZE active across a 10-second Prometheus scrape. ANALYZE and CREATE INDEX dashboard legends now use their actual `database_name`, `table_name`, and `phase` labels.
- **Exercise cleanup and auto-maintenance verification**: The unified workflow requires both autovacuum and autoanalyze to complete, reports effective per-table thresholds, restores temporary autovacuum settings on exit, and preserves recovery state when an interrupted cleanup cannot reach PostgreSQL.

### Removed
- **Superseded local workflow surface**: Removed the separate `just workload`, `just vacuum-workflow`, and `just autovacuum-workflow` recipes in favor of the single comprehensive `just exercise-collectors` entrypoint.
- **Orphaned process diagnostics**: Removed the unreferenced `compare-cpu-live.sh` and `monitor-exporter.sh` scripts; exporter process metrics and Prometheus now provide the maintained resource-monitoring path.

## [0.17.1] - 2026-07-15

### Fixed
- **Faster `pg_stat_statements` scrapes** ([#31](https://github.com/nbari/pg_exporter/issues/31)): Replaced the collector's per-row `BTRIM(REGEXP_REPLACE(...))` self-query filter with a stable direct `NOT LIKE` prefix match, avoiding full regex processing of large accumulated query texts. Added SQL-shape and live PostgreSQL self-exclusion regressions, a repository-wide guard against `regexp_replace` in production collectors, and an always-run configurable benchmark; local measurements showed roughly 56x faster predicate evaluation on the default dataset and 87x on a 5,000-row/~700 MB workload. Manual query-diagnostic scripts now also bound query text before whitespace normalization.

## [0.17.0] - 2026-07-14

### Added
- **Host CPU / memory metrics** ([#26](https://github.com/nbari/pg_exporter/issues/26)): new opt-in `--collector.system` (disabled by default; **Linux** and **FreeBSD** only) exposing node_exporter-style **host** CPU and memory for the machine running the exporter. CPU time is exported as true Prometheus cumulative **per-core counters** in seconds — `pg_system_cpu_seconds_total{cpu,mode}`, one series per logical core (Linux modes `user`/`nice`/`system`/`idle`/`iowait`/`irq`/`softirq`/`steal`, FreeBSD modes `user`/`nice`/`system`/`interrupt`/`idle`) — with small backwards OS accounting readings ignored, CPU hotplug/reset safely re-baselined, and offline CPU series removed. `pg_system_cpu_cores` comes from the same successful `/proc/stat` / `kern.cp_times` sample as the counters, avoiding host/container CPU-scope mismatches; `pg_system_cpu_cores_physical` and `pg_system_load1`/`_load5`/`_load15` provide topology/load context. Memory and swap are byte gauges: `pg_system_memory_total_bytes`/`_available_bytes`/`_free_bytes`/`_used_bytes` and `pg_system_swap_total_bytes`/`_used_bytes`/`_free_bytes`. The collector reads only the operating system (`/proc/stat` on Linux, the `kern.cp_times` sysctl on FreeBSD, and `sysinfo` for memory/load) and **never queries PostgreSQL**, so it adds zero query or connection load. CPU cardinality is bounded per host (modes × cores) and does **not** scale with the number of databases. It is only meaningful when the exporter is **co-located** with PostgreSQL; do **not** enable it for managed services such as AWS RDS/Aurora — the numbers would describe the exporter's host, not the database server — and the exporter logs a startup warning when `system` is enabled against a non-local DSN. On FreeBSD/Windows `sysinfo` reports `available == free`, so dashboards should compute memory pressure as `(total - available) / total`.
- **PostgreSQL process-group resource usage** (part of `--collector.system`): the `system` collector now also aggregates the host CPU and memory of every `postgres*` process (the postmaster and all backends) into fixed low-cardinality series labeled only `group="postgres"` (**no** per-PID label), to answer "is PostgreSQL itself eating the box, or a noisy neighbour?": `pg_system_process_group_cpu_seconds_total` (cumulative CPU seconds, built from summed positive per-PID deltas so backend churn never rewinds the counter — read as cores via `rate(...)`), `pg_system_process_group_memory_bytes`, and `pg_system_process_group_count`. On Linux the memory figure is **PSS** (`/proc/<pid>/smaps_rollup`), which counts `shared_buffers` **once** across all backends instead of multiplying it per connection — PSS requires the exporter to run as the `postgres` user or root, otherwise it falls back to **RSS** (`/proc/<pid>/statm`); on FreeBSD it is summed RSS.
- **Grafana "Host CPU / Memory" dashboard row** (requires `--collector.system`): an expanded row with CPU utilization by mode (`idle` excluded and normalized by averaging the same per-core series, avoiding a separate core-count denominator), total non-idle CPU, load average, memory/swap usage, a full-width per-CPU timeseries with one line per logical core to spot an unbalanced load, and — below the per-CPU panel — **PostgreSQL Process Group CPU** (cores) and **PostgreSQL Process Group Memory** (PSS/RSS bytes, with the live `postgres*` process count on a second axis). CPU panels use Grafana's adaptive `$__rate_interval`, retain `iowait`/`steal` as pressure diagnostics, and pin percent axes to 0-100%. The **Exporter Self-Monitoring** row remains last.

## [0.16.0] - 2026-07-13

### Added
- **`pg_stat_io` I/O metrics** ([#25](https://github.com/nbari/pg_exporter/issues/25)): New opt-in `--collector.stat_io` (disabled by default) exposing cluster-wide `pg_stat_io` statistics (PostgreSQL 16+), labeled by `backend_type`/`object`/`context`: operation counts (`pg_stat_io_reads_total`, `writes`, `writebacks`, `extends`, `hits`, `evictions`, `reuses`, `fsyncs`), byte throughput (`pg_stat_io_read_bytes_total`, `write_bytes`, `extend_bytes`), and timings in seconds (`pg_stat_io_read_time_seconds_total`, `write_time`, `writeback_time`, `extend_time`, `fsync_time`). `evictions` is a direct `shared_buffers`-pressure signal and the timings give storage latency from inside PostgreSQL — especially useful on managed services (RDS/Aurora) with no host access. The collector reads only the shared pool (no per-database fan-out). Byte totals use the native `read_bytes`/`write_bytes`/`extend_bytes` columns on PostgreSQL 18+ and are derived from `op_bytes` on 16/17; timing metrics require `track_io_timing` (and `track_wal_io_timing` for WAL rows on 18+). On servers older than PostgreSQL 16 the collector skips cleanly and logs a single warning that 16+ is required.
- **Grafana "I/O by backend type" dashboard section** (requires `--collector.stat_io`): a collapsed row with KPI stats (eviction rate, buffer cache hit ratio, read/write throughput) and time series for evictions (by backend type and context), cache hit ratio, cache hits vs disk reads, read/write/extend throughput, read/write/fsync latency, buffer reuses, and writebacks/fsyncs. The latency panels note that they require `track_io_timing = on`. Added cluster-wide I/O and shared-buffer pressure diagnostics to the troubleshooting guide.
- **Collector module-layout guard**: a build-time check (`tests/collector_safety.rs`) that keeps each collector's `mod.rs` a thin umbrella and forces metric/SQL implementation into a sibling file (for example `stat_io/pg_stat_io.rs`), matching the existing collector structure.
- **Vacuum blockers: xmin horizon holders** ([#27](https://github.com/nbari/pg_exporter/issues/27)): `--collector.vacuum` now exposes what pins the xmin horizon (and thus blocks vacuum from reclaiming bloat / holds back wraparound protection) even when every other metric looks normal: `pg_xmin_horizon_age_xids{holder}` (age in xids of the oldest xmin held by `backend`, `prepared_xact`, or `replication_slot`), `pg_prepared_xacts_count`, `pg_prepared_xacts_oldest_age_seconds`, and `pg_xmin_horizon_holder_age_xids{holder,identity}` naming the single worst offender (application_name / gid / slot_name). Sourced from the cheap in-memory views `pg_stat_activity`, `pg_prepared_xacts`, and `pg_replication_slots` (shared pool, no per-database fan-out).
- **Sequence exhaustion metric** ([#28](https://github.com/nbari/pg_exporter/issues/28)): new opt-in multi-database collector `--collector.sequences` exposing `pg_sequence_used_ratio{schemaname,sequencename,datname}` from `pg_sequences`, a months-in-advance warning before an `int4` primary-key sequence overflows (`nextval: reached maximum value of sequence`). To keep cardinality bounded it exports only sequences at or above `--sequences.min-ratio` (default `0.5`, env `PG_EXPORTER_SEQUENCES_MIN_RATIO`), so a healthy young database exports nothing. Alert on `pg_sequence_used_ratio > 0.75`.
- **Additional low-cardinality stats** ([#29](https://github.com/nbari/pg_exporter/issues/29)):
  - New opt-in `--collector.slru` exposing `pg_stat_slru` cache counters (`pg_stat_slru_blks_hit_total`, `_blks_read_total`, `_blks_zeroed_total`, `_blks_written_total`, `_blks_exists_total`, `_flushes_total`, `_truncates_total`, labeled by `name`; PostgreSQL 13+) — sustained `subtrans`/`multixact` disk reads reveal subtransaction/savepoint storms.
  - `--collector.database` gains connection-churn session stats (`pg_stat_database_sessions_total`, `_sessions_abandoned_total`, `_sessions_fatal_total`, `_sessions_killed_total`, `_session_time_seconds_total`; PostgreSQL 14+) and the data-corruption canary `pg_stat_database_checksum_failures_total` (+ `_checksum_last_failure_timestamp_seconds`; PostgreSQL 12+). The always-on `settings` collector now also exposes `pg_settings_data_checksums` (`1` = on / `0` = off) so a `0` failure count can be distinguished from a cluster with checksums *disabled* (the pre-PostgreSQL-18 default), where the canary can never fire.
  - `--collector.replication` gains logical-slot spill/stream stats from `pg_stat_replication_slots` (`spill_txns`/`spill_count`/`spill_bytes`, `stream_txns`/`stream_count`/`stream_bytes`, `total_txns`/`total_bytes`; PostgreSQL 14+).
  - `--collector.vacuum` gains `pg_stat_progress_create_index` (PostgreSQL 12+) and `pg_stat_progress_analyze` (PostgreSQL 13+) progress metrics for visibility during long migrations.
- **Grafana dashboard rows for the new signals**: added collapsed rows **Vacuum Horizon Blockers & Progress** (requires `--collector.vacuum`), **Sequence Exhaustion** (requires `--collector.sequences`), and **SLRU Cache** (requires `--collector.slru`); extended the **Replication** row with logical-slot spill/stream panels, the **Connection Analysis & Idle Age** row with session-churn panels (establishment/termination rate, session vs active time, and average session duration), and the **Critical Alerts** row with a data-checksum group after *Active vs Idle Connections* — a **Data Checksums** on/off state timeline (`pg_settings_data_checksums`), a **Checksum Failures Over Time** time series, and a last-failure-age stat. The **Exporter Self-Monitoring** row remains last. Added matching runbook sections (xmin horizon holders, sequence exhaustion, SLRU pressure, session churn / checksum / slot spill) to the troubleshooting guide.

## [0.15.2] - 2026-07-12

### Changed
- **Query ID Observability** ([#24](https://github.com/nbari/pg_exporter/issues/24)): Updated all `pg_stat_statements` Grafana dashboard panels to expose the `queryid` label in the legend alongside the truncated query string. Added a dedicated instructional panel detailing the exact SQL `JOIN` required to resolve a `queryid` to its full, untruncated SQL text on the database cluster.

## [0.15.1] - 2026-07-10

### Changed
- **Safer Default Connection Budget**: Reduced `--collectors.max-db-concurrency` / `PG_EXPORTER_MAX_DB_CONCURRENCY` from `5` to `2`, making the default per-process maximum the shared pool (`3`) plus two ephemeral per-database connections (`5` total). Configured concurrency is now restricted to `1..=16` to reject accidental connection waves at startup.
- **Dedicated Role Guidance**: Documented a `NOSUPERUSER CONNECTION LIMIT 5` exporter role with `pg_monitor` and per-database `CONNECT` as the database-side backstop for the application connection budget, including reserved-superuser-slot, approximate role-limit enforcement, and multi-replica behavior.

### Added
- **Five-Connection Regression Coverage**: Added a real limited-role test that runs every collector with `pg_monitor` but no application-table access, plus role-limit, locked-table, concurrent-scrape, abandoned-client, and soak assertions to prevent the issue #23 connection-exhaustion path from returning.

### Fixed
- **Package Hygiene**: Excluded generated benchmark artifacts from the published crate.

## [0.15.0] - 2026-07-09

### Added
- **Scrape Safety Defaults** ([#23](https://github.com/nbari/pg_exporter/issues/23)): Added timeout defaults to every exporter-managed scrape connection: connect/acquire timeout `5000ms`, server-side `lock_timeout=2000ms`, server-side `statement_timeout=10000ms`, and a whole `/metrics` scrape timeout of `15000ms`. These are configurable with `--scrape.connect-timeout-ms` / `PG_EXPORTER_CONNECT_TIMEOUT_MS`, `--scrape.lock-timeout-ms` / `PG_EXPORTER_LOCK_TIMEOUT_MS`, `--scrape.statement-timeout-ms` / `PG_EXPORTER_STATEMENT_TIMEOUT_MS`, and `--scrape.timeout-ms` / `PG_EXPORTER_SCRAPE_TIMEOUT_MS`.
- **Operator-Controlled Timeout Overrides**: `lock_timeout` can still be overridden from the DSN or `PGOPTIONS`, including `lock_timeout=0`, matching PostgreSQL's normal operator model. `statement_timeout=0` is rejected because it disables the server-side backstop; any custom `statement_timeout` must be positive and lower than the whole scrape timeout.
- **Scrape Gate and HTTP Failure Semantics**: Added a single-scrape gate for `/metrics`. A concurrent scrape returns `503 Service Unavailable`, collector/query/encoding failures return `503`, and a whole-scrape timeout returns `504 Gateway Timeout`. Plain PostgreSQL outages keep returning `200` with `pg_up 0` so alerting can distinguish "exporter down" from "database down".
- **Critical Regression Coverage**: Added locked-table reproductions for the failure mode from #23, an end-to-end `/metrics` regression that holds `ACCESS EXCLUSIVE` on a table, bounded-connection assertions, and a source-level safety test that prevents collectors from bypassing the shared connection budget or adding client-side database timeouts.
- **Bounded Per-Database Connects**: Restored the 5-second client-side timeout around ephemeral per-database `PgConnection::connect_with` calls and added a guardrail so future refactors cannot leave multi-database collectors with unbounded connection establishment.

### Changed
- **No Stale Collector Data on Failed Scrapes**: `/metrics` no longer returns stale collector data when the current scrape fails. Plain database outages still return `200`, but the response is filtered to fresh exporter-status metrics (`pg_up 0` and build info) instead of the previous collector snapshot.
- **Bounded Connection Model Tightened**: The exporter uses the shared pool for the default database and cluster-wide views, and ephemeral per-database connections behind the global `--collectors.max-db-concurrency` gate for non-default database catalog queries. The default maximum active database connections per exporter process is the shared pool (`3`) plus the per-database gate (`5`), for a default of about `8`.
- **Server-Side Timeout Model**: Removed collector-side client timeouts around database work so Rust does not abandon futures while PostgreSQL backends keep running. PostgreSQL's `lock_timeout` and `statement_timeout` are now the authoritative query-cancellation mechanisms, with the HTTP scrape timeout acting as the final user-facing boundary.

### Fixed
- **Locked Table Could Exhaust PostgreSQL Connections** ([#23](https://github.com/nbari/pg_exporter/issues/23)): A long-held `ACCESS EXCLUSIVE` lock could make scrape queries wait indefinitely, and repeated scrapes could accumulate blocked PostgreSQL backends until `max_connections` was exhausted. The default `lock_timeout` now makes lock-blocked scrape queries fail fast and release their connection slots.
- **Hidden Extra Pool Removed**: Removed the registry recovery pool so the exporter's connection footprint is easier to reason about and stays within the documented shared-pool plus per-database-concurrency budget.
- **Collector Safety Across Multi-Database Collectors**: Hardened `stat_user_tables`, `index_stats`, and `index_unused` so non-default database fan-out goes through the shared global permit and `util::open_db_connection`, keeping connections ephemeral and bounded across collectors.

## [0.14.0] - 2026-07-05

### Added
- **Table and Index Block I/O Metrics**: Added per-table block-I/O counters sourced from `pg_statio_user_tables` — `pg_stat_user_tables_heap_blks_read_total`, `pg_stat_user_tables_heap_blks_hit_total`, `pg_stat_user_tables_idx_blks_read_total`, `pg_stat_user_tables_idx_blks_hit_total`, `pg_stat_user_tables_toast_blks_read_total`, `pg_stat_user_tables_toast_blks_hit_total`, `pg_stat_user_tables_tidx_blks_read_total`, `pg_stat_user_tables_tidx_blks_hit_total` — and per-database index block-I/O from `pg_statio_user_indexes` — `pg_index_idx_blks_read_total`, `pg_index_idx_blks_hit_total`. These enable deep visibility into buffer cache hit ratios and raw disk I/O at the individual table and index level.
- **I/O Dashboard Panels**: Added new Grafana panels to visualize Table Cache Hit Ratio, Table Block I/O, Index Cache Hit Ratio, and Index Block I/O rates.
- **Documentation**: Added a "Buffer cache hit ratio (I/O pressure)" section to `docs/diagnosing-database-pressure.md` with `clamp_min`-guarded PromQL for the new block-I/O metrics, and updated the `README` with guidance for the new metrics and the `--collectors.max-db-concurrency` option.

### Changed
- **Dashboard Usability**: Enhanced existing dashboard panels (e.g. Cache Hit Ratios, Active Vacuum Progress) with detailed informational tooltips that include the raw `psql` diagnostic queries used behind the scenes.
- **Dashboard Legends**: Standardized aggregation methods across the dashboard (using `mean`) to ensure clearer scaling and visual grouping in legends.

### Fixed
- **Bounded Per-Database Connection Footprint**: The multi-database collectors (`stat_user_tables`, `index_stats`, `index_unused`) previously opened — and then *cached* — one connection per database, so a cluster with N databases pinned ~N connections. On instances with a low, shared `max_connections` (e.g. AWS RDS) this could exhaust connections and disrupt the application. Each collector now (a) caps concurrent per-database queries with a semaphore, and (b) opens each per-database connection **ephemerally** via `open_db_connection` — closed immediately after the query instead of cached. Together these keep the peak per-database connection footprint bounded by *scrape concurrency* (default 5), not by the *number of databases* (100 or 10,000 databases peak the same), dropping back to the shared pool between scrapes. The concurrency is configurable via `--collectors.max-db-concurrency` / `PG_EXPORTER_MAX_DB_CONCURRENCY` (default 5); raise it for faster scrapes on large clusters with connection headroom, or lower it on small/shared instances. Cluster-wide views (`pg_stat_activity`, `pg_locks`, `pg_stat_replication`, `pg_stat_progress_*`) still use only the shared pool. The ephemeral invariant is locked by a regression test (`tests/collectors/connection.rs`).
- **Multi-Database Vacuum Progress**: Reworked the `vacuum_progress` collector so vacuums running in non-default databases resolve human-readable `schema.table` labels instead of a bare numeric OID. It runs a single cluster-wide query against `pg_stat_progress_vacuum` (which already sees vacuums in every database, including template/non-connectable ones) and lazily opens a connection to another database only to resolve a table name when a cross-database vacuum is actually in progress — keeping the common idle case at one query with no extra connections. Metrics now expose separate `database` and `table` labels.
- **Collector Scrape Resilience**: Hardened the multi-database concurrency loop in `user_tables`, `index_stats`, and `index_unused` so a per-database failure (dropped or unreachable database) no longer fails the entire scrape, and an aggregated join-wait timeout is correctly counted as all pending databases failing (preventing a total stall from being silently reported as an empty snapshot).
- **Dashboard Database Filters**: Fixed 6 dashboard panels (including "Idle in Transaction", "Active vs Idle Connections", and "Database Freeze Age") that were missing the `datname=~"$database"` filter despite exporting database-specific metrics. They now perfectly respect the database drop-down selection.
- **Dashboard PG17 Compatibility**: Updated the `psql` diagnostic query provided in the "Active Vacuum Progress" info tooltip to remove `max_dead_tuples` and `num_dead_tuples`, ensuring the query runs cleanly across all supported PostgreSQL versions (PG14 through PG18).

## [0.13.1] - 2026-07-04

### Changed
- **Index Metrics Are Now Per-Database**: The `index` collector's metrics (`pg_index_scans_total`, `pg_index_tuples_read_total`, `pg_index_tuples_fetched_total`, `pg_index_size_bytes`, `pg_index_valid`, `pg_index_unused_count`, `pg_index_unused_size_bytes`, `pg_index_invalid_count`) now carry a `datname` label. The "Total Index Scans"/"Total Index Size" Grafana panels became per-database ("Index Scans by Database"/"Index Size by Database"); use `sum(...)` for a cluster-wide total.

### Fixed
- **Index Collector Multi-Database Support** ([#22](https://github.com/nbari/pg_exporter/issues/22), thanks @urbaned121): `--collector.index` queried the per-database catalog `pg_stat_user_indexes` only on the database named in the DSN, so index metrics reported 0 / no data for every other database (e.g. when connecting to `postgres` and excluding it). The collector now discovers all connectable, non-excluded databases and collects index statistics from each, reusing the shared per-database connection pooling in `collectors::util` (the same approach as the `stat` collector). A failing or unreachable database is skipped without failing the whole scrape.

## [0.13.0] - 2026-06-27

### Added
- **Blocking & Lock-Contention Metrics**: The `locks` collector now exposes `pg_blocked_sessions`, `pg_blocking_sessions`, `pg_longest_blocked_seconds`, and `pg_lock_waits{mode}` (ungranted locks by mode). These surface *who* is blocked, *who* is blocking, *how long* the worst wait has lasted, and *which* lock type is contended — the missing signals for diagnosing "a few queries are blocking the whole database".
- **On-CPU Backends Metric**: The `activity` collector now exposes `pg_stat_activity_on_cpu_backends` (active client backends with `wait_event IS NULL`, i.e. actually running on CPU). Compared against the instance vCPU count this is the cleanest CPU-saturation signal, especially for instances without a connection pooler.
- **Database-Pressure Diagnostics Guide**: Added `docs/diagnosing-database-pressure.md` with metrics-driven recipes for high CPU, blocking/lock contention, missing indexes (seq-scan vs index-scan PromQL), and connection saturation without pgbouncer.
- **Dashboard Panels**: Added Grafana panels for blocked vs blocking sessions, longest lock wait, ungranted lock waits by mode (Locks & Blocking row); average rows per seq scan, index-usage ratio, and seq-scan rate (Table Statistics row); and a new "CPU Pressure" row for on-CPU backends.
- **DevPod / Dev Containers Workspace**: Added a compose-based DevPod setup with an `app` container, PostgreSQL 18 with `pg_stat_statements`, mise-managed tooling, `scripts/dev-up`, `scripts/dev-ssh`, and a portable devcontainer config for Docker/remote providers.
- **Devcontainer Observability Stack**: Added `just metrics-dev` / `just metrics-dev-stop` for on-demand Prometheus + Grafana inside the devcontainer compose network, scraping the exporter at `app:9432` with dashboard hot-reload.
- **SQL Triage Recipes**: Added `just blocking`, `just on-cpu`, `just long-running`, `just seq-scans`, `just connections`, `just bloat`, and `just top-queries` for drilling from dashboard symptoms into live PostgreSQL sessions, locks, tables, and queries.

### Changed
- **CI Network Resilience**: Hardened GitHub Actions Rust downloads with cargo/rustup retry and transport settings to reduce transient hosted-runner failures.

## [0.12.0] - 2026-06-25

### Added
- **Checkpoint Tuning Metrics**: Added `pg_last_checkpoint_age_seconds` and `pg_wal_bytes_since_last_checkpoint` (sourced from `pg_control_checkpoint()`, available on all supported PostgreSQL versions) plus `pg_settings_max_wal_size_bytes` and `pg_settings_min_wal_size_bytes`. Together with the existing `pg_stat_checkpointer_*` counters these expose the `checkpoint_timeout` / `max_wal_size` / storage tradeoff: achieved checkpoint interval, checkpointer liveness, crash-recovery (RTO) volume, and whether checkpoints are time-driven or WAL-driven.
- **Checkpoints Dashboard Section**: Added a Grafana "Checkpoints" row with four panels (avg write+sync time per checkpoint, time since last checkpoint, WAL since last checkpoint, and checkpoints by trigger). Yellow reference lines overlay the live `checkpoint_timeout` and `max_wal_size` settings so they track configuration changes automatically.
- **Checkpoint Tuning Guide**: Added `src/collectors/default/README.md` documenting the checkpoint metrics and a metrics-driven decision tree for tuning `checkpoint_timeout` and `max_wal_size`.
- **24h Rust Soak Toolkit**: Added `scripts/benchmark/run-rust-soak.sh`, `scripts/benchmark/check-rust-soak.sh`, and a dedicated Grafana soak dashboard (`scripts/benchmark/rust-soak-dashboard.json`) to run phased long-duration stress tests and collect reliability/performance artifacts.
- **Idle Connection Triage Panel**: Added a dedicated Grafana panel ranking databases by idle, idle-in-transaction, and aborted-idle-in-transaction connection pressure so connection culprits are visible without drilling into broader state charts.

### Changed
- **Dependency Upgrades**: Upgraded major dependencies including `sqlx` 0.8 → 0.9, `tower-http` 0.6 → 0.7, the OpenTelemetry stack (`opentelemetry`, `opentelemetry-otlp`, `opentelemetry_sdk`, `opentelemetry-http`) 0.31 → 0.32, `tracing-opentelemetry` 0.32 → 0.33, and `sysinfo` 0.38 → 0.39, plus compatible bumps across the tree.
- **sqlx 0.9 Compatibility**: Adapted dynamic SQL call sites to sqlx 0.9's `SqlSafeStr` requirement using `AssertSqlSafe`, and enabled the `serde` `derive` feature explicitly (no longer pulled in transitively by sqlx).

### Fixed
- **Test Pool Close Deadlock**: Fixed a connection-collector test that held a pooled connection across `Pool::close()`, which deadlocks under sqlx 0.9's stricter close semantics that wait for all checked-out connections to be returned.

## [0.11.0] - 2026-03-07

### Added
- **Configurable Statements Top-N**: Added `--statements.top-n` and `PG_EXPORTER_STATEMENTS_TOP_N` so `pg_stat_statements` export cardinality can be tuned without code changes.
- **Recent Query Pressure Dashboard Row**: Added a second Grafana `pg_stat_statements` row focused on recent 5-minute pressure (DB time added, mean time, call rate, WAL added, temp blocks added) to complement the existing cumulative "since reset" view.
- **Vacuum Workflows for Local Testing**: Added `just vacuum-workflow` for deterministic manual vacuum testing and `just autovacuum-workflow` for PostgreSQL-managed cleanup testing against local `pgbench` data.
- **Vacuum / Bloat Dashboard Panels**: Added Grafana panels for estimated bloat ratio, estimated dead space, table size, autovacuum threshold pressure, and time since last autovacuum to make repack and `VACUUM FULL` candidates easier to identify.
- **Never-Autovacuumed Visibility**: Added `pg_stat_user_tables_never_autovacuumed` and `pg_stat_user_tables_never_autoanalyzed` so neglected tables are no longer indistinguishable from recently maintained ones.

### Changed
- **Statements Default Scope**: Reduced the default `pg_stat_statements` exporter-side top-N from 100 to 25 to lower scrape cost and time-series cardinality while keeping query-level visibility useful by default.
- **CLI Option Layout**: Documented the collector-specific option convention using long-only flags in `src/cli/commands/options.rs` with typed values carried through `src/collectors/config.rs`.
- **Statements Query Coverage**: Switched statement role resolution from `pg_user` to `pg_roles` with `LEFT JOIN` semantics so valid statement rows are not dropped when login-user views are too narrow.
- **Statements Extension Detection Overhead**: Cached `pg_stat_statements` extension availability inside the collector to avoid querying `pg_extension` on every scrape while still rechecking periodically if the extension is missing.
- **Query Dashboard Semantics**: Clarified Grafana `pg_stat_statements` panel titles, descriptions, and legends so cumulative panels explicitly mean "since reset" and legends include `datname` for better disambiguation.
- **PostgreSQL Support Floor**: Updated documentation and dashboard wording to reflect the supported PostgreSQL range of 14+.
- **Exporter Session Tagging**: Applied a default PostgreSQL `application_name` of `pg_exporter` to exporter-managed connections so logs and `pg_stat_activity` identify exporter sessions explicitly.
- **Vacuum Dashboard Focus**: Reworked the Grafana vacuum row to prioritize predictive vacuum debt and bloat signals before active vacuum execution, matching real-world repack and autovacuum troubleshooting better.
- **`stat_user_tables` Threshold Semantics**: Autovacuum and autoanalyze threshold ratios now honor per-table reloptions instead of assuming only global PostgreSQL settings.
- **Table DML Dashboard Semantics**: Switched the table DML panel from `rate(...)` to reset-safe `delta(...)/300` queries to avoid PromQL warnings on exporter snapshot metrics.
- **Activity Dashboard Semantics**: Reworked the long-running query panel to show the real exported metrics (`pg_stat_activity_oldest_query_age_seconds` and per-database max query duration) instead of duplicating one series under two labels.

### Fixed
- **Exporter Open-FD Inflation**: Fixed high `pg_exporter_process_open_fds` values on Linux by narrowing self-monitoring refresh scope to the exporter PID only (instead of refreshing all processes via `sysinfo`). This removes persistent `/proc/*/stat` descriptor caching for unrelated processes.
- **Statements Self-Observation Noise**: Excluded the statements collector's own `pg_stat_statements` query from exported top-N results so quiet systems are less likely to show the exporter itself as a top query.
- **Statements Failed-Scrape Data Loss**: Statement metrics are now reset only after a replacement query succeeds, preserving the last good snapshot across transient query failures.
- **Vacuum Progress Stale Series**: `pg_stat_progress_vacuum`-based metrics now clear finished table series instead of leaving stale vacuum progress visible after the vacuum ends.
- **Vacuum Stats Failed-Scrape Data Loss**: Vacuum freeze-age and autovacuum-worker metrics are now replaced only after all source queries succeed, preserving the last good snapshot across transient failures.
- **Autovacuum Detection Accuracy**: Vacuum progress now identifies autovacuum using backend type instead of query-text matching.
- **`stat_user_tables` Failed-Scrape Data Loss**: Table statistics are now fetched before reset so a full collection failure preserves the last good snapshot instead of blanking the metrics.
- **`stat_user_tables` Dynamic Database Churn**: Partial per-database failures (for example, databases dropped between discovery and scrape) are now logged while still publishing the successful portion of the snapshot.
- **Long-Running Query Stale Series**: `activity/queries` now clears finished long-running query label sets before publishing the current snapshot, preventing stale wait/state/duration series from lingering after incidents end.

## [0.10.3] - 2026-03-04

### Fixed
- **`pg_stat_statements` UTF-8 Label Truncation Panic**: Fixed a panic when truncating `query_short` labels at 80 bytes if PostgreSQL returned text ending mid-multibyte UTF-8 character. Truncation now uses a safe character boundary (`floor_char_boundary`) before appending `...`.

### Tests
- **Issue #15 Regression Coverage**: Added collector regression tests for multibyte UTF-8 query text (including Cyrillic) to verify byte-80 boundary behavior and ensure `pg_statements` collection remains panic-free.

## [0.10.2] - 2026-02-28

### Fixed
- **Startup / Reboot Resilience**: Improved behavior when the exporter starts before PostgreSQL is fully ready. The exporter now starts independently of immediate database availability, reports `pg_up = 0` during the outage window, and recovers on later scrapes once PostgreSQL becomes reachable again.
- **Systemd Startup Ordering**: Documented PostgreSQL-aware startup ordering in the bundled systemd unit so deployments can avoid boot-time races by starting `pg_exporter` after `postgresql.service`.
- **`pg_stat_statements` Test Compatibility**: Fixed the test helper to use the correct `pg_stat_statements_reset` function signature across PostgreSQL 14-18.

### CI
- **Coverage Database Setup**: Added the missing `pg_stat_statements` preload and initialization steps to the coverage workflow so coverage runs match the normal PostgreSQL test matrix setup.
- **Pre-commit SQL Check Scope**: Narrowed the `pg_stat_statements` cast warning in the pre-commit hook to staged collector source files so workflow and test-seed queries do not trigger false positives.

## [0.10.1] - 2026-02-24

### Changed
- **Replication Integration Topology**: Replaced shell-script-driven replication setup with in-test `testcontainers` orchestration for a real primary+replica PostgreSQL topology.
- **Replication Semantics Coverage**: Added end-to-end assertions for role and lag semantics across primary sentinel behavior, replica backlog (`lag > 0`), replica catch-up (`lag = 0`), and broken-path error handling.
- **WAL Dashboard Signals**: Updated WAL dashboards to focus on default collector runtime signals (throughput, record activity, buffer pressure, FPI ratio) and replication max lag bytes.
- **Dashboard Layout**: Reworked WAL layout to a 2x2 panel grid and adjusted section ordering/row behavior for consistent rendering.

### CI
- **Container Runtime Wiring**: Added explicit runtime socket detection/export (`DOCKER_HOST`) for testcontainers so replication topology tests run consistently with Docker or Podman sockets.

### Fixed
- **Uptime Panel Semantics**: Uptime now renders `DOWN` in red when `pg_up = 0` instead of showing stale uptime values.
- **Metrics Stack Provisioning**: Ensured dashboard provisioning file mode is readable in the Grafana container (`0644`) to prevent missing dashboard after `just restart-metrics`.
- **Dashboard Validator Portability**: Hardened `scripts/validate-dashboard.sh` with repo-root path resolution, temp-file safety, command checks, and portable regex usage.

## [0.10.0] - 2026-02-23

### Added
- **Database Outage Resilience**: The exporter now starts and stays available even if PostgreSQL is unreachable.
- **Scrape Behavior**: The `/metrics` endpoint now always returns HTTP 200 during database outages to prevent unnecessary exporter-down alerts. Use `pg_up` for database availability; `/health` continues to reflect database status.
- **`pg_up` Reporting**: Improved `pg_up` metric to accurately reflect database connectivity. It is now driven by a dedicated connectivity check and is no longer overwritten by other collectors.
- **Metric Omission**: Database-dependent metrics are now omitted from the output when the database is unreachable, rather than reporting stale or zero values.
- **Deferred Version Initialization**: If the PostgreSQL version cannot be detected at startup, it is automatically retried during the first successful scrape.
- **State Management**: Expanded metric `.reset()` coverage across collectors (activity, database, stat tables, locks, vacuum, TLS, and statements) so dropped/renamed objects are cleared from Prometheus output.
- **Dashboard Enhancements**: Added new panels for Replication Lag and Server Role to the Grafana dashboard.

### Fixed
- **Startup Hangs**: Implemented lazy connection pooling and startup timeouts to ensure the HTTP server binds and starts regardless of database state.
- **Collection Timeouts**: Added 5-second acquisition timeouts to prevent long-running scrapes when PostgreSQL is unresponsive.
- **Connections Collector**: Fixed a bug where `pg_stat_activity_max_connections` (and derived utilization metrics) would default to 100 if the query failed, leading to misleading data. Errors are now propagated correctly.
- **Replication Lag Type Handling**: Fixed replica lag SQL expression typing/null handling so `pg_replication_lag_seconds` reports correctly on standby lag scenarios instead of collapsing to `0`.

### Changed
- **Replication Lag Compatibility**: `pg_replication_lag_seconds` behavior is aligned with `postgres_exporter` semantics (`0` on primary and non-negative lag on replicas). Use `pg_replication_is_replica` to distinguish server role.
- **Precision Timing**: Updated `pg_stat_statements` latency metrics to use high-precision floating point math for sub-second visibility.
- **Code Quality**: Replaced hardcoded system schema strings and time conversion factors with shared constants for better maintainability.
- **Dependencies**: Updated all dependencies to their latest compatible versions.

## [0.9.7] - 2026-02-06

### Added
- **Docker Secrets Support**: Added `PG_EXPORTER_DSN_FILE` environment variable to read PostgreSQL DSN from a file (e.g., Docker secrets mounted at `/run/secrets/pg_dsn`). Priority: `PG_EXPORTER_DSN_FILE` > `PG_EXPORTER_DSN` > `--dsn` flag > default value.

## [0.9.6] - 2026-02-06

### Fixed
- **Version Collector**: Fixed duplicate version metrics after PostgreSQL upgrades. Metrics are now reset before collection to prevent exporting stale label combinations (e.g., both version 16 and 17 simultaneously).

## [0.9.5] - 2025-12-15

### Fixed
- **Scraper**: Fixed a bug where scrape metrics were recorded twice (once explicitly and once via `Drop`), causing inaccurate scrape counts and duration metrics. Implemented `recorded` flag in `ScrapeTimer` to ensure metrics are recorded exactly once (RAII safe).

## [0.9.4] - 2025-12-14

### Added
- **Local Metrics Stack Helpers**
  - Added `just metrics` and `just restart-metrics` recipes to spin up Prometheus + Grafana with the bundled dashboard and persistent Prometheus volume.
  - Auto-builds/rebuilds the Grafana stack image when `grafana/dashboard.json` changes and exposes Grafana (3000) and Prometheus (9090) for quick local testing against the exporter on port 9432.

### Changed
- **Dependencies**
  - Ran `cargo update` to pull in the latest compatible dependency patch releases.

## [0.9.3] - 2025-12-01

### Changed
- **Container Image Improvements**
  - Migrated from Alpine to distroless base image (gcr.io/distroless/static-debian12:nonroot)
  - Reduced final image size to ~12.8MB with improved security posture
  - Removed unnecessary runtime dependencies (postgresql-client)
  - Added dynamic port support via `PG_EXPORTER_PORT` environment variable
  - Documented all available environment variables in Containerfile
  - Removed misleading default DSN (now requires explicit configuration)
  - Runs as non-root user (UID 65532) by default
  - Removed built-in HEALTHCHECK (use external health checks with orchestrators)
  - Optimized layer caching for faster rebuilds

## [0.9.2] - 2025-12-01

### Fixed
- **Exporter Collector (`--collector.exporter`)**
  - Fixed `pg_exporter_metrics_total` to accurately count active time series/cardinality
  - Metric now matches `curl -s 0:9432/metrics | grep -vEc '^(#|\s*$)'`
  - Previously counted MetricFamily objects instead of actual exported time series
  - Improved UTF-8 handling with zero-copy optimization and lossy fallback for robustness
  - Fixed overflow handling to return 0 instead of i64::MAX for safer alert behavior

### Changed
- Updated `pg_exporter_metrics_total` description to clarify it tracks "Total active time series / cardinality (non-comment, non-empty lines)"
- Enhanced exporter collector documentation with explicit cardinality counting behavior

## [0.9.1] - 2025-11-22

### Fixed
- **Grafana Dashboard TLS/SSL Group**
  - Removed invalid database filter from TLS Version and Cipher queries
  - Fixed SSL Status and Certificate Status panels to display values only
  - Converted Certificate Expiry from gauge to stat panel
  - Changed Certificate Validity Period unit for better readability
  - Fixed TLS/SSL row structure with all 8 panels properly nested
  - Resolved duplicate panel IDs across dashboard

### Changed
- Enhanced TLS panel descriptions with cipher recommendations and security context
- Set TLS/SSL dashboard group to collapsed by default

### Development
- Added `test-tls` recipe to justfile for automated SSL-enabled PostgreSQL testing
- Improved `just test` to auto-start PostgreSQL container if not running
- Added idempotent deployment checks to prevent duplicate version bumps
- Fixed bump recipes to commit both Cargo.toml and Cargo.lock

## [0.9.0] - 2025-11-21

### Added
- **TLS/SSL Monitoring Collector** (disabled by default, enable with `--collector.tls`)
  - Certificate expiration tracking (`pg_ssl_certificate_expiry_seconds`)
  - Certificate validity status (`pg_ssl_certificate_valid`)
  - Certificate validity timestamps (`pg_ssl_certificate_not_before_timestamp`, `pg_ssl_certificate_not_after_timestamp`)
  - Server SSL configuration (`pg_ssl_enabled`)
  - Active connection TLS statistics (`pg_ssl_connections_total`, `pg_ssl_connections_by_version`, `pg_ssl_connections_by_cipher`, `pg_ssl_connection_bits_avg`)
  - Supports PostgreSQL 9.5+ for connection stats
  - Graceful handling of remote installations (certificate metrics require local filesystem access)
- Comprehensive TLS collector test suite with SSL-enabled PostgreSQL testing
- TLS testing documentation and helper scripts

### Changed
- **Strict Clippy Linting Enforced**
  - All Rust warnings denied
  - Pedantic clippy lints enabled
  - Safety-critical lints enforced (`unwrap_used`, `expect_used`, `panic`, `indexing_slicing`)
  - Code quality improvements across entire codebase
- **Full rustls migration** - Removed OpenSSL dependency entirely
  - Removed `[features]` section from Cargo.toml (musl feature no longer needed)
  - Removed openssl dependency from production builds (100% rustls)
  - Disabled default features for opentelemetry-otlp to avoid reqwest/native-tls
  - OpenTelemetry now uses tonic with rustls for gRPC TLS
  - Updated CI/CD workflows to build musl binaries without feature flags
  - Simplified dependency tree and improved binary portability
  - musl static binaries now build without vendored OpenSSL
- Cleaned up unnecessary `#[allow(clippy::unwrap_used)]` and `#[allow(clippy::expect_used)]` attributes from tests
- Improved dashboard validation script to correctly detect metrics with namespace prefixes

### Fixed
- Dashboard validation now correctly handles `postgres_*` prefixed metrics from namespaced collectors
- Removed 51 unnecessary `#[allow(clippy::unwrap_used)]` attributes from test functions
- Removed 41 unnecessary `#[allow(clippy::expect_used)]` attributes (31 from non-test code, 10 from tests)

### Documentation
- Added TLS_TESTING.md with comprehensive testing guide
- Updated documentation for all TLS-related collectors
- Improved code documentation with safety notes

### Internal
- Added x509-parser dependency for certificate parsing
- Enhanced error handling in collectors
- Improved test coverage with 415 total tests (171 unit + 10 integration + 234 collector tests)

### Backward Compatibility
- ✅ No breaking changes
- ✅ All existing command-line flags preserved
- ✅ Default collectors unchanged
- ✅ Existing metrics unchanged
- ✅ Dashboard compatibility maintained

## [0.8.5] - Previous Release

See git history for previous changes.
