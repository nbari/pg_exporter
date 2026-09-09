# `system` Collector

The `system` collector exposes **host-wide** CPU and memory usage for the machine
`pg_exporter` runs on. It never touches `PostgreSQL`: it reads only the operating
system (`/proc/stat` on Linux, the `kern.cp_time`/`kern.cp_times` sysctls on
FreeBSD, and `sysinfo` for memory and load average), so it adds **no query or
connection load** to the database.

This collector is **opt-in** and is only supported on **Linux** and **FreeBSD**.
On any other operating system it registers cleanly but exports no CPU series and
logs a single warning.

## When to enable it

Enable it only when the exporter runs on the **same host** as `PostgreSQL`:

```bash
pg_exporter --dsn postgresql:///postgres?user=postgres_exporter --collector.system
```

Do **not** enable it for managed services such as AWS RDS/Aurora. There the
exporter runs on a separate machine, so the CPU/memory numbers would describe the
exporter's host — not the database server — and be misleading. When `system` is
enabled and the DSN host is not a loopback/Unix-socket address, the exporter logs
a startup warning to that effect.

## Metrics

### CPU

CPU time is exposed as node_exporter-style cumulative **per-core** counters in
**seconds** (one series per logical core and mode), mirroring
`node_cpu_seconds_total`. Aggregate host utilization is derived in `PromQL`, so
there is no separate aggregate metric and no flag to configure. The exporter
advances each counter only by positive OS deltas, ignoring small backwards
accounting corrections and re-baselining after CPU hotplug/reset.

- `pg_system_cpu_seconds_total{cpu,mode}` — per-core CPU seconds.
  Linux modes: `user`, `nice`, `system`, `idle`, `iowait`, `irq`, `softirq`,
  `steal`. FreeBSD modes: `user`, `nice`, `system`, `interrupt`, `idle`.
- `pg_system_cpu_cores` — logical cores in the same `/proc/stat` /
  `kern.cp_times` sample as the counters.
- `pg_system_cpu_cores_physical` — physical cores, when the OS reports it.
- `pg_system_load1`, `pg_system_load5`, `pg_system_load15` — 1/5/15-minute load
  average (Linux; `0` where the OS does not provide it).

Cardinality is bounded per host (modes × cores) and does **not** scale with the
number of databases; on a 32-core host that is a few hundred fixed series.

### Memory

Memory and swap are reported in **bytes** as gauges.

- `pg_system_memory_total_bytes`
- `pg_system_memory_available_bytes`
- `pg_system_memory_free_bytes`
- `pg_system_memory_used_bytes`
- `pg_system_swap_total_bytes`
- `pg_system_swap_used_bytes`
- `pg_system_swap_free_bytes`

> **FreeBSD memory caveat:** on FreeBSD (and Windows) `sysinfo` reports
> `available == free`, which understates reclaimable memory (it excludes cache
> that could be evicted under pressure). The raw building blocks are exported so
> dashboards can compute a memory-used ratio as `(total - available) / total`,
> which is accurate on Linux.

### `PostgreSQL` process group

To answer *"is `PostgreSQL` itself eating the box, or a noisy neighbour?"* the
collector also aggregates the host CPU and memory of every process whose name
starts with `postgres` (the postmaster and all backends). These series are
labeled only by the fixed `group="postgres"` — there is **no per-PID label**, so
cardinality stays constant regardless of how many backends exist.

- `pg_system_process_group_cpu_seconds_total{group="postgres"}` — cumulative CPU
  **seconds** consumed by the group. It is built by summing positive per-PID
  deltas across scrapes, so backend churn never makes the counter go backwards.
  Read it as CPU-cores with `rate(...)`; divide by `pg_system_cpu_cores` for the
  busy fraction of the whole host.
- `pg_system_process_group_memory_bytes{group="postgres"}` — private resident
  memory of the group in **bytes** (summed RSS on FreeBSD).
- `pg_system_process_group_count{group="postgres"}` — number of live `postgres*`
  processes.

> **Private RSS by default, PSS opt-in:** on Linux the memory gauge is the
> group's **private resident memory** — `resident − shared` from
> `/proc/<pid>/statm`, a single cheap read per process. The shared field is
> subtracted because raw RSS charges `shared_buffers` to every backend that has
> touched it: summed over 208 backends on one production primary, that reported
> **312 GiB on a 93.8 GiB host**, and the series moved with connection count
> rather than memory pressure (issue #36). What the gauge tracks instead is the
> memory that grows with `work_mem`, sorts and hash joins; `shared_buffers`
> itself is static configuration, already exported via `pg_settings`. The result
> is anonymous RSS, so copy-on-write pages inherited from the postmaster are
> still charged per backend — a residual bounded by the postmaster's own
> footprint, not by `shared_buffers`. Passing
> `--system.process-memory=pss` switches to **PSS** (proportional set size) from
> `/proc/<pid>/smaps_rollup`, which divides shared pages proportionally so
> `shared_buffers` is counted **once** across all backends rather than excluded.
> PSS is more accurate but **far more expensive**: `smaps_rollup`
> makes the kernel walk every page-table entry of every mapping, costing
> `O(processes × resident pages)`. On a production primary with 253 `postgres`
> processes and `shared_buffers = 15939MB` it took **13.851 s** versus **0.016 s**
> for the equivalent `statm` reads — 92% of a 15 s scrape budget (issue #35). PSS
> also requires the exporter to run as the `postgres` user or as root; without
> that permission it falls back to the `statm` read. Only enable PSS if you have
> measured the cost on your own host. On FreeBSD the gauge is always summed
> **RSS**, which still over-counts shared memory.
>
> Watch `pg_exporter_collector_scrape_duration_seconds{collector="system"}` after
> changing this. That series belongs to the `exporter` collector, so it only exists
> if you also pass `--collector.exporter`; read the magnitude from `_sum / _count`,
> because the histogram's top bucket is 5 s and a PSS walk simply lands in `+Inf`.
> `_count` covers successes **and** aborted scrapes (an abort observes its
> time-until-abort, an error observes nothing), so during a run of scrape timeouts that
> ratio is pulled towards `--scrape.timeout-ms`. Subtract
> `pg_exporter_collector_scrape_aborted_total{collector="system"}` from the count to see
> what the scrapes that actually finished cost.
>
> If any system OS sample outlives its scrape, the overlapping scrape **skips** that
> sub-collector before submitting another blocking task and serves its previously published
> values. A started `spawn_blocking` task cannot be cancelled, so every system sub-collector
> has a one-slot submission guard. Skipped samples are invisible in the gauges themselves
> (they go stale until the in-flight sample publishes); watch
> `pg_exporter_collector_scrape_aborted_total{collector="system"}` for scrapes that timed out
> while system collection was in flight.

## Interpreting the Counters

- **Host-wide busy fraction**:
  `1 - avg without(cpu)(rate(pg_system_cpu_seconds_total{mode="idle"}[5m]))`.
- **Per-mode busy, normalized across all cores**:
  `avg without(cpu)(rate(pg_system_cpu_seconds_total{mode!="idle"}[5m]))`.
  Sustained high `iowait` points at storage pressure; high `steal` points at a
  noisy neighbor on the hypervisor.
- **Single hot core** (drill into per-core detail):
  `sum without(mode)(rate(pg_system_cpu_seconds_total{mode!="idle"}[5m]))` —
  keeps the `cpu` label so you can spot one saturated core (e.g. a pinned
  single-threaded job) while the rest idle.
- **Memory pressure**:
  `(pg_system_memory_total_bytes - pg_system_memory_available_bytes) / pg_system_memory_total_bytes`.
- **`PostgreSQL` share of the host CPU**:
  `rate(pg_system_process_group_cpu_seconds_total{group="postgres"}[5m]) / pg_system_cpu_cores` —
  the fraction of the whole machine burned by `postgres*` processes; compare it
  with the host-wide busy fraction to tell `PostgreSQL` apart from a neighbour.
- **`PostgreSQL` resident memory**:
  `pg_system_process_group_memory_bytes{group="postgres"}` (private resident
  memory on Linux by default — `shared_buffers` excluded, so the gauge tracks
  backend-private growth instead of connection count;
  `--system.process-memory=pss` reports PSS instead, at a significant scrape
  cost).
