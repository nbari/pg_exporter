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
- `pg_system_process_group_memory_bytes{group="postgres"}` — resident memory of
  the group in **bytes**.
- `pg_system_process_group_count{group="postgres"}` — number of live `postgres*`
  processes.

> **PSS vs RSS:** on Linux the memory gauge is **PSS** (proportional set size)
> from `/proc/<pid>/smaps_rollup`, which divides shared pages proportionally, so
> `shared_buffers` is counted **once** across all backends rather than multiplied
> per connection. PSS requires the exporter to run as the `postgres` user or as
> root; without that permission it falls back to **RSS** (from
> `/proc/<pid>/statm`), which over-counts shared memory. On FreeBSD the gauge is
> summed **RSS**.

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
  `pg_system_process_group_memory_bytes{group="postgres"}` (PSS on Linux) tracks
  the real footprint without multiplying `shared_buffers` per backend.
