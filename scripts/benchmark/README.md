# Benchmark Soak Workflows

This directory contains benchmark tooling for long-running Rust exporter soak
tests on the benchmark VMs.

The soak override enables every database-facing collector, including the
version/permission-gated collectors changed in 0.18.0, plus
`--collector.system`. The resulting `pg_system_*` metrics describe the exporter
VM; database-host CPU and memory remain separate and come from node_exporter.

## Files

- `run-rust-soak.sh`: Orchestrates a Rust-only phased soak run (default 24h),
  deploys a dedicated Grafana dashboard, configures the Rust exporter with the
  relevant collectors, starts workload phases on the DB host, and starts a
  Prometheus sampler on the metrics host. A separate PostgreSQL session keeps a
  transaction open while holding `ACCESS EXCLUSIVE` on a dedicated table for
  the entire workload. The sampler also probes `/metrics` directly with a
  20-second bound so a prompt `503` can be
  distinguished from an exporter hang. The DB host independently samples
  `pg_stat_activity` every 30 seconds and fails the run if `pg_exporter`
  exceeds its five-connection budget.
- `check-rust-soak.sh`: Prints workload/sampler status, connection-budget and
  lock checks, recent logs, and the accumulated comparison-signal summary for a
  given run id.
- `rust-soak-dashboard.json`: Grafana dashboard focused on exporter reliability,
  collector scrape cost, activity/locks/statements pressure, and vacuum debt.

## Quick Start

The launcher derives the required exporter version from the local `Cargo.toml`
and refuses to start if `/usr/local/bin/pg_exporter` on the Rust VM is older or
newer. Run the read-only preflight first:

```bash
./scripts/benchmark/run-rust-soak.sh --preflight-only
```

Then choose a stable run id and start the 24-hour run:

```bash
RUN_ID="release-$(sed -n 's/^version = "\([^"]*\)"/\1/p' Cargo.toml | head -n 1)-$(date -u +%Y%m%dT%H%M%SZ)"
./scripts/benchmark/run-rust-soak.sh --hours 24 --run-id "${RUN_ID}"
printf 'Run ID: %s\n' "${RUN_ID}"
```

The command returns after starting the remote jobs. Keep the run id; it is the
key for every status and artifact command.

## Lab Address and Prometheus Overrides

The checked-in defaults describe the local benchmark lab:

```bash
BENCH_RUST_SSH=10.246.1.90
BENCH_DB_SSH=10.246.1.92
BENCH_METRICS_SSH=10.246.1.93
BENCH_SSH_CONFIG=~/.ssh/config
PROM_JOB=pg_exporter_rust
BENCH_RUST_INSTANCE=10.246.1.90:9432
BENCH_DB_NODE_INSTANCE=10.246.1.92:9100
BENCH_RUST_METRICS_URL=http://10.246.1.90:9432/metrics
BENCH_RUST_DB_CLIENT_ADDR=10.246.1.90
BENCH_GRAFANA_URL=http://10.246.1.93:3000
```

Override the SSH endpoints separately from Prometheus's `instance` labels. This
matters when SSH uses aliases or `user@host` while Prometheus uses an IP address.
The launcher verifies both target labels before changing remote state.
The database-connection budget is filtered by `BENCH_RUST_DB_CLIENT_ADDR`, so a
second exporter using the same `application_name` does not contaminate the run.
After applying the systemd override it also performs a real scrape and requires
`pg_exporter_collector_last_scrape_success == 1` for every enabled top-level
collector. A version-gated or permission-gated collector may legitimately
publish no source series, but it must still settle successfully and cannot block
the scrape.

## Workload Schedule

For a 24-hour run, the phases are:

| Phase | Duration | Purpose |
| --- | ---: | --- |
| Baseline | 2h | Establish normal exporter CPU, RSS, FD, and scrape cost |
| Statements pressure | 4h | Stress `pg_stat_statements` and top-N processing |
| Lock/activity pressure | 4h | Exercise lock and activity collectors |
| Vacuum-debt build | 6h | Accumulate dead tuples with autovacuum disabled on benchmark tables |
| Autovacuum recovery | 4h | Observe recovery and progress metrics |
| Mixed churn | 4h | Combine statements, locks, and transactional load |

The workload restores the benchmark tables' autovacuum reloptions on normal
completion and from its exit trap. The systemd soak override intentionally
remains installed after the run so the final metrics remain inspectable.

## Monitor Progress

Run this whenever you want a snapshot:

```bash
./scripts/benchmark/check-rust-soak.sh --run-id <RUN_ID>
```

It reports the active phase and remaining phase time, exporter process uptime,
current `ps` CPU/RSS, accumulated exporter CPU/RSS/FD trends, scrape duration,
database-host CPU and available memory, PostgreSQL connection budget, direct
HTTP statuses, and recent workload/sampler logs. The sampler also writes a
one-line progress record every 15 samples (about 15 minutes).

The Grafana URL is printed by the launcher and stored in
`bench-artifacts/rust-soak/<RUN_ID>/run-meta.txt`.

After the run, collect the remote logs, CSV samples, state, and exporter journal:

```bash
./scripts/benchmark/check-rust-soak.sh --run-id <RUN_ID> --fetch
```

## Acceptance Checks

A healthy completed soak should have:

- `exporter_down=0` and `pg_down=0` in the resource summary;
- no direct-probe timeouts or HTTP `000` responses;
- exporter PostgreSQL connections never above the five-connection budget;
- no unexplained collector error growth or sustained scrape-duration increase;
- RSS and open FDs that settle rather than grow monotonically across phases;
- CPU increases that correlate with workload phases and returns toward the
  baseline afterward;
- the final workload state set to `status=complete`.

A bounded HTTP `503` can be informative during an intentionally failed scrape,
but inspect the exporter journal and error body. A timeout is always a failure.

## Restore the Exporter Service

After artifacts are collected, remove the soak-specific collector override if
the lab should return to its normal service configuration:

```bash
ssh 10.246.1.90 \
  'sudo mv /etc/systemd/system/pg_exporter.service.d/soak.conf /tmp/pg_exporter-soak.conf.disabled && \
   sudo systemctl daemon-reload && sudo systemctl restart pg_exporter'
```

The direct-probe summary reports HTTP status counts, maximum response duration,
and timeouts. While the lock is held, a bounded `200` (partial multi-database
collection) or `503` (failed or concurrent scrape) is acceptable. `000` or a
non-zero curl return code means the exporter did not answer within the client
bound.

The Prometheus sampler keeps the original direct-probe fields at columns 13-15
and appends comparison signals for the statements collector's 5-minute mean and
p95 duration, last-scrape success, and the database host's 5-minute busy CPU
ratio, available memory, and load average. This makes completed runs
self-contained even after Prometheus retention expires.

## pg_stat_statements Self-Filter Benchmark

The statements integration suite compares the previous
`BTRIM(REGEXP_REPLACE(...))` self-filter with the direct `NOT LIKE` prefix
filter. It creates an isolated local database and temporary synthetic query
texts, verifies both predicates return the same result, and prints their median
execution times and speedup. The benchmark runs as part of the normal test
suite.

Run only this benchmark:

```bash
PG_EXPORTER_DSN="postgresql://postgres:postgres@localhost:5432/postgres" \
cargo test --test collectors_tests benchmark_pg_statements_self_filter -- --nocapture
```

The default dataset is 5,000 rows with approximately 4 KiB per query. Override
its size with `PG_EXPORTER_STATEMENTS_BENCH_ROWS` and
`PG_EXPORTER_STATEMENTS_BENCH_QUERY_BYTES`. To approximate the workload from
issue #31:

```bash
PG_EXPORTER_DSN="postgresql://postgres:postgres@localhost:5432/postgres" \
PG_EXPORTER_STATEMENTS_BENCH_ROWS=5000 \
PG_EXPORTER_STATEMENTS_BENCH_QUERY_BYTES=140000 \
cargo test --test collectors_tests benchmark_pg_statements_self_filter -- --nocapture
```

The benchmark measures the predicate cost in isolation. PostgreSQL still loads
the external `pg_stat_statements` query-text file when reading the view, so the
reported speedup does not represent the collector's entire scrape duration.
