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
  relevant collectors, verifies bounded behavior and recovery with a short
  `ACCESS EXCLUSIVE` fault check, calibrates pgbench against database CPU and
  scrape-health limits, starts workload phases on the DB host, and samples the
  existing Prometheus target from the metrics host. Prometheus is the only
  continuous `/metrics` scraper during the measurement window. The DB host
  independently samples `pg_stat_activity` every 30 seconds and fails the run
  if `pg_exporter` exceeds its five-connection budget.
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

Before starting the remote jobs, the launcher may spend up to 12 minutes trying
pgbench client counts 8, 6, 4, and 2 for three minutes each. It selects the
highest candidate that keeps average DB CPU at or below 75%, DB CPU p95 at or
below 85%, scrape p95 below 12 seconds, and Prometheus `up` continuously equal
to one. Override the per-candidate duration with
`BENCH_CALIBRATION_SECONDS` (minimum 60 seconds). Keep the run id; it is the key
for every status and artifact command.

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
BENCH_CALIBRATION_SECONDS=180
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

Before recording the 24-hour measurement, the launcher holds an exclusive lock
for one bounded fault probe, releases it, and requires Prometheus to observe all
16 collectors succeeding again. The exclusive lock is never held during the
measurement window.

For a 24-hour run, the measured phases are:

| Phase | Duration | Purpose |
| --- | ---: | --- |
| Baseline | 2h | Establish normal exporter CPU, RSS, FD, and scrape cost |
| Statements pressure | 4h | Stress `pg_stat_statements` and top-N processing |
| Lock/activity pressure | 4h | Exercise lock and activity collectors |
| Vacuum-debt build | 6h | Accumulate dead tuples with autovacuum disabled on benchmark tables |
| Autovacuum recovery | 4h | Observe recovery and progress metrics |
| Mixed churn | 4h | Combine statements, locks, and transactional load |

Every phase uses the calibrated pgbench client/thread count. The heavy-query
loop is rate-limited and the lock storm uses two waiters, preserving collector
stimulus without intentionally saturating PostgreSQL. The workload restores the
benchmark tables' autovacuum reloptions on normal completion and from its exit
trap. The systemd soak override intentionally remains installed after the run
so the final metrics remain inspectable.

## Monitor Progress

Run this whenever you want a snapshot:

```bash
./scripts/benchmark/check-rust-soak.sh --run-id <RUN_ID>
```

It reports the active phase and remaining phase time, exporter process uptime,
current `ps` CPU/RSS, accumulated exporter CPU/RSS/FD trends, scrape duration,
database-host CPU and available memory, PostgreSQL connection budget, and
recent workload/sampler logs. The sampler also writes a
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
- no missing Prometheus samples and no target-down samples;
- exporter PostgreSQL connections never above the five-connection budget;
- no soak-owned exclusive-lock session during the measurement;
- no unexplained collector error growth or sustained scrape-duration increase;
- RSS and open FDs that settle rather than grow monotonically across phases;
- CPU increases that correlate with workload phases and returns toward the
  baseline afterward;
- the final workload state set to `status=complete`.

The isolated pre-measurement fault check may observe a bounded `200`, `503`, or
`504`, depending on whether the unlocked database provides a partial snapshot.
It must recover to a complete successful scrape within 60 seconds. Any `503` or
`504` during the measured soak is a failure.

## Restore the Exporter Service

After artifacts are collected, remove the soak-specific collector override if
the lab should return to its normal service configuration:

```bash
ssh 10.246.1.90 \
  'sudo mv /etc/systemd/system/pg_exporter.service.d/soak.conf /tmp/pg_exporter-soak.conf.disabled && \
   sudo systemctl daemon-reload && sudo systemctl restart pg_exporter'
```

The Prometheus sampler keeps the legacy direct-probe fields at columns 13-15
empty for artifact compatibility. It does not call `/metrics`: a second scraper
would exercise the single-scrape gate and manufacture `503` responses. The
remaining comparison signals include the statements collector's 5-minute mean
and p95 duration, last-scrape success, and the database host's 5-minute busy CPU
ratio, available memory, and load average. This makes completed runs
self-contained even after Prometheus retention expires.

## pg_stat_statements Spill Regression

The old regex-vs-prefix benchmark from issue #31 was removed when issue #33
eliminated query text from the scrape path entirely. The replacement live test
asserts that collecting statement metrics writes no PostgreSQL temporary files:

```bash
PG_EXPORTER_DSN="postgresql://postgres:postgres@localhost:5432/postgres" \
cargo test --test collectors_tests \
  test_pg_statements_scrape_writes_no_temp_files -- --nocapture
```

Source-level tests separately require the scrape query to use
`pg_stat_statements(false)`, prohibit `REGEXP_REPLACE` in production collectors,
and keep the rate-limited query-text lookup detached from the scrape.
