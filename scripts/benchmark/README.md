# Benchmark Soak Workflows

This directory contains benchmark tooling for long-running Rust exporter soak
tests on the benchmark VMs.

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

```bash
./scripts/benchmark/run-rust-soak.sh --hours 24
```

Optional host overrides:

```bash
BENCH_RUST_SSH=10.246.1.90 \
BENCH_DB_SSH=10.246.1.92 \
BENCH_METRICS_SSH=10.246.1.93 \
./scripts/benchmark/run-rust-soak.sh --hours 24
```

Check status:

```bash
./scripts/benchmark/check-rust-soak.sh --run-id <RUN_ID>
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
