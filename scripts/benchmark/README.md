# Benchmark Soak Workflows

This directory contains benchmark tooling for long-running Rust exporter soak
tests on the benchmark VMs.

## Files

- `run-rust-soak.sh`: Orchestrates a Rust-only phased soak run (default 24h),
  deploys a dedicated Grafana dashboard, configures the Rust exporter with the
  relevant collectors, starts workload phases on the DB host, and starts a
  Prometheus sampler on the metrics host.
- `check-rust-soak.sh`: Prints current workload/sampler status and a recent log
  tail for a given run id.
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
