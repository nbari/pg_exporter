[![Test & Build](https://github.com/nbari/pg_exporter/actions/workflows/build.yml/badge.svg)](https://github.com/nbari/pg_exporter/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/nbari/pg_exporter/graph/badge.svg?token=LR19CK9679)](https://codecov.io/gh/nbari/pg_exporter)
[![Crates.io](https://img.shields.io/crates/v/pg_exporter.svg)](https://crates.io/crates/pg_exporter)
[![License](https://img.shields.io/crates/l/pg_exporter.svg)](LICENSE)


# pg_exporter

A PostgreSQL metric exporter for Prometheus written in Rust

## Supported PostgreSQL Versions

`pg_exporter` supports PostgreSQL 14 and newer.

PostgreSQL 13 and older are no longer supported.

## Goals

`pg_exporter` is designed with a selective metrics approach:

* **Modular collectors** – Expose only the metrics you actually need instead of collecting everything by default.
* **Avoid unnecessary metrics** – Prevent exposing large numbers of unused metrics to Prometheus, reducing load and keeping monitoring efficient.
* **Customizable collectors** – Tailor the metrics to your specific requirements while maintaining compatibility with the official [postgres_exporter](https://github.com/prometheus-community/postgres_exporter).
* **Low memory footprint** – Designed to minimize memory usage and maximize efficiency while scraping metrics.

## Download or build

Install via Cargo:

    cargo install pg_exporter

Or download the latest release from the [releases page](https://github.com/nbari/pg_exporter/releases/latest).

### Docker/Podman

Container images are available at `ghcr.io/nbari/pg_exporter`:

```bash
# Using Docker
docker run -d \
  -e PG_EXPORTER_DSN="postgresql://postgres_exporter@postgres-host:5432/postgres" \
  -p 9432:9432 \
  ghcr.io/nbari/pg_exporter:latest

# Using Podman
podman run -d \
  -e PG_EXPORTER_DSN="postgresql://postgres_exporter@postgres-host:5432/postgres" \
  -p 9432:9432 \
  ghcr.io/nbari/pg_exporter:latest
```

**Connecting to host PostgreSQL from container:**
- Docker Desktop (Mac/Windows): use `host.docker.internal` instead of `localhost`
- Podman: use `host.containers.internal` instead of `localhost`
- Linux with `--network=host`: use `localhost` directly

Example with host connection:
```bash
podman run -d \
  -e PG_EXPORTER_DSN="postgresql://postgres_exporter@host.containers.internal:5432/postgres" \
  -p 9432:9432 \
  ghcr.io/nbari/pg_exporter:latest
```

## Usage

Run the exporter and use the socket directory:

    pg_exporter --dsn postgresql:///postgres?host=/var/run/postgresql&user=postgres_exporter

> in pg_hba.conf you need to allow the user `postgres_exporter` to connect, for example:

    local  all  postgres_exporter  trust

### Security Best Practices

Instead of using `trust` authentication (which allows connection without password), it is recommended to use `peer` authentication for local connections. This requires creating a system user named `postgres_exporter`.

1. Create the system user:
   ```bash
   sudo useradd -r -d /nonexistent -s /usr/bin/nologin postgres_exporter
   ```

2. Configure `pg_hba.conf` to use `peer` authentication:
   ```
   local  all  postgres_exporter  peer
   ```

3. Run the exporter as the `postgres_exporter` user:
   ```bash
   sudo -u postgres_exporter pg_exporter --dsn postgresql:///postgres?host=/var/run/postgresql&user=postgres_exporter
   ```

This ensures that only the system user `postgres_exporter` can connect to the database as the `postgres_exporter` role, significantly improving security.


You can also specify a custom port, for example `9187`:

    pg_exporter --dsn postgresql://postgres_exporter@localhost:5432/postgres --port 9187


## Environment Variables

`pg_exporter` supports standard PostgreSQL environment variables for connection configuration. This is useful when you want to avoid putting sensitive information like passwords in the DSN or command line arguments.

Supported variables include:
* `PGHOST`
* `PGPORT`
* `PGUSER`
* `PGPASSWORD`
* `PGDATABASE`

Example usage with `PGPASSWORD`:

    PGPASSWORD=secret pg_exporter --dsn postgresql://postgres@localhost:5432/postgres

You can also omit parts of the DSN and rely on environment variables:

    PGUSER=postgres PGPASSWORD=secret pg_exporter --dsn postgresql://localhost:5432/postgres

### Docker Secrets Support

For Docker Swarm or Kubernetes environments, you can use `PG_EXPORTER_DSN_FILE` to read the DSN from a file (e.g., Docker secrets):

```yaml
# docker-compose.yml for Docker Swarm
services:
  pg_exporter:
    image: ghcr.io/nbari/pg_exporter:latest
    environment:
      PG_EXPORTER_DSN_FILE: /run/secrets/pg_dsn
    secrets:
      - pg_dsn
    ports:
      - "9432:9432"

secrets:
  pg_dsn:
    external: true
```

Create the secret:
```bash
echo "postgresql://postgres_exporter:password@postgres:5432/postgres" | docker secret create pg_dsn -
```

Priority order: `PG_EXPORTER_DSN_FILE` > `PG_EXPORTER_DSN` > `--dsn` flag > default value

### Scrape safety: timeouts and connection budget

Every connection the exporter opens to scrape metrics gets timeout defaults:

* connect/acquire timeout `5000 ms` via `--scrape.connect-timeout-ms` / `PG_EXPORTER_CONNECT_TIMEOUT_MS`
* `lock_timeout = 2000 ms` via `--scrape.lock-timeout-ms` / `PG_EXPORTER_LOCK_TIMEOUT_MS`
* `statement_timeout = 10000 ms` via `--scrape.statement-timeout-ms` / `PG_EXPORTER_STATEMENT_TIMEOUT_MS`
* whole `/metrics` scrape timeout `15000 ms` via `--scrape.timeout-ms` / `PG_EXPORTER_SCRAPE_TIMEOUT_MS`

The connect/acquire timeout bounds DNS, TCP, TLS, authentication, and shared-pool
connection acquisition before PostgreSQL can enforce server-side timeouts. Scrape queries
normally take weak `AccessShareLock`s, but a concurrent
`AccessExclusiveLock` (routine DDL such as `ALTER TABLE`, `VACUUM FULL`, `REINDEX`,
`TRUNCATE`, or an abandoned transaction) can block them server-side. `lock_timeout` makes a
lock-blocked scrape fail fast and release its connection slot. `statement_timeout` is the
server-side backstop for slow queries after they start running. The whole-scrape timeout
turns an overlong HTTP scrape into a `504`.

The exporter allows the DSN/`PGOPTIONS` to override `lock_timeout`, including
`lock_timeout=0`, matching the usual PostgreSQL operator model. `statement_timeout=0` is
rejected because it disables the server-side query timeout; use a positive value or omit it
to keep the default. Any custom `statement_timeout` must be lower than the whole-scrape
timeout, so PostgreSQL aborts backend work before `/metrics` gives up.

    # raise connect timeout to 10s, lock_timeout to 5s, and statement_timeout to 30s;
    # scrape timeout must be higher than connect and statement timeouts
    pg_exporter \
      --scrape.connect-timeout-ms 10000 \
      --scrape.timeout-ms 40000 \
      --dsn "postgresql://postgres_exporter@localhost:5432/postgres?options=-c%20lock_timeout%3D5000%20-c%20statement_timeout%3D30000"

    # allowed: disable only lock-wait aborts, while statement/scrape timeouts still apply
    PGOPTIONS="-c lock_timeout=0" pg_exporter --dsn postgresql://localhost:5432/postgres

Only one `/metrics` scrape runs at a time. A plain PostgreSQL connectivity outage returns
`200` with `pg_up 0` and only fresh exporter-status metrics, so Prometheus can distinguish
"exporter down" from "database down". A concurrent scrape returns `503`; collector/query or
encoding failures return `503`; a whole-scrape timeout returns `504`. The exporter does not
return stale collector data on failed scrapes. If a scrape reaches the HTTP timeout, the
exporter keeps the scrape gate closed until in-flight collector work has unwound, preventing
a new scrape from starting another wave of PostgreSQL backend work while the previous one is
still cancelling server-side.

If Prometheus has a lower `scrape_timeout` than `--scrape.timeout-ms`, Prometheus may record
its own client-side timeout before the exporter can return `504`. Keep the Prometheus scrape
timeout higher than the exporter timeout when you want the HTTP status code to be visible.

## Available collectors

The following collectors are available:

* `--collector.default` [default](src/collectors/default/README.md) - Cheap, always-on signals (version, settings, bgwriter, checkpointer, archiver, WAL). Includes checkpoint tuning-insight metrics; see the [checkpoint tuning guide](src/collectors/default/README.md#why-tune-checkpoint_timeout-5m-vs-30m).
* `--collector.activity` [activity](src/collectors/activity/mod.rs) - Connection states, pool saturation, idle-age buckets, and `pg_stat_activity_on_cpu_backends` (active backends not waiting = on CPU). See the [database-pressure diagnostics guide](docs/diagnosing-database-pressure.md).
* `--collector.database` [database](src/collectors/database/mod.rs)
* `--collector.vacuum` [vacuum](src/collectors/vacuum/mod.rs)
* `--collector.locks` [locks](src/collectors/locks/mod.rs) - Lock counts plus blocking diagnostics (`pg_blocked_sessions`, `pg_blocking_sessions`, `pg_longest_blocked_seconds`, `pg_lock_waits`). See the [database-pressure diagnostics guide](docs/diagnosing-database-pressure.md#2-blocking--lock-contention).
* `--collector.stat` [stat](src/collectors/stat/mod.rs) - Per-table `pg_stat_user_tables` stats plus block-I/O counters from `pg_statio_user_tables` (heap/index/TOAST cache hits vs disk reads); use the seq-scan vs index-scan signals to [find missing indexes](docs/diagnosing-database-pressure.md#3-missing-indexes) and the [buffer cache hit ratio](docs/diagnosing-database-pressure.md#34-buffer-cache-hit-ratio-io-pressure) to spot I/O pressure.
* `--collector.replication` [replication](src/collectors/replication/mod.rs)
* `--collector.index` [index](src/collectors/index/mod.rs) - Per-database index usage from `pg_stat_user_indexes` plus index block-I/O from `pg_statio_user_indexes` (`pg_index_idx_blks_hit_total` / `pg_index_idx_blks_read_total`).
* `--collector.statements` [statements](src/collectors/statements/README.md) - Query performance metrics from `pg_stat_statements` (see [detailed guide](src/collectors/statements/README.md))
* `--collector.tls` [tls](src/collectors/tls/mod.rs) - SSL/TLS certificate monitoring and connection encryption stats (PostgreSQL 14+)
* `--collector.exporter` [exporter](src/collectors/exporter/mod.rs) - Exporter self-monitoring (process metrics, scrape performance, cardinality tracking)

You can enable `--collector.<name>` or disable `--no-collector.<name>` For example,
to disable the `vacuum` collector:

    pg_exporter --dsn postgresql:///postgres?host=/var/run/postgresql&user=postgres_exporter --no-collector.vacuum

Collector-specific runtime options use the `<collector>.<option>` long-flag format. For example,
to reduce `pg_stat_statements` cardinality and scrape cost:

    pg_exporter --collector.statements --statements.top-n 10

The `statements` collector defaults to `--statements.top-n 25` if not specified. You can also use
`PG_EXPORTER_STATEMENTS_TOP_N`.

On clusters with many databases, the multi-database collectors (`stat`, `index`) share a
global non-default-database concurrency limit. Each database needs its own ephemeral
connection, so this keeps peak exporter connections independent of the number of databases
(important on connection-limited instances such as AWS RDS). The shared pool uses at most
`3` connections and `--collectors.max-db-concurrency` defaults to `5`, so the default maximum
active database connections used by one exporter process is `3 + 5 = 8`. If you set
`--collectors.max-db-concurrency N`, the maximum is `3 + N` per exporter process. Multiple
exporter replicas multiply that budget. You can also use `PG_EXPORTER_MAX_DB_CONCURRENCY`.

    pg_exporter --collector.stat --collectors.max-db-concurrency 10

For local observability testing with Prometheus and Grafana, a practical flow is:

    just postgres
    cargo run -- --collector.statements --collector.activity --collector.locks --collector.database -v
    just metrics
    just workload duration=120 clients=10

`just workload` reuses `pgbench` to populate `pg_stat_statements` and generate live activity while you inspect `/metrics`, Prometheus, or Grafana.

Inside the **devcontainer**, prefer the on-demand compose stack instead, which
scrapes the exporter by service name (`app:9432`) and hot-reloads the dashboard:

    just watch           # in the app container (exporter on app:9432)
    just metrics-dev     # on the host: Grafana :3000 + Prometheus :9090
    just metrics-dev-stop

See [.devcontainer/README.md](.devcontainer/README.md#dashboards-prometheus--grafana--on-demand) for details (editing `grafana/dashboard.json` hot-reloads in Grafana within ~10s).

For vacuum-specific testing, use:

    just vacuum-workflow scale=20 rounds=5 sample_mod=5

`just vacuum-workflow` creates dead tuples in `pgbench_accounts`, shows the table-level autovacuum pressure metrics, and then runs a manual `VACUUM (VERBOSE, ANALYZE)` so the vacuum collector and dashboard have real maintenance activity to display.

For PostgreSQL-managed cleanup testing, use:

    just autovacuum-workflow scale=20 rounds=5 sample_mod=5 timeout=180

`just autovacuum-workflow` creates dead tuples, temporarily lowers the local autovacuum trigger for `pgbench_accounts`, shortens `autovacuum_naptime`, and then waits for PostgreSQL autovacuum to clean the table without a manual `VACUUM`.

For long-running benchmark VM soak tests (Rust exporter only), use:

    ./scripts/benchmark/run-rust-soak.sh --hours 24

This orchestrates a phased 24h workload (statements, locks/activity, vacuum debt, autovacuum recovery, mixed churn), deploys a dedicated Grafana soak dashboard, and starts a Prometheus sampler for post-run analysis.

For reclaiming physical space:

- `VACUUM` reclaims dead tuples for PostgreSQL reuse, but usually does not return table space to the OS.
- `ANALYZE` only refreshes planner statistics; it does not reclaim space.
- `pg_repack` is the preferred low-downtime option when a large table or index remains bloated and you need to compact it.
- `VACUUM FULL` rewrites the table and can return space to the OS, but it takes an `ACCESS EXCLUSIVE` lock and should be planned in a maintenance window.

In Grafana, the fastest way to spot likely `pg_repack` or `VACUUM FULL` candidates is the `Vacuum & Bloat Pressure` row, especially:

- `Top Repack Candidates by Estimated Dead Space`
- `Top Tables by Estimated Bloat Ratio`
- `Top Tables by Table Size`

### Enabled by default

This collectors are enabled by default:

* `default`
* `activity`
* `vacuum`


## Scrape Behavior

`pg_exporter` keeps `/metrics` scrapeable across plain PostgreSQL outages, while failing
visibly when the current collector data cannot be trusted:

* **HTTP server availability** - The exporter can start and bind even if PostgreSQL is down.
* **Database down** - `/metrics` returns `200` with `pg_up 0` and exporter-status metrics only.
* **Successful database scrapes** - `/metrics` returns `200` after the current collector scrape completes.
* **Failed collector scrapes** - concurrent scrapes, collector/query failures, and encoding failures return `503`; whole-scrape timeouts return `504`.
* **No stale collector metrics** - failed collector scrapes return an error body, and database-down scrapes filter out any previous collector snapshot.

## Systemd Boot Ordering

For systemd deployments, ensure exporter startup is ordered after PostgreSQL to avoid early boot races:

```ini
[Unit]
After=network-online.target postgresql.service
Wants=network-online.target
```

If your distribution uses a versioned unit name (for example `postgresql-16.service`), replace `postgresql.service` accordingly.

## Project layout

The project is structured as follows:

```
├── bin
├── cli
├── collectors
├── exporter
└── lib.rs
```

All the collectors are located in the `collectors` directory. Each collector is
in its own subdirectory, making it easy to manage and extend.

```
collectors
├── activity
│   ├── connections.rs
│   ├── mod.rs
│   └── wait.rs
├── config.rs
├── database
│   ├── catalog.rs
│   ├── mod.rs
│   ├── README.md
│   └── stats.rs
├── default
│   ├── mod.rs
│   ├── postmaster.rs
│   ├── settings.rs
│   └── version.rs
├── locks
│   ├── mod.rs
│   └── relations.rs
├── mod.rs <-- main file to register collectors
├── register_macro.rs
├── registry.rs
├── stat
│   ├── mod.rs
│   └── user_tables.rs
├── util.rs
└── vacuum
    ├── mod.rs
    ├── progress.rs
    └── stats.rs
```


In `mod.rs` file inside the `collectors` directory, you can see how each
collector is registered. This modular approach allows for easy addition or
removal of collectors as needed.

Each collector can then be extended with more specific metrics. For example,
the `vacuum` collector has two files: `progress.rs` and `stats.rs`, this allows
for better organization and separation of concerns within the collector and
better testability. (or that is the plan).

## Dev Container (DevPod / Dev Containers)

For a zero-setup, reproducible environment, the repo ships a compose-based
[Dev Container](https://containers.dev) under [`.devcontainer/`](.devcontainer/). It
bundles the Rust toolchain and a PostgreSQL service (with `pg_stat_statements`
preloaded), so `just test` works out of the box on **Linux, macOS, and Linux Atomic**
(e.g. fedora-atomic), locally or on a remote VM — no host database or container
plumbing required.

With [DevPod](https://devpod.sh):

```bash
scripts/dev-up                                          # build + start app + postgres
scripts/dev-ssh                                         # shell in as vscode
just test                                               # runs against the postgres service
```

Or open the folder in VS Code and choose **Dev Containers: Reopen in Container**.
See [`.devcontainer/README.md`](.devcontainer/README.md) for details (including the
`--ssh-config` / `Include ~/.ssh/devpod` setup used by `scripts/dev-up`).

## Testing

The project includes unit tests for each collector and integration tests for the
exporter as a whole. You can run the tests using:

    just test

> need just installed, see [just](https://github.com/casey/just)

For direct checks, these commands are also part of the normal validation flow:

    cargo fmt --all -- --check
    just clippy

To run with opentelemetry set the environment variable `OTEL_EXPORTER_OTLP_ENDPOINT`, for example:

    OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:4317"

Then you can run the exporter and it will send traces to the specified endpoint.

To run postgres and jaeger locally

    just postgres
    just jaeger
    just watch

For tracees add more verbosity with `-v`, for example:

    cargo watch -x 'run -- --collector.vacuum -vv'

open `jaeger` at http://localhost:16686 and select the `pg_exporter` service to see the traces.

## 🤝 Contributing

We welcome contributions of all kinds.

1. **Read the [Agent & Contributor Contract](AGENTS.md)**. It contains repository-specific rules for AI and human contributors, including testing, safety, and release-flow expectations.
2. **Read the [Development Guide](CONTRIBUTING.md)**. It covers local PostgreSQL setup, test workflows, and safe collector patterns.
3. **Run tests**: `just test` runs the standard validation flow for this crate.
4. **Formatting**: run `cargo fmt --all -- --check`.
5. **Linting**: run `just clippy` before submitting changes.
6. **Check recent release notes** in [CHANGELOG.md](CHANGELOG.md) so documentation and release notes stay aligned.

Related docs:

- [AGENTS.md](AGENTS.md)
- [CONTRIBUTING.md](CONTRIBUTING.md)
- [CHANGELOG.md](CHANGELOG.md)
- [tests/TESTING.md](tests/TESTING.md)

## Feedback

This project is a work in progress. Your feedback, suggestions, and
contributions are always welcome!
