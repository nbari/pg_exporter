[![Test & Build](https://github.com/nbari/pg_exporter/actions/workflows/build.yml/badge.svg)](https://github.com/nbari/pg_exporter/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/nbari/pg_exporter/graph/badge.svg?token=LR19CK9679)](https://codecov.io/gh/nbari/pg_exporter)
[![Crates.io](https://img.shields.io/crates/v/pg_exporter.svg)](https://crates.io/crates/pg_exporter)
[![License](https://img.shields.io/crates/l/pg_exporter.svg)](LICENSE)


# pg_exporter

A PostgreSQL metric exporter for Prometheus written in Rust

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

    pg_exporter --dsn postgresql:///postgres?user=postgres_exporter

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
   sudo -u postgres_exporter pg_exporter --dsn postgresql:///postgres?user=postgres_exporter
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

## Available collectors

The following collectors are available:

* `--collector.default` [default](src/collectors/default/mod.rs)
* `--collector.activity` [activity](src/collectors/activity/mod.rs)
* `--collector.database` [database](src/collectors/database/mod.rs)
* `--collector.vacuum` [vacuum](src/collectors/vacuum/mod.rs)
* `--collector.locks` [locks](src/collectors/locks/mod.rs)
* `--collector.stat` [stat](src/collectors/stat/mod.rs)
* `--collector.replication` [replication](src/collectors/replication/mod.rs)
* `--collector.index` [index](src/collectors/index/mod.rs)
* `--collector.statements` [statements](src/collectors/statements/README.md) - Query performance metrics from `pg_stat_statements` (see [detailed guide](src/collectors/statements/README.md))
* `--collector.tls` [tls](src/collectors/tls/mod.rs) - SSL/TLS certificate monitoring, connection encryption stats (PostgreSQL 9.5+)
* `--collector.exporter` [exporter](src/collectors/exporter/mod.rs) - Exporter self-monitoring (process metrics, scrape performance, cardinality tracking)

You can enable `--collector.<name>` or disable `--no-collector.<name>` For example,
to disable the `vacuum` collector:

    pg_exporter --dsn postgresql:///postgres?user=postgres_exporter --no-collector.vacuum

### Enabled by default

This collectors are enabled by default:

* `default`
* `activity`
* `vacuum`


## Scrape Behavior

`pg_exporter` is designed to be resilient to PostgreSQL outages:

* **High Availability** – The exporter starts and stays available even if the database is down.
* **HTTP 200 Always** – The `/metrics` endpoint always responds with HTTP 200 to avoid triggering unnecessary Prometheus "down" alerts for the exporter itself.
* **`pg_up` Metric** – Use the `pg_up` metric (1 for up, 0 for down) to monitor database connectivity.
* **Metric Omission** – When the database is unreachable, database-dependent metrics are omitted from the output rather than being reported as zero.

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

## Testing

The project includes unit tests for each collector and integration tests for the
exporter as a whole. You can run the tests using:

    just

> need just installed, see [just](https://github.com/casey/just)

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

## Feedback

This project is a work in progress. Your feedback, suggestions, and
contributions are always welcome!
