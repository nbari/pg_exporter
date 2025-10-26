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


## Usage

Run the exporter and use the socket directory:

    pg_exporter --dsn postgresql:///postgres?user=postgres_exporter

> in pg_hba.conf you need to allow the user `postgres_exporter` to connect, for example:

    local  all  postgres_exporter  trust


You can also specify a custom port, for example `9187`:

    pg_exporter --dsn postgresql://postgres_exporter@localhost:5432/postgres --port 9187


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
* `--collector.statements` [statements](src/collectors/statements/mod.rs) 🔥 **NEW - Critical for DBREs**

### 🔥 pg_stat_statements Collector (NEW)

The `statements` collector is **THE most important tool for Database Reliability Engineers**. It tracks query performance metrics from PostgreSQL's `pg_stat_statements` extension.

**Why DBREs need this:**
- Find slow queries during incidents ("What query is killing the database?")
- Detect N+1 query problems before they scale
- Identify performance regressions after deployments
- Optimize based on actual production query patterns
- Track resource-intensive queries (I/O, WAL generation, temp files)

**Prerequisites:**
```sql
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
```

Add to `postgresql.conf`:
```
shared_preload_libraries = 'pg_stat_statements'
pg_stat_statements.track = all
pg_stat_statements.max = 10000
```

**Enable the collector:**
```bash
pg_exporter --dsn postgresql:///postgres?user=postgres_exporter --collector.statements
```

**Key Metrics Exposed:**
- `pg_stat_statements_total_exec_time_seconds` - Total time per query
- `pg_stat_statements_mean_exec_time_seconds` - Average time per execution
- `pg_stat_statements_calls_total` - Execution frequency
- `pg_stat_statements_shared_blks_read_total` - Disk reads (cache misses)
- `pg_stat_statements_temp_blks_written_total` - Queries spilling to disk
- `pg_stat_statements_wal_bytes_total` - WAL generation per query
- `pg_stat_statements_cache_hit_ratio` - Query cache effectiveness

---

## Available collectors (continued)

You can enable `--colector.<name>` or disable `--no-collector.<name>` For example,
to disable the `vacuum` collector:

    pg_exporter --dsn postgresql:///postgres?user=postgres_exporter --no-collector.vacuum

### Enabled by default

This collectors are enabled by default:

* `default`
* `activity`
* `vacuum`


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
