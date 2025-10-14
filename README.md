[![Test & Build](https://github.com/nbari/pg_exporter/actions/workflows/build.yml/badge.svg)](https://github.com/nbari/pg_exporter/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/nbari/pg_exporter/graph/badge.svg?token=LR19CK9679)](https://codecov.io/gh/nbari/pg_exporter)

# pg_exporter

A PostgreSQL metric exporter for Prometheus written in Rust

## Goals

`pg_exporter` is designed with a selective metrics approach:

* `Modular collectors` Expose only the metrics you actually need instead of collecting everything by default.
* Prevent Prometheus from being overloaded with unnecessary data.
* Customizable collectors to allow users to tailor the metrics to their specific requirements.


## Download or build

Install via Cargo:

    cargo install pg_exporter

## Usage

Run the exporter and use the socket directory:

    pg_exporter --dsn postgresql:///postgres?user=pg_exporter

> in pg_hba.conf you need to allow the user `pg_exporter` to connect, for example:

    local   all             pg_exporter                            trust


You can also specify a custom port, for example `9187`:

    pg_exporter --dsn postgresql://postgres_exporter@localhost:5432/postgres --port 9187


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
├── config.rs
├── default <-- default collector
│   ├── mod.rs
│   └── version.rs
├── mod.rs <-- main file to register collectors
├── register_macro.rs
├── registry.rs
└── vacuum <-- vacuum collector
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


## Feedback

This project is a work in progress. Your feedback, suggestions, and
contributions are always welcome!
