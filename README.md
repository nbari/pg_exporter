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

## project layout

The project is structured as follows:

```
.
├── bin
│   └── pg_exporter.rs
├── cli
│   ├── actions
│   ├── commands
│   ├── dispatch
│   ├── mod.rs
│   ├── start.rs
│   └── telemetry.rs
├── collectors <-- Here are the individual collectors
│   ├── config.rs
│   ├── default
│   ├── mod.rs <-- This file registers all collectors
│   ├── register_macro.rs
│   ├── registry.rs
│   └── vacuum
├── exporter
│   ├── handlers
│   └── mod.rs
└── lib.rs
```

All the collectors are located in the `collectors` directory. Each collector is
in its own subdirectory, making it easy to manage and extend.

In `mod.rs` file inside the `collectors` directory, you can see how each
collector is registered. This modular approach allows for easy addition or
removal of collectors as needed.


## Feedback

This project is a work in progress. Your feedback, suggestions, and
contributions are always welcome!
