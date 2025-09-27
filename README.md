[![Test & Build](https://github.com/nbari/pg_exporter/actions/workflows/build.yml/badge.svg)](https://github.com/nbari/pg_exporter/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/nbari/pg_exporter/graph/badge.svg?token=LR19CK9679)](https://codecov.io/gh/nbari/pg_exporter)

# pg_exporter

A PostgreSQL metric exporter for Prometheus written in Rust

## Goals

`pg_exporter` is designed with a selective metrics approach:

* `Modular collectors` Expose only the metrics you actually need instead of collecting everything by default.
* Prevent Prometheus from being overloaded with unnecessary data.
* Allow dynamic enabling/disabling of metric modules for flexibility and efficiency.

This approach helps maintain performance, observability clarity, and operational simplicity.

## Download or build

Install via Cargo:

    cargo install pg_exporter


## Feedback

This project is a work in progress. Your feedback, suggestions, and
contributions are always welcome!
