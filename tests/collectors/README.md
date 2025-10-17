# Collector Integration Tests

This directory contains integration tests for all PostgreSQL collectors. These tests require a live PostgreSQL connection.

## Prerequisites

Tests expect PostgreSQL to be available at the DSN specified by the `PG_EXPORTER_DSN` environment variable, defaulting to `postgres://postgres@localhost:5432/postgres`.

The tests use PostgreSQL versions 16, 17, and 18 in CI (GitHub Actions).

## Running Tests

### Run all collector tests

    cargo test --test collectors_tests -- --nocapture

Default collectors (version, postmaster, settings)

    cargo test --test collectors_tests default

Activity collectors (connections, wait events)

    cargo test --test collectors_tests activity

# Run tests for a specific sub-collector

Default

    cargo test --test collectors_tests default::version
    cargo test --test collectors_tests default::postmaster
    cargo test --test collectors_tests default::settings

Activity

    cargo test --test collectors_tests activity::connections
    cargo test --test collectors_tests activity::wait

...

# Run a specific test

    cargo test --test collectors_tests test_version_collector_queries_database
    cargo test --test collectors_tests test_connections_collector_collects_from_database
