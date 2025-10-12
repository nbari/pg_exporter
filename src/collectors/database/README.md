# Database collector

This collector group exposes PostgreSQL database-level metrics, split into two sub-collectors:

- pg_stat_database (stats): compatibility with postgres_exporter’s `pg_stat_database_*` metrics.
- pg_database (catalog): database size and connection limit via `pg_database_*` metrics, with optional excludes.

The goal is to keep names and labels compatible with the Go postgres_exporter wherever possible.

References:
- Postgres exporter (Go) implementation for pg_stat_database: [pg_stat_database.go](https://github.com/prometheus-community/postgres_exporter/blob/main/collector/pg_stat_database.go)
- Postgres exporter (Go) implementation for pg_database: [database.go](https://github.com/prometheus-community/postgres_exporter/blob/main/collector/database.go)

---

## Environment variables

- PG_EXPORTER_EXCLUDE_DATABASES
  - Comma-separated list of database names to exclude (exact, case-sensitive matches).
  - Example:
    ```bash
    export PG_EXPORTER_EXCLUDE_DATABASES="postgres,template0,template1"
    ```

Notes:
- Exclusions are applied server-side in a single query (efficient).
- The variable is read at startup; restart the exporter to apply changes.


## PromQL examples

- Total DB size across all databases:
  ```promql
  sum(pg_database_size_bytes)
  ```

- Top 5 databases by size:
  ```promql
  topk(5, pg_database_size_bytes)
  ```

- Transactions per second (instance-wide):
  ```promql
  sum by (instance) (rate(pg_stat_database_xact_commit[5m]) + rate(pg_stat_database_xact_rollback[5m]))
  ```

- Buffer hit ratio (per database):
  ```promql
  sum by (datname) (rate(pg_stat_database_blks_hit[5m]))
  /
  (
    sum by (datname) (rate(pg_stat_database_blks_hit[5m]))
    +
    sum by (datname) (rate(pg_stat_database_blks_read[5m]))
  )
  ```

- Statement execution time (rate; PG ≥ 14):
  ```promql
  rate(pg_stat_database_active_time_seconds_total[5m])
  ```
