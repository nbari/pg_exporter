# Sequences collector

The `sequences` collector is an opt-in, multi-database collector for detecting
sequence exhaustion before integer primary keys overflow. It reads `pg_sequences`
in every connectable, non-excluded `PostgreSQL` database and exports only
sequences whose usage is at or above the configured threshold.

## Metric

```text
pg_sequence_used_ratio{schemaname,sequencename,datname}
```

The value is `last_value / max_value`, normalized to `0.0` for sequences that
have not been used yet. Series are reset each scrape, so sequences that drop
below the threshold disappear.

## Required privileges

`pg_sequences.last_value` is `NULL` for a sequence the exporter role may not read, and
`pg_monitor` does not grant that. With the role described in the main README the
collector therefore succeeds, exports nothing, and logs nothing, even for a sequence that
is nearly exhausted. Grant `SELECT` on the sequences to watch, in each database, and
cover sequences created later:

```sql
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO postgres_exporter;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON SEQUENCES TO postgres_exporter;
```

`SELECT` lets the role read a sequence's current value; it does not allow `nextval`.
Schema `USAGE` is not required. To check what the exporter can see, run as that role:

```sql
SELECT count(*) FILTER (WHERE last_value IS NULL) AS unreadable, count(*) AS total
FROM pg_sequences;
```

A sequence that has never been used also shows `NULL`, so compare against a superuser
session when the numbers are unexpected.

## Lock usage in databases with many sequences

`pg_sequences` reads each sequence's value through `pg_sequence_last_value()`, which
takes a `RowExclusiveLock` on that sequence and holds it until the statement ends. One
scrape of a database therefore holds one lock per sequence at the same time, whether or
not the role can read the value. The shared lock table has about
`max_locks_per_transaction * (max_connections + max_prepared_transactions)` entries for
all sessions together. On a default server (`64 * 100`), a database with 12,000 sequences
was collected without error and one with 20,000 failed with `out of shared memory`; the
collector then logs the error and skips that database. Close to the limit the query also
competes with application sessions for lock table entries.

Before enabling the collector on a database with thousands of sequences (typically
schema-per-tenant designs), raise `max_locks_per_transaction` or list the database in
`--exclude-databases`.

## Configuration

- Enable with `--collector.sequences`
- Set the emission threshold with `--sequences.min-ratio` (default `0.5`)

## Example alert

```promql
pg_sequence_used_ratio > 0.75
```

This alert warns when any exported sequence has consumed more than 75% of its
available range.
