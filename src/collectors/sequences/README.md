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

## Configuration

- Enable with `--collector.sequences`
- Set the emission threshold with `--sequences.min-ratio` (default `0.5`)

## Example alert

```promql
pg_sequence_used_ratio > 0.75
```

This alert warns when any exported sequence has consumed more than 75% of its
available range.
