# `slru` Collector

The `slru` collector exposes cluster-wide `PostgreSQL` SLRU cache counters from
`pg_stat_slru`. It is useful for diagnosing subtransaction, multixact, CLOG
(`Xact`), notification, and serializable-state cache pressure.

This collector is **opt-in** and requires `PostgreSQL` 13 or newer.

## Usage

Enable it explicitly:

```bash
pg_exporter --dsn postgresql:///postgres?user=postgres_exporter --collector.slru
```

## Metrics

All metrics use a single `name` label containing the SLRU cache name reported by
`pg_stat_slru`.

- `pg_stat_slru_blks_zeroed_total`
- `pg_stat_slru_blks_hit_total`
- `pg_stat_slru_blks_read_total`
- `pg_stat_slru_blks_written_total`
- `pg_stat_slru_blks_exists_total`
- `pg_stat_slru_flushes_total`
- `pg_stat_slru_truncates_total`

## Interpreting the Counters

Flat, near-zero `pg_stat_slru_blks_read_total` is healthy: the relevant SLRU
pages are staying in cache.

Sustained reads for `Subtrans`, `MultiXactMember`, or `MultiXactOffset` are the
smoking gun for SLRU pressure. They often point to very large transaction trees,
long-lived transactions, or multixact-heavy workloads that are forcing
`PostgreSQL` to read transaction state from disk during normal query execution.

Use `rate()` or `increase()` in `PromQL`, because the counters are cumulative
until the corresponding `pg_stat_slru` statistics are reset.
