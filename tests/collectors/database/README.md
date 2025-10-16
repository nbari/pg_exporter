# Database Collector Tests

Tests for the database collector group, which includes:
- Database stats: metrics from `pg_stat_database`
- Database catalog: metrics from `pg_database` (size, connection limit)

## Running Tests

- Run all database collector tests:
```bash
cargo test --test collectors_tests database
```

- Run specific sub-collector tests:
```bash
# Stats collector (pg_stat_database)
cargo test --test collectors_tests database::stats

# Catalog collector (pg_database)
cargo test --test collectors_tests database::catalog
```

- Run specific tests:
```bash
cargo test --test collectors_tests test_database_stats_has_all_metrics_after_collection
cargo test --test collectors_tests test_database_catalog_has_metrics_after_collection
```

- Run with output:
```bash
cargo test --test collectors_tests database -- --nocapture
```
