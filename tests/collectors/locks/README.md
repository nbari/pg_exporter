# Locks Collector Tests

Tests for the locks collector (relations-focused), which exposes:
- pg_locks_waiting{relation}
- pg_locks_granted{relation}

These metrics are derived from `pg_locks` grouped by relation name.

## Running Tests

- Run all locks collector tests:
```bash
cargo test --test collectors_tests locks
```

- Run only relations tests:
```bash
cargo test --test collectors_tests locks::relations
```

- Run a specific test:
```bash
cargo test --test collectors_tests test_locks_relations_detects_waiting_lock -- --nocapture
```
