# Testing Guide

This document describes the testing strategy for pg_exporter to prevent production issues.

## Testing Philosophy

All collectors MUST be tested with:
1. **Extension/Feature availability tests** - Handle missing extensions gracefully
2. **Edge case tests** - NULL values, empty results, utility statements
3. **Type compatibility tests** - Ensure SQL types match Rust types
4. **Realistic workload tests** - Test with actual data and queries

## Running Tests

### Local Testing

```bash
# Set up PostgreSQL with pg_stat_statements
export PG_EXPORTER_DSN="postgresql://postgres:postgres@localhost:5432/postgres"

# Enable pg_stat_statements in postgresql.conf:
# shared_preload_libraries = 'pg_stat_statements'

# Then in psql:
# CREATE EXTENSION pg_stat_statements;

# Run all tests
cargo test

# Run specific collector tests
cargo test --test collectors_tests statements

# Run with output
cargo test -- --nocapture
```

For rootless Podman with `testcontainers`, export:

```bash
export DOCKER_HOST="unix:///run/user/$UID/podman/podman.sock"
```

Testcontainers-based tests (e.g. `tests/connection_budget.rs`) prefer spinning up
their own disposable PostgreSQL container. When no container runtime socket is
available (for example inside the compose devcontainer, which has no podman/docker
but already runs a local `postgres` sidecar), they fall back to the server from
`PG_EXPORTER_DSN` and clean up every object they create (role, databases, grants).
With neither a runtime nor `PG_EXPORTER_DSN`, they skip — unless `CI=true` or
`PG_EXPORTER_REQUIRE_TESTCONTAINERS=1`, which makes a missing runtime a failure.

### CI Testing

The CI pipeline automatically:
- Tests against PostgreSQL 14, 15, 16, 17, and 18
- Installs and configures pg_stat_statements extension
- Runs all integration tests

## Writing Collector Tests

When adding a new collector, you MUST include these test categories:

### 1. Registration Test
```rust
#[tokio::test]
async fn test_collector_registers_without_error() -> Result<()> {
    let collector = MyCollector::new();
    let registry = Registry::new();
    collector.register_metrics(&registry)?;
    Ok(())
}
```

### 2. Extension/Feature Availability Test
```rust
#[tokio::test]
async fn test_collector_handles_missing_extension() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let collector = MyCollector::new();
    let registry = Registry::new();
    
    collector.register_metrics(&registry)?;
    let result = collector.collect(&pool).await;
    
    // Should not panic
    assert!(result.is_ok());
    Ok(())
}
```

### 3. Edge Case Tests

Test for common edge cases that cause panics:

```rust
#[tokio::test]
async fn test_collector_handles_null_values() -> Result<()> {
    // Test queries that may return NULL
    // Utility statements (VACUUM, ANALYZE)
    // Empty result sets
    // Zero values
}

#[tokio::test]
async fn test_collector_handles_type_mismatches() -> Result<()> {
    // Ensure SQL types (NUMERIC, BIGINT) match Rust types
    // Use explicit casts in SQL if needed: value::bigint
}
```

### 4. Realistic Workload Test
```rust
#[tokio::test]
async fn test_collector_with_realistic_data() -> Result<()> {
    // Create test data
    // Generate realistic workload
    // Verify metrics are collected correctly
}
```

## Common Pitfalls and Solutions

**Detailed code examples with inline comments are in `src/collectors/statements/pg_statements.rs`.**

### 1. Type Mismatches (CRITICAL)

**Problem:** PostgreSQL NUMERIC type doesn't match Rust i64/f64  
**Solution:** Always cast in SQL: `SELECT column::bigint FROM table`

### 2. NULL Values (CRITICAL)

**Problem:** Using `row.get()` panics on NULL  
**Solution:** Use `row.try_get()` with fallbacks (see code examples)

### 3. Missing Extensions

**Problem:** Assuming extensions are installed  
**Solution:** Check with `fetch_optional()` and handle gracefully

### 4. Division by Zero

**Problem:** Dividing without checking denominator  
**Solution:** Check `if total > 0` before division

**See `src/collectors/statements/pg_statements.rs` for production examples of all these patterns.**

## Dashboard Contract Tests

`grafana/dashboard.json` is validated by [tests/dashboard.rs](dashboard.rs), which runs
as part of `cargo test` (and therefore `just test` and CI). A panel is only as good as
the metric it queries: a renamed metric leaves the JSON valid and the panel empty, so
these tests tie every panel query back to a metric the exporter actually produces.

| Test | Needs a database | What it catches |
| --- | --- | --- |
| `dashboard_metrics_are_declared_in_source` | no | a panel querying a metric name that exists nowhere in `src/` (typos, renames) |
| `dashboard_metrics_are_exported_by_collectors` | yes | a metric that exists as a string in the source but never actually reaches `/metrics` |
| `conditional_metrics_are_still_referenced_by_the_dashboard` | no | stale `CONDITIONAL_METRICS` entries that would mask a real regression |
| `temp_disk_pressure_row_metrics_are_exported` | yes | the Temp Disk Pressure row spans four collectors; pins which ones it needs |
| `temp_safeguard_settings_keep_their_sentinels` | yes | `-1` ("unlimited"/"disabled") being scaled to `-1024` by the kB-to-bytes conversion |

Metric names are extracted from every `targets[].expr`, with label matchers stripped
first so a regex label *value* is never mistaken for a metric name.

`dashboard_metrics_are_exported_by_collectors` **seeds its own fixtures** before scraping
(`seed_scrape_fixtures`): a small table with insert/update/delete/index-scan/`ANALYZE`
activity, and a sequence pushed past `--sequences.min-ratio`. Without them the scrape is
correctly silent — `pg_stat_user_tables` has no rows on a database with no user tables,
and the `sequences` collector deliberately exports nothing below the ratio. That is what
made this test pass on a lived-in development database and fail on every clean CI
container. Only the *presence* of a series is asserted, not its value, so one seeded table
is enough for all 22 `pg_stat_user_tables_*` families. The fixtures are dropped even when
the scrape fails, so a failure cannot pollute the shared database.

### Adding a panel

If a new panel queries a metric that a local single-node instance cannot produce, the
live test will fail. Pick the mechanism that matches *why* it is absent:

| Situation | Where it goes |
| --- | --- |
| Needs workload state (a table, rows, an advanced sequence) | seed it in `seed_scrape_fixtures` |
| Only exists from a newer PostgreSQL | `VERSION_GATED_PREFIXES`, with a minimum `server_version_num` |
| Genuinely cannot be created here (replica, blocked session, in-flight `VACUUM`, TLS clients, co-located host) | `CONDITIONAL_METRICS`, **with a reason** |

`CONDITIONAL_METRICS` is a flat list, so it cannot express "absent on 14/15, required on
16+" — putting a version-gated metric there would stop the live check from verifying it on
the versions that *do* have it. `VERSION_GATED_PREFIXES` is enforced in both directions:
skipped below the minimum, and required above it.

Verify a change against a **clean** database, not your development one, or you will
reproduce exactly the false green this test exists to prevent:

```sh
podman run -d --name pgclean -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=postgres -p 55416:5432 postgres:16 \
  -c shared_preload_libraries=pg_stat_statements
podman exec pgclean psql -U postgres -c 'CREATE EXTENSION pg_stat_statements'
PG_EXPORTER_DSN=postgresql://postgres:postgres@localhost:55416/postgres cargo test --test dashboard
```

`scripts/validate-dashboard.sh` (`just validate-dashboard`) remains as a quick manual
check. It also verifies JSON validity, the `job`/`instance`/`database` template
variables, and job-filter coverage, which the Rust tests do not.

## Index and Permit Timing Regressions

`tests/index_scrape.rs` runs in its own process so configuration in process-wide cells does
not race unrelated tests. It compares all ten index metrics against the frozen pre-merge SQL
in `tests/collectors/index/reference.sql`, checks exactly one session and one healthy query
per non-default database, and exercises permissions, retirement, and top-level timing labels.
The workload must actually produce scans and invalid indexes before assertions run.

CI runs it at both connection limits on every PostgreSQL matrix version. To repeat in DevPod:

```sh
PG_EXPORTER_TEST_DB_CONCURRENCY=1 cargo test --test index_scrape
PG_EXPORTER_TEST_DB_CONCURRENCY=2 cargo test --test index_scrape
```

Use the compose `PG_EXPORTER_DSN` inside DevPod; on the host explicitly set the local test
DSN as above. Permit timing unit tests use a paused clock and controlled semaphore admission
to verify attribution, concurrent waits, cancellation, and release without timing thresholds.
The dashboard fixture also creates a non-default database so permit metrics are tested on a
clean PostgreSQL instance, not only on a populated developer cluster.

## Test Coverage Requirements

Before merging:
- [ ] All new collectors have registration tests
- [ ] All new collectors have extension availability tests
- [ ] Edge cases (NULL, zero, empty) are tested
- [ ] Type conversions are tested with realistic data
- [ ] CI passes on all PostgreSQL versions
- [ ] New dashboard panels query metrics the exporter really exports (`cargo test --test dashboard`)

## Debugging Test Failures

```bash
# Run single test with output
cargo test test_name -- --nocapture

# Run with RUST_LOG for detailed tracing
RUST_LOG=debug cargo test test_name -- --nocapture

# Connect to test database to inspect state
psql $PG_EXPORTER_DSN

# Check pg_stat_statements data
SELECT * FROM pg_stat_statements LIMIT 5;

# Check extension installation
SELECT * FROM pg_extension;
```

## PostgreSQL Version Compatibility

`pg_exporter` supports PostgreSQL 14 and newer.

The test matrix currently covers PostgreSQL 14, 15, 16, 17, and 18. Some features may vary across supported versions:

- `pg_stat_checkpointer` - Added in PostgreSQL 17
- Use explicit version guards only for features that vary across supported versions
- Always use COALESCE for nullable or version-specific columns when applicable:
  ```sql
  COALESCE(wal_bytes, 0)::bigint as wal_bytes
  ```

## When to Skip Tests

Tests should be skipped (not fail) when:
- Required extension is not installed
- PostgreSQL version doesn't support a feature within the supported PostgreSQL 14+ range
- Running in a restricted environment

```rust
if ext_check.is_none() {
    println!("Extension not available, skipping test");
    return Ok(());
}
```

## Continuous Improvement

After any production panic:
1. Add a test that reproduces the panic
2. Fix the code
3. Verify the test now passes
4. Update this guide with lessons learned

After any production scrape-safety issue:
1. Add an integration test that reproduces the database-side condition, such as a held lock
2. Verify plain PostgreSQL outages still return `200` with `pg_up 0`
3. Verify timeout or stale-data risks fail visibly (`503` or `504`) instead of returning stale `200` data
4. Assert the exporter connection footprint remains bounded while the condition is active
5. Keep `tests/collector_safety.rs` green so collectors cannot bypass the shared connection
   and timeout model
