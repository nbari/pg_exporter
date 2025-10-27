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

### CI Testing

The CI pipeline automatically:
- Tests against PostgreSQL 16, 17, and 18
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

## Test Coverage Requirements

Before merging:
- [ ] All new collectors have registration tests
- [ ] All new collectors have extension availability tests
- [ ] Edge cases (NULL, zero, empty) are tested
- [ ] Type conversions are tested with realistic data
- [ ] CI passes on all PostgreSQL versions

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

We test against PostgreSQL 16, 17, and 18. Some features may vary:

- `pg_stat_statements.wal_bytes` - Added in PostgreSQL 13
- Always use COALESCE for version-specific columns:
  ```sql
  COALESCE(wal_bytes, 0)::bigint as wal_bytes
  ```

## When to Skip Tests

Tests should be skipped (not fail) when:
- Required extension is not installed
- PostgreSQL version doesn't support a feature
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
