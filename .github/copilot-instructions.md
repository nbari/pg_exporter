# GitHub Copilot Instructions for pg_exporter

## Project Overview
This is a PostgreSQL metrics exporter written in Rust. It collects metrics from PostgreSQL and exposes them in Prometheus format. Safety and reliability are paramount - all code must handle edge cases gracefully without panics.

## Language & Tooling
- **Language**: Rust (latest stable)
- **Formatting**: `cargo fmt --all -- --check`
- **Linting**: `cargo clippy --all-targets --all-features -- -D warnings`
- **Testing**: `just test` (runs clippy, fmt, and all tests)
- **Build Tool**: `just` command runner (see `.justfile`)

## Critical Safety Rules

### 1. Always Use `try_get()` Instead of `get()`
**NEVER** use `row.get()` - it panics on NULL or type mismatches.

```rust
// ❌ WRONG - Will panic on NULL
let value: i64 = row.get("column");

// ✅ CORRECT - Safe handling
let value: i64 = row.try_get("column").unwrap_or(0);
```

### 2. Always Cast SQL Numeric Columns
PostgreSQL NUMERIC type doesn't directly map to Rust types. Always use explicit casts.

```rust
// ❌ WRONG - Type mismatch
SELECT total_time FROM pg_stat_statements

// ✅ CORRECT - Explicit cast
SELECT total_time::double precision FROM pg_stat_statements
SELECT calls::bigint FROM pg_stat_statements
```

### 3. Always Check Denominator Before Division
Prevent division by zero in both SQL and Rust.

```rust
// ❌ WRONG - Division by zero
let ratio = hits / (hits + misses);

// ✅ CORRECT - Safe division
let total = hits + misses;
let ratio = if total > 0 { hits as f64 / total as f64 } else { 0.0 };
```

SQL version:
```sql
-- ✅ CORRECT
CASE WHEN (shared_blks_hit + shared_blks_read) > 0
     THEN shared_blks_hit::double precision / (shared_blks_hit + shared_blks_read)
     ELSE 0.0
END as cache_hit_ratio
```

### 4. Always Check Extension Availability
Handle missing extensions gracefully with `fetch_optional()`.

```rust
// ✅ CORRECT - Check extension exists
let result = sqlx::query("SELECT * FROM pg_stat_statements LIMIT 1")
    .fetch_optional(&pool)
    .await?;

if result.is_none() {
    warn!("pg_stat_statements extension not available");
    return Ok(());
}
```

## Code Style

### Comments
- Only comment code that needs clarification
- Don't over-comment - code should be self-documenting
- Use doc comments (`///`) for public APIs

### Error Handling
- Use `anyhow::Result` for application errors
- Log warnings for non-critical issues with `warn!()`
- Use `debug!()` for verbose operational info
- Never panic in production code

### Async Patterns
```rust
#[instrument(skip(self, pool))]
async fn collect(&self, pool: &PgPool) -> Result<()> {
    // Implementation
}
```

### Metrics Registration
All collectors must implement the `Collector` trait:
```rust
impl Collector for MyCollector {
    fn register(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.metric.clone()))?;
        Ok(())
    }

    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            // Implementation
        })
    }
}
```

## Testing Requirements

### Required Test Categories for New Collectors
Every collector **must** include these tests:

1. **Registration Test** - Verify metrics register without errors
2. **Extension Availability Test** - Handle missing extensions gracefully
3. **NULL Value Handling Test** - Test with NULL values, utility statements
4. **Type Conversion Test** - Verify SQL type conversions work
5. **Realistic Workload Test** - Test with actual data

### Test Setup
Tests run against local PostgreSQL on `localhost:5432`:
```rust
#[tokio::test]
async fn test_my_collector() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect("postgresql://postgres:postgres@localhost:5432/postgres")
        .await
        .expect("Failed to connect to test database");
    
    // Test implementation
}
```

### Edge Cases to Test
- NULL values in all columns
- Empty result sets
- Utility statements (VACUUM, ANALYZE)
- Zero values (division by zero)
- Missing extensions/tables
- Type mismatches

## Pre-Commit Workflow

Before committing:
```bash
# 1. Start PostgreSQL
just postgres

# 2. Verify test database setup
./scripts/setup-local-test-db.sh

# 3. Run all checks (clippy, fmt, tests)
just test

# 4. Commit (pre-commit hook runs automatically)
git commit
```

Install pre-commit hook:
```bash
cp scripts/pre-commit-hook.sh .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
```

## Common Patterns

### Collector Structure
```rust
#[derive(Clone)]
pub struct MyCollector {
    metric: GaugeVec,
    counter: IntGaugeVec,
}

impl MyCollector {
    pub fn new() -> Self {
        let labels = vec!["label1", "label2"];
        
        let metric = GaugeVec::new(
            Opts::new("metric_name", "Help text")
                .namespace("postgres"),
            &labels,
        )
        .expect("metric_name");
        
        Self { metric }
    }
}
```

### Safe Row Extraction
```rust
let value: i64 = row.try_get("column").unwrap_or(0);
let text: String = row.try_get("text_col").unwrap_or_default();
let nullable: Option<i64> = row.try_get("nullable_col").ok();
```

### Tracing
```rust
use tracing::{debug, warn, instrument};

#[instrument(skip(pool))]
async fn my_function(pool: &PgPool) -> Result<()> {
    debug!("Starting operation");
    warn!("Non-critical issue detected");
    Ok(())
}
```

## Documentation References
- [CONTRIBUTING.md](../CONTRIBUTING.md) - Complete development guide
- [tests/TESTING.md](../tests/TESTING.md) - Testing patterns and examples
- [PRE_RELEASE_CHECKLIST.md](../PRE_RELEASE_CHECKLIST.md) - Release process

## Zero Tolerance for Panics
All code must gracefully handle:
- Missing extensions/tables
- NULL values
- Type mismatches
- Division by zero
- Empty result sets
- Utility statements

**If you're not sure, test it locally with `./scripts/setup-local-test-db.sh` before committing.**

---

**Remember**: Every production panic is a test we didn't write.
