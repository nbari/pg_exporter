# Development Guide

## Quick Start

```bash
# Start PostgreSQL with proper configuration
just postgres

# Verify test database setup
./scripts/setup-local-test-db.sh

# Run all tests
just test
```

> **Prefer a zero-setup environment?** The repo ships a compose-based
> [Dev Container](.devcontainer/README.md) (Rust + PostgreSQL with
> `pg_stat_statements`). With [DevPod](https://devpod.sh): `scripts/dev-up`, then
> `just test` inside — works on Linux, macOS, and fedora-atomic with no host
> database. See [`.devcontainer/README.md`](.devcontainer/README.md).

---

## Local Setup

### Prerequisites

- PostgreSQL 16+ (via podman/docker or locally)
- Rust toolchain (latest stable)
- `just` command runner (optional)

### PostgreSQL Configuration

The local PostgreSQL **must** have `pg_stat_statements` enabled. This is already configured in `db/config/postgres/postgresql.conf`:

```ini
shared_preload_libraries = 'pg_stat_statements'
pg_stat_statements.track = all
pg_stat_statements.max = 10000
```

Start PostgreSQL:
```bash
just postgres
```

Verify setup before testing:
```bash
./scripts/setup-local-test-db.sh
```

This script will:
- ✓ Check PostgreSQL is running
- ✓ Verify pg_stat_statements is loaded
- ✓ Create the extension if missing
- ✓ Generate realistic test data
- ✓ Validate query capture works

---

## Testing

### Run Tests

```bash
# The 'just test' command automatically uses local PostgreSQL
# (overrides any PG_EXPORTER_DSN in .envrc)
just test

# Or manually set DSN for specific tests
PG_EXPORTER_DSN="postgresql://postgres:postgres@localhost:5432/postgres" cargo test --test collectors_tests statements
```

**Important:** Tests always run against **local PostgreSQL** on `localhost:5432`, not remote databases. The `just test` command handles this automatically, even if you have `PG_EXPORTER_DSN` set in `.envrc`.

### Required Tests for New Collectors

Every collector **must** include these test categories:

1. **Registration Test** - Verify metrics register without errors
2. **Extension Availability Test** - Handle missing extensions gracefully
3. **NULL Value Handling Test** - Test with NULL values, utility statements
4. **Type Conversion Test** - Verify SQL type conversions work
5. **Realistic Workload Test** - Test with actual data

See [tests/TESTING.md](tests/TESTING.md) for detailed patterns and examples.

---

## Safe Coding Patterns

**All patterns are documented inline in the code.** See `src/collectors/statements/pg_statements.rs` for detailed examples.

### Key Rules

1. **Always use `try_get()` instead of `get()`** - Prevents panics on NULL values
2. **Always cast SQL numeric columns** - Use `::bigint` or `::double precision`  
3. **Always check denominator before division** - Prevents division by zero
4. **Always check extension availability** - Handle missing extensions gracefully
5. **Keep per-database connections ephemeral** - Never cache a pool/connection per database

See the code for inline comments explaining why these patterns matter.

### Multi-Database Connection Model

Per-database catalogs (`pg_stat_user_tables`, `pg_stat_user_indexes`, `pg_statio_*`) can only
be read from a connection **to that database**, so the `stat`/`index` collectors fan out
across databases. To keep the exporter's connection footprint safe on large or
connection-constrained clusters (e.g. AWS RDS), that fan-out follows a strict model:

- **Shared pool** (`src/exporter/mod.rs`, `max_connections(3)`): used by *every* collector for
  the default database and all **cluster-wide** views (`pg_stat_activity`, `pg_locks`,
  `pg_stat_replication`, `pg_stat_database`, `pg_stat_progress_*`). Those never need fan-out.
- **Ephemeral per-database connections** (`util::open_db_connection`): opened per scrape query
  and **closed on drop** — never cached. Combined with the `--collectors.max-db-concurrency`
  semaphore (default 5), the peak per-database connection count is bounded by *concurrency*,
  not by the *number of databases* (so 100 or 10,000 databases both peak at ~concurrency).

**Do not reintroduce a per-database pool cache.** Caching pins ~one persistent connection per
database and can exhaust `max_connections`. This invariant is locked by
`tests/collectors/connection.rs` (fresh backend PID per call + closed on drop); keep it green.

---

## Git Hooks

### Installing Pre-Commit Hook

The pre-commit hook catches unsafe patterns before they're committed:

```bash
# Install the hook
cp scripts/pre-commit-hook.sh .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
```

The hook will:
- Check if PostgreSQL is running (when modifying collectors)
- Verify pg_stat_statements extension is available
- Detect unsafe `row.get()` usage
- Warn about missing SQL type casts

### Hook Workflow

When you commit collector changes:

```bash
git commit -m "feat: new collector"

# Hook runs automatically:
# 🔍 Running pre-commit checks...
# 📊 Collector code changed, verifying test database setup...
# ✓ PostgreSQL is running
# ✓ pg_stat_statements extension exists
# 🔎 Checking for unsafe patterns...
# ✅ Pre-commit checks passed
```

If issues are found, you can:
- Fix them and commit again
- Or bypass with `git commit --no-verify` (not recommended)

---

## Pre-Commit Checklist

Before committing code:

- [ ] PostgreSQL is running with pg_stat_statements enabled
- [ ] `./scripts/setup-local-test-db.sh` passes without errors
- [ ] `just test` passes (all tests)
- [ ] No `row.get()` calls without error handling
- [ ] All SQL numeric columns have explicit type casts
- [ ] New collectors have all required test categories
- [ ] Edge cases tested (NULL, zero, utility statements)

---

## Common Mistakes

### 1. Trusting SQL Types

**Problem**: PostgreSQL NUMERIC type doesn't match Rust i64  
**Solution**: Always cast explicitly: `column::bigint`, `(value)::double precision`

### 2. Using `.get()` Instead of `.try_get()`

**Problem**: `row.get()` panics on NULL or type mismatches  
**Solution**: Use `row.try_get()` with appropriate fallbacks

### 3. Not Testing Edge Cases

**Problem**: Tests only check "happy path"  
**Solution**: Test NULL values, utility statements, empty results, zero values

### 4. Skipping Extension Checks

**Problem**: Assuming extensions are always installed  
**Solution**: Check availability and handle gracefully with `fetch_optional()`

---

## Debugging

### PostgreSQL Not Running

```bash
# Start PostgreSQL
just postgres

# Verify connection
psql -h localhost -U postgres -d postgres -c "SELECT 1"
```

### pg_stat_statements Not Loaded

```bash
# Check if loaded
psql -h localhost -U postgres -d postgres -c "SHOW shared_preload_libraries"
# Should show: pg_stat_statements

# If not loaded, stop and restart PostgreSQL
just stop-containers
just postgres
```

### Extension Not Created

```bash
# Create extension
psql -h localhost -U postgres -d postgres -c "CREATE EXTENSION pg_stat_statements"
```

### Tests Fail with Type Errors

Check actual PostgreSQL column types and add explicit casts in SQL.

---

## CI/CD

GitHub Actions automatically:
1. Starts PostgreSQL with pg_stat_statements preloaded
2. Creates the extension
3. Runs all tests across PostgreSQL 16, 17, and 18

If CI fails but local tests pass, you probably don't have pg_stat_statements enabled locally.

---

## Quick Reference

```bash
# Daily workflow
just postgres                          # Start PostgreSQL
./scripts/setup-local-test-db.sh      # Verify setup
just test                              # Run all tests
git commit                             # Pre-commit hook runs

# Install pre-commit hook
cp scripts/pre-commit-hook.sh .git/hooks/pre-commit

```

---

## Documentation

- [tests/TESTING.md](tests/TESTING.md) - Comprehensive testing guide with examples
- [README.md](README.md) - Project overview and usage

---

## Zero Tolerance for Panics

All code must handle:
- Missing extensions/tables
- NULL values
- Type mismatches
- Division by zero
- Empty result sets
- Utility statements

**If you're not sure, TEST IT LOCALLY with the setup script before committing.**

---

**Remember**: Every production panic is a test we didn't write.
