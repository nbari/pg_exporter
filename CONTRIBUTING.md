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
# The 'just test' command checks both default and telemetry-enabled builds
# and automatically uses local PostgreSQL
# (overrides any PG_EXPORTER_DSN in .envrc)
just test

# Or manually set DSN for specific tests
PG_EXPORTER_DSN="postgresql://postgres:postgres@localhost:5432/postgres" cargo test --test collectors_tests statements
```

**Important:** Tests always run against **local PostgreSQL** on `localhost:5432`, not remote databases. The `just test` command handles this automatically, even if you have `PG_EXPORTER_DSN` set in `.envrc`.

Inside DevPod, use its bundled `postgres:5432` service and provided environment
instead of starting a second host database.

### Optional Telemetry

The default build excludes the OTLP exporter and its dependencies. Build with
`cargo build --features telemetry` or use `just watch telemetry` when debugging
with distributed traces. An `OTEL_EXPORTER_OTLP_ENDPOINT` and `-v`/`RUST_LOG=info`
are also needed at runtime; see [the README](README.md#optional-opentelemetry-tracing).
`pg_exporter --version` reports whether support was compiled in.
Without the feature, a configured `OTEL_EXPORTER_OTLP_ENDPOINT` produces a single
startup warning on stderr, even at the default error level or `RUST_LOG=off`.
The endpoint and authentication values are never included in that warning.

Keep OTLP initialization in `src/cli/telemetry/otlp.rs` and HTTP trace propagation
in `src/exporter/telemetry.rs`, both gated by `#[cfg(feature = "telemetry")]`.
Ordinary `tracing` instrumentation, spawned-task span propagation, request IDs,
and local logging must remain available without that feature. Prometheus metrics
are not part of the optional telemetry stack.

`just clippy` checks default and all-feature configurations; `just test` runs both
test suites. When running Cargo directly inside DevPod:

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets
cargo clippy --locked --all-targets --all-features
cargo test --locked
cargo test --locked --features telemetry
```

#### Telemetry Footprint

Local comparison on 2026-09-19, x86_64 GNU/Linux in DevPod, Rust 1.98.1,
PostgreSQL 18.4, the same lockfile and release profile (fat LTO, one codegen unit,
stripped symbols):

| Measurement | Default | `--features telemetry` |
| --- | ---: | ---: |
| Binary bytes | 8,026,168 | 10,010,064 |
| Unique normal dependency packages, including this crate | 213 | 247 |
| Scrape median at `RUST_LOG=error` | 1.248 ms | 1.232 ms |
| Scrape median at `RUST_LOG=info` | 1.334 ms | 1.307 ms |

The default binary is 19.8% smaller. Timings are medians of six run medians,
alternating build order, with 20 warmup requests followed by 250 sequential
keep-alive `/metrics` requests per run. Both binaries used default collectors,
the same local test database, no OTLP endpoint, and logs redirected to `/dev/null`.
All runs exposed the same 73 metric families. Per-run latency ranges overlapped;
this sample does **not** establish a runtime speedup (nor does it measure the cost
of active OTLP export). SQL work and ordinary tracing instrumentation remain.

To repeat the size comparison without overwriting your normal build artifacts:

```sh
telemetry_bench=$(mktemp -d)
cargo build --release --locked --no-default-features --target-dir "$telemetry_bench/target"
cp "$telemetry_bench/target/release/pg_exporter" "$telemetry_bench/default"
cargo build --release --locked --features telemetry --target-dir "$telemetry_bench/target"
cp "$telemetry_bench/target/release/pg_exporter" "$telemetry_bench/telemetry"
wc -c "$telemetry_bench/default" "$telemetry_bench/telemetry"
```

Count normal dependency packages with
`cargo tree --locked --edges normal --prefix none --no-dedupe --format '{p}' | sort -u | wc -l`,
then repeat with `--features telemetry`. Build dependencies and dev-only crates
are not part of that count; `testcontainers` can still pull tonic into tests.

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
  semaphore (default 2), the peak per-database connection count is bounded by *concurrency*,
  not by the *number of databases* (so 100 or 10,000 databases both peak at ~concurrency).

**Do not reintroduce a per-database pool cache.** Caching pins ~one persistent connection per
database and can exhaust `max_connections`. This invariant is locked by
`tests/collectors/connection.rs` (fresh backend PID per call + closed on drop); keep it green.

The `index` collector discovers databases once and collects its ten metric families with
one combined query per database. On healthy scrapes it opens one ephemeral connection per
non-default database. Permission/feature errors can fall back to separate metric-group
queries on that same connection; connection, lock, and timeout errors are not retried.
Keep the broader `pg_index` scope of the invalid-index count, including partitioned index
parents that are absent from `pg_stat_user_indexes`. Tests in `tests/index_scrape.rs` compare
against frozen 0.20.0 SQL and verify the session/query count and readable-subset behavior.

When spawning per-database tasks, wrap their futures with `permit_metrics::inherit` **before**
passing them to Tokio. The optional task-local context attributes shared permit wait/hold
measurements to the top-level collector; it is not inherited automatically by spawned tasks.

Tracing context also needs explicit propagation: instrument the future with
`Span::current()` **before** handing it to `tokio::spawn` or `JoinSet::spawn`.
`permit_metrics::inherit` propagates permit attribution only; it does not propagate
the request's tracing span. Keep both wrappers on per-database tasks so query spans
and log events retain the `/metrics` request's correlation fields.

`tokio::task::spawn_blocking` needs propagation too: capture `Span::current()`
before spawning, then enter it **inside the synchronous closure**. Never hold an
entered span guard across an await. OS samplers use `blocking::offload_coalesced`,
which handles this centrally. A started blocking sample cannot be cancelled and
retains its originating span until it finishes; coalescing still limits it to one
in-flight sample per reader. With telemetry enabled, a sample that never returns
prevents the retained spans (including the HTTP request span) from closing and
being exported; other completed spans in that trace can still be exported.

Deliberately detached work is different: statement text refreshes run under an
explicit `statements.text_refresh` root span with a `follows_from` link to the
originating span. That link supplies trace correlation without keeping the HTTP
request span open after the response. Local background logs share the refresh
span; they do not inherit the request's fields. Keep this distinction when adding
background jobs.

### Scrape Safety Model

Every exporter-managed scrape connection must pass through the shared hardening in
`src/collectors/util.rs`: default `application_name`, bounded connection establishment,
server-side `lock_timeout`, and server-side `statement_timeout`. The connect timeout bounds
DNS/TCP/TLS/authentication before PostgreSQL can enforce server-side settings. `lock_timeout`
prevents lock-blocked scrapes from piling up behind `ACCESS EXCLUSIVE` locks;
`statement_timeout` prevents already-running scrape queries from outliving the HTTP scrape
indefinitely.

Do not add collector-local `tokio::time::timeout(...)` wrappers around database work. A
client-side timeout can drop the Rust future while the PostgreSQL backend continues waiting
or running server-side. Prefer PostgreSQL's server-side cancellation knobs for query work,
and keep any new per-database collector work behind `util::acquire_db_query_permit` plus
`util::open_db_connection`, which is the only place that may apply a client-side timeout to
connection establishment.

The source-level guard in `tests/collector_safety.rs` blocks direct collector-side
`PgConnection::connect`, collector-local pools, collector-local semaphores, and client-side
database timeouts. Update that guard only when the connection-safety model itself changes.

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
3. Runs all tests across PostgreSQL 14, 15, 16, 17, and 18

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
