# AGENTS.md

These guidelines are mandatory for contributors and for any AI coding agent
operating in this repository. The goal is to land changes safely, keep security
invariants intact, and keep the workspace consistent over time.

## Project Shape

- This repository is a single Rust crate, not a Cargo workspace.
- Prefer `cargo test` over `cargo test --workspace`.
- The main entrypoint is `pg_exporter`, a PostgreSQL metrics exporter written in Rust.

## First Steps

Before making non-trivial changes:

1. Read [CONTRIBUTING.md](/home/nbari/projects/rust/pg_exporter/CONTRIBUTING.md).
2. Read [tests/TESTING.md](/home/nbari/projects/rust/pg_exporter/tests/TESTING.md) if touching collectors or tests.
3. Check [.justfile](/home/nbari/projects/rust/pg_exporter/.justfile) for the supported local workflows.

## Local Commands

- Start local PostgreSQL: `just postgres`
- Verify the local test database: `./scripts/setup-local-test-db.sh`
- Run the standard local validation flow: `just test`
- Formatting is required: `cargo fmt --all -- --check`
- Clippy is required: `cargo clippy --all-targets --all-features`
- Run tests directly when the local DB is already ready:
  - `PG_EXPORTER_DSN="postgresql://postgres:postgres@localhost:5432/postgres" cargo test`

These are not advisory commands. A change is not ready for review if formatting,
Clippy, or tests fail locally.

## Test Database Rules

- Tests must run against local PostgreSQL on `localhost:5432`.
- Do not run the test suite against a remote `PG_EXPORTER_DSN`.
- The local PostgreSQL instance must have `pg_stat_statements` preloaded and the extension created.
- Rootless Podman is used in local workflows. Podman networking may invoke `pasta`; errors from `pasta` are container-networking failures, not Rust test failures by themselves.

## Coding Rules

- Do not introduce panics in production code.
- Prefer `row.try_get(...)` over `row.get(...)`.
- Cast PostgreSQL numeric values explicitly in SQL with `::bigint` or `::double precision`.
- Guard all divisions against zero.
- Handle missing extensions and version-specific features gracefully.
- Keep behavior resilient when PostgreSQL is unavailable. `pg_up` should reflect DB state without crashing the exporter.

## Lint Contract

This repository treats lint failures as merge blockers.

- Rust warnings are denied.
- Clippy runs with `cargo clippy --all-targets --all-features`.
- The crate denies broad Clippy groups including `all`, `pedantic`, `correctness`, `suspicious`, `perf`, and `complexity`.
- The crate explicitly denies `unwrap_used`, `expect_used`, `panic`, `indexing_slicing`, and `await_holding_lock`.

Contributors and AI agents must assume the following:

- `unwrap()` is not acceptable in normal code.
- `expect()` is not acceptable in normal code.
- introducing `panic!()` in production paths is not acceptable.
- direct indexing and unchecked slicing should be avoided when they can panic.
- adding `#[allow(clippy::...)]` to bypass a lint is not an acceptable default fix.

If a PR fails on Clippy, fix the code to satisfy the lint instead of weakening
the lint policy.

Tests may use narrowly-scoped allowances when there is a clear reason, but
production code should follow the lint contract strictly.

## Collector Changes

New or modified collectors should include:

1. Registration tests
2. Extension or feature availability tests
3. Edge-case coverage for NULL, empty results, utility statements, and zero values
4. Type compatibility coverage
5. Realistic workload coverage where applicable

If you modify files under `src/collectors/`:

- Expect the pre-commit hook to check that local PostgreSQL is reachable on `localhost`.
- Expect the pre-commit hook to check that `pg_stat_statements` is installed.
- Run `./scripts/setup-local-test-db.sh` before committing if there is any doubt about local DB state.
- Do not add `row.get(...)` calls; the hook treats that as unsafe.
- If you touch queries against `pg_stat_statements`, add explicit numeric casts such as `::bigint` or `::double precision`.

## Pre-Commit Expectations

The installed hook in [scripts/pre-commit-hook.sh](/home/nbari/projects/rust/pg_exporter/scripts/pre-commit-hook.sh) enforces or warns about:

- collector changes without a running local PostgreSQL instance
- missing `pg_stat_statements` in the local test database
- unsafe `row.get(...)` usage in staged changes
- `pg_stat_statements` queries added without explicit type casts

Treat those as repository constraints, not optional guidance.

In practice, new PRs often fail because generated code introduces `unwrap()`,
`expect()`, or panic-prone shortcuts. Do not submit code in that state.

## Commits

- This repository expects signed commits.
- Do not use `git commit --no-gpg-sign`.

## Release Flow

- The repository release flow is defined in [.justfile](/home/nbari/projects/rust/pg_exporter/.justfile).
- Version bumps happen from `develop` via `just bump`, `just bump-minor`, or `just bump-major`.
- Release tagging and merge flow happen via `just deploy`, `just deploy-minor`, or `just deploy-major`.
- Keep docs aligned with the real release flow; do not add references to missing checklist files.

## When Updating Docs

- Prefer updating existing docs instead of creating parallel instructions.
- If you change commands, tests, or release steps, update the relevant docs in the same change.
