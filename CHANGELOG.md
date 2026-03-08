# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.11.1] - Unreleased

### Added
- **24h Rust Soak Toolkit**: Added `scripts/benchmark/run-rust-soak.sh`, `scripts/benchmark/check-rust-soak.sh`, and a dedicated Grafana soak dashboard (`scripts/benchmark/rust-soak-dashboard.json`) to run phased long-duration stress tests and collect reliability/performance artifacts.

## [0.11.0] - 2026-03-07

### Added
- **Configurable Statements Top-N**: Added `--statements.top-n` and `PG_EXPORTER_STATEMENTS_TOP_N` so `pg_stat_statements` export cardinality can be tuned without code changes.
- **Recent Query Pressure Dashboard Row**: Added a second Grafana `pg_stat_statements` row focused on recent 5-minute pressure (DB time added, mean time, call rate, WAL added, temp blocks added) to complement the existing cumulative "since reset" view.
- **Vacuum Workflows for Local Testing**: Added `just vacuum-workflow` for deterministic manual vacuum testing and `just autovacuum-workflow` for PostgreSQL-managed cleanup testing against local `pgbench` data.
- **Vacuum / Bloat Dashboard Panels**: Added Grafana panels for estimated bloat ratio, estimated dead space, table size, autovacuum threshold pressure, and time since last autovacuum to make repack and `VACUUM FULL` candidates easier to identify.
- **Never-Autovacuumed Visibility**: Added `pg_stat_user_tables_never_autovacuumed` and `pg_stat_user_tables_never_autoanalyzed` so neglected tables are no longer indistinguishable from recently maintained ones.

### Changed
- **Statements Default Scope**: Reduced the default `pg_stat_statements` exporter-side top-N from 100 to 25 to lower scrape cost and time-series cardinality while keeping query-level visibility useful by default.
- **CLI Option Layout**: Documented the collector-specific option convention using long-only flags in `src/cli/commands/options.rs` with typed values carried through `src/collectors/config.rs`.
- **Statements Query Coverage**: Switched statement role resolution from `pg_user` to `pg_roles` with `LEFT JOIN` semantics so valid statement rows are not dropped when login-user views are too narrow.
- **Statements Extension Detection Overhead**: Cached `pg_stat_statements` extension availability inside the collector to avoid querying `pg_extension` on every scrape while still rechecking periodically if the extension is missing.
- **Query Dashboard Semantics**: Clarified Grafana `pg_stat_statements` panel titles, descriptions, and legends so cumulative panels explicitly mean "since reset" and legends include `datname` for better disambiguation.
- **PostgreSQL Support Floor**: Updated documentation and dashboard wording to reflect the supported PostgreSQL range of 14+.
- **Exporter Session Tagging**: Applied a default PostgreSQL `application_name` of `pg_exporter` to exporter-managed connections so logs and `pg_stat_activity` identify exporter sessions explicitly.
- **Vacuum Dashboard Focus**: Reworked the Grafana vacuum row to prioritize predictive vacuum debt and bloat signals before active vacuum execution, matching real-world repack and autovacuum troubleshooting better.
- **`stat_user_tables` Threshold Semantics**: Autovacuum and autoanalyze threshold ratios now honor per-table reloptions instead of assuming only global PostgreSQL settings.
- **Table DML Dashboard Semantics**: Switched the table DML panel from `rate(...)` to reset-safe `delta(...)/300` queries to avoid PromQL warnings on exporter snapshot metrics.
- **Activity Dashboard Semantics**: Reworked the long-running query panel to show the real exported metrics (`pg_stat_activity_oldest_query_age_seconds` and per-database max query duration) instead of duplicating one series under two labels.

### Fixed
- **Exporter Open-FD Inflation**: Fixed high `pg_exporter_process_open_fds` values on Linux by narrowing self-monitoring refresh scope to the exporter PID only (instead of refreshing all processes via `sysinfo`). This removes persistent `/proc/*/stat` descriptor caching for unrelated processes.
- **Statements Self-Observation Noise**: Excluded the statements collector's own `pg_stat_statements` query from exported top-N results so quiet systems are less likely to show the exporter itself as a top query.
- **Statements Failed-Scrape Data Loss**: Statement metrics are now reset only after a replacement query succeeds, preserving the last good snapshot across transient query failures.
- **Vacuum Progress Stale Series**: `pg_stat_progress_vacuum`-based metrics now clear finished table series instead of leaving stale vacuum progress visible after the vacuum ends.
- **Vacuum Stats Failed-Scrape Data Loss**: Vacuum freeze-age and autovacuum-worker metrics are now replaced only after all source queries succeed, preserving the last good snapshot across transient failures.
- **Autovacuum Detection Accuracy**: Vacuum progress now identifies autovacuum using backend type instead of query-text matching.
- **`stat_user_tables` Failed-Scrape Data Loss**: Table statistics are now fetched before reset so a full collection failure preserves the last good snapshot instead of blanking the metrics.
- **`stat_user_tables` Dynamic Database Churn**: Partial per-database failures (for example, databases dropped between discovery and scrape) are now logged while still publishing the successful portion of the snapshot.
- **Long-Running Query Stale Series**: `activity/queries` now clears finished long-running query label sets before publishing the current snapshot, preventing stale wait/state/duration series from lingering after incidents end.

## [0.10.3] - 2026-03-04

### Fixed
- **`pg_stat_statements` UTF-8 Label Truncation Panic**: Fixed a panic when truncating `query_short` labels at 80 bytes if PostgreSQL returned text ending mid-multibyte UTF-8 character. Truncation now uses a safe character boundary (`floor_char_boundary`) before appending `...`.

### Tests
- **Issue #15 Regression Coverage**: Added collector regression tests for multibyte UTF-8 query text (including Cyrillic) to verify byte-80 boundary behavior and ensure `pg_statements` collection remains panic-free.

## [0.10.2] - 2026-02-28

### Fixed
- **Startup / Reboot Resilience**: Improved behavior when the exporter starts before PostgreSQL is fully ready. The exporter now starts independently of immediate database availability, reports `pg_up = 0` during the outage window, and recovers on later scrapes once PostgreSQL becomes reachable again.
- **Systemd Startup Ordering**: Documented PostgreSQL-aware startup ordering in the bundled systemd unit so deployments can avoid boot-time races by starting `pg_exporter` after `postgresql.service`.
- **`pg_stat_statements` Test Compatibility**: Fixed the test helper to use the correct `pg_stat_statements_reset` function signature across PostgreSQL 14-18.

### CI
- **Coverage Database Setup**: Added the missing `pg_stat_statements` preload and initialization steps to the coverage workflow so coverage runs match the normal PostgreSQL test matrix setup.
- **Pre-commit SQL Check Scope**: Narrowed the `pg_stat_statements` cast warning in the pre-commit hook to staged collector source files so workflow and test-seed queries do not trigger false positives.

## [0.10.1] - 2026-02-24

### Changed
- **Replication Integration Topology**: Replaced shell-script-driven replication setup with in-test `testcontainers` orchestration for a real primary+replica PostgreSQL topology.
- **Replication Semantics Coverage**: Added end-to-end assertions for role and lag semantics across primary sentinel behavior, replica backlog (`lag > 0`), replica catch-up (`lag = 0`), and broken-path error handling.
- **WAL Dashboard Signals**: Updated WAL dashboards to focus on default collector runtime signals (throughput, record activity, buffer pressure, FPI ratio) and replication max lag bytes.
- **Dashboard Layout**: Reworked WAL layout to a 2x2 panel grid and adjusted section ordering/row behavior for consistent rendering.

### CI
- **Container Runtime Wiring**: Added explicit runtime socket detection/export (`DOCKER_HOST`) for testcontainers so replication topology tests run consistently with Docker or Podman sockets.

### Fixed
- **Uptime Panel Semantics**: Uptime now renders `DOWN` in red when `pg_up = 0` instead of showing stale uptime values.
- **Metrics Stack Provisioning**: Ensured dashboard provisioning file mode is readable in the Grafana container (`0644`) to prevent missing dashboard after `just restart-metrics`.
- **Dashboard Validator Portability**: Hardened `scripts/validate-dashboard.sh` with repo-root path resolution, temp-file safety, command checks, and portable regex usage.

## [0.10.0] - 2026-02-23

### Added
- **Database Outage Resilience**: The exporter now starts and stays available even if PostgreSQL is unreachable.
- **Scrape Behavior**: The `/metrics` endpoint now always returns HTTP 200 during database outages to prevent unnecessary exporter-down alerts. Use `pg_up` for database availability; `/health` continues to reflect database status.
- **`pg_up` Reporting**: Improved `pg_up` metric to accurately reflect database connectivity. It is now driven by a dedicated connectivity check and is no longer overwritten by other collectors.
- **Metric Omission**: Database-dependent metrics are now omitted from the output when the database is unreachable, rather than reporting stale or zero values.
- **Deferred Version Initialization**: If the PostgreSQL version cannot be detected at startup, it is automatically retried during the first successful scrape.
- **State Management**: Expanded metric `.reset()` coverage across collectors (activity, database, stat tables, locks, vacuum, TLS, and statements) so dropped/renamed objects are cleared from Prometheus output.
- **Dashboard Enhancements**: Added new panels for Replication Lag and Server Role to the Grafana dashboard.

### Fixed
- **Startup Hangs**: Implemented lazy connection pooling and startup timeouts to ensure the HTTP server binds and starts regardless of database state.
- **Collection Timeouts**: Added 5-second acquisition timeouts to prevent long-running scrapes when PostgreSQL is unresponsive.
- **Connections Collector**: Fixed a bug where `pg_stat_activity_max_connections` (and derived utilization metrics) would default to 100 if the query failed, leading to misleading data. Errors are now propagated correctly.
- **Replication Lag Type Handling**: Fixed replica lag SQL expression typing/null handling so `pg_replication_lag_seconds` reports correctly on standby lag scenarios instead of collapsing to `0`.

### Changed
- **Replication Lag Compatibility**: `pg_replication_lag_seconds` behavior is aligned with `postgres_exporter` semantics (`0` on primary and non-negative lag on replicas). Use `pg_replication_is_replica` to distinguish server role.
- **Precision Timing**: Updated `pg_stat_statements` latency metrics to use high-precision floating point math for sub-second visibility.
- **Code Quality**: Replaced hardcoded system schema strings and time conversion factors with shared constants for better maintainability.
- **Dependencies**: Updated all dependencies to their latest compatible versions.

## [0.9.7] - 2026-02-06

### Added
- **Docker Secrets Support**: Added `PG_EXPORTER_DSN_FILE` environment variable to read PostgreSQL DSN from a file (e.g., Docker secrets mounted at `/run/secrets/pg_dsn`). Priority: `PG_EXPORTER_DSN_FILE` > `PG_EXPORTER_DSN` > `--dsn` flag > default value.

## [0.9.6] - 2026-02-06

### Fixed
- **Version Collector**: Fixed duplicate version metrics after PostgreSQL upgrades. Metrics are now reset before collection to prevent exporting stale label combinations (e.g., both version 16 and 17 simultaneously).

## [0.9.5] - 2025-12-15

### Fixed
- **Scraper**: Fixed a bug where scrape metrics were recorded twice (once explicitly and once via `Drop`), causing inaccurate scrape counts and duration metrics. Implemented `recorded` flag in `ScrapeTimer` to ensure metrics are recorded exactly once (RAII safe).

## [0.9.4] - 2025-12-14

### Added
- **Local Metrics Stack Helpers**
  - Added `just metrics` and `just restart-metrics` recipes to spin up Prometheus + Grafana with the bundled dashboard and persistent Prometheus volume.
  - Auto-builds/rebuilds the Grafana stack image when `grafana/dashboard.json` changes and exposes Grafana (3000) and Prometheus (9090) for quick local testing against the exporter on port 9432.

### Changed
- **Dependencies**
  - Ran `cargo update` to pull in the latest compatible dependency patch releases.

## [0.9.3] - 2025-12-01

### Changed
- **Container Image Improvements**
  - Migrated from Alpine to distroless base image (gcr.io/distroless/static-debian12:nonroot)
  - Reduced final image size to ~12.8MB with improved security posture
  - Removed unnecessary runtime dependencies (postgresql-client)
  - Added dynamic port support via `PG_EXPORTER_PORT` environment variable
  - Documented all available environment variables in Containerfile
  - Removed misleading default DSN (now requires explicit configuration)
  - Runs as non-root user (UID 65532) by default
  - Removed built-in HEALTHCHECK (use external health checks with orchestrators)
  - Optimized layer caching for faster rebuilds

## [0.9.2] - 2025-12-01

### Fixed
- **Exporter Collector (`--collector.exporter`)**
  - Fixed `pg_exporter_metrics_total` to accurately count active time series/cardinality
  - Metric now matches `curl -s 0:9432/metrics | grep -vEc '^(#|\s*$)'`
  - Previously counted MetricFamily objects instead of actual exported time series
  - Improved UTF-8 handling with zero-copy optimization and lossy fallback for robustness
  - Fixed overflow handling to return 0 instead of i64::MAX for safer alert behavior

### Changed
- Updated `pg_exporter_metrics_total` description to clarify it tracks "Total active time series / cardinality (non-comment, non-empty lines)"
- Enhanced exporter collector documentation with explicit cardinality counting behavior

## [0.9.1] - 2025-11-22

### Fixed
- **Grafana Dashboard TLS/SSL Group**
  - Removed invalid database filter from TLS Version and Cipher queries
  - Fixed SSL Status and Certificate Status panels to display values only
  - Converted Certificate Expiry from gauge to stat panel
  - Changed Certificate Validity Period unit for better readability
  - Fixed TLS/SSL row structure with all 8 panels properly nested
  - Resolved duplicate panel IDs across dashboard

### Changed
- Enhanced TLS panel descriptions with cipher recommendations and security context
- Set TLS/SSL dashboard group to collapsed by default

### Development
- Added `test-tls` recipe to justfile for automated SSL-enabled PostgreSQL testing
- Improved `just test` to auto-start PostgreSQL container if not running
- Added idempotent deployment checks to prevent duplicate version bumps
- Fixed bump recipes to commit both Cargo.toml and Cargo.lock

## [0.9.0] - 2025-11-21

### Added
- **TLS/SSL Monitoring Collector** (disabled by default, enable with `--collector.tls`)
  - Certificate expiration tracking (`pg_ssl_certificate_expiry_seconds`)
  - Certificate validity status (`pg_ssl_certificate_valid`)
  - Certificate validity timestamps (`pg_ssl_certificate_not_before_timestamp`, `pg_ssl_certificate_not_after_timestamp`)
  - Server SSL configuration (`pg_ssl_enabled`)
  - Active connection TLS statistics (`pg_ssl_connections_total`, `pg_ssl_connections_by_version`, `pg_ssl_connections_by_cipher`, `pg_ssl_connection_bits_avg`)
  - Supports PostgreSQL 9.5+ for connection stats
  - Graceful handling of remote installations (certificate metrics require local filesystem access)
- Comprehensive TLS collector test suite with SSL-enabled PostgreSQL testing
- TLS testing documentation and helper scripts

### Changed
- **Strict Clippy Linting Enforced**
  - All Rust warnings denied
  - Pedantic clippy lints enabled
  - Safety-critical lints enforced (`unwrap_used`, `expect_used`, `panic`, `indexing_slicing`)
  - Code quality improvements across entire codebase
- **Full rustls migration** - Removed OpenSSL dependency entirely
  - Removed `[features]` section from Cargo.toml (musl feature no longer needed)
  - Removed openssl dependency from production builds (100% rustls)
  - Disabled default features for opentelemetry-otlp to avoid reqwest/native-tls
  - OpenTelemetry now uses tonic with rustls for gRPC TLS
  - Updated CI/CD workflows to build musl binaries without feature flags
  - Simplified dependency tree and improved binary portability
  - musl static binaries now build without vendored OpenSSL
- Cleaned up unnecessary `#[allow(clippy::unwrap_used)]` and `#[allow(clippy::expect_used)]` attributes from tests
- Improved dashboard validation script to correctly detect metrics with namespace prefixes

### Fixed
- Dashboard validation now correctly handles `postgres_*` prefixed metrics from namespaced collectors
- Removed 51 unnecessary `#[allow(clippy::unwrap_used)]` attributes from test functions
- Removed 41 unnecessary `#[allow(clippy::expect_used)]` attributes (31 from non-test code, 10 from tests)

### Documentation
- Added TLS_TESTING.md with comprehensive testing guide
- Updated documentation for all TLS-related collectors
- Improved code documentation with safety notes

### Internal
- Added x509-parser dependency for certificate parsing
- Enhanced error handling in collectors
- Improved test coverage with 415 total tests (171 unit + 10 integration + 234 collector tests)

### Backward Compatibility
- ✅ No breaking changes
- ✅ All existing command-line flags preserved
- ✅ Default collectors unchanged
- ✅ Existing metrics unchanged
- ✅ Dashboard compatibility maintained

## [0.8.5] - Previous Release

See git history for previous changes.
