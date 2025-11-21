# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
