# TLS Collector Testing Guide

This guide explains how to test the TLS collector with SSL-enabled PostgreSQL using Podman.

## Overview

The TLS collector provides SSL/TLS monitoring for PostgreSQL with three sub-collectors:

1. **ServerTlsConfigCollector** - Server SSL configuration (works remotely)
2. **CertificateCollector** - Certificate expiration monitoring (requires local filesystem access)
3. **ConnectionTlsCollector** - Per-connection SSL statistics (works remotely, PostgreSQL 9.5+)

### Remote vs Local Installation

- **Local Installation** (exporter runs on same machine as PostgreSQL): All collectors work
- **Remote Installation** (exporter runs on different machine): Certificate collector will not export metrics (filesystem access required)

The certificate collector gracefully handles remote installations by logging debug messages when certificate files are inaccessible.

## Quick Start

### Using the Helper Scripts (Recommended)

```bash
# 1. Start SSL-enabled PostgreSQL (automatically generates certs if needed)
./tests/start-ssl-postgres.sh

# 2. Run tests
PG_EXPORTER_DSN='postgresql://postgres:postgres@localhost:5433/postgres?sslmode=require' \
  cargo test --test collectors_tests tls

# 3. Run integration tests
PG_EXPORTER_DSN='postgresql://postgres:postgres@localhost:5433/postgres?sslmode=require' \
  cargo test --test tls_metrics_integration

# 4. Clean up
./tests/stop-ssl-postgres.sh
```

### Manual Podman Setup

```bash
# 1. Generate SSL certificates
./tests/generate-ssl-certs.sh

# 2. Start PostgreSQL with SSL
podman run -d \
  --name pg-exporter-ssl-test \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_DB=postgres \
  -p 5433:5432 \
  -v ./tests/ssl-certs:/var/lib/postgresql/ssl:Z \
  -v ./tests/init-ssl.sh:/docker-entrypoint-initdb.d/init-ssl.sh:Z \
  postgres:16 \
  postgres \
  -c ssl=on \
  -c ssl_cert_file=/var/lib/postgresql/ssl/server.crt \
  -c ssl_key_file=/var/lib/postgresql/ssl/server.key \
  -c ssl_ciphers='HIGH:MEDIUM:+3DES:!aNULL' \
  -c ssl_prefer_server_ciphers=on \
  -c ssl_min_protocol_version=TLSv1.2

# 3. Wait for PostgreSQL to be ready
until podman exec pg-exporter-ssl-test pg_isready -U postgres; do
  echo "Waiting for PostgreSQL..."
  sleep 1
done

# 4. Fix permissions inside container
podman exec pg-exporter-ssl-test chown postgres:postgres /var/lib/postgresql/ssl/server.key
podman exec pg-exporter-ssl-test chmod 600 /var/lib/postgresql/ssl/server.key
podman exec pg-exporter-ssl-test psql -U postgres -c "SELECT pg_reload_conf();"

# 5. Run tests
PG_EXPORTER_DSN='postgresql://postgres:postgres@localhost:5433/postgres?sslmode=require' \
  cargo test --test collectors_tests tls

# 6. Clean up
podman stop pg-exporter-ssl-test && podman rm pg-exporter-ssl-test
```

## TLS Collector Test Coverage

The TLS collector test suite includes:

### Unit Tests (`tests/collectors/tls/`)

1. **ServerTlsConfigCollector** (`server_config.rs`)
   - ✅ Metric registration
   - ✅ SSL status detection (on/off)
   - ✅ Graceful error handling

2. **CertificateCollector** (`certificate.rs`)
   - ✅ Metric registration
   - ✅ Certificate file reading
   - ✅ Expiration time calculation
   - ✅ Validity checking
   - ✅ Missing certificate handling (remote installations)

3. **ConnectionTlsCollector** (`connection_stats.rs`)
   - ✅ Metric registration
   - ✅ Per-connection SSL statistics
   - ✅ Version-aware (PostgreSQL 9.5+ check)
   - ✅ Cipher suite tracking
   - ✅ TLS version tracking

4. **TlsCollector** (`mod.rs`)
   - ✅ Main collector integration
   - ✅ Sub-collector orchestration
   - ✅ Disabled by default verification

### Integration Tests (`tests/tls_metrics_integration.rs`)

- ✅ Metrics endpoint exposure
- ✅ Prometheus format validation
- ✅ TLS collector enablement
- ✅ Metrics availability

## SSL Certificate Generation

### Self-Signed Certificates for Testing

The `generate-ssl-certs.sh` script creates self-signed certificates:

```bash
./tests/generate-ssl-certs.sh
```

This generates:
- `tests/ssl-certs/server.key` - Private key (600 permissions)
- `tests/ssl-certs/server.crt` - Certificate (valid for 365 days)
- `tests/ssl-certs/cert-info.txt` - Certificate details

### Manual Certificate Generation

```bash
# Create directory
mkdir -p tests/ssl-certs

# Generate private key
openssl genrsa -out tests/ssl-certs/server.key 2048

# Generate certificate (valid for 365 days)
openssl req -new -x509 \
  -key tests/ssl-certs/server.key \
  -out tests/ssl-certs/server.crt \
  -days 365 \
  -subj "/C=US/ST=Test/L=Test/O=Test/CN=localhost"

# Set permissions
chmod 600 tests/ssl-certs/server.key
chmod 644 tests/ssl-certs/server.crt
```

## Test Scenarios

### Scenario 1: SSL Disabled

```bash
# Start PostgreSQL without SSL
podman run -d --name pg-no-ssl \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_USER=postgres \
  -p 5432:5432 \
  postgres:16

# Run tests (should report SSL disabled)
PG_EXPORTER_DSN='postgresql://postgres:postgres@localhost:5432/postgres' \
  cargo test --test collectors_tests tls -- --nocapture

# Expected: pg_ssl_enabled = 0

# Clean up
podman stop pg-no-ssl && podman rm pg-no-ssl
```

### Scenario 2: SSL Enabled (Require Mode)

```bash
# Start PostgreSQL with SSL
./tests/start-ssl-postgres.sh

# Run tests with SSL required
PG_EXPORTER_DSN='postgresql://postgres:postgres@localhost:5433/postgres?sslmode=require' \
  cargo test --test collectors_tests tls -- --nocapture

# Expected:
# - pg_ssl_enabled = 1
# - pg_ssl_certificate_* metrics populated (if running locally)
# - pg_ssl_connections_total >= 1

# Clean up
./tests/stop-ssl-postgres.sh
```

### Scenario 3: Certificate Expiration Testing

```bash
# Generate an expired certificate
openssl req -new -x509 \
  -key tests/ssl-certs/server.key \
  -out tests/ssl-certs/server-expired.crt \
  -days -1 \
  -subj "/C=US/ST=Test/L=Test/O=Test/CN=localhost"

# Start PostgreSQL with expired cert
podman run -d --name pg-expired-cert \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_USER=postgres \
  -p 5433:5432 \
  -v ./tests/ssl-certs:/var/lib/postgresql/ssl:Z \
  postgres:16 \
  postgres \
  -c ssl=on \
  -c ssl_cert_file=/var/lib/postgresql/ssl/server-expired.crt \
  -c ssl_key_file=/var/lib/postgresql/ssl/server.key

# Fix permissions
podman exec pg-expired-cert chown postgres:postgres /var/lib/postgresql/ssl/server.key
podman exec pg-expired-cert chmod 600 /var/lib/postgresql/ssl/server.key

# Run tests
PG_EXPORTER_DSN='postgresql://postgres:postgres@localhost:5433/postgres?sslmode=require' \
  cargo test --test collectors_tests tls -- --nocapture

# Expected (if running locally):
# - pg_ssl_certificate_valid = 0
# - pg_ssl_certificate_expiry_seconds < 0 (negative = expired)

# Clean up
podman stop pg-expired-cert && podman rm pg-expired-cert
```

### Scenario 4: Remote Installation Simulation

```bash
# Start PostgreSQL with SSL (certificate files inside container)
./tests/start-ssl-postgres.sh

# Run exporter pointing to remote PostgreSQL (certificate files not accessible)
cargo run -- \
  --dsn 'postgresql://postgres:postgres@localhost:5433/postgres?sslmode=require' \
  --collector.tls \
  --port 9432

# Query metrics
curl http://localhost:9432/metrics | grep pg_ssl

# Expected:
# - pg_ssl_enabled = 1 (works remotely)
# - pg_ssl_connections_* metrics present (works remotely)
# - pg_ssl_certificate_* metrics NOT present (requires local filesystem)

# Check logs for debug message:
# "Certificate file not accessible: ... (this is expected when running remotely)"
```

### Scenario 5: Multiple PostgreSQL Versions

```bash
# Test with PostgreSQL 14
PG_VERSION=14 ./tests/start-ssl-postgres.sh
PG_EXPORTER_DSN='postgresql://postgres:postgres@localhost:5433/postgres?sslmode=require' \
  cargo test --test collectors_tests tls
./tests/stop-ssl-postgres.sh

# Test with PostgreSQL 15
PG_VERSION=15 ./tests/start-ssl-postgres.sh
PG_EXPORTER_DSN='postgresql://postgres:postgres@localhost:5433/postgres?sslmode=require' \
  cargo test --test collectors_tests tls
./tests/stop-ssl-postgres.sh

# Test with PostgreSQL 16 (default)
./tests/start-ssl-postgres.sh
PG_EXPORTER_DSN='postgresql://postgres:postgres@localhost:5433/postgres?sslmode=require' \
  cargo test --test collectors_tests tls
./tests/stop-ssl-postgres.sh
```

## Metrics Reference

### Server Configuration Metrics (Always Available)

```prometheus
# Whether SSL is enabled on the server
pg_ssl_enabled 1
```

### Certificate Metrics (Local Installation Only)

```prometheus
# Seconds until certificate expires (negative if expired)
pg_ssl_certificate_expiry_seconds 31536000  # ~365 days

# Whether certificate is currently valid
pg_ssl_certificate_valid 1

# Unix timestamp when certificate becomes valid
pg_ssl_certificate_not_before_timestamp 1704067200

# Unix timestamp when certificate expires
pg_ssl_certificate_not_after_timestamp 1735689600
```

### Connection Statistics (PostgreSQL 9.5+, Always Available)

```prometheus
# Total number of SSL connections
pg_ssl_connections_total 3

# Connections by TLS version
pg_ssl_connections_by_version{version="TLSv1.3"} 3

# Connections by cipher suite
pg_ssl_connections_by_cipher{cipher="TLS_AES_256_GCM_SHA384"} 3

# Average encryption bits across connections
pg_ssl_connection_bits_avg 256
```

## Troubleshooting

### PostgreSQL SSL Not Starting

```bash
# Check PostgreSQL logs
podman logs pg-exporter-ssl-test

# Common issues:
# 1. Certificate file permissions (should be 600 for key, 644 for cert)
# 2. Certificate ownership (should be owned by postgres user)
# 3. SELinux context (use :Z flag when mounting volumes)
```

### Tests Failing with Connection Errors

```bash
# Verify PostgreSQL is accessible
podman exec pg-exporter-ssl-test psql -U postgres -c "SELECT 1;"

# Check SSL is enabled
podman exec pg-exporter-ssl-test psql -U postgres -c "SHOW ssl;"

# Verify certificate paths
podman exec pg-exporter-ssl-test psql -U postgres -c "SHOW ssl_cert_file;"
podman exec pg-exporter-ssl-test psql -U postgres -c "SHOW ssl_key_file;"
```

### Certificate Metrics Not Showing (Remote Installation)

This is expected behavior when the exporter runs remotely. The certificate collector requires filesystem access to read certificate files.

**Debug output:**
```
Certificate file not accessible: /var/lib/postgresql/ssl/server.crt (this is expected when running remotely)
```

**Solutions:**
1. **Mount certificate files:** Use NFS/CIFS to mount PostgreSQL's certificate directory to the exporter machine
2. **Copy certificates:** Periodically copy certificate files to the exporter machine
3. **Accept limitation:** Use only the SSL configuration and connection statistics metrics

### Certificate Permission Issues

```bash
# Fix permissions inside container
podman exec pg-exporter-ssl-test chown postgres:postgres /var/lib/postgresql/ssl/server.key
podman exec pg-exporter-ssl-test chmod 600 /var/lib/postgresql/ssl/server.key
podman exec pg-exporter-ssl-test chmod 644 /var/lib/postgresql/ssl/server.crt

# Reload PostgreSQL configuration
podman exec pg-exporter-ssl-test psql -U postgres -c "SELECT pg_reload_conf();"
```

### Podman Permission Issues

```bash
# If you get permission denied errors with volumes:
# Use :Z flag for SELinux relabeling
-v ./tests/ssl-certs:/var/lib/postgresql/ssl:Z

# Or run with --privileged (not recommended)
podman run --privileged ...
```

## PostgreSQL SSL Configuration Reference

### Supported SSL Modes

- `disable` - No SSL
- `allow` - Try SSL, fallback to non-SSL
- `prefer` - Prefer SSL, fallback to non-SSL
- `require` - Require SSL (no certificate verification)
- `verify-ca` - Require SSL + verify CA
- `verify-full` - Require SSL + verify CA + verify hostname

### PostgreSQL SSL Parameters

```sql
-- Check SSL status
SHOW ssl;

-- Check SSL certificate paths
SHOW ssl_cert_file;
SHOW ssl_key_file;
SHOW ssl_ca_file;

-- Check SSL ciphers
SHOW ssl_ciphers;

-- Check TLS versions
SHOW ssl_min_protocol_version;
SHOW ssl_max_protocol_version;

-- View active SSL connections (PostgreSQL 9.5+)
SELECT * FROM pg_stat_ssl;
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: TLS Collector Tests

on: [push, pull_request]

jobs:
  test-tls:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - name: Install Podman
        run: |
          sudo apt-get update
          sudo apt-get install -y podman

      - name: Start SSL-enabled PostgreSQL
        run: |
          ./tests/start-ssl-postgres.sh

      - name: Run TLS collector tests
        env:
          PG_EXPORTER_DSN: postgresql://postgres:postgres@localhost:5433/postgres?sslmode=require
        run: |
          cargo test --test collectors_tests tls
          cargo test --test tls_metrics_integration

      - name: Stop PostgreSQL
        if: always()
        run: |
          ./tests/stop-ssl-postgres.sh
```

### GitLab CI Example

```yaml
test:tls:
  image: rust:latest
  services:
    - name: postgres:16
      alias: postgres
      command: [
        "postgres",
        "-c", "ssl=on",
        "-c", "ssl_cert_file=/etc/ssl/certs/ssl-cert-snakeoil.pem",
        "-c", "ssl_key_file=/etc/ssl/private/ssl-cert-snakeoil.key"
      ]
  variables:
    POSTGRES_PASSWORD: postgres
    POSTGRES_USER: postgres
    PG_EXPORTER_DSN: "postgresql://postgres:postgres@postgres:5432/postgres?sslmode=require"
  before_script:
    - apt-get update && apt-get install -y postgresql-client
  script:
    - cargo test --test collectors_tests tls
    - cargo test --test tls_metrics_integration
```

## Performance Considerations

- **Certificate reading**: Only happens once per scrape, minimal overhead (~1-2ms)
- **Connection stats**: Queries `pg_stat_ssl` view, very fast (<1ms)
- **No additional connections**: All collectors reuse the existing connection pool
- **Recommended scrape interval**: 30-60 seconds

## Security Notes

1. **Certificate file access**: Only required for local installations
2. **Connection string security**: Use environment variables for DSN
3. **Certificate validation**: Test with `verify-ca` and `verify-full` modes
4. **Certificate rotation**: Metrics automatically reflect new certificates on next scrape

## Helper Scripts Reference

### `start-ssl-postgres.sh`

Starts SSL-enabled PostgreSQL container with proper configuration.

**Options:**
```bash
# Use different port
PG_PORT=5434 ./tests/start-ssl-postgres.sh

# Use different PostgreSQL version
PG_VERSION=15 ./tests/start-ssl-postgres.sh

# Combine options
PG_PORT=5434 PG_VERSION=14 ./tests/start-ssl-postgres.sh
```

### `stop-ssl-postgres.sh`

Stops and removes the SSL-enabled PostgreSQL container.

### `generate-ssl-certs.sh`

Generates self-signed SSL certificates for testing.

### `init-ssl.sh`

Initialization script that runs inside the container to:
- Create pg_stat_statements extension
- Create test tables
- Display SSL configuration

## Additional Resources

- [PostgreSQL SSL Documentation](https://www.postgresql.org/docs/current/ssl-tcp.html)
- [pg_stat_ssl View](https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-SSL-VIEW)
- [OpenSSL Certificate Generation](https://www.openssl.org/docs/man1.1.1/man1/openssl-req.html)
- [Podman Documentation](https://docs.podman.io/)
