# Container Image Improvements - Summary

This document summarizes the container improvements made to pg_exporter that can be applied to mariadb_exporter.

## Changes Made

### 1. Migrated from Alpine to Distroless

**Before:**
```dockerfile
FROM alpine:latest
RUN apk add --no-cache ca-certificates postgresql-client
```

**After:**
```dockerfile
FROM gcr.io/distroless/static-debian12:nonroot
```

**Benefits:**
- Minimal attack surface (no shell, package manager, or utilities)
- Smaller image size (~12.8MB vs ~10MB Alpine, but more secure)
- Non-root by default (UID 65532)
- Better security posture

### 2. Removed Unnecessary Dependencies

**Removed:**
- `postgresql-client` - Not needed since the exporter uses rustls for connections
- Shell utilities (implicitly removed with distroless)

**Kept:**
- `ca-certificates` (included in distroless/static)

### 3. Dynamic Port Configuration

**Added environment variable support:**
```dockerfile
ENV PG_EXPORTER_PORT="9432"
```

This allows users to override the port without rebuilding the image.

### 4. Documented All Environment Variables

```dockerfile
# Default environment variables
# Note: Override PG_EXPORTER_DSN to point to your actual PostgreSQL instance
ENV PG_EXPORTER_PORT="9432"

# Optional TLS/SSL configuration (uncomment and set as needed):
# ENV PG_EXPORTER_TLS_CERT="/app/certs/cert.pem"
# ENV PG_EXPORTER_TLS_KEY="/app/certs/key.pem"
# ENV PG_EXPORTER_LISTEN="0.0.0.0"

# Optional: Exclude specific databases from monitoring
# ENV PG_EXPORTER_EXCLUDE_DATABASES="template0,template1"
```

### 5. Removed Misleading Default DSN

**Before:**
```dockerfile
ENV PG_EXPORTER_DSN="postgresql://postgres:postgres@localhost:5432/postgres"
```

**Problem:** `localhost:5432` doesn't work in container context

**After:**
Removed the default - users must explicitly provide DSN (fail-fast approach)

### 6. Removed Built-in HEALTHCHECK

**Reason:** Distroless has no `wget`/`curl`/`shell`

**Replacement:** Use external health checks:
- Kubernetes: `livenessProbe` with `httpGet`
- Docker Compose: `healthcheck` with external tools
- Prometheus: Already monitors via `/metrics` scraping

**Example for Docker Compose:**
```yaml
healthcheck:
  test: ["CMD", "wget", "--no-verbose", "--tries=1", "--spider", "http://localhost:9432/health"]
  interval: 30s
  timeout: 3s
  retries: 3
```

### 7. Optimized COPY Command

**Before:**
```dockerfile
COPY --from=builder /build/output/pg_exporter /usr/local/bin/pg_exporter
RUN chmod +x /usr/local/bin/pg_exporter
```

**After:**
```dockerfile
COPY --from=builder --chmod=755 /build/output/pg_exporter /usr/local/bin/pg_exporter
```

**Benefits:**
- One less layer
- Cleaner Containerfile

## Complete Containerfile Structure

See the final `Containerfile` in this repository for the complete implementation.

## Testing

Testing was done locally with podman:

```bash
# Build the image
podman build -t pg_exporter:test -f Containerfile .

# Test with default port
podman run -d \
  -e PG_EXPORTER_DSN="postgresql://user@host:5432/postgres" \
  -p 9432:9432 \
  pg_exporter:test

# Test with custom port
podman run -d \
  -e PG_EXPORTER_DSN="postgresql://user@host:5432/postgres" \
  -e PG_EXPORTER_PORT="8080" \
  -p 8080:8080 \
  pg_exporter:test

# Verify health endpoint
curl http://localhost:9432/health

# Verify metrics endpoint
curl http://localhost:9432/metrics
```

## Applying to mariadb_exporter

1. **Identify current base image** - Check if using Alpine or other
2. **Identify runtime dependencies** - Determine what's actually needed (likely just ca-certificates)
3. **Check environment variables** - Document all `MARIADB_EXPORTER_*` variables
4. **Update Containerfile** - Apply the same distroless pattern
5. **Test thoroughly** - Build and test with podman/docker
6. **Update documentation** - Update README with new usage examples

## Key Differences to Consider for mariadb_exporter

- Environment variable names will be `MARIADB_EXPORTER_*` instead of `PG_EXPORTER_*`
- Default port might be different
- DSN format will be MySQL/MariaDB specific
- TLS configuration might differ

## CHANGELOG Entry Template

```markdown
## [X.Y.Z] - YYYY-MM-DD

### Changed
- **Container Image Improvements**
  - Migrated from Alpine to distroless base image (gcr.io/distroless/static-debian12:nonroot)
  - Reduced final image size to ~XX.XMB with improved security posture
  - Removed unnecessary runtime dependencies
  - Added dynamic port support via `MARIADB_EXPORTER_PORT` environment variable
  - Documented all available environment variables in Containerfile
  - Removed misleading default DSN (now requires explicit configuration)
  - Runs as non-root user (UID 65532) by default
  - Removed built-in HEALTHCHECK (use external health checks with orchestrators)
  - Optimized layer caching for faster rebuilds
```

## References

- [Distroless Container Images](https://github.com/GoogleContainerTools/distroless)
- [Docker Best Practices](https://docs.docker.com/develop/dev-best-practices/)
- [pg_exporter Containerfile](./Containerfile)

## Questions & Issues

If you encounter issues while applying these changes to mariadb_exporter:
- Check that the binary is truly static (musl build)
- Verify no dynamic library dependencies with `ldd`
- Test locally before pushing to CI/CD

---

**Created:** 2025-12-01
**Applied to:** pg_exporter v0.9.3
