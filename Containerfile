# Build stage
FROM rust:1-alpine AS builder

ARG TARGETPLATFORM

# Install build dependencies (rustls only, no OpenSSL needed)
RUN apk add --no-cache musl-dev

WORKDIR /build

# Determine the Rust target based on platform early
RUN RUST_TARGET=""; \
    case "$TARGETPLATFORM" in \
        linux/amd64) RUST_TARGET=x86_64-unknown-linux-musl ;; \
        linux/arm64) RUST_TARGET=aarch64-unknown-linux-musl ;; \
        *) echo "Unsupported platform: $TARGETPLATFORM" && exit 1 ;; \
    esac && \
    echo "RUST_TARGET=$RUST_TARGET" > /tmp/rust_target.env && \
    echo "Building for target: $RUST_TARGET" && \
    if [ "$RUST_TARGET" != "x86_64-unknown-linux-musl" ]; then \
        rustup target add "$RUST_TARGET"; \
    fi

# Copy manifests only (for dependency caching)
COPY Cargo.toml Cargo.lock build.rs ./

# Create dummy main.rs to build dependencies only
RUN mkdir -p src && \
    echo 'fn main() {}' > src/main.rs && \
    . /tmp/rust_target.env && \
    cargo build --release --target "$RUST_TARGET" && \
    rm -rf src

# Copy actual source code
COPY src ./src

# Build the actual application (dependencies are cached)
RUN . /tmp/rust_target.env && \
    cargo build --release --target "$RUST_TARGET" && \
    mkdir -p /build/output && \
    cp "/build/target/$RUST_TARGET/release/pg_exporter" /build/output/

# Runtime stage - using distroless for minimal attack surface
# Distroless images contain only your application and runtime dependencies
# No shell, package manager, or other unnecessary tools
FROM gcr.io/distroless/static-debian12:nonroot

# Copy binary from builder
COPY --from=builder --chmod=755 /build/output/pg_exporter /usr/local/bin/pg_exporter

# Default environment variables
# Note: Override PG_EXPORTER_DSN to point to your actual PostgreSQL instance
ENV PG_EXPORTER_PORT="9432"

# Optional TLS/SSL configuration (uncomment and set as needed):
# ENV PG_EXPORTER_TLS_CERT="/app/certs/cert.pem"
# ENV PG_EXPORTER_TLS_KEY="/app/certs/key.pem"
# ENV PG_EXPORTER_LISTEN="0.0.0.0"

# Optional: Exclude specific databases from monitoring
# ENV PG_EXPORTER_EXCLUDE_DATABASES="template0,template1"

# Default port (can be overridden with PG_EXPORTER_PORT)
EXPOSE 9432

# Note: No built-in HEALTHCHECK in distroless images (no shell/wget/curl)
# Use external health checks instead:
# - Kubernetes: livenessProbe/readinessProbe with httpGet on /health
# - Docker Compose: healthcheck with wget/curl from host
# - Prometheus: Scraping /metrics already monitors availability

ENTRYPOINT ["/usr/local/bin/pg_exporter"]
