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

# Runtime stage
FROM alpine:latest

# Install runtime dependencies
RUN apk add --no-cache \
    ca-certificates \
    postgresql-client

# Create non-root user
RUN addgroup -g 10001 exporter && \
    adduser -D -u 10001 -G exporter exporter

WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/output/pg_exporter /usr/local/bin/pg_exporter

# Make binary executable
RUN chmod +x /usr/local/bin/pg_exporter

# Switch to non-root user
USER exporter

# Default port
EXPOSE 9432

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:9432/health || exit 1

# Default command - using TCP connection
# Override with docker run -e PG_EXPORTER_DSN="postgresql://..."
ENV PG_EXPORTER_DSN="postgresql://postgres:postgres@localhost:5432/postgres"

ENTRYPOINT ["/usr/local/bin/pg_exporter"]
