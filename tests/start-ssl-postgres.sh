#!/bin/bash
# Start SSL-enabled PostgreSQL using Podman for testing

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CERT_DIR="${SCRIPT_DIR}/ssl-certs"
CONTAINER_NAME="pg-exporter-ssl-test"
PG_PORT="${PG_PORT:-5433}"
PG_VERSION="${PG_VERSION:-16}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

error() {
    echo -e "${RED}❌ Error: $1${NC}" >&2
    exit 1
}

info() {
    echo -e "${GREEN}ℹ️  $1${NC}"
}

warn() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

# Check if podman is installed
if ! command -v podman &> /dev/null; then
    error "podman is not installed. Please install podman first."
fi

# Check if container already exists
if podman ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    warn "Container ${CONTAINER_NAME} already exists. Removing..."
    podman stop "${CONTAINER_NAME}" 2>/dev/null || true
    podman rm "${CONTAINER_NAME}" 2>/dev/null || true
fi

# Check if certificates exist
if [ ! -f "${CERT_DIR}/server.key" ] || [ ! -f "${CERT_DIR}/server.crt" ]; then
    info "SSL certificates not found. Generating..."
    "${SCRIPT_DIR}/generate-ssl-certs.sh"
fi

# Fix certificate permissions and ownership for container access
# PostgreSQL requires key file to have 0600 permissions and be owned by postgres user
# Use podman unshare to set both permissions and ownership in the user namespace
info "Setting certificate permissions for container access..."
podman unshare sh -c "chown 999:999 '${CERT_DIR}/server.key' '${CERT_DIR}/server.crt' && chmod 600 '${CERT_DIR}/server.key' && chmod 644 '${CERT_DIR}/server.crt'"

info "Starting PostgreSQL ${PG_VERSION} with SSL on port ${PG_PORT}..."

# Start PostgreSQL with SSL
# Note: Removed init-ssl.sh mount due to permission issues with podman unshare
podman run -d \
    --name "${CONTAINER_NAME}" \
    -e POSTGRES_PASSWORD=postgres \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_DB=postgres \
    -p "${PG_PORT}:5432" \
    -v "${CERT_DIR}:/var/lib/postgresql/ssl:Z" \
    "postgres:${PG_VERSION}" \
    postgres \
    -c ssl=on \
    -c ssl_cert_file=/var/lib/postgresql/ssl/server.crt \
    -c ssl_key_file=/var/lib/postgresql/ssl/server.key \
    -c ssl_ciphers='HIGH:MEDIUM:+3DES:!aNULL' \
    -c ssl_prefer_server_ciphers=on \
    -c ssl_min_protocol_version=TLSv1.2 \
    -c log_connections=on

info "Waiting for PostgreSQL to be ready..."
max_attempts=30
attempt=0

while [ $attempt -lt $max_attempts ]; do
    if podman exec "${CONTAINER_NAME}" pg_isready -U postgres >/dev/null 2>&1; then
        info "PostgreSQL is ready!"
        break
    fi
    attempt=$((attempt + 1))
    sleep 1
done

if [ $attempt -eq $max_attempts ]; then
    error "PostgreSQL failed to start within ${max_attempts} seconds"
fi

# Verify SSL is enabled
info "Verifying SSL configuration..."
SSL_STATUS=$(podman exec "${CONTAINER_NAME}" psql -U postgres -t -c "SHOW ssl;" | xargs)

if [ "$SSL_STATUS" = "on" ]; then
    info "✅ SSL is enabled!"
else
    warn "SSL status: ${SSL_STATUS}"
fi

echo ""
info "PostgreSQL SSL Test Container Started Successfully!"
echo ""
echo "Container: ${CONTAINER_NAME}"
echo "Port:      ${PG_PORT}"
echo "Database:  postgres"
echo "User:      postgres"
echo "Password:  postgres"
echo ""
echo "Connection strings:"
echo "  psql:    psql 'postgresql://postgres:postgres@localhost:${PG_PORT}/postgres?sslmode=require'"
echo "  DSN:     postgresql://postgres:postgres@localhost:${PG_PORT}/postgres?sslmode=require"
echo ""
echo "To run tests:"
echo "  PG_EXPORTER_DSN='postgresql://postgres:postgres@localhost:${PG_PORT}/postgres?sslmode=require' cargo test --test collectors_tests tls"
echo ""
echo "To stop:"
echo "  ${SCRIPT_DIR}/stop-ssl-postgres.sh"
echo ""
echo "To view logs:"
echo "  podman logs -f ${CONTAINER_NAME}"
