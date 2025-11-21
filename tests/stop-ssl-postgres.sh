#!/bin/bash
# Stop SSL-enabled PostgreSQL test container

set -e

CONTAINER_NAME="pg-exporter-ssl-test"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

info() {
    echo -e "${GREEN}ℹ️  $1${NC}"
}

warn() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

# Check if podman is installed
if ! command -v podman &> /dev/null; then
    warn "podman is not installed"
    exit 1
fi

# Check if container exists
if ! podman ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    warn "Container ${CONTAINER_NAME} does not exist"
    exit 0
fi

info "Stopping PostgreSQL SSL test container..."

# Stop and remove container
podman stop "${CONTAINER_NAME}" 2>/dev/null || true
podman rm "${CONTAINER_NAME}" 2>/dev/null || true

# Clean up SSL certificates (owned by subuid after podman unshare)
# This ensures fresh certificates with proper ownership on next run
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CERT_DIR="${SCRIPT_DIR}/ssl-certs"
if [ -d "${CERT_DIR}" ]; then
    info "Cleaning up SSL certificates..."
    # Use podman unshare to remove files owned by subuid
    podman unshare rm -rf "${CERT_DIR}"
fi

if podman ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    info "✅ Container ${CONTAINER_NAME} stopped and removed"
else
    info "✅ Cleanup complete"
fi
