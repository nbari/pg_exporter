#!/usr/bin/env bash
# Stop and remove the replication test topology.

set -euo pipefail

NETWORK_NAME="${NETWORK_NAME:-pg-exporter-repl-net}"
PRIMARY_CONTAINER="${PRIMARY_CONTAINER:-pg-exporter-repl-primary}"
REPLICA_CONTAINER="${REPLICA_CONTAINER:-pg-exporter-repl-replica}"
CONTAINER_RUNTIME="${CONTAINER_RUNTIME:-}"

info() {
    echo "[INFO] $1"
}

detect_runtime() {
    if [ -n "${CONTAINER_RUNTIME}" ]; then
        if command -v "${CONTAINER_RUNTIME}" >/dev/null 2>&1; then
            echo "${CONTAINER_RUNTIME}"
            return
        fi
        echo "[WARN] requested runtime '${CONTAINER_RUNTIME}' is not installed" >&2
        exit 0
    fi

    if [ "${CI:-}" = "true" ] && command -v docker >/dev/null 2>&1; then
        echo "docker"
        return
    fi

    if command -v podman >/dev/null 2>&1; then
        echo "podman"
        return
    fi

    if command -v docker >/dev/null 2>&1; then
        echo "docker"
        return
    fi

    echo "[WARN] neither podman nor docker is installed" >&2
    exit 0
}

RUNTIME="$(detect_runtime)"
info "Using container runtime: ${RUNTIME}"

for container in "${REPLICA_CONTAINER}" "${PRIMARY_CONTAINER}"; do
    if "${RUNTIME}" ps -a --format '{{.Names}}' | grep -q "^${container}$"; then
        info "Stopping ${container}"
        "${RUNTIME}" stop "${container}" >/dev/null 2>&1 || true
        "${RUNTIME}" rm "${container}" >/dev/null 2>&1 || true
    fi
done

if "${RUNTIME}" network inspect "${NETWORK_NAME}" >/dev/null 2>&1; then
    info "Removing network ${NETWORK_NAME}"
    "${RUNTIME}" network rm "${NETWORK_NAME}" >/dev/null 2>&1 || true
fi

info "Replication topology cleanup complete"
