#!/usr/bin/env bash
# Start a primary + replica PostgreSQL topology using podman or docker.

set -euo pipefail

NETWORK_NAME="${NETWORK_NAME:-pg-exporter-repl-net}"
PRIMARY_CONTAINER="${PRIMARY_CONTAINER:-pg-exporter-repl-primary}"
REPLICA_CONTAINER="${REPLICA_CONTAINER:-pg-exporter-repl-replica}"
PRIMARY_PORT="${PRIMARY_PORT:-55432}"
REPLICA_PORT="${REPLICA_PORT:-55433}"
PG_VERSION="${PG_VERSION:-16}"
CONTAINER_RUNTIME="${CONTAINER_RUNTIME:-}"

info() {
    echo "[INFO] $1"
}

error() {
    echo "[ERROR] $1" >&2
    exit 1
}

detect_runtime() {
    if [ -n "${CONTAINER_RUNTIME}" ]; then
        if ! command -v "${CONTAINER_RUNTIME}" >/dev/null 2>&1; then
            error "requested runtime '${CONTAINER_RUNTIME}' is not installed"
        fi
        echo "${CONTAINER_RUNTIME}"
        return
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

    error "neither podman nor docker is installed"
}

RUNTIME="$(detect_runtime)"
info "Using container runtime: ${RUNTIME}"

wait_ready() {
    local container="$1"
    local attempts=60
    local i=0

    until "${RUNTIME}" exec "${container}" pg_isready -U postgres >/dev/null 2>&1; do
        i=$((i + 1))
        if [ "${i}" -ge "${attempts}" ]; then
            return 1
        fi
        sleep 1
    done
}

if "${RUNTIME}" ps -a --format '{{.Names}}' | grep -q "^${REPLICA_CONTAINER}$"; then
    "${RUNTIME}" stop "${REPLICA_CONTAINER}" >/dev/null 2>&1 || true
    "${RUNTIME}" rm "${REPLICA_CONTAINER}" >/dev/null 2>&1 || true
fi

if "${RUNTIME}" ps -a --format '{{.Names}}' | grep -q "^${PRIMARY_CONTAINER}$"; then
    "${RUNTIME}" stop "${PRIMARY_CONTAINER}" >/dev/null 2>&1 || true
    "${RUNTIME}" rm "${PRIMARY_CONTAINER}" >/dev/null 2>&1 || true
fi

if "${RUNTIME}" network inspect "${NETWORK_NAME}" >/dev/null 2>&1; then
    "${RUNTIME}" network rm "${NETWORK_NAME}" >/dev/null 2>&1 || true
fi

"${RUNTIME}" network create "${NETWORK_NAME}" >/dev/null

info "Starting primary PostgreSQL (${PRIMARY_CONTAINER}) on port ${PRIMARY_PORT}"
"${RUNTIME}" run -d \
    --name "${PRIMARY_CONTAINER}" \
    --network "${NETWORK_NAME}" \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_PASSWORD=postgres \
    -e POSTGRES_DB=postgres \
    -e POSTGRES_HOST_AUTH_METHOD=trust \
    -p "${PRIMARY_PORT}:5432" \
    "postgres:${PG_VERSION}" \
    postgres \
    -c listen_addresses='*' \
    -c wal_level=replica \
    -c max_wal_senders=10 \
    -c max_replication_slots=10 \
    -c hot_standby=on >/dev/null

if ! wait_ready "${PRIMARY_CONTAINER}"; then
    error "primary did not become ready in time"
fi

info "Configuring replication role and pg_hba"
"${RUNTIME}" exec "${PRIMARY_CONTAINER}" psql -U postgres -d postgres -c \
    "DO \$\$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'replicator') THEN CREATE ROLE replicator WITH REPLICATION LOGIN; END IF; END \$\$;" >/dev/null

"${RUNTIME}" exec "${PRIMARY_CONTAINER}" bash -ceu \
    "echo \"host replication replicator 0.0.0.0/0 trust\" >> \"\$PGDATA/pg_hba.conf\""

"${RUNTIME}" exec "${PRIMARY_CONTAINER}" psql -U postgres -d postgres -c \
    "SELECT pg_reload_conf();" >/dev/null

info "Starting replica PostgreSQL (${REPLICA_CONTAINER}) on port ${REPLICA_PORT}"
"${RUNTIME}" run -d \
    --name "${REPLICA_CONTAINER}" \
    --network "${NETWORK_NAME}" \
    --user postgres \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_PASSWORD=postgres \
    -e POSTGRES_DB=postgres \
    -e POSTGRES_HOST_AUTH_METHOD=trust \
    -e PRIMARY_HOST="${PRIMARY_CONTAINER}" \
    -e REPLICATION_USER=replicator \
    -p "${REPLICA_PORT}:5432" \
    "postgres:${PG_VERSION}" \
    bash -ceu '
        DATA_DIR="/tmp/pg-replica-data"
        rm -rf "${DATA_DIR}"
        mkdir -p "${DATA_DIR}"

        until pg_basebackup -h "${PRIMARY_HOST}" -D "${DATA_DIR}" -U "${REPLICATION_USER}" -Fp -Xs -P -R; do
            sleep 1
        done

        chmod 700 "${DATA_DIR}"
        echo "hot_standby = on" >> "${DATA_DIR}/postgresql.conf"
        exec postgres -D "${DATA_DIR}" -c hot_standby=on
    ' >/dev/null

if ! wait_ready "${REPLICA_CONTAINER}"; then
    error "replica did not become ready in time"
fi

IS_RECOVERY=$("${RUNTIME}" exec "${REPLICA_CONTAINER}" psql -U postgres -d postgres -tAc "SELECT pg_is_in_recovery();")
if [ "${IS_RECOVERY}" != "t" ]; then
    error "replica is not in recovery mode"
fi

info "Priming replication stream"
"${RUNTIME}" exec "${PRIMARY_CONTAINER}" psql -U postgres -d postgres -c \
    "CREATE TABLE IF NOT EXISTS podman_replication_smoke(id BIGSERIAL PRIMARY KEY, note TEXT NOT NULL); \
     INSERT INTO podman_replication_smoke(note) VALUES ('ready');" >/dev/null

for _ in $(seq 1 30); do
    ROWS=$("${RUNTIME}" exec "${REPLICA_CONTAINER}" psql -U postgres -d postgres -tAc "SELECT COUNT(*) FROM podman_replication_smoke;") || true
    if [ "${ROWS:-0}" -ge 1 ] 2>/dev/null; then
        info "Replication topology is ready"
        echo "PRIMARY_DSN=postgresql://postgres:postgres@localhost:${PRIMARY_PORT}/postgres"
        echo "REPLICA_DSN=postgresql://postgres:postgres@localhost:${REPLICA_PORT}/postgres"
        exit 0
    fi
    sleep 1
done

error "replica did not replay initial transaction"
