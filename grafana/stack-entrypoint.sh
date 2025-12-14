#!/usr/bin/env bash
set -euo pipefail

TARGET="${EXPORTER_TARGET:-host.docker.internal:9432}"
PROM_CONFIG="/opt/prometheus.yml"

# Render Prometheus config from template with the target
sed "s/__EXPORTER_TARGET__/${TARGET}/g" /opt/prometheus.yml.tmpl >"$PROM_CONFIG"

# Start Prometheus
/opt/prometheus --config.file="$PROM_CONFIG" --storage.tsdb.path=/var/lib/prometheus --web.enable-lifecycle --web.listen-address="0.0.0.0:9090" &
PROM_PID=$!

cleanup() {
    kill "$PROM_PID" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

# Start Grafana (uses base image run.sh)
exec /run.sh
