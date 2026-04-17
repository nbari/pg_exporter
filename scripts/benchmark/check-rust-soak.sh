#!/usr/bin/env bash

set -euo pipefail

BENCH_DB_SSH="${BENCH_DB_SSH:-10.246.1.92}"
BENCH_METRICS_SSH="${BENCH_METRICS_SSH:-10.246.1.93}"
RUN_ID=""

SSH_OPTS=(
    -o BatchMode=yes
    -o ConnectTimeout=10
    -o ControlMaster=no
    -o ControlPath=none
)

usage() {
    cat <<USAGE
Check status of a running Rust soak run.

Usage:
  $(basename "$0") --run-id ID

Options:
  --run-id ID      Run id produced by run-rust-soak.sh
  --help           Show this help
USAGE
}

err() {
    printf '[%s] ERROR: %s\n' "$(date -u +%FT%TZ)" "$*" >&2
}

ssh_run() {
    local host="$1"
    shift
    ssh "${SSH_OPTS[@]}" "${host}" "$@"
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
        --run-id)
            RUN_ID="$2"
            shift 2
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            err "Unknown option: $1"
            usage
            exit 1
            ;;
        esac
    done

    if [[ -z "${RUN_ID}" ]]; then
        err "--run-id is required"
        usage
        exit 1
    fi
}

main() {
    parse_args "$@"

    local db_log="/tmp/pg_exporter_rust_soak_${RUN_ID}.log"
    local db_pid="/tmp/pg_exporter_rust_soak_${RUN_ID}.pid"
    local sampler_log="/tmp/pg_exporter_rust_soak_sampler_${RUN_ID}.log"
    local sampler_pid="/tmp/pg_exporter_rust_soak_sampler_${RUN_ID}.pid"

    echo "== DB workload =="
    ssh_run "${BENCH_DB_SSH}" \
        "set -euo pipefail; \
         if [ -f '${db_pid}' ]; then pid=\$(cat '${db_pid}'); echo pid=\${pid}; ps -p \${pid} -o pid,etime,pcpu,pmem,cmd || true; else echo 'pid file not found'; fi; \
         if [ -f '${db_log}' ]; then tail -n 20 '${db_log}'; else echo 'log not found'; fi"

    echo ""
    echo "== Metrics sampler =="
    ssh_run "${BENCH_METRICS_SSH}" \
        "set -euo pipefail; \
         if [ -f '${sampler_pid}' ]; then pid=\$(cat '${sampler_pid}'); echo pid=\${pid}; ps -p \${pid} -o pid,etime,pcpu,pmem,cmd || true; else echo 'pid file not found'; fi; \
         if [ -f '${sampler_log}' ]; then tail -n 20 '${sampler_log}'; else echo 'log not found'; fi"

    echo ""
    echo "== Prometheus health =="
    ssh_run "${BENCH_METRICS_SSH}" \
        "set -euo pipefail; \
         curl -fsS http://127.0.0.1:9090/api/v1/query --get --data-urlencode 'query=up{job=\"pg_exporter_rust\"}' | \
         jq -r '.data.result[] | [.metric.instance, .value[1]] | @tsv'"
}

main "$@"
