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
    local sampler_csv="/tmp/pg_exporter_rust_soak_${RUN_ID}_prom.csv"
    local connection_csv="/tmp/pg_exporter_rust_soak_${RUN_ID}_connections.csv"

    echo "== DB workload =="
    ssh_run "${BENCH_DB_SSH}" \
        "set -euo pipefail; \
         if [ -f '${db_pid}' ]; then pid=\$(cat '${db_pid}'); echo pid=\${pid}; ps -p \${pid} -o pid,etime,pcpu,pmem,cmd || true; else echo 'pid file not found'; fi; \
         if [ -f '${db_log}' ]; then tail -n 20 '${db_log}'; else echo 'log not found'; fi"

    echo ""
    echo "== Exporter PostgreSQL connection budget =="
    ssh_run "${BENCH_DB_SSH}" \
        "set -euo pipefail; \
         if [ ! -f '${connection_csv}' ]; then echo 'connection sampler CSV not found'; exit 1; fi; \
         awk -F, 'NR > 1 { \
             samples++; \
             if (\$4 != 0) errors++; \
             if (\$2 + 0 > max) max=\$2 + 0; \
             if (\$2 + 0 > 5) over_budget++ \
         } END { \
             printf \"samples=%d max_connections=%d budget=5 over_budget=%d query_errors=%d\\n\", samples, max, over_budget, errors; \
             exit(samples == 0 || over_budget > 0 || errors > 0) \
         }' '${connection_csv}'; \
         tail -n 5 '${connection_csv}'; \
         current=\$(sudo -u postgres psql -d postgres -Atqc \"SELECT count(*)::bigint FROM pg_stat_activity WHERE application_name = 'pg_exporter'\"); \
         echo current_connections=\${current}; \
         test \"\${current}\" -le 5"

    echo ""
    echo "== ACCESS EXCLUSIVE session =="
    ssh_run "${BENCH_DB_SSH}" \
        "set -euo pipefail; \
         sudo -u postgres psql -d postgres -Atqc \"
             SELECT datname, pid, state, COALESCE(wait_event_type, ''),
                    floor(EXTRACT(EPOCH FROM (clock_timestamp() - xact_start)))::bigint
             FROM pg_stat_activity
             WHERE application_name = 'pg_exporter_soak_table_lock_${RUN_ID}'
         \""

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

    echo ""
    echo "== Direct /metrics probe =="
    ssh_run "${BENCH_METRICS_SSH}" \
        "set -euo pipefail; \
         if [ -f '${sampler_csv}' ]; then \
             awk -F, 'NR > 1 { \
                 samples++; \
                 if (\$15 != 0 || \$13 == \"000\") failures++; \
                 if ((\$14 + 0) > max) max=\$14 + 0; \
                 status[\$13]++ \
             } END { \
                 printf \"samples=%d timeouts=%d max_duration_s=%.3f statuses=\", samples, failures, max; \
                 for (code in status) printf \"%s:%d \", code, status[code]; \
                 print \"\" \
             }' '${sampler_csv}'; \
             tail -n 5 '${sampler_csv}'; \
         else \
             echo 'sampler CSV not found'; \
         fi"
}

main "$@"
