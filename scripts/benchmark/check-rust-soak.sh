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
    echo "== Comparison signals =="
    ssh_run "${BENCH_METRICS_SSH}" \
        "set -euo pipefail; \
         if [ -f '${sampler_csv}' ]; then \
             awk -F, ' \
                 NR == 1 { \
                     new_schema = (\$16 == \"statements_mean_duration_5m_s\" && \$21 == \"db_load1\"); \
                     next \
                 } \
                 new_schema && \$16 != \"\" { \
                     statements_mean_samples++; statements_mean_sum += \$16; \
                     if (\$16 + 0 > statements_mean_max) statements_mean_max = \$16 + 0 \
                 } \
                 new_schema && \$17 != \"\" { \
                     statements_p95_samples++; statements_p95_sum += \$17; \
                     if (\$17 + 0 > statements_p95_max) statements_p95_max = \$17 + 0 \
                 } \
                 new_schema && \$18 != \"\" { \
                     statements_success_samples++; \
                     if (\$18 + 0 != 1) statements_failures++ \
                 } \
                 new_schema && \$19 != \"\" { \
                     cpu_samples++; cpu_sum += \$19; \
                     if (\$19 + 0 > cpu_max) cpu_max = \$19 + 0 \
                 } \
                 new_schema && \$20 != \"\" { \
                     memory_samples++; \
                     if (memory_samples == 1 || \$20 + 0 < memory_min) memory_min = \$20 + 0 \
                 } \
                 new_schema && \$21 != \"\" { \
                     load_samples++; load_sum += \$21; \
                     if (\$21 + 0 > load_max) load_max = \$21 + 0 \
                 } \
                 END { \
                     if (!new_schema) { \
                         print \"comparison signals unavailable (legacy sampler schema)\"; \
                         exit \
                     } \
                     printf \"statements_mean_5m_samples=%d avg_s=%.4f max_s=%.4f\\n\", \
                         statements_mean_samples, statements_mean_samples ? statements_mean_sum / statements_mean_samples : 0, statements_mean_max; \
                     printf \"statements_p95_5m_samples=%d avg_s=%.4f max_s=%.4f success_failures=%d/%d\\n\", \
                         statements_p95_samples, statements_p95_samples ? statements_p95_sum / statements_p95_samples : 0, statements_p95_max, statements_failures, statements_success_samples; \
                     printf \"db_cpu_samples=%d avg_busy_pct=%.2f max_busy_pct=%.2f memory_min_gib=%.2f load1_avg=%.2f load1_max=%.2f\\n\", \
                         cpu_samples, cpu_samples ? 100 * cpu_sum / cpu_samples : 0, 100 * cpu_max, memory_samples ? memory_min / 1073741824 : 0, load_samples ? load_sum / load_samples : 0, load_max \
                 }' '${sampler_csv}'; \
         else \
             echo 'sampler CSV not found'; \
         fi"

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
