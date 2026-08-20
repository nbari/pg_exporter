#!/usr/bin/env bash

set -euo pipefail

BENCH_RUST_SSH="${BENCH_RUST_SSH:-10.246.1.90}"
BENCH_DB_SSH="${BENCH_DB_SSH:-10.246.1.92}"
BENCH_METRICS_SSH="${BENCH_METRICS_SSH:-10.246.1.93}"
BENCH_SSH_CONFIG="${BENCH_SSH_CONFIG:-}"
PROM_JOB="${PROM_JOB:-pg_exporter_rust}"
BENCH_RUST_DB_CLIENT_ADDR="${BENCH_RUST_DB_CLIENT_ADDR:-${BENCH_RUST_SSH}}"
LOCAL_ARTIFACT_ROOT="${LOCAL_ARTIFACT_ROOT:-bench-artifacts/rust-soak}"
RUN_ID=""
FETCH_ARTIFACTS=false

SSH_OPTS=()
if [[ -n "${BENCH_SSH_CONFIG}" ]]; then
    SSH_OPTS+=(-F "${BENCH_SSH_CONFIG}")
fi
SSH_OPTS+=(
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
  --fetch          Copy current logs and CSV files into the local artifact directory
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
        --fetch)
            FETCH_ARTIFACTS=true
            shift
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
    if ! [[ "${RUN_ID}" =~ ^[A-Za-z0-9._-]+$ ]]; then
        err "--run-id may contain only letters, numbers, dots, underscores, and dashes"
        exit 1
    fi
}

fetch_artifacts() {
    local artifact_dir db_prefix sampler_prefix
    artifact_dir="${LOCAL_ARTIFACT_ROOT}/${RUN_ID}"
    db_prefix="/tmp/pg_exporter_rust_soak_${RUN_ID}"
    sampler_prefix="/tmp/pg_exporter_rust_soak_sampler_${RUN_ID}"
    mkdir -p "${artifact_dir}"

    scp "${SSH_OPTS[@]}" "${BENCH_DB_SSH}:${db_prefix}.log" "${artifact_dir}/workload.log"
    scp "${SSH_OPTS[@]}" "${BENCH_DB_SSH}:${db_prefix}_connections.csv" "${artifact_dir}/connections.csv"
    scp "${SSH_OPTS[@]}" "${BENCH_DB_SSH}:${db_prefix}_state.env" "${artifact_dir}/state.env"
    scp "${SSH_OPTS[@]}" "${BENCH_METRICS_SSH}:${sampler_prefix}.log" "${artifact_dir}/sampler.log"
    scp "${SSH_OPTS[@]}" "${BENCH_METRICS_SSH}:${db_prefix}_prom.csv" "${artifact_dir}/prometheus-samples.csv"
    ssh_run "${BENCH_RUST_SSH}" \
        "sudo journalctl -u pg_exporter --since '25 hours ago' --no-pager" > "${artifact_dir}/pg-exporter-journal.log"
    printf 'Artifacts copied to %s\n' "${artifact_dir}"
}

main() {
    parse_args "$@"

    local overall_status=0
    local measurement_profile="legacy_adversarial"
    local run_meta="${LOCAL_ARTIFACT_ROOT}/${RUN_ID}/run-meta.txt"
    if [[ -f "${run_meta}" ]]; then
        measurement_profile=$(sed -n 's/^measurement_profile=//p' "${run_meta}" | head -n 1)
        measurement_profile="${measurement_profile:-legacy_adversarial}"
    fi
    local db_log="/tmp/pg_exporter_rust_soak_${RUN_ID}.log"
    local db_pid="/tmp/pg_exporter_rust_soak_${RUN_ID}.pid"
    local sampler_log="/tmp/pg_exporter_rust_soak_sampler_${RUN_ID}.log"
    local sampler_pid="/tmp/pg_exporter_rust_soak_sampler_${RUN_ID}.pid"
    local sampler_csv="/tmp/pg_exporter_rust_soak_${RUN_ID}_prom.csv"
    local connection_csv="/tmp/pg_exporter_rust_soak_${RUN_ID}_connections.csv"
    local state_file="/tmp/pg_exporter_rust_soak_${RUN_ID}_state.env"

    echo "== Run progress =="
    ssh_run "${BENCH_DB_SSH}" \
        "set -euo pipefail; \
         if [ -f '${state_file}' ]; then \
             cat '${state_file}'; \
             end=\$(awk -F= '\$1 == \"phase_ends_epoch\" {print \$2}' '${state_file}'); \
             now=\$(date +%s); \
             if [ -n \"\${end}\" ] && [ \"\${end}\" -gt \"\${now}\" ]; then echo phase_remaining_seconds=\$((end - now)); else echo phase_remaining_seconds=0; fi; \
         else echo 'state file not found'; fi"

    echo ""
    echo "== Exporter process =="
    ssh_run "${BENCH_RUST_SSH}" \
        "set -euo pipefail; \
         systemctl is-active pg_exporter; \
         /usr/local/bin/pg_exporter --version; \
         pid=\$(systemctl show pg_exporter -p MainPID --value); \
         ps -p \"\${pid}\" -o pid,etime,pcpu,pmem,rss,vsz,nlwp,cmd"

    echo ""
    echo "== DB workload =="
    ssh_run "${BENCH_DB_SSH}" \
        "set -euo pipefail; \
         if [ -f '${db_pid}' ]; then pid=\$(cat '${db_pid}'); echo pid=\${pid}; ps -p \${pid} -o pid,etime,pcpu,pmem,cmd || true; else echo 'pid file not found'; fi; \
         if [ -f '${db_log}' ]; then tail -n 20 '${db_log}'; else echo 'log not found'; fi"

    echo ""
    echo "== Exporter PostgreSQL connection budget =="
    if ! ssh_run "${BENCH_DB_SSH}" \
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
         current=\$(sudo -u postgres psql -d postgres -Atqc \"SELECT count(*)::bigint FROM pg_stat_activity WHERE application_name = 'pg_exporter' AND client_addr = inet '${BENCH_RUST_DB_CLIENT_ADDR}'\"); \
         echo current_connections=\${current}; \
         test \"\${current}\" -le 5"; then
        overall_status=1
    fi

    echo ""
    echo "== Soak lock sessions =="
    if ! ssh_run "${BENCH_DB_SSH}" \
        "set -euo pipefail; \
         lock_count=\$(sudo -u postgres psql -d postgres -Atqc \"SELECT count(*)::bigint FROM pg_stat_activity WHERE application_name IN ('pg_exporter_soak_table_lock_${RUN_ID}', 'pg_exporter_soak_fault_${RUN_ID}')\"); \
         if [ \"\${lock_count}\" -eq 0 ]; then \
             echo none; \
         else \
             sudo -u postgres psql -d postgres -Atqc \"SELECT datname, pid, state, COALESCE(wait_event_type, ''), floor(EXTRACT(EPOCH FROM (clock_timestamp() - xact_start)))::bigint FROM pg_stat_activity WHERE application_name IN ('pg_exporter_soak_table_lock_${RUN_ID}', 'pg_exporter_soak_fault_${RUN_ID}')\"; \
         fi; \
         if [ '${measurement_profile}' = reliable_single_scraper_v2 ]; then test \"\${lock_count}\" -eq 0; fi"; then
        overall_status=1
    fi

    echo ""
    echo "== Metrics sampler =="
    ssh_run "${BENCH_METRICS_SSH}" \
        "set -euo pipefail; \
         if [ -f '${sampler_pid}' ]; then pid=\$(cat '${sampler_pid}'); echo pid=\${pid}; ps -p \${pid} -o pid,etime,pcpu,pmem,cmd || true; else echo 'pid file not found'; fi; \
         if [ -f '${sampler_log}' ]; then tail -n 20 '${sampler_log}'; else echo 'log not found'; fi"

    echo ""
    echo "== Exporter resource trend =="
    if ! ssh_run "${BENCH_METRICS_SSH}" \
        "set -euo pipefail; \
         if [ ! -f '${sampler_csv}' ]; then echo 'sampler CSV not found'; exit 1; fi; \
         awk_status=0; \
         awk -F, 'NR == 1 { next } { \
             rows++; \
             if (\$2 == \"\") exporter_missing++; else if (\$2 + 0 != 1) exporter_down++; \
             if (\$3 == \"\") pg_missing++; else if (\$3 + 0 != 1) pg_down++; \
             if (\$4 != \"\") { \
                 rss_samples++; rss_last=\$4 + 0; rss_sum += \$4; \
                 rss[rss_samples]=rss_last; \
                 if (rss_samples == 1) { rss_first=rss_last; rss_min=rss_last } \
                 if (rss_last < rss_min) rss_min=rss_last; \
                 if (rss_last > rss_max) rss_max=rss_last \
             } \
             if (\$5 != \"\") { cpu_samples++; cpu_sum += \$5; if (\$5 + 0 > cpu_max) cpu_max=\$5 + 0 } \
             if (\$6 != \"\") { \
                 fd_samples++; fd_last=\$6 + 0; \
                 if (fd_samples == 1) fd_first=fd_last; \
                 if (fd_last > fd_max) fd_max=fd_last \
             } \
             if (\$7 != \"\") { scrape_samples++; scrape_sum += \$7; if (\$7 + 0 > scrape_max) scrape_max=\$7 + 0 } \
             if (\$22 != \"\") elapsed=\$22 + 0; \
             if (\$23 != \"\") remaining=\$23 + 0 \
         } END { \
             window = rss_samples < 60 ? rss_samples : 60; \
             for (i = 1; i <= window; i++) rss_first_window_sum += rss[i]; \
             for (i = rss_samples - window + 1; i <= rss_samples; i++) rss_last_window_sum += rss[i]; \
             printf \"samples=%d elapsed_s=%d remaining_s=%d exporter_down=%d exporter_missing=%d pg_down=%d pg_missing=%d\\n\", \
                 rows, elapsed, remaining, exporter_down, exporter_missing, pg_down, pg_missing; \
             printf \"rss_mib first=%.2f last=%.2f delta=%.2f min=%.2f avg=%.2f max=%.2f\\n\", \
                 rss_first / 1048576, rss_last / 1048576, (rss_last - rss_first) / 1048576, rss_min / 1048576, rss_samples ? rss_sum / rss_samples / 1048576 : 0, rss_max / 1048576; \
             printf \"rss_window_mib samples=%d first_avg=%.2f last_avg=%.2f delta=%.2f\\n\", \
                 window, window ? rss_first_window_sum / window / 1048576 : 0, window ? rss_last_window_sum / window / 1048576 : 0, \
                 window ? (rss_last_window_sum - rss_first_window_sum) / window / 1048576 : 0; \
             printf \"cpu_percent avg=%.2f max=%.2f fds first=%d last=%d max=%d\\n\", \
                 cpu_samples ? cpu_sum / cpu_samples : 0, cpu_max, fd_first, fd_last, fd_max; \
             printf \"scrape_duration_s avg=%.4f max=%.4f\\n\", \
                 scrape_samples ? scrape_sum / scrape_samples : 0, scrape_max; \
             exit(rows == 0 || exporter_down > 0 || exporter_missing > 0 || pg_down > 0 || pg_missing > 0) \
         }' '${sampler_csv}' || awk_status=\$?; \
         tail -n 3 '${sampler_csv}'; \
         exit \"\${awk_status}\""; then
        overall_status=1
    fi

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
         curl -fsS http://127.0.0.1:9090/api/v1/query --get --data-urlencode 'query=up{job=\"${PROM_JOB}\"}' | \
         jq -r '.data.result[] | [.metric.instance, .value[1]] | @tsv'"

    echo ""
    echo "== Direct /metrics probe =="
    ssh_run "${BENCH_METRICS_SSH}" \
        "set -euo pipefail; \
         if [ -f '${sampler_csv}' ]; then \
             awk -F, 'NR > 1 && \$13 != \"\" { \
                 samples++; \
                 if (\$15 != 0 || \$13 == \"000\") failures++; \
                 if ((\$14 + 0) > max) max=\$14 + 0; \
                 status[\$13]++ \
             } END { \
                 if (samples == 0) { \
                     print \"disabled during measurement (Prometheus is the sole scraper)\"; \
                     exit \
                 } \
                 printf \"samples=%d timeouts=%d max_duration_s=%.3f statuses=\", samples, failures, max; \
                 for (code in status) printf \"%s:%d \", code, status[code]; \
                 print \"\" \
             }' '${sampler_csv}'; \
             tail -n 5 '${sampler_csv}'; \
         else \
             echo 'sampler CSV not found'; \
         fi"

    if [[ "${FETCH_ARTIFACTS}" == true ]]; then
        echo ""
        echo "== Fetching artifacts =="
        fetch_artifacts
    fi

    return "${overall_status}"
}

main "$@"
