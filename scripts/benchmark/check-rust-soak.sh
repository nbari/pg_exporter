#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
BENCH_RUST_SSH="${BENCH_RUST_SSH:-10.246.1.90}"
BENCH_DB_SSH="${BENCH_DB_SSH:-10.246.1.92}"
BENCH_METRICS_SSH="${BENCH_METRICS_SSH:-10.246.1.93}"
BENCH_SSH_CONFIG="${BENCH_SSH_CONFIG:-}"
BENCH_SSH_USER="${BENCH_SSH_USER:-devops}"
BENCH_SSH_PORT="${BENCH_SSH_PORT:-31025}"
PROM_JOB="${PROM_JOB:-pg_exporter_rust}"
BENCH_RUST_DB_CLIENT_ADDR="${BENCH_RUST_DB_CLIENT_ADDR:-${BENCH_RUST_SSH}}"
LOCAL_ARTIFACT_ROOT="${LOCAL_ARTIFACT_ROOT:-bench-artifacts/rust-soak}"
RUN_ID=""
FETCH_ARTIFACTS=false
FINALIZE=false

SSH_OPTS=()
if [[ -n "${BENCH_SSH_CONFIG}" ]]; then
    SSH_OPTS+=(-F "${BENCH_SSH_CONFIG}")
fi
SSH_OPTS+=(
    -o "User=${BENCH_SSH_USER}"
    -o "Port=${BENCH_SSH_PORT}"
    -o BatchMode=yes
    -o ConnectTimeout=10
    -o ControlMaster=no
    -o ControlPath=none
)

usage() {
    cat <<USAGE
Check status of a running Rust soak run.

Usage:
  $(basename "$0") --run-id ID [--fetch|--finalize]

Options:
  --run-id ID      Run id produced by run-rust-soak.sh
  --fetch          Copy current logs and CSV files into the local artifact directory
  --finalize       Fetch evidence, validate a finished run, and clean runtime state
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
        --finalize)
            FETCH_ARTIFACTS=true
            FINALIZE=true
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
    if ! [[ "${BENCH_SSH_USER}" =~ ^[A-Za-z_][A-Za-z0-9._-]*$ ]]; then
        err "BENCH_SSH_USER contains invalid characters"
        exit 1
    fi
    if ! [[ "${BENCH_SSH_PORT}" =~ ^[0-9]+$ ]] || (( BENCH_SSH_PORT < 1 || BENCH_SSH_PORT > 65535 )); then
        err "BENCH_SSH_PORT must be an integer between 1 and 65535"
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
    local db_name="pgbench_test"
    local expected_version=""
    local eligible_nondefault_databases="0"
    local state_contents=""
    local run_status="unknown"
    if [[ -f "${run_meta}" ]]; then
        measurement_profile=$(sed -n 's/^measurement_profile=//p' "${run_meta}" | head -n 1)
        measurement_profile="${measurement_profile:-legacy_adversarial}"
        db_name=$(sed -n 's/^db_name=//p' "${run_meta}" | head -n 1)
        db_name="${db_name:-pgbench_test}"
        expected_version=$(sed -n 's/^expected_exporter_version=//p' "${run_meta}" | head -n 1)
        eligible_nondefault_databases=$(sed -n 's/^eligible_nondefault_databases=//p' "${run_meta}" | head -n 1)
        eligible_nondefault_databases="${eligible_nondefault_databases:-0}"
    elif [[ "${FINALIZE}" == true ]]; then
        err "Cannot finalize without local run metadata: ${run_meta}"
        exit 1
    fi
    if [[ "${FINALIZE}" == true ]]; then
        mkdir -p "${LOCAL_ARTIFACT_ROOT}/${RUN_ID}"
        exec > >(tee "${LOCAL_ARTIFACT_ROOT}/${RUN_ID}/validation-summary.txt") 2>&1
    fi
    local db_log="/tmp/pg_exporter_rust_soak_${RUN_ID}.log"
    local db_pid="/tmp/pg_exporter_rust_soak_${RUN_ID}.pid"
    local sampler_log="/tmp/pg_exporter_rust_soak_sampler_${RUN_ID}.log"
    local sampler_pid="/tmp/pg_exporter_rust_soak_sampler_${RUN_ID}.pid"
    local sampler_csv="/tmp/pg_exporter_rust_soak_${RUN_ID}_prom.csv"
    local connection_csv="/tmp/pg_exporter_rust_soak_${RUN_ID}_connections.csv"
    local state_file="/tmp/pg_exporter_rust_soak_${RUN_ID}_state.env"

    echo "== Run progress =="
    state_contents=$(ssh_run "${BENCH_DB_SSH}" \
        "set -euo pipefail; \
         if [ -f '${state_file}' ]; then cat '${state_file}'; else echo 'status=missing'; fi")
    printf '%s\n' "${state_contents}"
    run_status=$(printf '%s\n' "${state_contents}" | sed -n 's/^status=//p' | head -n 1)
    run_status="${run_status:-unknown}"
    ssh_run "${BENCH_DB_SSH}" \
        "set -euo pipefail; \
         if [ -f '${state_file}' ]; then \
             end=\$(awk -F= '\$1 == \"phase_ends_epoch\" {print \$2}' '${state_file}'); \
             now=\$(date +%s); \
             if [ -n \"\${end}\" ] && [ \"\${end}\" -gt \"\${now}\" ]; then echo phase_remaining_seconds=\$((end - now)); else echo phase_remaining_seconds=0; fi; \
         else echo phase_remaining_seconds=0; fi"
    if [[ "${run_status}" == failed || "${run_status}" == missing ]]; then
        overall_status=1
    fi
    if [[ "${FINALIZE}" == true && "${run_status}" == running ]]; then
        err "Run ${RUN_ID} is still active; refusing to finalize"
        exit 1
    fi

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
         awk -F, -v strict='${run_status}' 'NR == 1 { next } { \
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
             rss_first_window_avg = window ? rss_first_window_sum / window : 0; \
             rss_last_window_avg = window ? rss_last_window_sum / window : 0; \
             rss_leak = strict == \"complete\" && window > 0 && rss_last_window_avg > rss_first_window_avg * 1.20 && rss_last_window_avg - rss_first_window_avg > 16777216; \
             fd_growth = strict == \"complete\" && fd_samples > 0 && fd_last > fd_first + 2; \
             scrape_slow = strict == \"complete\" && scrape_samples > 0 && scrape_max >= 15; \
             printf \"cpu_percent avg=%.2f max=%.2f fds first=%d last=%d max=%d\\n\", \
                 cpu_samples ? cpu_sum / cpu_samples : 0, cpu_max, fd_first, fd_last, fd_max; \
             printf \"scrape_duration_s avg=%.4f max=%.4f rss_leak=%d fd_growth=%d scrape_slow=%d\\n\", \
                 scrape_samples ? scrape_sum / scrape_samples : 0, scrape_max, rss_leak, fd_growth, scrape_slow; \
             exit(rows == 0 || exporter_down > 0 || exporter_missing > 0 || pg_down > 0 || pg_missing > 0 || rss_leak || fd_growth || scrape_slow) \
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

    if [[ "${measurement_profile}" == multi_database_permit_v3 ]]; then
        echo ""
        echo "== 0.21 multi-database permit signals =="
        local strict_validation=0
        if [[ "${run_status}" == complete || "${run_status}" == failed ]]; then
            strict_validation=1
        fi
        if ! [[ "${eligible_nondefault_databases}" =~ ^[0-9]+$ ]] \
            || (( eligible_nondefault_databases <= 0 )); then
            err "Invalid eligible database count in ${run_meta}: ${eligible_nondefault_databases}"
            overall_status=1
        elif ! ssh "${SSH_OPTS[@]}" "${BENCH_METRICS_SSH}" \
            "SAMPLER_CSV='${sampler_csv}' ELIGIBLE='${eligible_nondefault_databases}' STRICT='${strict_validation}' bash -s" <<'REMOTE_PERMIT_CHECK'
set -euo pipefail

if [[ ! -f "${SAMPLER_CSV}" ]]; then
    echo 'sampler CSV not found'
    exit 1
fi

awk -F, -v eligible="${ELIGIBLE}" -v strict="${STRICT}" '
    function numeric(value) {
        return value ~ /^-?([0-9]+([.][0-9]*)?|[.][0-9]+)([eE][+-]?[0-9]+)?$/
    }
    NR == 1 {
        for (i = 1; i <= NF; i++) column[$i] = i
        required = "exporter_process_start_time_seconds exporter_scrapes_total collector_errors_total collector_aborted_total target_scrape_p95_5m_s"
        split(required, required_names, " ")
        for (i in required_names) if (!(required_names[i] in column)) missing_header++
        split("index stat sequences", collectors, " ")
        for (i = 1; i <= 3; i++) {
            name = collectors[i]
            for (j = 1; j <= 7; j++) {
                if (j == 1) suffix = "mean_duration_5m_s"
                else if (j == 2) suffix = "p95_duration_5m_s"
                else if (j == 3) suffix = "success"
                else if (j == 4) suffix = "permit_wait_mean_5m_s"
                else if (j == 5) suffix = "permit_hold_mean_5m_s"
                else if (j == 6) suffix = "permit_wait_count"
                else suffix = "permit_hold_count"
                if (!((name "_" suffix) in column)) missing_header++
            }
        }
        next
    }
    {
        rows++
        process_start = $(column["exporter_process_start_time_seconds"])
        scrapes = $(column["exporter_scrapes_total"])
        errors = $(column["collector_errors_total"])
        aborted = $(column["collector_aborted_total"])
        target_p95 = $(column["target_scrape_p95_5m_s"])
        if (!numeric(process_start) || !numeric(scrapes) || !numeric(errors) || !numeric(aborted)) {
            core_signal_failures++
        }
        if (rows == 1) {
            first_process_start = process_start
            first_scrapes = scrapes
            first_errors = errors
            first_aborted = aborted
        }
        last_process_start = process_start
        last_scrapes = scrapes
        last_errors = errors
        last_aborted = aborted
        if (numeric(target_p95)) {
            target_p95_samples++
            if (target_p95 + 0 > target_p95_max) target_p95_max = target_p95 + 0
        }
        for (i = 1; i <= 3; i++) {
            name = collectors[i]
            success = $(column[name "_success"])
            wait_mean = $(column[name "_permit_wait_mean_5m_s"])
            hold_mean = $(column[name "_permit_hold_mean_5m_s"])
            wait_count = $(column[name "_permit_wait_count"])
            hold_count = $(column[name "_permit_hold_count"])
            if (!numeric(success) || success + 0 != 1) success_failures[name]++
            if (!numeric(wait_count) || !numeric(hold_count)) count_failures[name]++
            if (numeric(wait_mean) && numeric(hold_mean)) {
                timing_samples[name]++
                wait_sum[name] += wait_mean
                hold_sum[name] += hold_mean
                if (wait_mean + 0 > wait_max[name]) wait_max[name] = wait_mean + 0
                if (hold_mean + 0 > hold_max[name]) hold_max[name] = hold_mean + 0
            }
            if (rows == 1) {
                first_wait[name] = wait_count
                first_hold[name] = hold_count
            }
            last_wait[name] = wait_count
            last_hold[name] = hold_count
        }
    }
    END {
        failed = (missing_header > 0 || rows == 0 || core_signal_failures > 0)
        if (first_process_start != last_process_start) failed = 1
        if (last_errors + 0 != first_errors + 0 || last_aborted + 0 != first_aborted + 0) failed = 1
        scrape_delta = last_scrapes - first_scrapes
        printf "samples=%d eligible_nondefault=%d process_start_changed=%d errors_delta=%.0f aborted_delta=%.0f scrapes_delta=%.0f target_p95_samples=%d target_scrape_p95_max_s=%.4f core_signal_failures=%d\n",
            rows, eligible, first_process_start != last_process_start, last_errors - first_errors,
            last_aborted - first_aborted, scrape_delta, target_p95_samples, target_p95_max,
            core_signal_failures
        if (strict && (target_p95_samples == 0 || target_p95_max >= 12)) failed = 1
        for (i = 1; i <= 3; i++) {
            name = collectors[i]
            wait_delta = last_wait[name] - first_wait[name]
            hold_delta = last_hold[name] - first_hold[name]
            holds_per_scrape = scrape_delta > 0 ? hold_delta / scrape_delta : 0
            printf "%s timing_samples=%d wait_avg_s=%.6f wait_max_s=%.6f hold_avg_s=%.6f hold_max_s=%.6f wait_delta=%.0f hold_delta=%.0f holds_per_scrape=%.2f success_failures=%d count_failures=%d\n",
                name, timing_samples[name], timing_samples[name] ? wait_sum[name] / timing_samples[name] : 0,
                wait_max[name], timing_samples[name] ? hold_sum[name] / timing_samples[name] : 0,
                hold_max[name], wait_delta, hold_delta, holds_per_scrape, success_failures[name],
                count_failures[name]
            if (success_failures[name] > 0 || count_failures[name] > 0) failed = 1
            if (strict && (timing_samples[name] == 0 || wait_delta != hold_delta || scrape_delta <= 0 ||
                holds_per_scrape < eligible * 0.90 || holds_per_scrape > eligible * 1.10)) failed = 1
        }
        exit(failed)
    }
' "${SAMPLER_CSV}"
REMOTE_PERMIT_CHECK
        then
            overall_status=1
        fi
    fi

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

    if [[ "${FINALIZE}" == true ]]; then
        local cleanup_args=(--cleanup-only --run-id "${RUN_ID}" --db "${db_name}")
        if [[ -n "${expected_version}" ]]; then
            cleanup_args+=(--expected-version "${expected_version}")
        fi
        if [[ "${run_status}" != complete ]]; then
            err "Run ended with status=${run_status}; evidence was fetched before cleanup"
            overall_status=1
        fi
        echo ""
        echo "== Final runtime cleanup =="
        if ! "${SCRIPT_DIR}/run-rust-soak.sh" "${cleanup_args[@]}"; then
            overall_status=1
        fi
    fi

    if (( overall_status == 0 )); then
        echo "Soak validation passed"
    else
        err "Soak validation failed"
    fi

    return "${overall_status}"
}

main "$@"
