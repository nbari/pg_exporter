#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
DASHBOARD_JSON="${SCRIPT_DIR}/rust-soak-dashboard.json"

BENCH_RUST_SSH="${BENCH_RUST_SSH:-10.246.1.90}"
BENCH_DB_SSH="${BENCH_DB_SSH:-10.246.1.92}"
BENCH_METRICS_SSH="${BENCH_METRICS_SSH:-10.246.1.93}"
BENCH_SSH_CONFIG="${BENCH_SSH_CONFIG:-}"
PROM_JOB="${PROM_JOB:-pg_exporter_rust}"
BENCH_RUST_INSTANCE="${BENCH_RUST_INSTANCE:-${BENCH_RUST_SSH}:9432}"
BENCH_DB_NODE_INSTANCE="${BENCH_DB_NODE_INSTANCE:-${BENCH_DB_SSH}:9100}"
BENCH_GRAFANA_URL="${BENCH_GRAFANA_URL:-http://${BENCH_METRICS_SSH}:3000}"
BENCH_RUST_METRICS_URL="${BENCH_RUST_METRICS_URL:-http://${BENCH_RUST_SSH}:9432/metrics}"
BENCH_RUST_DB_CLIENT_ADDR="${BENCH_RUST_DB_CLIENT_ADDR:-${BENCH_RUST_SSH}}"

DB_NAME="${DB_NAME:-pgbench_test}"
DB_COUNT="${DB_COUNT:-1}"
DB_SCALE="${DB_SCALE:-20}"
CALIBRATION_SECONDS="${BENCH_CALIBRATION_SECONDS:-180}"
HOURS=24
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
LOCAL_ARTIFACT_ROOT="${LOCAL_ARTIFACT_ROOT:-bench-artifacts/rust-soak}"
EXPECTED_EXPORTER_VERSION="${EXPECTED_EXPORTER_VERSION:-$(sed -n 's/^version = "\([^"]*\)"/\1/p' "${SCRIPT_DIR}/../../Cargo.toml" | head -n 1)}"
DEPLOY_DASHBOARD=true
CONFIGURE_EXPORTER=true
PREFLIGHT_ONLY=false
CALIBRATED_CLIENTS=""
CALIBRATION_RESULT=""

CALIBRATION_CLIENT_CANDIDATES=(8 6 4 2)
CALIBRATION_DB_CPU_AVG_MAX="0.75"
CALIBRATION_DB_CPU_P95_MAX="0.85"
CALIBRATION_SCRAPE_P95_MAX_SECONDS="12"

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
Run a Rust-only phased soak test (default: 24h) on benchmark VMs.

Usage:
  $(basename "$0") [options]

Options:
  --hours N                 Total soak hours (default: 24)
  --run-id ID               Custom run id (default: UTC timestamp)
  --db NAME                 Database name prefix (default: ${DB_NAME})
  --db-count N              Number of databases to create/benchmark (default: ${DB_COUNT})
  --scale N                 pgbench scale if init is needed (default: ${DB_SCALE})
  --expected-version V      Required remote pg_exporter version (default: ${EXPECTED_EXPORTER_VERSION})
  --preflight-only          Validate hosts, tools, version, and Prometheus, then exit
  --no-dashboard-deploy     Do not copy dashboard to metrics VM
  --no-exporter-config      Do not apply soak collector override on rust VM
  --help                    Show this help

Prometheus label overrides:
  PROM_JOB                  Scrape job (default: ${PROM_JOB})
  BENCH_RUST_INSTANCE       Exporter target label (default: ${BENCH_RUST_INSTANCE})
  BENCH_DB_NODE_INSTANCE    DB node_exporter label (default: ${BENCH_DB_NODE_INSTANCE})
  BENCH_GRAFANA_URL         Browser-visible Grafana base URL (default: ${BENCH_GRAFANA_URL})
  BENCH_RUST_METRICS_URL    Direct probe URL (default: ${BENCH_RUST_METRICS_URL})
  BENCH_RUST_DB_CLIENT_ADDR PostgreSQL client address for the Rust exporter (default: ${BENCH_RUST_DB_CLIENT_ADDR})
  BENCH_SSH_CONFIG          Optional ssh_config path for SSH and SCP
  BENCH_CALIBRATION_SECONDS Seconds per pgbench calibration candidate (default: ${CALIBRATION_SECONDS})
USAGE
}

log() {
    printf '[%s] %s\n' "$(date -u +%FT%TZ)" "$*"
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
        --hours)
            HOURS="$2"
            shift 2
            ;;
        --run-id)
            RUN_ID="$2"
            shift 2
            ;;
        --db)
            DB_NAME="$2"
            shift 2
            ;;
        --db-count)
            DB_COUNT="$2"
            shift 2
            ;;
        --scale)
            DB_SCALE="$2"
            shift 2
            ;;
        --expected-version)
            EXPECTED_EXPORTER_VERSION="$2"
            shift 2
            ;;
        --preflight-only)
            PREFLIGHT_ONLY=true
            shift
            ;;
        --no-dashboard-deploy)
            DEPLOY_DASHBOARD=false
            shift
            ;;
        --no-exporter-config)
            CONFIGURE_EXPORTER=false
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
}

validate_inputs() {
    if ! [[ "${HOURS}" =~ ^[0-9]+$ ]] || (( HOURS <= 0 )); then
        err "--hours must be a positive integer"
        exit 1
    fi
    if ! [[ "${DB_COUNT}" =~ ^[0-9]+$ ]] || (( DB_COUNT <= 0 )); then
        err "--db-count must be a positive integer"
        exit 1
    fi
    if ! [[ "${DB_SCALE}" =~ ^[0-9]+$ ]] || (( DB_SCALE <= 0 )); then
        err "--scale must be a positive integer"
        exit 1
    fi
    if ! [[ "${CALIBRATION_SECONDS}" =~ ^[0-9]+$ ]] || (( CALIBRATION_SECONDS < 60 )); then
        err "BENCH_CALIBRATION_SECONDS must be an integer of at least 60 seconds"
        exit 1
    fi
    if ! [[ "${RUN_ID}" =~ ^[A-Za-z0-9._-]+$ ]]; then
        err "--run-id may contain only letters, numbers, dots, underscores, and dashes"
        exit 1
    fi
    if ! [[ "${DB_NAME}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
        err "--db must be a PostgreSQL identifier containing only letters, numbers, and underscores"
        exit 1
    fi
    if [[ -z "${EXPECTED_EXPORTER_VERSION}" ]]; then
        err "Could not determine the expected exporter version"
        exit 1
    fi
    for label_value in "${PROM_JOB}" "${BENCH_RUST_INSTANCE}" "${BENCH_DB_NODE_INSTANCE}"; do
        if [[ "${label_value}" == *\"* || "${label_value}" == *\'* || "${label_value}" == *$'\n'* ]]; then
            err "Prometheus job and instance labels must not contain quotes or newlines"
            exit 1
        fi
    done
    if [[ "${BENCH_RUST_METRICS_URL}" == *\'* || "${BENCH_RUST_METRICS_URL}" == *$'\n'* ]]; then
        err "BENCH_RUST_METRICS_URL must not contain single quotes or newlines"
        exit 1
    fi
    if ! [[ "${BENCH_RUST_DB_CLIENT_ADDR}" =~ ^[0-9A-Fa-f:.]+$ ]]; then
        err "BENCH_RUST_DB_CLIENT_ADDR must be an IPv4 or IPv6 address"
        exit 1
    fi
    if [[ "${DEPLOY_DASHBOARD}" == true && ! -f "${DASHBOARD_JSON}" ]]; then
        err "Dashboard file not found: ${DASHBOARD_JSON}"
        exit 1
    fi
    if [[ "${DEPLOY_DASHBOARD}" == true ]] && ! command -v jq >/dev/null; then
        err "jq is required locally to prepare the Grafana dashboard"
        exit 1
    fi
}

preflight() {
    local remote_version

    log "Checking SSH connectivity"
    ssh_run "${BENCH_RUST_SSH}" "echo rust_ok >/dev/null"
    ssh_run "${BENCH_DB_SSH}" "echo db_ok >/dev/null"
    ssh_run "${BENCH_METRICS_SSH}" "echo metrics_ok >/dev/null"

    log "Checking db tooling on ${BENCH_DB_SSH}"
    ssh_run "${BENCH_DB_SSH}" "command -v pgbench >/dev/null && command -v psql >/dev/null"

    log "Checking exporter host tooling and release version on ${BENCH_RUST_SSH}"
    ssh_run "${BENCH_RUST_SSH}" \
        "command -v curl >/dev/null && command -v systemctl >/dev/null && test -x /usr/local/bin/pg_exporter"
    remote_version=$(ssh_run "${BENCH_RUST_SSH}" \
        "/usr/local/bin/pg_exporter --version | awk '{print \$2}'")
    remote_version=$(printf '%s' "${remote_version}" | tr -d '\r\n')
    if [[ "${remote_version}" != "${EXPECTED_EXPORTER_VERSION}" ]]; then
        err "Exporter host is running binary version ${remote_version}; expected ${EXPECTED_EXPORTER_VERSION}"
        err "Deploy the release binary before starting the soak"
        exit 1
    fi

    log "Checking Prometheus and Grafana tooling on ${BENCH_METRICS_SSH}"
    ssh_run "${BENCH_METRICS_SSH}" "command -v curl >/dev/null && command -v jq >/dev/null"
    ssh_run "${BENCH_METRICS_SSH}" \
        "curl -fsS http://127.0.0.1:9090/api/v1/query --get --data-urlencode query=up >/dev/null"
    ssh_run "${BENCH_METRICS_SSH}" \
        "set -euo pipefail; \
         curl -fsS http://127.0.0.1:9090/api/v1/query --get \
             --data-urlencode 'query=up{job=\"${PROM_JOB}\",instance=\"${BENCH_RUST_INSTANCE}\"}' | \
             jq -e '.status == \"success\" and (.data.result | length == 1) and .data.result[0].value[1] == \"1\"' >/dev/null; \
         curl -fsS http://127.0.0.1:9090/api/v1/query --get \
             --data-urlencode 'query=node_memory_MemAvailable_bytes{instance=\"${BENCH_DB_NODE_INSTANCE}\"}' | \
             jq -e '.status == \"success\" and (.data.result | length == 1)' >/dev/null"

    log "Preflight passed exporter_version=${remote_version} prometheus_job=${PROM_JOB} exporter_instance=${BENCH_RUST_INSTANCE} db_node_instance=${BENCH_DB_NODE_INSTANCE}"
}

deploy_dashboard() {
    if [[ "${DEPLOY_DASHBOARD}" != true ]]; then
        return
    fi

    log "Deploying rust soak dashboard to ${BENCH_METRICS_SSH}"
    jq --arg instance "${BENCH_RUST_INSTANCE}" --arg job "${PROM_JOB}" \
        --arg db_node_instance "${BENCH_DB_NODE_INSTANCE}" \
        '((.. | strings) |= (gsub("pg_exporter_rust"; $job) | gsub("10\\.246\\.1\\.92:9100"; $db_node_instance))) |
         (.templating.list[] | select(.name == "instance").current) = {text: $instance, value: $instance}' \
        "${DASHBOARD_JSON}" | ssh "${SSH_OPTS[@]}" "${BENCH_METRICS_SSH}" \
        "set -euo pipefail; cat > /tmp/rust-soak-${RUN_ID}.json; \
         sudo install -d -m 0755 /var/lib/grafana/dashboards/pg-exporter-bakeoff; \
         sudo install -m 0644 /tmp/rust-soak-${RUN_ID}.json /var/lib/grafana/dashboards/pg-exporter-bakeoff/rust-soak.json; \
         jq -r '.uid + \"\\t\" + .title' /var/lib/grafana/dashboards/pg-exporter-bakeoff/rust-soak.json"
}

configure_exporter() {
    if [[ "${CONFIGURE_EXPORTER}" != true ]]; then
        return
    fi

    log "Applying soak collector override on ${BENCH_RUST_SSH}"
    cat <<'CFG' | ssh "${SSH_OPTS[@]}" "${BENCH_RUST_SSH}" "cat > /tmp/pg_exporter_soak.conf"
[Service]
ExecStart=
ExecStart=/usr/local/bin/pg_exporter \
    --listen 0.0.0.0 \
    --collector.default \
    --collector.activity \
    --collector.vacuum \
    --collector.database \
    --collector.locks \
    --collector.stat \
    --collector.stat_io \
    --collector.slru \
    --collector.temp \
    --collector.system \
    --collector.replication \
    --collector.index \
    --collector.sequences \
    --collector.statements \
    --collector.exporter \
    --collector.tls \
    --collectors.max-db-concurrency 2 \
    --statements.top-n 25
CFG

    ssh_run "${BENCH_RUST_SSH}" \
        "set -euo pipefail; \
         sudo install -d -m 0755 /etc/systemd/system/pg_exporter.service.d; \
         sudo install -m 0644 /tmp/pg_exporter_soak.conf /etc/systemd/system/pg_exporter.service.d/soak.conf; \
         sudo systemctl daemon-reload; \
         sudo systemctl restart pg_exporter; \
         sleep 2; \
         systemctl is-active pg_exporter"
}

wait_for_prometheus_target() {
    log "Waiting for Prometheus to record a successful ${EXPECTED_EXPORTER_VERSION} scrape"
    ssh_run "${BENCH_METRICS_SSH}" \
        "set -euo pipefail; \
         for attempt in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30; do \
             up_value=\$(curl -fsS http://127.0.0.1:9090/api/v1/query --get --data-urlencode 'query=up{job=\"${PROM_JOB}\",instance=\"${BENCH_RUST_INSTANCE}\"}' | jq -r '.data.result[0].value[1] // \"\"'); \
             build_value=\$(curl -fsS http://127.0.0.1:9090/api/v1/query --get --data-urlencode 'query=pg_exporter_build_info{job=\"${PROM_JOB}\",instance=\"${BENCH_RUST_INSTANCE}\",version=\"${EXPECTED_EXPORTER_VERSION}\"}' | jq -r '.data.result[0].value[1] // \"\"'); \
             rss_value=\$(curl -fsS http://127.0.0.1:9090/api/v1/query --get --data-urlencode 'query=pg_exporter_process_resident_memory_bytes{job=\"${PROM_JOB}\",instance=\"${BENCH_RUST_INSTANCE}\"}' | jq -r '.data.result[0].value[1] // \"\"'); \
             collector_count=\$(curl -fsS http://127.0.0.1:9090/api/v1/query --get --data-urlencode 'query=count(pg_exporter_collector_last_scrape_success{job=\"${PROM_JOB}\",instance=\"${BENCH_RUST_INSTANCE}\"} == 1)' | jq -r '.data.result[0].value[1] // \"\"'); \
             if [ \"\${up_value}\" = 1 ] && [ \"\${build_value}\" = 1 ] && [ -n \"\${rss_value}\" ] && [ \"\${collector_count}\" = 16 ]; then \
                 echo \"Prometheus target ready: up=\${up_value} version=${EXPECTED_EXPORTER_VERSION} rss_bytes=\${rss_value} collectors=\${collector_count}\"; \
                 exit 0; \
             fi; \
             sleep 2; \
         done; \
         echo 'Prometheus did not record a complete exporter scrape within 60 seconds' >&2; \
         exit 1"
}

release_fault_lock() {
    local fault_app="$1"

    ssh_run "${BENCH_DB_SSH}" \
        "sudo -u postgres psql -d postgres -Atqc \"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = '${fault_app}' AND pid <> pg_backend_pid()\" >/dev/null" \
        || true
}

run_lock_recovery_check() {
    local fault_app fault_db lock_pid acquired probe_result current_connections
    fault_app="pg_exporter_soak_fault_${RUN_ID}"
    fault_db="${DB_NAME}"
    if (( DB_COUNT > 1 )); then
        fault_db="${DB_NAME}_1"
    fi

    log "Running isolated ACCESS EXCLUSIVE fault check on ${fault_db}"
    lock_pid=$(ssh_run "${BENCH_DB_SSH}" \
        "set -euo pipefail; \
         nohup sudo -u postgres env PGAPPNAME='${fault_app}' \
             psql -v ON_ERROR_STOP=1 -d '${fault_db}' -c \
             \"BEGIN; LOCK TABLE pg_exporter_soak_lock_target IN ACCESS EXCLUSIVE MODE; SELECT pg_sleep(90); ROLLBACK;\" \
             > '/tmp/pg_exporter_rust_soak_${RUN_ID}_fault.log' 2>&1 < /dev/null & \
         echo \$!")
    lock_pid=$(printf '%s' "${lock_pid}" | tr -d '\r\n')
    if ! [[ "${lock_pid}" =~ ^[0-9]+$ ]]; then
        err "Failed to start isolated lock holder"
        exit 1
    fi

    acquired=false
    for _ in $(seq 1 100); do
        if ssh_run "${BENCH_DB_SSH}" \
            "sudo -u postgres psql -d '${fault_db}' -Atqc \"
                SELECT 1
                FROM pg_locks l
                JOIN pg_class c ON c.oid = l.relation
                JOIN pg_stat_activity a ON a.pid = l.pid
                WHERE c.relname = 'pg_exporter_soak_lock_target'
                  AND l.mode = 'AccessExclusiveLock'
                  AND l.granted
                  AND a.application_name = '${fault_app}'
                LIMIT 1
            \" | grep -q 1"; then
            acquired=true
            break
        fi
        sleep 0.1
    done
    if [[ "${acquired}" != true ]]; then
        release_fault_lock "${fault_app}"
        err "Isolated lock holder did not acquire ACCESS EXCLUSIVE within 10 seconds"
        exit 1
    fi

    if ! probe_result=$(ssh_run "${BENCH_METRICS_SSH}" \
        "set -euo pipefail; \
         body='/tmp/pg_exporter_rust_soak_${RUN_ID}_fault_probe.txt'; \
         for attempt in \$(seq 1 30); do \
             result=\$(curl -sS -o \"\${body}\" --connect-timeout 5 --max-time 20 -w '%{http_code},%{time_total}' '${BENCH_RUST_METRICS_URL}') || result='000,20'; \
             code=\${result%%,*}; \
             if [ \"\${code}\" = 503 ] && grep -q 'another /metrics scrape is already running' \"\${body}\"; then \
                 sleep 2; \
                 continue; \
             fi; \
             case \"\${code}\" in 200|503|504) printf '%s\\n' \"\${result}\"; exit 0 ;; esac; \
             echo \"fault-check scrape returned unexpected HTTP \${code}\" >&2; \
             exit 1; \
         done; \
         echo 'fault-check scrape never acquired the single-scrape gate' >&2; \
         exit 1"); then
        release_fault_lock "${fault_app}"
        err "Isolated lock fault check failed"
        exit 1
    fi

    release_fault_lock "${fault_app}"
    wait_for_prometheus_target

    current_connections=$(ssh_run "${BENCH_DB_SSH}" \
        "sudo -u postgres psql -d postgres -Atqc \"SELECT count(*)::bigint FROM pg_stat_activity WHERE application_name = 'pg_exporter' AND client_addr = inet '${BENCH_RUST_DB_CLIENT_ADDR}'\"")
    current_connections=$(printf '%s' "${current_connections}" | tr -d '\r\n')
    if ! [[ "${current_connections}" =~ ^[0-9]+$ ]] || (( current_connections > 5 )); then
        err "Exporter connection count did not recover within budget after fault check: ${current_connections}"
        exit 1
    fi

    log "Fault check passed response=${probe_result} recovered_collectors=16 connections=${current_connections}/5"
}

prometheus_query_value() {
    local expression="$1"

    ssh_run "${BENCH_METRICS_SSH}" \
        "curl -fsS http://127.0.0.1:9090/api/v1/query --get --data-urlencode 'query=${expression}' | jq -r '.data.result[0].value[1] // \"\"'"
}

calibrate_workload() {
    local clients threads target_db range cpu_avg cpu_p95 scrape_p95 up_min calibration_log
    target_db="${DB_NAME}"
    if (( DB_COUNT > 1 )); then
        target_db="${DB_NAME}_1"
    fi
    range="${CALIBRATION_SECONDS}s"

    log "Calibrating pgbench load (${CALIBRATION_SECONDS}s per candidate, candidates=${CALIBRATION_CLIENT_CANDIDATES[*]})"
    for clients in "${CALIBRATION_CLIENT_CANDIDATES[@]}"; do
        threads="${clients}"
        if (( threads > 4 )); then
            threads=4
        fi
        calibration_log="/tmp/pg_exporter_rust_soak_${RUN_ID}_calibration_${clients}.log"
        log "Calibration candidate clients=${clients} threads=${threads}"
        ssh_run "${BENCH_DB_SSH}" \
            "sudo -u postgres pgbench -h localhost -p 5432 -U postgres -c '${clients}' -j '${threads}' -T '${CALIBRATION_SECONDS}' --progress=60 '${target_db}' > '${calibration_log}' 2>&1"

        cpu_avg=$(prometheus_query_value \
            "avg_over_time((1 - avg(rate(node_cpu_seconds_total{instance=\"${BENCH_DB_NODE_INSTANCE}\",mode=\"idle\"}[1m])))[$range:15s])")
        cpu_p95=$(prometheus_query_value \
            "quantile_over_time(0.95, (1 - avg(rate(node_cpu_seconds_total{instance=\"${BENCH_DB_NODE_INSTANCE}\",mode=\"idle\"}[1m])))[$range:15s])")
        scrape_p95=$(prometheus_query_value \
            "quantile_over_time(0.95, scrape_duration_seconds{job=\"${PROM_JOB}\",instance=\"${BENCH_RUST_INSTANCE}\"}[$range])")
        up_min=$(prometheus_query_value \
            "min_over_time(up{job=\"${PROM_JOB}\",instance=\"${BENCH_RUST_INSTANCE}\"}[$range])")

        log "Calibration result clients=${clients} db_cpu_avg=${cpu_avg:-missing} db_cpu_p95=${cpu_p95:-missing} scrape_p95_s=${scrape_p95:-missing} exporter_up_min=${up_min:-missing}"
        if [[ "${up_min}" = 1 && -n "${cpu_avg}" && -n "${cpu_p95}" && -n "${scrape_p95}" ]] \
            && awk -v value="${cpu_avg}" -v limit="${CALIBRATION_DB_CPU_AVG_MAX}" 'BEGIN { exit !(value <= limit) }' \
            && awk -v value="${cpu_p95}" -v limit="${CALIBRATION_DB_CPU_P95_MAX}" 'BEGIN { exit !(value <= limit) }' \
            && awk -v value="${scrape_p95}" -v limit="${CALIBRATION_SCRAPE_P95_MAX_SECONDS}" 'BEGIN { exit !(value < limit) }'; then
            CALIBRATED_CLIENTS="${clients}"
            CALIBRATION_RESULT="clients=${clients},cpu_avg=${cpu_avg},cpu_p95=${cpu_p95},scrape_p95_s=${scrape_p95},up_min=${up_min}"
            log "Calibration selected clients=${CALIBRATED_CLIENTS}"
            return
        fi
    done

    err "No pgbench calibration candidate kept DB CPU, scrape p95, and exporter availability within bounds"
    exit 1
}

prepare_db() {
    log "Preparing ${DB_COUNT} benchmark database(s) on ${BENCH_DB_SSH} (prefix=${DB_NAME}, scale=${DB_SCALE})"

    ssh_run "${BENCH_DB_SSH}" \
        "set -euo pipefail; \
         for i in \$(seq 1 ${DB_COUNT}); do \
             if [ ${DB_COUNT} -eq 1 ]; then target_db='${DB_NAME}'; else target_db=\"${DB_NAME}_\${i}\"; fi; \
             if ! sudo -u postgres psql -Atqc \"SELECT 1 FROM pg_database WHERE datname='\${target_db}'\" | grep -q 1; then \
                 sudo -u postgres createdb \"\${target_db}\"; \
             fi; \
             sudo -u postgres psql -d \"\${target_db}\" -c \"CREATE EXTENSION IF NOT EXISTS pg_stat_statements;\" >/dev/null; \
             if ! sudo -u postgres psql -d \"\${target_db}\" -Atqc \"SELECT 1 FROM pg_class WHERE relname = 'pgbench_accounts'\" | grep -q 1; then \
                 sudo -u postgres pgbench -i -s '${DB_SCALE}' \"\${target_db}\"; \
             fi; \
             sudo -u postgres psql -v ON_ERROR_STOP=1 -d \"\${target_db}\" -c \
                 \"CREATE TABLE IF NOT EXISTS pg_exporter_soak_lock_target (id bigint PRIMARY KEY);\" >/dev/null; \
         done"
}

write_remote_workload_script() {
    local total_seconds baseline statements locks debt recovery mixed calibrated_threads
    total_seconds=$((HOURS * 3600))
    baseline=$((total_seconds * 2 / 24))
    statements=$((total_seconds * 4 / 24))
    locks=$((total_seconds * 4 / 24))
    debt=$((total_seconds * 6 / 24))
    recovery=$((total_seconds * 4 / 24))
    mixed=$((total_seconds * 4 / 24))
    calibrated_threads="${CALIBRATED_CLIENTS}"
    if (( calibrated_threads > 4 )); then
        calibrated_threads=4
    fi

    log "Writing phased workload script on ${BENCH_DB_SSH}"

    cat <<EOF | ssh "${SSH_OPTS[@]}" "${BENCH_DB_SSH}" "cat > /tmp/pg_exporter_rust_soak_${RUN_ID}.sh"
#!/usr/bin/env bash
set -euo pipefail

DB_NAME="${DB_NAME}"
DB_COUNT="${DB_COUNT}"
RUN_ID="${RUN_ID}"

DUR_BASELINE=${baseline}
DUR_STATEMENTS=${statements}
DUR_LOCKS=${locks}
DUR_DEBT=${debt}
DUR_RECOVERY=${recovery}
DUR_MIXED=${mixed}
BASE_CLIENTS=${CALIBRATED_CLIENTS}
BASE_THREADS=${calibrated_threads}
CONNECTION_MONITOR_PID=""
CONNECTION_OUT="/tmp/pg_exporter_rust_soak_\${RUN_ID}_connections.csv"
STATE_OUT="/tmp/pg_exporter_rust_soak_\${RUN_ID}_state.env"
CONNECTION_BUDGET=5
EXPORTER_DB_CLIENT_ADDR="${BENCH_RUST_DB_CLIENT_ADDR}"

log() {
    printf '[%s] [soak:%s] %s\\n' "\$(date -u +%FT%TZ)" "\${RUN_ID}" "\$*"
}

get_random_db() {
    if [ \${DB_COUNT} -eq 1 ]; then
        echo "\${DB_NAME}"
    else
        local r=\$(( RANDOM % \${DB_COUNT} + 1 ))
        echo "\${DB_NAME}_\${r}"
    fi
}

psql_exec() {
    local sql="\$1"
    for i in \$(seq 1 \${DB_COUNT}); do
        local target_db="\${DB_NAME}"
        if [ \${DB_COUNT} -gt 1 ]; then target_db="\${DB_NAME}_\${i}"; fi
        sudo -u postgres psql -v ON_ERROR_STOP=1 -d "\${target_db}" -c "\${sql}" >/dev/null
    done
}

run_pgbench() {
    local clients="\$1"
    local threads="\$2"
    local duration="\$3"
    local mode="\${4:-}"
    local stop_at=\$((\$(date +%s) + duration))
    
    while (( \$(date +%s) < stop_at )); do
        local target_db=\$(get_random_db)
        local remaining=\$((\$stop_at - \$(date +%s)))
        local slice=60
        if (( remaining < 60 )); then slice=\$remaining; fi
        if (( slice <= 0 )); then break; fi
        
        if [[ -n "\${mode}" ]]; then
            sudo -u postgres pgbench -h localhost -p 5432 -U postgres "\${mode}" -c "\${clients}" -j "\${threads}" -T "\${slice}" --progress=60 "\${target_db}"
        else
            sudo -u postgres pgbench -h localhost -p 5432 -U postgres -c "\${clients}" -j "\${threads}" -T "\${slice}" --progress=60 "\${target_db}"
        fi
    done
}

run_heavy_query_loop() {
    local duration="\$1"
    local stop_at=\$((\$(date +%s) + duration))
    while (( \$(date +%s) < stop_at )); do
        local target_db=\$(get_random_db)
        sudo -u postgres psql -v ON_ERROR_STOP=1 -d "\${target_db}" -c \
            "SELECT aid, sum(abalance) FROM pgbench_accounts GROUP BY aid ORDER BY sum(abalance) DESC LIMIT 50;" >/dev/null
        sleep 2
    done
}

run_lock_storm() {
    local duration="\$1"
    local stop_at=\$((\$(date +%s) + duration))

    while (( \$(date +%s) < stop_at )); do
        local target_db=\$(get_random_db)
        local remaining=\$((\$stop_at - \$(date +%s)))
        local slice=60
        if (( remaining < 60 )); then slice=\$remaining; fi
        if (( slice <= 0 )); then break; fi
        
        local slice_stop=\$((\$(date +%s) + slice))

        locker() {
            while (( \$(date +%s) < slice_stop )); do
                sudo -u postgres psql -v ON_ERROR_STOP=1 -d "\${target_db}" -c \
                    "BEGIN; UPDATE pgbench_accounts SET abalance = abalance + 1 WHERE aid = 1; SELECT pg_sleep(20); ROLLBACK;" >/dev/null
            done
        }

        waiter() {
            while (( \$(date +%s) < slice_stop )); do
                sudo -u postgres psql -v ON_ERROR_STOP=0 -d "\${target_db}" -c \
                    "SET lock_timeout='5s'; UPDATE pgbench_accounts SET abalance = abalance + 1 WHERE aid = 1;" >/dev/null 2>&1 || true
                sleep 0.2
            done
        }

        locker &
        local pids=()
        pids+=(\$!)
        for _ in \$(seq 1 2); do
            waiter &
            pids+=(\$!)
        done
        for pid in "\${pids[@]}"; do
            wait "\${pid}" || true
        done
    done
}

monitor_exporter_connections() {
    echo "ts,exporter_connections,exporter_lock_waiters,query_error" > "\${CONNECTION_OUT}"
    while true; do
        local ts sample
        ts="\$(date -u +%FT%TZ)"
        if sample="\$(sudo -u postgres psql -d postgres -AtF, -v ON_ERROR_STOP=1 -c \
            "SELECT count(*)::bigint,
                    count(*) FILTER (WHERE wait_event_type = 'Lock')::bigint
             FROM pg_stat_activity
             WHERE application_name = 'pg_exporter'
               AND client_addr = inet '\${EXPORTER_DB_CLIENT_ADDR}'")"; then
            echo "\${ts},\${sample},0" >> "\${CONNECTION_OUT}"
        else
            echo "\${ts},,,1" >> "\${CONNECTION_OUT}"
        fi
        sleep 30
    done
}

stop_connection_monitor() {
    if [[ -n "\${CONNECTION_MONITOR_PID}" ]]; then
        kill "\${CONNECTION_MONITOR_PID}" 2>/dev/null || true
        wait "\${CONNECTION_MONITOR_PID}" 2>/dev/null || true
    fi
}

restore_pgbench_reloptions() {
    psql_exec "ALTER TABLE pgbench_accounts RESET (autovacuum_enabled, autovacuum_vacuum_scale_factor, autovacuum_vacuum_threshold, autovacuum_analyze_scale_factor, autovacuum_analyze_threshold);" || true
}

cleanup() {
    local status=\$?
    stop_connection_monitor
    restore_pgbench_reloptions
    if (( status != 0 )); then
        printf 'status=failed\nphase=failed\nphase_started_epoch=%s\nphase_ends_epoch=%s\n' \
            "\$(date +%s)" "\$(date +%s)" > "\${STATE_OUT}"
    fi
    return "\${status}"
}

phase() {
    local name="\$1"
    local duration="\$2"
    local started_at ends_at
    shift 2

    started_at="\$(date +%s)"
    ends_at=\$((started_at + duration))
    printf 'status=running\nphase=%s\nphase_started_epoch=%s\nphase_ends_epoch=%s\n' \
        "\${name}" "\${started_at}" "\${ends_at}" > "\${STATE_OUT}"
    log "PHASE_START name=\${name} duration_sec=\${duration}"
    "\$@"
    log "PHASE_END name=\${name}"
}

baseline_phase() {
    run_pgbench "\${BASE_CLIENTS}" "\${BASE_THREADS}" "\${DUR_BASELINE}"
}

statements_phase() {
    run_heavy_query_loop "\${DUR_STATEMENTS}" &
    local heavy_pid=\$!
    run_pgbench "\${BASE_CLIENTS}" "\${BASE_THREADS}" "\${DUR_STATEMENTS}"
    wait "\${heavy_pid}" || true
}

locks_phase() {
    run_lock_storm "\${DUR_LOCKS}" &
    local lock_pid=\$!
    run_pgbench "\${BASE_CLIENTS}" "\${BASE_THREADS}" "\${DUR_LOCKS}"
    wait "\${lock_pid}" || true
}

vacuum_debt_phase() {
    psql_exec "ALTER TABLE pgbench_accounts SET (autovacuum_enabled = false);"
    run_pgbench "\${BASE_CLIENTS}" "\${BASE_THREADS}" "\${DUR_DEBT}" "-N"
}

autovac_recovery_phase() {
    psql_exec "ALTER TABLE pgbench_accounts RESET (autovacuum_enabled);"
    psql_exec "ALTER TABLE pgbench_accounts SET (autovacuum_vacuum_scale_factor = 0.001, autovacuum_vacuum_threshold = 50, autovacuum_analyze_scale_factor = 0.001, autovacuum_analyze_threshold = 50);"
    run_pgbench "\${BASE_CLIENTS}" "\${BASE_THREADS}" "\${DUR_RECOVERY}" "-N"
}

mixed_phase() {
    run_heavy_query_loop "\${DUR_MIXED}" &
    local heavy_pid=\$!
    run_lock_storm "\${DUR_MIXED}" &
    local lock_pid=\$!
    run_pgbench "\${BASE_CLIENTS}" "\${BASE_THREADS}" "\${DUR_MIXED}"
    wait "\${heavy_pid}" || true
    wait "\${lock_pid}" || true
}

summary() {
    log "SUMMARY top tables by dead tuples (random db)"
    local target_db=\$(get_random_db)
    sudo -u postgres psql -d "\${target_db}" -Atc \\
        "SELECT relname, n_dead_tup, n_live_tup, round((n_dead_tup::numeric / NULLIF(n_live_tup + n_dead_tup,0)) * 100, 2) AS dead_pct, COALESCE(last_autovacuum::text,'null') FROM pg_stat_user_tables ORDER BY n_dead_tup DESC LIMIT 10;"

    log "SUMMARY top statements by total exec time (random db)"
    sudo -u postgres psql -d "\${target_db}" -Atc \\
        "SELECT left(query, 120), calls, round(total_exec_time::numeric, 2) FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT 10;"
}

main() {
    trap cleanup EXIT
    monitor_exporter_connections &
    CONNECTION_MONITOR_PID=\$!

    log "Soak start db=\${DB_NAME} clients=\${BASE_CLIENTS} threads=\${BASE_THREADS} baseline=\${DUR_BASELINE}s statements=\${DUR_STATEMENTS}s locks=\${DUR_LOCKS}s debt=\${DUR_DEBT}s recovery=\${DUR_RECOVERY}s mixed=\${DUR_MIXED}s"

    phase baseline "\${DUR_BASELINE}" baseline_phase
    phase statements_pressure "\${DUR_STATEMENTS}" statements_phase
    phase locks_activity "\${DUR_LOCKS}" locks_phase
    phase vacuum_debt_build "\${DUR_DEBT}" vacuum_debt_phase
    phase autovacuum_recovery "\${DUR_RECOVERY}" autovac_recovery_phase
    phase mixed_churn "\${DUR_MIXED}" mixed_phase

    summary
    local connection_samples peak_connections monitor_errors
    read -r connection_samples peak_connections monitor_errors < <(
        awk -F, 'NR > 1 { samples++; if (\$4 != 0) errors++; if (\$2 + 0 > max) max=\$2 + 0 }
                   END { print samples + 0, max + 0, errors + 0 }' "\${CONNECTION_OUT}"
    )
    log "SUMMARY exporter connection samples=\${connection_samples} peak=\${peak_connections} budget=\${CONNECTION_BUDGET} monitor_errors=\${monitor_errors}"
    if (( connection_samples == 0 || peak_connections > CONNECTION_BUDGET || monitor_errors > 0 )); then
        log "ERROR exporter connection-budget validation failed"
        return 1
    fi
    printf 'status=complete\nphase=complete\nphase_started_epoch=%s\nphase_ends_epoch=%s\n' \
        "\$(date +%s)" "\$(date +%s)" > "\${STATE_OUT}"
    log "Soak finished"
}

main
EOF

    ssh_run "${BENCH_DB_SSH}" "chmod 0755 /tmp/pg_exporter_rust_soak_${RUN_ID}.sh"
}

write_remote_sampler_script() {
    local total_seconds
    total_seconds=$((HOURS * 3600 + 900))

    log "Writing Prometheus sampler script on ${BENCH_METRICS_SSH}"

    cat <<EOF | ssh "${SSH_OPTS[@]}" "${BENCH_METRICS_SSH}" "cat > /tmp/pg_exporter_rust_soak_sampler_${RUN_ID}.sh"
#!/usr/bin/env bash
set -euo pipefail

RUN_ID="${RUN_ID}"
PROM_JOB="${PROM_JOB}"
INSTANCE="${BENCH_RUST_INSTANCE}"
DB_NODE_INSTANCE="${BENCH_DB_NODE_INSTANCE}"
PROM="http://127.0.0.1:9090/api/v1/query"
OUT="/tmp/pg_exporter_rust_soak_${RUN_ID}_prom.csv"
STARTED_AT=\$(date +%s)
WORKLOAD_SECONDS=$((HOURS * 3600))
STOP_AT=\$((STARTED_AT + ${total_seconds}))
SAMPLE_COUNT=0

query_one() {
    local expr="\$1"
    local response
    if ! response="\$(curl -fsS "\${PROM}" --get --data-urlencode "query=\${expr}")"; then
        printf '[%s] Prometheus query failed: %s\n' "\$(date -u +%FT%TZ)" "\${expr}" >&2
        printf '\n'
        return 0
    fi
    printf '%s' "\${response}" | jq -r '.data.result[0].value[1] // ""' 2>/dev/null || printf '\n'
}

echo "ts,exporter_up,pg_up,rss_bytes,cpu_percent,open_fds,scrape_duration_s,scrape_samples,dead_tup_max,locks_sum,long_query_age_s,autovacuum_ratio_max,direct_http_status,direct_scrape_duration_s,direct_curl_rc,statements_mean_duration_5m_s,statements_p95_duration_5m_s,statements_success,db_cpu_busy_ratio_5m,db_memory_available_bytes,db_load1,elapsed_seconds,remaining_seconds" > "\${OUT}"

while (( \$(date +%s) < STOP_AT )); do
    ts="\$(date -u +%FT%TZ)"
    exporter_up="\$(query_one "up{job=\"\${PROM_JOB}\",instance=\"\${INSTANCE}\"}")"
    pg_up="\$(query_one "pg_up{job=\"\${PROM_JOB}\",instance=\"\${INSTANCE}\"}")"
    rss_bytes="\$(query_one "pg_exporter_process_resident_memory_bytes{job=\"\${PROM_JOB}\",instance=\"\${INSTANCE}\"}")"
    cpu_percent="\$(query_one "pg_exporter_process_cpu_percent{job=\"\${PROM_JOB}\",instance=\"\${INSTANCE}\"}")"
    open_fds="\$(query_one "pg_exporter_process_open_fds{job=\"\${PROM_JOB}\",instance=\"\${INSTANCE}\"}")"
    scrape_duration="\$(query_one "scrape_duration_seconds{job=\"\${PROM_JOB}\",instance=\"\${INSTANCE}\"}")"
    scrape_samples="\$(query_one "scrape_samples_scraped{job=\"\${PROM_JOB}\",instance=\"\${INSTANCE}\"}")"
    dead_tup_max="\$(query_one "max(pg_stat_user_tables_n_dead_tup{job=\"\${PROM_JOB}\",instance=\"\${INSTANCE}\"})")"
    locks_sum="\$(query_one "sum(pg_locks_count{job=\"\${PROM_JOB}\",instance=\"\${INSTANCE}\"})")"
    long_query_age="\$(query_one "max(pg_stat_activity_oldest_query_age_seconds{job=\"\${PROM_JOB}\",instance=\"\${INSTANCE}\"})")"
    autovacuum_ratio_max="\$(query_one "max(pg_stat_user_tables_autovacuum_threshold_ratio{job=\"\${PROM_JOB}\",instance=\"\${INSTANCE}\"})")"
    statements_mean_duration="\$(query_one "sum(rate(pg_exporter_collector_scrape_duration_seconds_sum{job=\"\${PROM_JOB}\",instance=\"\${INSTANCE}\",collector=\"statements\"}[5m])) / sum(rate(pg_exporter_collector_scrape_duration_seconds_count{job=\"\${PROM_JOB}\",instance=\"\${INSTANCE}\",collector=\"statements\"}[5m]))")"
    statements_p95_duration="\$(query_one "histogram_quantile(0.95, sum by (le) (rate(pg_exporter_collector_scrape_duration_seconds_bucket{job=\"\${PROM_JOB}\",instance=\"\${INSTANCE}\",collector=\"statements\"}[5m])))")"
    statements_success="\$(query_one "pg_exporter_collector_last_scrape_success{job=\"\${PROM_JOB}\",instance=\"\${INSTANCE}\",collector=\"statements\"}")"
    db_cpu_busy_ratio="\$(query_one "1 - avg(rate(node_cpu_seconds_total{instance=\"\${DB_NODE_INSTANCE}\",mode=\"idle\"}[5m]))")"
    db_memory_available="\$(query_one "node_memory_MemAvailable_bytes{instance=\"\${DB_NODE_INSTANCE}\"}")"
    db_load1="\$(query_one "node_load1{instance=\"\${DB_NODE_INSTANCE}\"}")"

    # Preserve the CSV schema used by older artifacts, but leave the direct-probe
    # columns empty. Prometheus is intentionally the only scraper during the
    # measurement window so the exporter's single-scrape gate measures exporter
    # availability rather than collisions created by the benchmark itself.
    direct_http_status=""
    direct_scrape_duration=""
    direct_curl_rc=""

    now_epoch="\$(date +%s)"
    elapsed_seconds=\$((now_epoch - STARTED_AT))
    remaining_seconds=\$((WORKLOAD_SECONDS - elapsed_seconds))
    if (( remaining_seconds < 0 )); then remaining_seconds=0; fi
    echo "\${ts},\${exporter_up},\${pg_up},\${rss_bytes},\${cpu_percent},\${open_fds},\${scrape_duration},\${scrape_samples},\${dead_tup_max},\${locks_sum},\${long_query_age},\${autovacuum_ratio_max},\${direct_http_status},\${direct_scrape_duration},\${direct_curl_rc},\${statements_mean_duration},\${statements_p95_duration},\${statements_success},\${db_cpu_busy_ratio},\${db_memory_available},\${db_load1},\${elapsed_seconds},\${remaining_seconds}" >> "\${OUT}"
    SAMPLE_COUNT=\$((SAMPLE_COUNT + 1))
    if (( SAMPLE_COUNT % 15 == 0 )); then
        printf '[%s] progress elapsed=%ss remaining=%ss exporter_cpu=%s%% exporter_rss=%sB exporter_fds=%s scrape=%ss\n' \
            "\${ts}" "\${elapsed_seconds}" "\${remaining_seconds}" "\${cpu_percent:-unknown}" \
            "\${rss_bytes:-unknown}" "\${open_fds:-unknown}" "\${scrape_duration:-unknown}"
    fi
    sleep 60
done
EOF

    ssh_run "${BENCH_METRICS_SSH}" "chmod 0755 /tmp/pg_exporter_rust_soak_sampler_${RUN_ID}.sh"
}

start_remote_jobs() {
    local workload_pid sampler_pid source_commit dashboard_url
    local db_script="/tmp/pg_exporter_rust_soak_${RUN_ID}.sh"
    local db_log="/tmp/pg_exporter_rust_soak_${RUN_ID}.log"
    local db_pid="/tmp/pg_exporter_rust_soak_${RUN_ID}.pid"

    local sampler_script="/tmp/pg_exporter_rust_soak_sampler_${RUN_ID}.sh"
    local sampler_log="/tmp/pg_exporter_rust_soak_sampler_${RUN_ID}.log"
    local sampler_pidfile="/tmp/pg_exporter_rust_soak_sampler_${RUN_ID}.pid"
    source_commit=$(git -C "${SCRIPT_DIR}/../.." rev-parse HEAD)
    dashboard_url="${BENCH_GRAFANA_URL}/d/pg-exp-soak-rust/pg-exporter-rust-soak-24h?orgId=1&from=now-6h&to=now&timezone=browser&refresh=30s"

    log "Starting phased workload on ${BENCH_DB_SSH}"
    workload_pid=$(ssh_run "${BENCH_DB_SSH}" \
        "set -euo pipefail; nohup bash '${db_script}' > '${db_log}' 2>&1 < /dev/null & echo \$! | tee '${db_pid}'")
    workload_pid=$(echo "${workload_pid}" | tr -d '\r' | tail -n 1)

    if ! [[ "${workload_pid}" =~ ^[0-9]+$ ]]; then
        err "Failed to start remote workload script"
        exit 1
    fi

    log "Starting Prometheus sampler on ${BENCH_METRICS_SSH}"
    sampler_pid=$(ssh_run "${BENCH_METRICS_SSH}" \
        "set -euo pipefail; nohup bash '${sampler_script}' > '${sampler_log}' 2>&1 < /dev/null & echo \$! | tee '${sampler_pidfile}'")
    sampler_pid=$(echo "${sampler_pid}" | tr -d '\r' | tail -n 1)

    if ! [[ "${sampler_pid}" =~ ^[0-9]+$ ]]; then
        err "Failed to start Prometheus sampler script"
        exit 1
    fi

    log "Started workload pid=${workload_pid}, sampler pid=${sampler_pid}"

    mkdir -p "${LOCAL_ARTIFACT_ROOT}/${RUN_ID}"
    cat > "${LOCAL_ARTIFACT_ROOT}/${RUN_ID}/run-meta.txt" <<META
run_id=${RUN_ID}
measurement_profile=reliable_single_scraper_v2
hours=${HOURS}
db_name=${DB_NAME}
db_count=${DB_COUNT}
db_scale=${DB_SCALE}
bench_rust_ssh=${BENCH_RUST_SSH}
bench_db_ssh=${BENCH_DB_SSH}
bench_metrics_ssh=${BENCH_METRICS_SSH}
prom_job=${PROM_JOB}
bench_rust_instance=${BENCH_RUST_INSTANCE}
bench_db_node_instance=${BENCH_DB_NODE_INSTANCE}
bench_rust_metrics_url=${BENCH_RUST_METRICS_URL}
bench_rust_db_client_addr=${BENCH_RUST_DB_CLIENT_ADDR}
expected_exporter_version=${EXPECTED_EXPORTER_VERSION}
source_commit=${source_commit}
db_script=${db_script}
db_log=${db_log}
db_pid_file=${db_pid}
sampler_script=${sampler_script}
sampler_log=${sampler_log}
sampler_pid_file=${sampler_pidfile}
calibration_seconds=${CALIBRATION_SECONDS}
calibrated_clients=${CALIBRATED_CLIENTS}
calibration_result=${CALIBRATION_RESULT}
fault_check_application_name=pg_exporter_soak_fault_${RUN_ID}
connection_sampler=/tmp/pg_exporter_rust_soak_${RUN_ID}_connections.csv
state_file=/tmp/pg_exporter_rust_soak_${RUN_ID}_state.env
connection_budget=5
dashboard_url=${dashboard_url}
started_at_utc=$(date -u +%FT%TZ)
META

    echo ""
    echo "Run started:"
    echo "  Run ID: ${RUN_ID}"
    echo "  DB workload log: ssh ${BENCH_DB_SSH} 'tail -f ${db_log}'"
    echo "  Sampler log: ssh ${BENCH_METRICS_SSH} 'tail -f ${sampler_log}'"
    echo "  Dashboard: ${dashboard_url}"
    echo "  Metadata: ${LOCAL_ARTIFACT_ROOT}/${RUN_ID}/run-meta.txt"
    echo ""
}

main() {
    parse_args "$@"
    validate_inputs
    preflight
    if [[ "${PREFLIGHT_ONLY}" == true ]]; then
        log "Preflight-only check complete; no remote state was changed"
        return
    fi
    deploy_dashboard
    configure_exporter
    wait_for_prometheus_target
    prepare_db
    run_lock_recovery_check
    calibrate_workload
    write_remote_workload_script
    write_remote_sampler_script
    start_remote_jobs
}

main "$@"
