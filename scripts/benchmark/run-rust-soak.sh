#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
DASHBOARD_JSON="${SCRIPT_DIR}/rust-soak-dashboard.json"

BENCH_RUST_SSH="${BENCH_RUST_SSH:-10.246.1.90}"
BENCH_DB_SSH="${BENCH_DB_SSH:-10.246.1.92}"
BENCH_METRICS_SSH="${BENCH_METRICS_SSH:-10.246.1.93}"

DB_NAME="${DB_NAME:-pgbench_test}"
DB_SCALE="${DB_SCALE:-20}"
HOURS=24
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
LOCAL_ARTIFACT_ROOT="${LOCAL_ARTIFACT_ROOT:-bench-artifacts/rust-soak}"
DEPLOY_DASHBOARD=true
CONFIGURE_EXPORTER=true

SSH_OPTS=(
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
  --db NAME                 Database name (default: ${DB_NAME})
  --scale N                 pgbench scale if init is needed (default: ${DB_SCALE})
  --no-dashboard-deploy     Do not copy dashboard to metrics VM
  --no-exporter-config      Do not apply soak collector override on rust VM
  --help                    Show this help
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
        --scale)
            DB_SCALE="$2"
            shift 2
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
    if ! [[ "${DB_SCALE}" =~ ^[0-9]+$ ]] || (( DB_SCALE <= 0 )); then
        err "--scale must be a positive integer"
        exit 1
    fi
    if [[ "${DEPLOY_DASHBOARD}" == true && ! -f "${DASHBOARD_JSON}" ]]; then
        err "Dashboard file not found: ${DASHBOARD_JSON}"
        exit 1
    fi
}

preflight() {
    log "Checking SSH connectivity"
    ssh_run "${BENCH_RUST_SSH}" "echo rust_ok >/dev/null"
    ssh_run "${BENCH_DB_SSH}" "echo db_ok >/dev/null"
    ssh_run "${BENCH_METRICS_SSH}" "echo metrics_ok >/dev/null"

    log "Checking db tooling on ${BENCH_DB_SSH}"
    ssh_run "${BENCH_DB_SSH}" "command -v pgbench >/dev/null && command -v psql >/dev/null"

    log "Checking Prometheus API on ${BENCH_METRICS_SSH}"
    ssh_run "${BENCH_METRICS_SSH}" \
        "curl -fsS http://127.0.0.1:9090/api/v1/query --get --data-urlencode query=up >/dev/null"
}

deploy_dashboard() {
    if [[ "${DEPLOY_DASHBOARD}" != true ]]; then
        return
    fi

    log "Deploying rust soak dashboard to ${BENCH_METRICS_SSH}"
    cat "${DASHBOARD_JSON}" | ssh "${SSH_OPTS[@]}" "${BENCH_METRICS_SSH}" \
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
    --collector.activity \
    --collector.vacuum \
    --collector.database \
    --collector.locks \
    --collector.stat \
    --collector.replication \
    --collector.index \
    --collector.statements \
    --collector.exporter \
    --statements.top-n 25
CFG

    ssh_run "${BENCH_RUST_SSH}" \
        "set -euo pipefail; \
         sudo install -d -m 0755 /etc/systemd/system/pg_exporter.service.d; \
         sudo install -m 0644 /tmp/pg_exporter_soak.conf /etc/systemd/system/pg_exporter.service.d/soak.conf; \
         sudo systemctl daemon-reload; \
         sudo systemctl restart pg_exporter; \
         sleep 2; \
         systemctl is-active pg_exporter; \
         curl -fsS http://127.0.0.1:9432/metrics | \
         awk '/pg_stat_activity_count|pg_stat_user_tables_n_dead_tup|postgres_pg_stat_statements_calls_total|pg_exporter_collector_last_scrape_success/ {print; if (++n == 8) exit} END {if (n == 0) exit 1}'"
}

prepare_db() {
    log "Preparing benchmark database on ${BENCH_DB_SSH} (db=${DB_NAME}, scale=${DB_SCALE})"

    ssh_run "${BENCH_DB_SSH}" \
        "set -euo pipefail; \
         if ! sudo -u postgres psql -Atqc \"SELECT 1 FROM pg_database WHERE datname='${DB_NAME}'\" | grep -q 1; then \
             sudo -u postgres createdb '${DB_NAME}'; \
         fi; \
         sudo -u postgres psql -d '${DB_NAME}' -c \"CREATE EXTENSION IF NOT EXISTS pg_stat_statements;\" >/dev/null; \
         if ! sudo -u postgres psql -d '${DB_NAME}' -Atqc \"SELECT 1 FROM pg_class WHERE relname = 'pgbench_accounts'\" | grep -q 1; then \
             sudo -u postgres pgbench -i -s '${DB_SCALE}' '${DB_NAME}'; \
         fi"
}

write_remote_workload_script() {
    local total_seconds baseline statements locks debt recovery mixed
    total_seconds=$((HOURS * 3600))
    baseline=$((total_seconds * 2 / 24))
    statements=$((total_seconds * 4 / 24))
    locks=$((total_seconds * 4 / 24))
    debt=$((total_seconds * 6 / 24))
    recovery=$((total_seconds * 4 / 24))
    mixed=$((total_seconds * 4 / 24))

    log "Writing phased workload script on ${BENCH_DB_SSH}"

    cat <<EOF | ssh "${SSH_OPTS[@]}" "${BENCH_DB_SSH}" "cat > /tmp/pg_exporter_rust_soak_${RUN_ID}.sh"
#!/usr/bin/env bash
set -euo pipefail

DB_NAME="${DB_NAME}"
RUN_ID="${RUN_ID}"

DUR_BASELINE=${baseline}
DUR_STATEMENTS=${statements}
DUR_LOCKS=${locks}
DUR_DEBT=${debt}
DUR_RECOVERY=${recovery}
DUR_MIXED=${mixed}

log() {
    printf '[%s] [soak:%s] %s\\n' "\$(date -u +%FT%TZ)" "\${RUN_ID}" "\$*"
}

psql_exec() {
    local sql="\$1"
    sudo -u postgres psql -v ON_ERROR_STOP=1 -d "\${DB_NAME}" -c "\${sql}" >/dev/null
}

run_pgbench() {
    local clients="\$1"
    local threads="\$2"
    local duration="\$3"
    local mode="\${4:-}"
    if [[ -n "\${mode}" ]]; then
        sudo -u postgres pgbench -h localhost -p 5432 -U postgres "\${mode}" -c "\${clients}" -j "\${threads}" -T "\${duration}" --progress=60 "\${DB_NAME}"
    else
        sudo -u postgres pgbench -h localhost -p 5432 -U postgres -c "\${clients}" -j "\${threads}" -T "\${duration}" --progress=60 "\${DB_NAME}"
    fi
}

run_heavy_query_loop() {
    local duration="\$1"
    local stop_at=\$((\$(date +%s) + duration))
    while (( \$(date +%s) < stop_at )); do
        sudo -u postgres psql -v ON_ERROR_STOP=1 -d "\${DB_NAME}" -c \\
            "SELECT aid, sum(abalance) FROM pgbench_accounts GROUP BY aid ORDER BY sum(abalance) DESC LIMIT 50;" >/dev/null
    done
}

run_lock_storm() {
    local duration="\$1"
    local stop_at=\$((\$(date +%s) + duration))

    locker() {
        while (( \$(date +%s) < stop_at )); do
            sudo -u postgres psql -v ON_ERROR_STOP=1 -d "\${DB_NAME}" -c \\
                "BEGIN; UPDATE pgbench_accounts SET abalance = abalance + 1 WHERE aid = 1; SELECT pg_sleep(20); ROLLBACK;" >/dev/null
        done
    }

    waiter() {
        while (( \$(date +%s) < stop_at )); do
            sudo -u postgres psql -v ON_ERROR_STOP=0 -d "\${DB_NAME}" -c \\
                "SET lock_timeout='5s'; UPDATE pgbench_accounts SET abalance = abalance + 1 WHERE aid = 1;" >/dev/null 2>&1 || true
            sleep 0.2
        done
    }

    locker &
    local pids=()
    pids+=(\$!)
    for _ in \$(seq 1 6); do
        waiter &
        pids+=(\$!)
    done
    for pid in "\${pids[@]}"; do
        wait "\${pid}" || true
    done
}

phase() {
    local name="\$1"
    local duration="\$2"
    shift 2

    log "PHASE_START name=\${name} duration_sec=\${duration}"
    "\$@"
    log "PHASE_END name=\${name}"
}

baseline_phase() {
    run_pgbench 10 2 "\${DUR_BASELINE}"
}

statements_phase() {
    run_heavy_query_loop "\${DUR_STATEMENTS}" &
    local heavy_pid=\$!
    run_pgbench 25 4 "\${DUR_STATEMENTS}"
    wait "\${heavy_pid}" || true
}

locks_phase() {
    run_lock_storm "\${DUR_LOCKS}" &
    local lock_pid=\$!
    run_pgbench 12 3 "\${DUR_LOCKS}"
    wait "\${lock_pid}" || true
}

vacuum_debt_phase() {
    psql_exec "ALTER TABLE pgbench_accounts SET (autovacuum_enabled = false);"
    run_pgbench 35 6 "\${DUR_DEBT}" "-N"
}

autovac_recovery_phase() {
    psql_exec "ALTER TABLE pgbench_accounts RESET (autovacuum_enabled);"
    psql_exec "ALTER TABLE pgbench_accounts SET (autovacuum_vacuum_scale_factor = 0.001, autovacuum_vacuum_threshold = 50, autovacuum_analyze_scale_factor = 0.001, autovacuum_analyze_threshold = 50);"
    run_pgbench 20 4 "\${DUR_RECOVERY}" "-N"
}

mixed_phase() {
    run_heavy_query_loop "\${DUR_MIXED}" &
    local heavy_pid=\$!
    run_lock_storm "\${DUR_MIXED}" &
    local lock_pid=\$!
    run_pgbench 18 4 "\${DUR_MIXED}"
    wait "\${heavy_pid}" || true
    wait "\${lock_pid}" || true
}

summary() {
    log "SUMMARY top tables by dead tuples"
    sudo -u postgres psql -d "\${DB_NAME}" -Atc \\
        "SELECT relname, n_dead_tup, n_live_tup, round((n_dead_tup::numeric / NULLIF(n_live_tup + n_dead_tup,0)) * 100, 2) AS dead_pct, COALESCE(last_autovacuum::text,'null') FROM pg_stat_user_tables ORDER BY n_dead_tup DESC LIMIT 10;"

    log "SUMMARY top statements by total exec time"
    sudo -u postgres psql -d "\${DB_NAME}" -Atc \\
        "SELECT left(query, 120), calls, round(total_exec_time::numeric, 2) FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT 10;"
}

main() {
    log "Soak start db=\${DB_NAME} baseline=\${DUR_BASELINE}s statements=\${DUR_STATEMENTS}s locks=\${DUR_LOCKS}s debt=\${DUR_DEBT}s recovery=\${DUR_RECOVERY}s mixed=\${DUR_MIXED}s"

    phase baseline "\${DUR_BASELINE}" baseline_phase
    phase statements_pressure "\${DUR_STATEMENTS}" statements_phase
    phase locks_activity "\${DUR_LOCKS}" locks_phase
    phase vacuum_debt_build "\${DUR_DEBT}" vacuum_debt_phase
    phase autovacuum_recovery "\${DUR_RECOVERY}" autovac_recovery_phase
    phase mixed_churn "\${DUR_MIXED}" mixed_phase

    summary
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
INSTANCE="10.246.1.90:9432"
PROM="http://127.0.0.1:9090/api/v1/query"
OUT="/tmp/pg_exporter_rust_soak_${RUN_ID}_prom.csv"
STOP_AT=\$((\$(date +%s) + ${total_seconds}))

query_one() {
    local expr="\$1"
    curl -fsS "\${PROM}" --get --data-urlencode "query=\${expr}" | jq -r '.data.result[0].value[1] // ""'
}

echo "ts,exporter_up,pg_up,rss_bytes,cpu_percent,open_fds,scrape_duration_s,scrape_samples,dead_tup_max,locks_sum,long_query_age_s,autovacuum_ratio_max" > "\${OUT}"

while (( \$(date +%s) < STOP_AT )); do
    ts="\$(date -u +%FT%TZ)"
    exporter_up="\$(query_one "up{job=\"pg_exporter_rust\",instance=\"\${INSTANCE}\"}")"
    pg_up="\$(query_one "pg_up{job=\"pg_exporter_rust\",instance=\"\${INSTANCE}\"}")"
    rss_bytes="\$(query_one "pg_exporter_process_resident_memory_bytes{job=\"pg_exporter_rust\",instance=\"\${INSTANCE}\"}")"
    cpu_percent="\$(query_one "pg_exporter_process_cpu_percent{job=\"pg_exporter_rust\",instance=\"\${INSTANCE}\"}")"
    open_fds="\$(query_one "pg_exporter_process_open_fds{job=\"pg_exporter_rust\",instance=\"\${INSTANCE}\"}")"
    scrape_duration="\$(query_one "scrape_duration_seconds{job=\"pg_exporter_rust\",instance=\"\${INSTANCE}\"}")"
    scrape_samples="\$(query_one "scrape_samples_scraped{job=\"pg_exporter_rust\",instance=\"\${INSTANCE}\"}")"
    dead_tup_max="\$(query_one "max(pg_stat_user_tables_n_dead_tup{job=\"pg_exporter_rust\",instance=\"\${INSTANCE}\"})")"
    locks_sum="\$(query_one "sum(pg_locks_count{job=\"pg_exporter_rust\",instance=\"\${INSTANCE}\"})")"
    long_query_age="\$(query_one "max(pg_stat_activity_oldest_query_age_seconds{job=\"pg_exporter_rust\",instance=\"\${INSTANCE}\"})")"
    autovacuum_ratio_max="\$(query_one "max(pg_stat_user_tables_autovacuum_threshold_ratio{job=\"pg_exporter_rust\",instance=\"\${INSTANCE}\"})")"

    echo "\${ts},\${exporter_up},\${pg_up},\${rss_bytes},\${cpu_percent},\${open_fds},\${scrape_duration},\${scrape_samples},\${dead_tup_max},\${locks_sum},\${long_query_age},\${autovacuum_ratio_max}" >> "\${OUT}"
    sleep 60
done
EOF

    ssh_run "${BENCH_METRICS_SSH}" "chmod 0755 /tmp/pg_exporter_rust_soak_sampler_${RUN_ID}.sh"
}

start_remote_jobs() {
    local workload_pid sampler_pid
    local db_script="/tmp/pg_exporter_rust_soak_${RUN_ID}.sh"
    local db_log="/tmp/pg_exporter_rust_soak_${RUN_ID}.log"
    local db_pid="/tmp/pg_exporter_rust_soak_${RUN_ID}.pid"

    local sampler_script="/tmp/pg_exporter_rust_soak_sampler_${RUN_ID}.sh"
    local sampler_log="/tmp/pg_exporter_rust_soak_sampler_${RUN_ID}.log"
    local sampler_pidfile="/tmp/pg_exporter_rust_soak_sampler_${RUN_ID}.pid"

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
hours=${HOURS}
db_name=${DB_NAME}
db_scale=${DB_SCALE}
bench_rust_ssh=${BENCH_RUST_SSH}
bench_db_ssh=${BENCH_DB_SSH}
bench_metrics_ssh=${BENCH_METRICS_SSH}
db_script=${db_script}
db_log=${db_log}
db_pid_file=${db_pid}
sampler_script=${sampler_script}
sampler_log=${sampler_log}
sampler_pid_file=${sampler_pidfile}
dashboard_url=http://10.246.1.93:3000/d/pg-exp-soak-rust/pg-exporter-rust-soak-24h?orgId=1&from=now-6h&to=now&timezone=browser&refresh=30s
started_at_utc=$(date -u +%FT%TZ)
META

    echo ""
    echo "Run started:"
    echo "  Run ID: ${RUN_ID}"
    echo "  DB workload log: ssh ${BENCH_DB_SSH} 'tail -f ${db_log}'"
    echo "  Sampler log: ssh ${BENCH_METRICS_SSH} 'tail -f ${sampler_log}'"
    echo "  Dashboard: http://10.246.1.93:3000/d/pg-exp-soak-rust/pg-exporter-rust-soak-24h?orgId=1&from=now-6h&to=now&timezone=browser&refresh=30s"
    echo "  Metadata: ${LOCAL_ARTIFACT_ROOT}/${RUN_ID}/run-meta.txt"
    echo ""
}

main() {
    parse_args "$@"
    validate_inputs
    preflight
    deploy_dashboard
    configure_exporter
    prepare_db
    write_remote_workload_script
    write_remote_sampler_script
    start_remote_jobs
}

main "$@"
