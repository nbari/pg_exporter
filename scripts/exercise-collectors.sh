#!/usr/bin/env bash
set -euo pipefail

DB="pgbench_test"
SCALE="50"
DURATION="90"
CLIENTS="5"
ROUNDS="5"
SAMPLE_MOD="5"
TABLE="pgbench_accounts"
AUTOVACUUM_TIMEOUT="180"
POLL_SECONDS="5"
AUTOVACUUM_NAPTIME="5s"
DO_SETUP=1
DO_IO_TIMING=1
DO_GROUP_B=1
DO_CLEANUP=0

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/pg-connection.sh"

usage() {
    cat <<'EOF'
Usage: exercise-collectors.sh [options]

Exercise every collector that can be stimulated on a local single-node PostgreSQL:
mixed pgbench traffic, sessions, locks, statements, table/index I/O, SLRU,
sequences, progress views, manual VACUUM, autovacuum, and autoanalyze.

Collectors that require restart-time PostgreSQL features are attempted and
reported as skipped when unavailable.

Options:
  --db NAME                 Target database (default: pgbench_test)
  --scale N                 pgbench scale (default: 50)
  --duration N              Seconds for concurrent load/progress stimuli (default: 90)
  --clients N               Clients for the mixed pgbench workload (default: 5)
  --rounds N                Table-churn rounds for each vacuum phase (default: 5)
  --sample-mod N            Update rows where aid % N = 0 (default: 5)
  --table NAME              Table used by vacuum phases (default: pgbench_accounts)
  --autovacuum-timeout N    Max seconds to wait for auto-maintenance (default: 180)
  --poll N                  Auto-maintenance poll interval (default: 5)
  --naptime VALUE           Temporary autovacuum_naptime (default: 5s)
  --no-setup                Reuse an existing pgbench dataset
  --io-timing               Enable track_io_timing (default)
  --no-io-timing            Do not change track_io_timing
  --group-b                 Attempt prepared-xact/logical-slot stimuli (default)
  --no-group-b              Skip restart-dependent stimuli
  --cleanup                 Remove persistent demo objects, then exit
  -h, --help                Show this help
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --db) DB="$2"; shift 2 ;;
        --scale) SCALE="$2"; shift 2 ;;
        --duration) DURATION="$2"; shift 2 ;;
        --clients) CLIENTS="$2"; shift 2 ;;
        --rounds) ROUNDS="$2"; shift 2 ;;
        --sample-mod) SAMPLE_MOD="$2"; shift 2 ;;
        --table) TABLE="$2"; shift 2 ;;
        --autovacuum-timeout) AUTOVACUUM_TIMEOUT="$2"; shift 2 ;;
        --poll) POLL_SECONDS="$2"; shift 2 ;;
        --naptime) AUTOVACUUM_NAPTIME="$2"; shift 2 ;;
        --no-setup) DO_SETUP=0; shift ;;
        --io-timing) DO_IO_TIMING=1; shift ;;
        --no-io-timing) DO_IO_TIMING=0; shift ;;
        --group-b) DO_GROUP_B=1; shift ;;
        --no-group-b) DO_GROUP_B=0; shift ;;
        --cleanup) DO_CLEANUP=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage >&2; exit 1 ;;
    esac
done

require_positive_integer() {
    local name="$1"
    local value="$2"
    if [[ ! "${value}" =~ ^[1-9][0-9]*$ ]]; then
        echo "❌ ${name} must be a positive integer, got: ${value}" >&2
        exit 1
    fi
}

if [[ ! "${DB}" =~ ^[A-Za-z_][A-Za-z0-9_-]*$ ]]; then
    echo "❌ db must be a simple database name, got: ${DB}" >&2
    exit 1
fi
if [[ ! "${TABLE}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
    echo "❌ table must be an unqualified SQL identifier, got: ${TABLE}" >&2
    exit 1
fi

if [[ "${DO_CLEANUP}" -eq 1 ]]; then
    cleanup_failed=0
    bash "${SCRIPT_DIR}/run-autovacuum-workflow.sh" \
        --db "${DB}" --table "${TABLE}" --cleanup || cleanup_failed=1
    "${SCRIPT_DIR}/exercise-collector-load.sh" \
        --db "${DB}" --cleanup || cleanup_failed=1
    exit "${cleanup_failed}"
fi

for numeric_option in \
    "scale:${SCALE}" \
    "duration:${DURATION}" \
    "clients:${CLIENTS}" \
    "rounds:${ROUNDS}" \
    "sample-mod:${SAMPLE_MOD}" \
    "autovacuum-timeout:${AUTOVACUUM_TIMEOUT}" \
    "poll:${POLL_SECONDS}"; do
    require_positive_integer "${numeric_option%%:*}" "${numeric_option#*:}"
done

if [[ ! "${AUTOVACUUM_NAPTIME}" =~ ^[1-9][0-9]*(ms|s|min)$ ]]; then
    echo "❌ naptime must use PostgreSQL duration syntax such as 500ms, 5s, or 1min" >&2
    exit 1
fi

run_phase() {
    local title="$1"
    shift
    echo
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "▶ ${title}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    "$@"
}

core_args=(
    --db "${DB}"
    --scale "${SCALE}"
    --duration "${DURATION}"
    --clients "${CLIENTS}"
    --no-setup
)
if [[ "${DO_IO_TIMING}" -eq 1 ]]; then
    core_args+=(--io-timing)
fi
if [[ "${DO_GROUP_B}" -eq 1 ]]; then
    core_args+=(--group-b)
fi

for required_command in psql pgbench; do
    if ! command -v "${required_command}" >/dev/null 2>&1; then
       echo "❌ ${required_command} not found in PATH" >&2
       exit 1
    fi
done

if ! pg_connection_psql_cmd postgres -c "SELECT 1" >/dev/null 2>&1; then
    echo "❌ PostgreSQL is not reachable at $(pg_connection_description postgres)" >&2
    echo "Start it first with: just postgres" >&2
    exit 1
fi

if [[ "${DO_SETUP}" -eq 1 ]]; then
    if [[ "${DB}" != "pgbench_test" ]]; then
       echo "❌ automatic setup supports db=pgbench_test; initialize ${DB} yourself and use --no-setup" >&2
       exit 1
    fi

    if ! run_phase "Initialize the pgbench dataset once" \
       "${SCRIPT_DIR}/setup-local-test-db.sh" --pgbench --pgbench-scale "${SCALE}"; then
       echo "⚠️  Dataset setup returned non-zero; verifying the resulting tables directly..."
    fi
fi

if ! pg_connection_psql_cmd "${DB}" -c "SELECT 1 FROM pgbench_accounts LIMIT 1" >/dev/null 2>&1; then
    echo "❌ ${DB}.pgbench_accounts is unavailable; run without --no-setup or initialize it first" >&2
    exit 1
fi

echo "🎯 Full collector exercise: database=${DB} scale=${SCALE}"
echo "   Expected runtime: at least ${DURATION}s, plus vacuum work and up to ${AUTOVACUUM_TIMEOUT}s"

run_phase "Mixed workload, sessions, I/O, SLRU, sequences, and progress views" \
    "${SCRIPT_DIR}/exercise-collector-load.sh" "${core_args[@]}"

run_phase "Manual VACUUM and ANALYZE" \
    bash "${SCRIPT_DIR}/run-vacuum-workflow.sh" \
    --db "${DB}" \
    --scale "${SCALE}" \
    --rounds "${ROUNDS}" \
    --sample-mod "${SAMPLE_MOD}" \
    --table "${TABLE}" \
    --no-setup

run_phase "Automatic VACUUM and ANALYZE" \
    bash "${SCRIPT_DIR}/run-autovacuum-workflow.sh" \
    --db "${DB}" \
    --scale "${SCALE}" \
    --rounds "${ROUNDS}" \
    --sample-mod "${SAMPLE_MOD}" \
    --table "${TABLE}" \
    --timeout "${AUTOVACUUM_TIMEOUT}" \
    --poll "${POLL_SECONDS}" \
    --naptime "${AUTOVACUUM_NAPTIME}" \
    --require-success \
    --no-setup

echo
echo "✅ Full collector exercise completed"
echo "   Optional features reported as skipped require PostgreSQL configuration or a replica."
echo "   Run 'just exercise-collectors --cleanup' to remove persistent demo objects."
