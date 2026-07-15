#!/usr/bin/env bash
set -euo pipefail

DB="pgbench_test"
SCALE="20"
ROUNDS="5"
SAMPLE_MOD="5"
TABLE="pgbench_accounts"
TIMEOUT_SECONDS="180"
POLL_SECONDS="5"
AUTOVACUUM_NAPTIME="5s"
DO_SETUP=1
REQUIRE_SUCCESS=0
DO_CLEANUP=0

ORIGINAL_AUTOVACUUM_NAPTIME=""
STATE_FILE=""

source "$(dirname "${BASH_SOURCE[0]}")/pg-connection.sh"

usage() {
    cat <<'EOF'
Usage: run-autovacuum-workflow.sh [options]

Create dead tuples, lower the local autovacuum trigger for the target table, and
wait for PostgreSQL autovacuum to clean it up without running a manual VACUUM.

Options:
  --db NAME           Target database (default: pgbench_test)
  --scale N           pgbench scale for setup-local-test-db.sh (default: 20)
  --rounds N          Number of update rounds to create dead tuples (default: 5)
  --sample-mod N      Update rows where aid % N = 0 (default: 5)
  --table NAME        Table to pressure (default: pgbench_accounts)
  --timeout N         Max seconds to wait for autovacuum (default: 180)
  --poll N            Poll interval in seconds (default: 5)
  --naptime VALUE     Temporary autovacuum_naptime (default: 5s)
  --no-setup          Reuse an existing pgbench dataset
  --require-success   Exit non-zero unless autovacuum and autoanalyze both run
  --cleanup           Restore state left by an interrupted workflow, then exit
  -h, --help          Show this help
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --db)
            DB="$2"
            shift 2
            ;;
        --scale)
            SCALE="$2"
            shift 2
            ;;
        --rounds)
            ROUNDS="$2"
            shift 2
            ;;
        --sample-mod)
            SAMPLE_MOD="$2"
            shift 2
            ;;
        --table)
            TABLE="$2"
            shift 2
            ;;
        --timeout)
            TIMEOUT_SECONDS="$2"
            shift 2
            ;;
        --poll)
            POLL_SECONDS="$2"
            shift 2
            ;;
        --naptime)
            AUTOVACUUM_NAPTIME="$2"
            shift 2
            ;;
        --no-setup)
            DO_SETUP=0
            shift
            ;;
        --require-success)
            REQUIRE_SUCCESS=1
            shift
            ;;
        --cleanup)
            DO_CLEANUP=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

if ! command -v psql >/dev/null 2>&1; then
    echo "❌ psql not found in PATH"
    exit 1
fi

psql_cmd() {
    local db="$1"
    shift

    pg_connection_psql_cmd "${db}" "$@"
}

if ! psql_cmd postgres -c "SELECT 1" >/dev/null 2>&1; then
    echo "❌ PostgreSQL is not reachable at $(pg_connection_description postgres)"
    echo "Start it first with: just postgres, or set PG_EXPORTER_DSN/PG_HOST/PG_PORT"
    exit 1
fi

state_key=$(printf '%s_%s' "${DB}" "${TABLE}" | tr -c 'A-Za-z0-9_.-' '_')
STATE_FILE="${TMPDIR:-/tmp}/pg_exporter-autovacuum-${state_key}.state"

restore_autovacuum_state() {
    local saved_naptime="${ORIGINAL_AUTOVACUUM_NAPTIME}"
    local restore_failed=0
    local state_exists=0

    if [[ -f "${STATE_FILE}" ]]; then
        state_exists=1
        if [[ -z "${saved_naptime}" ]]; then
            IFS= read -r saved_naptime < "${STATE_FILE}"
        fi
    fi

    if [[ -n "${saved_naptime}" ]]; then
        if ! psql_cmd postgres --set ON_ERROR_STOP=1 \
            -c "ALTER SYSTEM SET autovacuum_naptime = '${saved_naptime}';" >/dev/null; then
            restore_failed=1
        elif ! psql_cmd postgres --set ON_ERROR_STOP=1 \
            -c "SELECT pg_reload_conf();" >/dev/null; then
            restore_failed=1
        fi
    fi

    if [[ "${state_exists}" -eq 1 || -n "${ORIGINAL_AUTOVACUUM_NAPTIME}" ]]; then
        if ! psql_cmd "${DB}" --set ON_ERROR_STOP=1 \
            -c "ALTER TABLE ${TABLE} RESET (autovacuum_vacuum_scale_factor, autovacuum_vacuum_threshold, autovacuum_analyze_scale_factor, autovacuum_analyze_threshold);" >/dev/null; then
            restore_failed=1
        fi
    fi

    if [[ "${restore_failed}" -eq 1 ]]; then
        echo "⚠️  Could not fully restore autovacuum settings; retry with --cleanup" >&2
        return 1
    fi

    rm -f "${STATE_FILE}"
}

cleanup() {
    restore_autovacuum_state || true
}
trap cleanup EXIT

if [[ "${DO_CLEANUP}" -eq 1 ]]; then
    if ! restore_autovacuum_state; then
        trap - EXIT
        exit 1
    fi
    trap - EXIT
    echo "✅ Interrupted autovacuum workflow state restored"
    exit 0
fi

if ! command -v pgbench >/dev/null 2>&1; then
    echo "❌ pgbench not found in PATH"
    echo "Install postgresql-contrib (or equivalent) to use this workflow."
    exit 1
fi

if [[ "${DO_SETUP}" -eq 1 ]]; then
    echo "🔧 Ensuring pgbench dataset exists (scale=${SCALE})..."
    "$(dirname "${BASH_SOURCE[0]}")/setup-local-test-db.sh" --pgbench --pgbench-scale "${SCALE}"
fi

show_vacuum_stats() {
    local title="$1"
    echo
    echo "📊 ${title}"
    psql_cmd "${DB}" --tuples-only --no-align <<SQL
SELECT
    s.relname,
    s.n_live_tup,
    s.n_dead_tup,
    ROUND(
        CASE
            WHEN (cfg.vacuum_threshold + cfg.vacuum_scale_factor * s.n_live_tup) > 0
            THEN s.n_dead_tup::numeric /
                 (cfg.vacuum_threshold + cfg.vacuum_scale_factor * s.n_live_tup)
            ELSE 0
        END,
        3
    ) AS autovacuum_threshold_ratio,
    s.autovacuum_count,
    COALESCE(EXTRACT(EPOCH FROM (now() - s.last_autovacuum))::bigint, 0) AS last_autovacuum_seconds_ago
FROM pg_stat_user_tables s
JOIN pg_class c ON c.oid = s.relid
CROSS JOIN LATERAL (
    SELECT
        COALESCE(
            MAX(option_value::numeric) FILTER (WHERE option_name = 'autovacuum_vacuum_threshold'),
            current_setting('autovacuum_vacuum_threshold')::numeric
        ) AS vacuum_threshold,
        COALESCE(
            MAX(option_value::numeric) FILTER (WHERE option_name = 'autovacuum_vacuum_scale_factor'),
            current_setting('autovacuum_vacuum_scale_factor')::numeric
        ) AS vacuum_scale_factor
    FROM pg_options_to_table(c.reloptions)
) cfg
WHERE s.schemaname = 'public'
ORDER BY s.n_dead_tup DESC
LIMIT 10;
SQL
}

ORIGINAL_AUTOVACUUM_NAPTIME=$(
    psql_cmd postgres --tuples-only --no-align \
        -c "SHOW autovacuum_naptime"
)
printf '%s\n' "${ORIGINAL_AUTOVACUUM_NAPTIME}" > "${STATE_FILE}"

echo "⚙️  Temporarily setting autovacuum_naptime to ${AUTOVACUUM_NAPTIME} (was ${ORIGINAL_AUTOVACUUM_NAPTIME})..."
psql_cmd postgres --set ON_ERROR_STOP=1 \
    -c "ALTER SYSTEM SET autovacuum_naptime = '${AUTOVACUUM_NAPTIME}';" >/dev/null
psql_cmd postgres --set ON_ERROR_STOP=1 \
    -c "SELECT pg_reload_conf();" >/dev/null

echo "⚙️  Lowering autovacuum thresholds for ${DB}.${TABLE}..."
psql_cmd "${DB}" --set ON_ERROR_STOP=1 \
    -c "ALTER TABLE ${TABLE} SET (
            autovacuum_vacuum_scale_factor = 0.0,
            autovacuum_vacuum_threshold = 50,
            autovacuum_analyze_scale_factor = 0.0,
            autovacuum_analyze_threshold = 50
        );" >/dev/null

IFS=$'\t' read -r initial_autovacuum_count initial_autoanalyze_count <<<"$(
    psql_cmd "${DB}" --tuples-only --no-align --field-separator $'\t' \
        -c "SELECT COALESCE(autovacuum_count, 0)::bigint, COALESCE(autoanalyze_count, 0)::bigint FROM pg_stat_user_tables WHERE schemaname = 'public' AND relname = '${TABLE}';"
)"

show_vacuum_stats "Top tables before churn"

echo
echo "🧪 Generating dead tuples in ${DB}.${TABLE} with ${ROUNDS} update rounds..."
for round in $(seq 1 "${ROUNDS}"); do
    echo "  • round ${round}/${ROUNDS}"
    psql_cmd "${DB}" --set ON_ERROR_STOP=1 \
        -c "UPDATE ${TABLE} SET abalance = abalance + 1 WHERE aid % ${SAMPLE_MOD} = 0;" >/dev/null
done

show_vacuum_stats "Top tables after churn"

echo
echo "⏳ Waiting up to ${TIMEOUT_SECONDS}s for autovacuum on ${DB}.${TABLE}..."
echo "   Watch Grafana: Vacuum & Bloat Pressure and Activity panels."

deadline=$(( $(date +%s) + TIMEOUT_SECONDS ))
autovacuum_triggered=0
autoanalyze_triggered=0
maintenance_completed=0

while (( $(date +%s) < deadline )); do
    IFS=$'\t' read -r autovacuum_count autoanalyze_count dead_tuples threshold_ratio last_autovacuum_age last_autoanalyze_age active_vacuums <<<"$(
        psql_cmd "${DB}" --tuples-only --no-align --field-separator $'\t' <<SQL
SELECT
    COALESCE(s.autovacuum_count, 0)::bigint,
    COALESCE(s.autoanalyze_count, 0)::bigint,
    COALESCE(s.n_dead_tup, 0)::bigint,
    ROUND(
        CASE
            WHEN (cfg.vacuum_threshold + cfg.vacuum_scale_factor * s.n_live_tup) > 0
            THEN s.n_dead_tup::numeric /
                 (cfg.vacuum_threshold + cfg.vacuum_scale_factor * s.n_live_tup)
            ELSE 0
        END,
        3
    ),
    COALESCE(EXTRACT(EPOCH FROM (now() - s.last_autovacuum))::bigint, 0),
    COALESCE(EXTRACT(EPOCH FROM (now() - s.last_autoanalyze))::bigint, 0),
    (
        SELECT COUNT(*)::bigint
        FROM pg_stat_progress_vacuum p
        JOIN pg_database d ON d.oid = p.datid
        WHERE d.datname = '${DB}'
    )
FROM pg_stat_user_tables s
JOIN pg_class c ON c.oid = s.relid
CROSS JOIN LATERAL (
    SELECT
        COALESCE(
            MAX(option_value::numeric) FILTER (WHERE option_name = 'autovacuum_vacuum_threshold'),
            current_setting('autovacuum_vacuum_threshold')::numeric
        ) AS vacuum_threshold,
        COALESCE(
            MAX(option_value::numeric) FILTER (WHERE option_name = 'autovacuum_vacuum_scale_factor'),
            current_setting('autovacuum_vacuum_scale_factor')::numeric
        ) AS vacuum_scale_factor
    FROM pg_options_to_table(c.reloptions)
) cfg
WHERE s.schemaname = 'public'
  AND s.relname = '${TABLE}';
SQL
    )"

    printf '  • autovacuum_count=%s autoanalyze_count=%s dead_tuples=%s threshold_ratio=%s last_autovacuum_age=%ss last_autoanalyze_age=%ss active_vacuums=%s\n' \
        "${autovacuum_count}" "${autoanalyze_count}" "${dead_tuples}" "${threshold_ratio}" \
        "${last_autovacuum_age}" "${last_autoanalyze_age}" "${active_vacuums}"

    if (( autovacuum_count > initial_autovacuum_count )); then
        autovacuum_triggered=1
    fi
    if (( autoanalyze_count > initial_autoanalyze_count )); then
        autoanalyze_triggered=1
    fi

    if (( autovacuum_triggered == 1 && autoanalyze_triggered == 1 && dead_tuples == 0 )); then
        maintenance_completed=1
        echo "✅ Autovacuum and autoanalyze ran; ${TABLE} is clean"
        break
    fi

    sleep "${POLL_SECONDS}"
done

if (( autovacuum_triggered == 0 )); then
    echo "⚠️  Timed out waiting for autovacuum_count to increase"
fi
if (( autoanalyze_triggered == 0 )); then
    echo "⚠️  Timed out waiting for autoanalyze_count to increase"
fi

show_vacuum_stats "Top tables after autovacuum wait"

echo
if (( maintenance_completed == 0 && REQUIRE_SUCCESS == 1 )); then
    echo "❌ Automatic maintenance workflow did not complete before the timeout" >&2
    exit 1
fi

echo "✅ Automatic maintenance workflow completed"
