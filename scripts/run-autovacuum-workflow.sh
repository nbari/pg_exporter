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

ORIGINAL_AUTOVACUUM_NAPTIME=""

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

if ! command -v pgbench >/dev/null 2>&1; then
    echo "❌ pgbench not found in PATH"
    echo "Install postgresql-contrib (or equivalent) to use this workflow."
    exit 1
fi

if ! command -v psql >/dev/null 2>&1; then
    echo "❌ psql not found in PATH"
    exit 1
fi

psql_cmd() {
    psql --no-psqlrc -h localhost -p 5432 -U postgres "$@"
}

if ! psql_cmd -d postgres -c "SELECT 1" >/dev/null 2>&1; then
    echo "❌ PostgreSQL is not reachable on localhost:5432"
    echo "Start it first with: just postgres"
    exit 1
fi

cleanup() {
    if [[ -n "${ORIGINAL_AUTOVACUUM_NAPTIME}" ]]; then
        psql_cmd -d postgres --set ON_ERROR_STOP=1 \
            -c "ALTER SYSTEM SET autovacuum_naptime = '${ORIGINAL_AUTOVACUUM_NAPTIME}';" >/dev/null || true
        psql_cmd -d postgres --set ON_ERROR_STOP=1 \
            -c "SELECT pg_reload_conf();" >/dev/null || true
    fi

    psql_cmd -d "${DB}" --set ON_ERROR_STOP=1 \
        -c "ALTER TABLE ${TABLE} RESET (autovacuum_vacuum_scale_factor, autovacuum_vacuum_threshold, autovacuum_analyze_scale_factor, autovacuum_analyze_threshold);" >/dev/null || true
}
trap cleanup EXIT

echo "🔧 Ensuring pgbench dataset exists (scale=${SCALE})..."
./scripts/setup-local-test-db.sh --pgbench --pgbench-scale "${SCALE}"

show_vacuum_stats() {
    local title="$1"
    echo
    echo "📊 ${title}"
    psql_cmd -d "${DB}" --tuples-only --no-align <<SQL
SELECT
    relname,
    n_live_tup,
    n_dead_tup,
    ROUND(
        CASE
            WHEN (
                current_setting('autovacuum_vacuum_threshold')::numeric +
                current_setting('autovacuum_vacuum_scale_factor')::numeric * n_live_tup
            ) > 0
            THEN n_dead_tup::numeric / (
                current_setting('autovacuum_vacuum_threshold')::numeric +
                current_setting('autovacuum_vacuum_scale_factor')::numeric * n_live_tup
            )
            ELSE 0
        END,
        3
    ) AS autovacuum_threshold_ratio,
    autovacuum_count,
    COALESCE(EXTRACT(EPOCH FROM (now() - last_autovacuum))::bigint, 0) AS last_autovacuum_seconds_ago
FROM pg_stat_user_tables
WHERE schemaname = 'public'
ORDER BY n_dead_tup DESC
LIMIT 10;
SQL
}

ORIGINAL_AUTOVACUUM_NAPTIME=$(
    psql_cmd -d postgres --tuples-only --no-align \
        -c "SHOW autovacuum_naptime"
)

echo "⚙️  Temporarily setting autovacuum_naptime to ${AUTOVACUUM_NAPTIME} (was ${ORIGINAL_AUTOVACUUM_NAPTIME})..."
psql_cmd -d postgres --set ON_ERROR_STOP=1 \
    -c "ALTER SYSTEM SET autovacuum_naptime = '${AUTOVACUUM_NAPTIME}';" >/dev/null
psql_cmd -d postgres --set ON_ERROR_STOP=1 \
    -c "SELECT pg_reload_conf();" >/dev/null

echo "⚙️  Lowering autovacuum thresholds for ${DB}.${TABLE}..."
psql_cmd -d "${DB}" --set ON_ERROR_STOP=1 \
    -c "ALTER TABLE ${TABLE} SET (
            autovacuum_vacuum_scale_factor = 0.0,
            autovacuum_vacuum_threshold = 50,
            autovacuum_analyze_scale_factor = 0.0,
            autovacuum_analyze_threshold = 50
        );" >/dev/null

initial_autovacuum_count=$(
    psql_cmd -d "${DB}" --tuples-only --no-align \
        -c "SELECT COALESCE(autovacuum_count, 0)::bigint FROM pg_stat_user_tables WHERE schemaname = 'public' AND relname = '${TABLE}';"
)

show_vacuum_stats "Top tables before churn"

echo
echo "🧪 Generating dead tuples in ${DB}.${TABLE} with ${ROUNDS} update rounds..."
for round in $(seq 1 "${ROUNDS}"); do
    echo "  • round ${round}/${ROUNDS}"
    psql_cmd -d "${DB}" --set ON_ERROR_STOP=1 \
        -c "UPDATE ${TABLE} SET abalance = abalance + 1 WHERE aid % ${SAMPLE_MOD} = 0;" >/dev/null
done

show_vacuum_stats "Top tables after churn"

echo
echo "⏳ Waiting up to ${TIMEOUT_SECONDS}s for autovacuum on ${DB}.${TABLE}..."
echo "   Watch Grafana: Vacuum & Bloat Pressure and Activity panels."

deadline=$(( $(date +%s) + TIMEOUT_SECONDS ))
autovacuum_triggered=0

while (( $(date +%s) < deadline )); do
    IFS=$'\t' read -r autovacuum_count dead_tuples threshold_ratio last_auto_age active_vacuums <<<"$(
        psql_cmd -d "${DB}" --tuples-only --no-align --field-separator $'\t' <<SQL
SELECT
    COALESCE(s.autovacuum_count, 0)::bigint,
    COALESCE(s.n_dead_tup, 0)::bigint,
    ROUND(
        CASE
            WHEN (
                current_setting('autovacuum_vacuum_threshold')::numeric +
                current_setting('autovacuum_vacuum_scale_factor')::numeric * s.n_live_tup
            ) > 0
            THEN s.n_dead_tup::numeric / (
                current_setting('autovacuum_vacuum_threshold')::numeric +
                current_setting('autovacuum_vacuum_scale_factor')::numeric * s.n_live_tup
            )
            ELSE 0
        END,
        3
    ),
    COALESCE(EXTRACT(EPOCH FROM (now() - s.last_autovacuum))::bigint, 0),
    (
        SELECT COUNT(*)::bigint
        FROM pg_stat_progress_vacuum p
        JOIN pg_database d ON d.oid = p.datid
        WHERE d.datname = '${DB}'
    )
FROM pg_stat_user_tables s
WHERE s.schemaname = 'public'
  AND s.relname = '${TABLE}';
SQL
    )"

    printf '  • autovacuum_count=%s dead_tuples=%s threshold_ratio=%s last_autovacuum_seconds_ago=%s active_vacuums=%s\n' \
        "${autovacuum_count}" "${dead_tuples}" "${threshold_ratio}" "${last_auto_age}" "${active_vacuums}"

    if (( autovacuum_count > initial_autovacuum_count )); then
        autovacuum_triggered=1
    fi

    if (( autovacuum_triggered == 1 )) && (( dead_tuples == 0 )); then
        echo "✅ Autovacuum ran and cleaned ${TABLE}"
        break
    fi

    sleep "${POLL_SECONDS}"
done

if (( autovacuum_triggered == 0 )); then
    echo "⚠️  Timed out waiting for autovacuum_count to increase"
fi

show_vacuum_stats "Top tables after autovacuum wait"

echo
echo "✅ Autovacuum workflow completed"
