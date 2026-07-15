#!/usr/bin/env bash
set -euo pipefail

DB="pgbench_test"
SCALE="20"
ROUNDS="5"
SAMPLE_MOD="5"
TABLE="pgbench_accounts"
DO_SETUP=1

source "$(dirname "${BASH_SOURCE[0]}")/pg-connection.sh"

usage() {
    cat <<'EOF'
Usage: run-vacuum-workflow.sh [options]

Create churn in pgbench data, show vacuum-related table stats, and run a manual
VACUUM so the vacuum collector and dashboard have a real workload to observe.

Options:
  --db NAME           Target database (default: pgbench_test)
  --scale N           pgbench scale for setup-local-test-db.sh (default: 20)
  --rounds N          Number of update rounds to create dead tuples (default: 5)
  --sample-mod N      Update rows where aid % N = 0 (default: 5)
  --table NAME        Table to vacuum manually (default: pgbench_accounts)
  --no-setup          Reuse an existing pgbench dataset
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
        --no-setup)
            DO_SETUP=0
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
    local db="$1"
    shift

    pg_connection_psql_cmd "${db}" "$@"
}

if ! psql_cmd postgres -c "SELECT 1" >/dev/null 2>&1; then
    echo "❌ PostgreSQL is not reachable at $(pg_connection_description postgres)"
    echo "Start it first with: just postgres, or set PG_EXPORTER_DSN/PG_HOST/PG_PORT"
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
ORDER BY n_dead_tup DESC,
         (
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
             END
         ) DESC
LIMIT 10;
SQL
}

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
echo "🚀 Running VACUUM (VERBOSE, ANALYZE) on ${DB}.${TABLE}..."
echo "   Keep Grafana open on the Vacuum & Maintenance row and the table-stat panels."
psql_cmd "${DB}" --set ON_ERROR_STOP=1 \
    -c "VACUUM (VERBOSE, ANALYZE) ${TABLE};"

show_vacuum_stats "Top tables after manual vacuum"

echo
echo "✅ Vacuum workflow completed"
