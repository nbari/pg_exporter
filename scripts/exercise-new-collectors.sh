#!/usr/bin/env bash
set -euo pipefail

# Exercise the 0.16.0 collectors (#25 stat_io, #27 vacuum blockers, #28 sequences,
# #29 slru / session churn / checksum / slot spill / progress views) so their
# Grafana panels light up with real data.
#
# Group A stimuli work against a stock local PostgreSQL. Group B (prepared xacts,
# logical replication slots) needs server settings that require a restart; the
# script detects whether they are available and either runs them or prints the
# exact steps to enable them.

DB="pgbench_test"
SCALE="50"
DURATION="90"
DO_SETUP=1
DO_IO_TIMING=0
DO_GROUP_B=0
DO_CLEANUP=0
GROUP_B_HOLD="${GROUP_B_HOLD:-30}"

source "$(dirname "${BASH_SOURCE[0]}")/pg-connection.sh"

usage() {
    cat <<'EOF'
Usage: exercise-new-collectors.sh [options]

Generate load/data so every new 0.16.0 collector exports non-trivial series.

Options:
  --db NAME        Target database for pgbench data (default: pgbench_test)
  --scale N        pgbench scale; use >= 100 to make CREATE INDEX progress span a
                   scrape and > shared_buffers to force stat_io evictions (default: 50)
  --duration N     Seconds to run the background load (default: 90)
  --no-setup       Skip ./scripts/setup-local-test-db.sh (assume data already loaded)
  --io-timing      Enable track_io_timing (reload only) so stat_io latency panels populate
  --group-b        Also run prepared-xact and logical-slot stimuli if the server allows them
  --cleanup        Drop the demo objects this script creates, then exit
  -h, --help       Show this help
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --db) DB="$2"; shift 2 ;;
        --scale) SCALE="$2"; shift 2 ;;
        --duration) DURATION="$2"; shift 2 ;;
        --no-setup) DO_SETUP=0; shift ;;
        --io-timing) DO_IO_TIMING=1; shift ;;
        --group-b) DO_GROUP_B=1; shift ;;
        --cleanup) DO_CLEANUP=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage >&2; exit 1 ;;
    esac
done

psql_db() { pg_connection_psql_cmd "$DB" "$@"; }
psqlq() { pg_connection_psql_cmd "$DB" -qAt -c "$1"; }

if ! psql_db -c 'SELECT 1' >/dev/null 2>&1; then
    echo "❌ PostgreSQL is not reachable ($(pg_connection_description "$DB")). Start it with: just postgres" >&2
    exit 1
fi

BG_PIDS=()
cleanup_bg() {
    for pid in "${BG_PIDS[@]:-}"; do
        [[ -n "${pid}" ]] && kill "${pid}" 2>/dev/null || true
    done
    psqlq "DROP INDEX CONCURRENTLY IF EXISTS ex_demo_idx" >/dev/null 2>&1 || true
    psqlq "DROP TABLE IF EXISTS ex_spill_demo" >/dev/null 2>&1 || true
    # Roll back a lingering prepared xact if we created one.
    psqlq "SELECT gid FROM pg_prepared_xacts WHERE gid = 'exercise_blocker'" 2>/dev/null | grep -q exercise_blocker \
        && psqlq "ROLLBACK PREPARED 'exercise_blocker'" >/dev/null 2>&1 || true
    psqlq "SELECT 1 FROM pg_replication_slots WHERE slot_name = 'exercise_spill'" 2>/dev/null | grep -q 1 \
        && psqlq "SELECT pg_drop_replication_slot('exercise_spill')" >/dev/null 2>&1 || true
}

drop_demo() {
    echo "🧹 Dropping demo objects..."
    psqlq "DROP SEQUENCE IF EXISTS seq_demo_hot" || true
    psqlq "DROP SEQUENCE IF EXISTS seq_demo_warn" || true
    psqlq "DROP INDEX CONCURRENTLY IF EXISTS ex_demo_idx" || true
    psqlq "DROP TABLE IF EXISTS ex_spill_demo" || true
    psqlq "SELECT gid FROM pg_prepared_xacts WHERE gid = 'exercise_blocker'" | grep -q exercise_blocker \
        && psqlq "ROLLBACK PREPARED 'exercise_blocker'" || true
    psqlq "SELECT 1 FROM pg_replication_slots WHERE slot_name = 'exercise_spill'" | grep -q 1 \
        && psqlq "SELECT pg_drop_replication_slot('exercise_spill')" || true
    echo "✅ Cleanup complete."
}

if [[ "${DO_CLEANUP}" -eq 1 ]]; then
    drop_demo
    exit 0
fi

trap cleanup_bg EXIT

echo "🎯 Target: $(pg_connection_description "$DB")  |  duration=${DURATION}s  scale=${SCALE}"

# An interrupted --group-b run can leave a prepared xact behind, which would block
# this run's CREATE INDEX CONCURRENTLY loop forever. Clear any such leftovers first.
if psqlq "SELECT gid FROM pg_prepared_xacts WHERE gid = 'exercise_blocker'" 2>/dev/null | grep -q exercise_blocker; then
    echo "🧹 Rolling back a leftover 'exercise_blocker' prepared xact from a prior run..."
    psqlq "ROLLBACK PREPARED 'exercise_blocker'" >/dev/null 2>&1 || true
fi
if psqlq "SELECT 1 FROM pg_replication_slots WHERE slot_name = 'exercise_spill'" 2>/dev/null | grep -q 1; then
    echo "🧹 Dropping a leftover 'exercise_spill' replication slot from a prior run..."
    psqlq "SELECT pg_drop_replication_slot('exercise_spill')" >/dev/null 2>&1 || true
fi

if [[ "${DO_SETUP}" -eq 1 ]]; then
    echo "🔧 Ensuring pgbench dataset exists (scale=${SCALE})..."
    # setup-local-test-db.sh pipes `pgbench | grep` with pipefail, so a benign
    # pgbench exit code can surface even when the dataset built fine. Don't let
    # that abort the exercise — verify the dataset itself instead of trusting
    # the exit status.
    if ! "$(dirname "${BASH_SOURCE[0]}")/setup-local-test-db.sh" --pgbench --pgbench-scale "${SCALE}"; then
        echo "⚠️  setup-local-test-db.sh returned non-zero; verifying the dataset directly..."
    fi
    if ! psqlq "SELECT 1 FROM pg_class WHERE relname = 'pgbench_accounts' AND relkind = 'r'" | grep -q 1; then
        echo "❌ pgbench dataset is missing after setup. Fix the DB, then re-run 'just exercise-collectors' (or pass --no-setup once data exists)." >&2
        exit 1
    fi
fi

if [[ "${DO_IO_TIMING}" -eq 1 ]]; then
    echo "⏱️  Enabling track_io_timing (reload only)..."
    pg_connection_psql_cmd postgres -qAt -c "ALTER SYSTEM SET track_io_timing = on" || true
    pg_connection_psql_cmd postgres -qAt -c "SELECT pg_reload_conf()" >/dev/null || true
fi

# --- #28 Sequence exhaustion: seed two sequences above --sequences.min-ratio ---
echo "🔢 [#28] Seeding demo sequences (~0.95 and ~0.60 consumed)..."
psqlq "CREATE SEQUENCE IF NOT EXISTS seq_demo_hot  AS integer MAXVALUE 2147483647" >/dev/null
psqlq "CREATE SEQUENCE IF NOT EXISTS seq_demo_warn AS integer MAXVALUE 2147483647" >/dev/null
psqlq "SELECT setval('seq_demo_hot',  2050000000)" >/dev/null
psqlq "SELECT setval('seq_demo_warn', 1300000000)" >/dev/null

# --- #29.1 SLRU: subtransaction storm ---
echo "🧠 [#29.1] Running subtransaction storm (SLRU counters)..."
psqlq "DO \$\$ BEGIN FOR i IN 1..200000 LOOP BEGIN PERFORM 1; EXCEPTION WHEN OTHERS THEN NULL; END; END LOOP; END \$\$" >/dev/null

# --- #27 Idle-in-transaction backend pinning the xmin horizon ---
echo "🛑 [#27] Opening an idle-in-transaction snapshot holder for ${DURATION}s..."
psql_db -qAt -c "BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT 1; SELECT pg_sleep(${DURATION});" >/dev/null 2>&1 &
BG_PIDS+=("$!")

have_pgbench=1
if ! command -v pgbench >/dev/null 2>&1; then
    have_pgbench=0
    echo "⚠️  pgbench not found — skipping session-churn and stat_io load (install postgresql-contrib)."
fi

if [[ "${have_pgbench}" -eq 1 ]]; then
    # --- #29.2 Session churn: reconnect per transaction ---
    echo "🔁 [#29.2] pgbench -C session churn for ${DURATION}s..."
    pg_connection_pgbench_cmd "$DB" -C -c 10 -T "${DURATION}" >/dev/null 2>&1 &
    BG_PIDS+=("$!")

    # --- #25 stat_io: read-heavy load to force buffer evictions ---
    echo "💾 [#25] pgbench select-only load for ${DURATION}s (buffer evictions)..."
    pg_connection_pgbench_cmd "$DB" -S -c 8 -T "${DURATION}" >/dev/null 2>&1 &
    BG_PIDS+=("$!")
fi

# --- #29.5 Progress views: keep a CREATE INDEX + ANALYZE running ---
echo "🏗️  [#29.5] Looping CREATE INDEX CONCURRENTLY + ANALYZE for ${DURATION}s..."
(
    end=$((SECONDS + DURATION))
    while [[ ${SECONDS} -lt ${end} ]]; do
        psqlq "DROP INDEX CONCURRENTLY IF EXISTS ex_demo_idx" >/dev/null 2>&1 || true
        psqlq "CREATE INDEX CONCURRENTLY ex_demo_idx ON pgbench_accounts (abalance)" >/dev/null 2>&1 || true
        psqlq "ANALYZE pgbench_accounts" >/dev/null 2>&1 || true
    done
) &
BG_PIDS+=("$!")

# Group B (prepared xacts #27, logical slot spill #29.4) runs AFTER the background
# load finishes — see below. A prepared transaction pins a snapshot that blocks the
# #29.5 CREATE INDEX CONCURRENTLY loop and logical-slot creation indefinitely, so it
# must never overlap them.

cat <<EOF

▶️  Background load is running for ~${DURATION}s. Open Grafana (http://localhost:3000)
   and expand the rows below. Panels populate within a scrape interval or two.

EOF

echo "⌛ Waiting ${DURATION}s for the load to finish..."
wait || true

# --- Group B: logical slot spill (#29.4) then prepared xact (#27) ---
# Runs only now that the CREATE INDEX CONCURRENTLY loop has drained. The slot is
# created before the prepared xact because a pending prepared xact would block
# pg_create_logical_replication_slot from reaching a consistent snapshot.
if [[ "${DO_GROUP_B}" -eq 1 ]]; then
    wal_level="$(psqlq 'SHOW wal_level' | tr -d '[:space:]')"
    max_prep="$(psqlq 'SHOW max_prepared_transactions' | tr -d '[:space:]')"
    group_b_ran=0

    if [[ "${wal_level}" == "logical" ]]; then
        echo "💧 [#29.4] Creating logical slot 'exercise_spill' and forcing a spill to disk..."
        psqlq "SELECT pg_create_logical_replication_slot('exercise_spill','test_decoding')" >/dev/null 2>&1 || true
        psqlq "CREATE TABLE IF NOT EXISTS ex_spill_demo (id integer, filler text)" >/dev/null 2>&1 || true
        psqlq "INSERT INTO ex_spill_demo SELECT g, repeat('x', 100) FROM generate_series(1, 500000) g" >/dev/null 2>&1 || true
        # A tiny logical_decoding_work_mem forces the reorder buffer to spill to disk
        # while decoding, so pg_stat_replication_slots.spill_* increments.
        psqlq "SET logical_decoding_work_mem = '64kB'; SELECT count(*) FROM pg_logical_slot_peek_changes('exercise_spill', NULL, NULL)" >/dev/null 2>&1 || true
        group_b_ran=1
    else
        echo "⏭️  [#29.4] wal_level=${wal_level:-?} — logical replication slots need wal_level=logical."
        echo "     Enable with: ALTER SYSTEM SET wal_level = logical;  (then restart PostgreSQL)"
    fi

    if [[ "${max_prep:-0}" -gt 0 ]]; then
        echo "📌 [#27] Creating prepared transaction 'exercise_blocker' (holds an xid on the xmin horizon)..."
        psqlq "BEGIN; SELECT txid_current(); PREPARE TRANSACTION 'exercise_blocker'" >/dev/null 2>&1 || true
        group_b_ran=1
    else
        echo "⏭️  [#27] max_prepared_transactions=0 — prepared xacts are disabled."
        echo "     Enable with: ALTER SYSTEM SET max_prepared_transactions = 10;  (then restart PostgreSQL)"
    fi

    if [[ "${group_b_ran}" -eq 1 ]]; then
        echo "⏳ Holding Group B objects for ${GROUP_B_HOLD}s so their panels can scrape, then cleaning up..."
        sleep "${GROUP_B_HOLD}"
    fi
fi

echo "✅ Exercise complete. Demo sequences remain so the Sequence panel stays populated."
echo "   Run with --cleanup to remove all demo objects."
