#!/usr/bin/env bash
set -uo pipefail

# Runs on every container start (DevPod postStartCommand). Best-effort: it must not
# fail `devpod up`. Waits for the postgres sibling to be ready and seeds the local
# test database (pgbench data + pg_stat_statements) so `just test` is ready to go.

export PATH="$HOME/.local/bin:$HOME/.local/share/mise/shims:$PATH"
cd /workspaces/pg_exporter 2>/dev/null || exit 0

# Re-apply optional git identity/signing on every start so updates to forwarded
# DevPod workspace env are reflected without rebuilding the container.
sh .devcontainer/configure-git.sh || true

PG_HOST="${PG_HOST:-postgres}"
PG_PORT="${PG_PORT:-5432}"

# Wait for PostgreSQL (compose healthcheck usually has it ready already).
for _ in $(seq 1 30); do
  if PGPASSWORD=postgres psql -h "$PG_HOST" -p "$PG_PORT" -U postgres -d postgres \
    -c "SELECT 1" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if [ -x scripts/setup-local-test-db.sh ]; then
  if scripts/setup-local-test-db.sh >/dev/null 2>&1; then
    echo "✓ Workspace ready. PostgreSQL seeded at ${PG_HOST}:${PG_PORT}."
    echo "  Run: just test"
  else
    echo "post-start: test DB seeding did not complete (continuing)." >&2
    echo "  You can run it manually: scripts/setup-local-test-db.sh" >&2
  fi
fi

exit 0
