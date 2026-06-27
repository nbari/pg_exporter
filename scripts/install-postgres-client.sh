#!/usr/bin/env bash
set -euo pipefail

# Install the PostgreSQL client tools (psql + pgbench) matching the server version.
#
# Why this is not just `apt-get install postgresql-client`:
#   On Debian/Ubuntu the `pgbench` binary does NOT ship in `postgresql-client-NN` —
#   it lives in the *server* package `postgresql-NN`. Installing only the client
#   package gives a `/usr/bin/pgbench` wrapper with no backing binary, which fails
#   with "You must install at least one postgresql-client-<version> package".
#
# So we add the PGDG apt repo (to match the postgres:18 service used by the
# devcontainer), disable automatic local-cluster creation (we only want the pgbench
# binary, not a running server), and install the client + the server package that
# provides pgbench.
#
# PG_MAJOR can override the version (defaults to 18 to match .devcontainer/compose.yaml).

PG_MAJOR="${PG_MAJOR:-18}"

# Already have a real pgbench? Nothing to do (idempotent).
if command -v pgbench >/dev/null 2>&1 && pgbench --version >/dev/null 2>&1; then
  echo "pgbench already installed: $(pgbench --version)"
  exit 0
fi

sudo apt-get update -qq
sudo apt-get install -y -qq ca-certificates curl gnupg lsb-release >/dev/null

# Add the PostgreSQL APT (PGDG) repository if it is not already configured.
if [ ! -f /etc/apt/sources.list.d/pgdg.list ]; then
  sudo install -d /usr/share/postgresql-common/pgdg
  sudo curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
    -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc
  codename="$(. /etc/os-release && echo "${VERSION_CODENAME}")"
  echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt ${codename}-pgdg main" |
    sudo tee /etc/apt/sources.list.d/pgdg.list >/dev/null
  sudo apt-get update -qq
fi

# Do not create a local cluster when the server package is installed — we only need
# the pgbench binary it ships, not a running PostgreSQL instance.
sudo install -d /etc/postgresql-common
echo "create_main_cluster = false" |
  sudo tee /etc/postgresql-common/createcluster.conf >/dev/null

# psql + libpq from the client package; pgbench from the server package.
sudo apt-get install -y -qq \
  "postgresql-client-${PG_MAJOR}" \
  "postgresql-${PG_MAJOR}" \
  libpq-dev >/dev/null

echo "✓ Installed: $(psql --version), $(pgbench --version)"
