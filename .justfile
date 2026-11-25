# Get the current user's UID and GID
uid := `id -u`
gid := `id -g`
container_cmd := `if [ -n "${PG_EXPORTER_CONTAINER_CMD:-}" ]; then echo "${PG_EXPORTER_CONTAINER_CMD}"; elif command -v podman >/dev/null 2>&1; then echo "podman"; else echo "docker"; fi`

# SELinux relabel suffix for bind mounts: ":Z" on Linux (needed on SELinux hosts
# such as Fedora Atomic, harmless where SELinux is absent), empty on macOS
# (Darwin) for Docker Desktop / podman machine portability. Keyed on the OS
# rather than getenforce so it stays correct when 'just' runs inside a container
# (e.g. a toolbox) whose own SELinux view differs from the host that runs podman.
selinux_volume_flag := `if [ "$(uname -s)" = "Linux" ]; then echo ":Z"; fi`

default: test
  @just --list

# Test suite
test: clippy fmt
  @echo "🧪 Checking PostgreSQL..."
  @if PGPASSWORD="${PGPASSWORD:-postgres}" psql -h "${PG_HOST:-localhost}" -p "${PG_PORT:-5432}" -U postgres -d postgres -c "SELECT 1" >/dev/null 2>&1; then \
    echo "✅ PostgreSQL already reachable on ${PG_HOST:-localhost}:${PG_PORT:-5432} (using existing instance)"; \
  elif command -v {{container_cmd}} >/dev/null 2>&1; then \
    echo "🚀 PostgreSQL not reachable, starting it with {{container_cmd}}..."; \
    just postgres; \
    echo "⏳ Waiting for PostgreSQL to be ready..."; \
    sleep 3; \
    timeout 30 bash -c 'until PGPASSWORD="${PGPASSWORD:-postgres}" psql -h "${PG_HOST:-localhost}" -p "${PG_PORT:-5432}" -U postgres -d postgres -c "SELECT 1" &>/dev/null; do sleep 1; done' || (echo "❌ PostgreSQL failed to start" && exit 1); \
    echo "✅ PostgreSQL is ready"; \
  else \
    echo "❌ PostgreSQL not reachable on ${PG_HOST:-localhost}:${PG_PORT:-5432} and no container runtime (podman/docker) found."; \
    echo "   Start PostgreSQL yourself (e.g. run 'just postgres' on the host) or set PG_EXPORTER_CONTAINER_CMD."; \
    exit 1; \
  fi
  @echo "🧪 Running setup check..."
  @if [ -f scripts/setup-local-test-db.sh ]; then \
    scripts/setup-local-test-db.sh || (echo "❌ Test database setup failed. Fix the issues above before running tests." && exit 1); \
  fi
  @echo "🔧 Using local test database (overriding .envrc)..."
  PG_EXPORTER_DSN="postgresql://postgres:${PGPASSWORD:-postgres}@${PG_HOST:-localhost}:${PG_PORT:-5432}/postgres" cargo test --locked -- --nocapture
  PG_EXPORTER_DSN="postgresql://postgres:${PGPASSWORD:-postgres}@${PG_HOST:-localhost}:${PG_PORT:-5432}/postgres" cargo test --locked --features telemetry -- --nocapture

# Run only the replication topology integration test (primary+replica via testcontainers)
test-replica:
    #!/usr/bin/env bash
    set -euo pipefail

    docker_host="${DOCKER_HOST:-}"

    if [[ -z "${docker_host}" ]]; then
        # Prefer podman (the project's primary runtime). On macOS/Windows the podman
        # daemon runs in a VM and exposes a host-side socket under a machine-specific
        # path (e.g. /var/folders/.../podman-machine-default-api.sock) that only
        # `podman machine inspect` can reveal, so check that before the Linux paths.
        machine_sock=""
        if command -v podman >/dev/null 2>&1; then
            machine_sock="$(podman machine inspect --format '{{{{.ConnectionInfo.PodmanSocket.Path}}' 2>/dev/null || true)"
        fi

        if [[ -n "${machine_sock}" && -S "${machine_sock}" ]]; then
            docker_host="unix://${machine_sock}"
        elif [[ -n "${XDG_RUNTIME_DIR:-}" && -S "${XDG_RUNTIME_DIR}/podman/podman.sock" ]]; then
            docker_host="unix://${XDG_RUNTIME_DIR}/podman/podman.sock"
        elif [[ -S "/run/user/$(id -u)/podman/podman.sock" ]]; then
            docker_host="unix:///run/user/$(id -u)/podman/podman.sock"
        elif [[ -S /run/podman/podman.sock ]]; then
            docker_host="unix:///run/podman/podman.sock"
        elif [[ -S /var/run/docker.sock ]]; then
            docker_host="unix:///var/run/docker.sock"
        else
            echo "❌ No Docker/Podman socket found for testcontainers" >&2
            echo "Set DOCKER_HOST, e.g.:" >&2
            echo "  # macOS podman machine:" >&2
            echo "  export DOCKER_HOST=\"unix://\$(podman machine inspect --format '{{{{.ConnectionInfo.PodmanSocket.Path}}')\"" >&2
            echo "  # rootless Linux:" >&2
            echo "  export DOCKER_HOST=unix:///run/user/\$UID/podman/podman.sock" >&2
            exit 1
        fi
    fi

    echo "🧪 Running replication topology test with DOCKER_HOST=${docker_host}"
    DOCKER_HOST="${docker_host}" PG_EXPORTER_REQUIRE_TESTCONTAINERS=1 \
      cargo test --test collectors_tests \
      replication::replica_topology::replication_lag_and_role_semantics_from_postgres_primary_replica_pair \
      -- --nocapture

# Linting
clippy:
  cargo clippy --locked --all-targets
  cargo clippy --locked --all-targets --all-features

# Formatting check
fmt:
  cargo fmt --all -- --check

# Coverage report
coverage:
  CARGO_INCREMENTAL=0 RUSTFLAGS='-Cinstrument-coverage' LLVM_PROFILE_FILE='coverage-%p-%m.profraw' cargo test --all-features
  grcov . --binary-path ./target/debug/deps/ -s . -t html --branch --ignore-not-existing --ignore '../*' --ignore "/*" -o target/coverage/html
  firefox target/coverage/html/index.html
  rm -rf *.profraw

# Update dependencies
update:
  cargo update

# Clean build artifacts
clean:
  cargo clean

# Get current version
version:
    @cargo metadata --no-deps --format-version 1 | jq -r '.packages[0].version'

# Check if working directory is clean
check-clean:
    #!/usr/bin/env bash
    if [[ -n $(git status --porcelain) ]]; then
        echo "❌ Working directory is not clean. Commit or stash your changes first."
        git status --short
        exit 1
    fi
    echo "✅ Working directory is clean"

# Guard: refuse the fast-forward-only branches main and develop (bump from a working branch)
check-branch:
    #!/usr/bin/env bash
    set -euo pipefail
    current_branch="$(git branch --show-current)"
    if [[ "$current_branch" == "main" || "$current_branch" == "develop" ]]; then
        echo "❌ Branch '$current_branch' is fast-forward-only; bump from a working branch (sandbox)."
        exit 1
    fi
    echo "✅ Branch '$current_branch' is a working branch"

# Guard: refuse while cargo watch runs (a watcher races builds during the bump)
check-no-watch:
    #!/usr/bin/env bash
    set -euo pipefail
    if pgrep -f '[c]argo watch|[c]argo-watch' >/dev/null; then
        echo "❌ cargo watch is running. Stop 'just watch' before bumping."
        exit 1
    fi
    echo "✅ No cargo watcher is running"

# Private: bump the version, refresh dependencies, run tests, and commit (no push)
_bump bump_kind: check-clean check-no-watch check-branch
    #!/usr/bin/env bash
    set -euo pipefail

    bump_kind="{{bump_kind}}"
    version_files=(Cargo.toml Cargo.lock grafana/dashboard.json)

    cleanup() {
        status=$?
        if [ $status -ne 0 ]; then
            echo "↩️  Restoring version files after failure..."
            rm -f grafana/dashboard.json.tmp
            if ! git restore --staged --worktree --source=HEAD -- "${version_files[@]}"; then
                echo "⚠️ Failed to restore ${version_files[*]} from HEAD; fix them manually."
            fi
        fi
        exit $status
    }
    trap cleanup EXIT
    # An interrupt must reach the cleanup with a failing status, whatever was running.
    trap 'exit 130' INT
    trap 'exit 143' TERM

    previous_version=$(just version)
    echo "ℹ️  Current version: ${previous_version}"

    echo "🔧 Bumping ${bump_kind} version..."
    cargo set-version --bump "${bump_kind}"
    new_version=$(just version)
    echo "📝 New version: ${new_version}"

    validate_bump() {
        local previous=$1 bump=$2 current=$3
        IFS=. read -r prev_major prev_minor prev_patch <<<"${previous}"
        IFS=. read -r new_major new_minor new_patch <<<"${current}"

        case "${bump}" in
            patch)
                (( new_major == prev_major && new_minor == prev_minor && new_patch == prev_patch + 1 )) || { echo "❌ Expected patch bump from ${previous}, got ${current}"; exit 1; }
                ;;
            minor)
                (( new_major == prev_major && new_minor == prev_minor + 1 && new_patch == 0 )) || { echo "❌ Expected minor bump from ${previous}, got ${current}"; exit 1; }
                ;;
            major)
                (( new_major == prev_major + 1 && new_minor == 0 && new_patch == 0 )) || { echo "❌ Expected major bump from ${previous}, got ${current}"; exit 1; }
                ;;
        esac
    }

    validate_bump "${previous_version}" "${bump_kind}" "${new_version}"

    echo "🔍 Verifying tag does not exist for ${new_version}..."
    git fetch --tags --quiet
    if git rev-parse -q --verify "refs/tags/${new_version}" >/dev/null 2>&1; then
        echo "❌ Tag ${new_version} already exists!"
        exit 1
    fi

    echo "🔄 Updating dependencies..."
    cargo update

    echo "📊 Updating Grafana dashboard version to ${new_version}..."
    jq --arg ver "${new_version}" '
      .tags = ((.tags // []) | map(select(test("^[0-9]+\\.[0-9]+\\.[0-9]+") | not))) + [$ver] |
      .version = .version + 1
    ' grafana/dashboard.json > grafana/dashboard.json.tmp
    mv grafana/dashboard.json.tmp grafana/dashboard.json

    echo "🧪 Running tests with new version (via just test)..."
    just test

    git add "${version_files[@]}"
    git commit -m "bump version to ${new_version}"

    echo "✅ Version bumped to ${new_version} (commit $(git rev-parse --short HEAD))."
    echo "Next steps: push, fast-forward develop to this commit when Test & Build is green, then run 'just release'."

# Bump version and commit (patch level)
bump:
    @just _bump patch

# Bump minor version
bump-minor:
    @just _bump minor

# Bump major version
bump-major:
    @just _bump major

# Releases (see RELEASING.md): the version bump is done with `just bump` (or
# `bump-minor`/`bump-major`) as the last commit of the work, and the Release workflow
# builds and publishes that exact commit once CI passed.
# `main` is moved here, after the run, because a protected branch refuses an update while
# a `CI OK` check of the same commit is still pending (develop's own Test & Build).

# Release develop: start the release workflow, follow it, then fast-forward main
release:
    #!/usr/bin/env bash
    set -euo pipefail
    git fetch --quiet origin develop
    sha="$(git rev-parse origin/develop)"
    last="$(gh run list --workflow release.yml --branch develop --event workflow_dispatch --limit 1 --json databaseId --jq '.[0].databaseId // 0')"
    gh workflow run release.yml --ref develop
    # The dispatched run is the first new one for this commit; never just "the latest".
    run=""
    for _ in $(seq 30); do
        run="$(gh run list --workflow release.yml --branch develop --event workflow_dispatch --limit 10 --json databaseId,headSha --jq "[.[] | select(.databaseId > ${last} and .headSha == \"${sha}\")][0].databaseId // empty")"
        [[ -z "${run}" ]] || break
        sleep 2
    done
    [[ -n "${run}" ]] || { echo "The dispatched release run did not appear." >&2; exit 1; }
    gh run watch "${run}" --exit-status
    # develop's own Test & Build must have finished, and passed, before main can move.
    ci=""
    for _ in $(seq 30); do
        ci="$(gh run list --workflow build.yml --branch develop --commit "${sha}" --limit 1 --json databaseId --jq '.[0].databaseId // empty')"
        [[ -z "${ci}" ]] || break
        sleep 2
    done
    [[ -n "${ci}" ]] || { echo "No Test & Build run of develop for ${sha}; push it to main yourself when CI OK passed." >&2; exit 1; }
    echo "Waiting for develop's Test & Build (run ${ci}) before moving main..."
    # The run as a whole can fail while `CI OK` passed (coverage is reported, not a gate),
    # so wait for it to finish and then ask for the check itself, the one `main` requires.
    gh run watch "${ci}" > /dev/null || true
    [[ "$(gh run view "${ci}" --json status --jq .status)" == completed ]] || { echo "Test & Build (run ${ci}) has not finished; when CI OK passed: git push origin ${sha}:refs/heads/main" >&2; exit 1; }
    ci_ok="$(gh api "repos/{owner}/{repo}/commits/${sha}/check-runs?check_name=CI%20OK&status=completed&per_page=100" --jq '[.check_runs[]] | sort_by(.completed_at) | last | .conclusion // "missing"')"
    [[ "${ci_ok}" == success ]] || { echo "CI OK is '${ci_ok}' on ${sha}; re-run the failed jobs of Test & Build, then: git push origin ${sha}:refs/heads/main" >&2; exit 1; }
    git push origin "${sha}:refs/heads/main"

# List every registered collector as a --collector.<name> flag
collector-flags:
    #!/usr/bin/env bash
    set -euo pipefail
    sed -n '/register_collectors! {/,/^}/p' src/collectors/mod.rs \
        | sed -n 's/^[[:space:]]*\([a-z0-9_]\+\)[[:space:]]*=>.*/--collector.\1/p' \
        | tr '\n' ' '

# Watch for changes and run with every collector enabled
watch features="":
    #!/usr/bin/env bash
    set -euo pipefail
    flags=$(just collector-flags)
    echo "▶️  enabling: ${flags}"
    cargo watch -x "run --features '{{features}}' -- ${flags} -v"

# get metrics curl
curl:
  curl -s 0:9432/metrics

# Show who is blocking whom right now (lock-wait root cause for the Grafana panels)
blocking:
    #!/usr/bin/env bash
    set -euo pipefail
    pg_host="${PG_HOST:-localhost}"; pg_port="${PG_PORT:-5432}"
    PGPASSWORD="${PGPASSWORD:-postgres}" psql -h "${pg_host}" -p "${pg_port}" -U postgres \
        -d "${PGDATABASE:-postgres}" -P pager=off -f scripts/blocking-tree.sql

# Show what is running on CPU right now (CPU-saturation root cause for the Grafana panels)
on-cpu:
    #!/usr/bin/env bash
    set -euo pipefail
    pg_host="${PG_HOST:-localhost}"; pg_port="${PG_PORT:-5432}"
    PGPASSWORD="${PGPASSWORD:-postgres}" psql -h "${pg_host}" -p "${pg_port}" -U postgres \
        -d "${PGDATABASE:-postgres}" -P pager=off -f scripts/on-cpu-queries.sql

# Show long-running and idle-in-transaction sessions (blocking / leak root cause)
long-running:
    #!/usr/bin/env bash
    set -euo pipefail
    pg_host="${PG_HOST:-localhost}"; pg_port="${PG_PORT:-5432}"
    PGPASSWORD="${PGPASSWORD:-postgres}" psql -h "${pg_host}" -p "${pg_port}" -U postgres \
        -d "${PGDATABASE:-postgres}" -P pager=off -f scripts/long-running.sql

# Show tables most likely missing an index (high seq-scan, low index usage)
seq-scans:
    #!/usr/bin/env bash
    set -euo pipefail
    pg_host="${PG_HOST:-localhost}"; pg_port="${PG_PORT:-5432}"
    PGPASSWORD="${PGPASSWORD:-postgres}" psql -h "${pg_host}" -p "${pg_port}" -U postgres \
        -d "${PGDATABASE:-postgres}" -P pager=off -f scripts/seq-scans.sql

# Show connection breakdown by database / user / application / state
connections:
    #!/usr/bin/env bash
    set -euo pipefail
    pg_host="${PG_HOST:-localhost}"; pg_port="${PG_PORT:-5432}"
    PGPASSWORD="${PGPASSWORD:-postgres}" psql -h "${pg_host}" -p "${pg_port}" -U postgres \
        -d "${PGDATABASE:-postgres}" -P pager=off -f scripts/connections.sql

# Show top tables by dead tuples (VACUUM / pg_repack candidates)
bloat:
    #!/usr/bin/env bash
    set -euo pipefail
    pg_host="${PG_HOST:-localhost}"; pg_port="${PG_PORT:-5432}"
    PGPASSWORD="${PGPASSWORD:-postgres}" psql -h "${pg_host}" -p "${pg_port}" -U postgres \
        -d "${PGDATABASE:-postgres}" -P pager=off -f scripts/bloat.sql

# Show top queries by total execution time (requires pg_stat_statements)
top-queries:
    #!/usr/bin/env bash
    set -euo pipefail
    pg_host="${PG_HOST:-localhost}"; pg_port="${PG_PORT:-5432}"
    if ! PGPASSWORD="${PGPASSWORD:-postgres}" psql -h "${pg_host}" -p "${pg_port}" -U postgres \
        -d "${PGDATABASE:-postgres}" -tAc "SELECT 1 FROM pg_extension WHERE extname='pg_stat_statements'" | grep -q 1; then
        echo "pg_stat_statements not installed. Enable it (shared_preload_libraries + CREATE EXTENSION pg_stat_statements)."; exit 1
    fi
    PGPASSWORD="${PGPASSWORD:-postgres}" psql -h "${pg_host}" -p "${pg_port}" -U postgres \
        -d "${PGDATABASE:-postgres}" -P pager=off -f scripts/top-queries.sql

# Exercise every collector that can be stimulated on a local PostgreSQL.
# This includes the normal workload, progress views, manual vacuum, and automatic
# vacuum/analyze workflows. Restart-dependent features are attempted when available.
# Pass flags through to the script, e.g.:
#   just exercise-collectors --duration 120 --scale 100 --clients 10
# Clean up afterwards with: just exercise-collectors --cleanup
exercise-collectors *args:
    ./scripts/exercise-collectors.sh {{args}}

postgres version="latest":
  mkdir -p db/log/postgres
  {{container_cmd}} run --rm -d --name pg_exporter_postgres \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_HOST_AUTH_METHOD=trust \
    -e PGDATA=/db/data/{{ version }} \
    -p 5432:5432 \
    -v $(pwd)/db:/db{{ selinux_volume_flag }} \
    -v $(pwd)/db/config/postgres:/etc/postgresql/config{{ selinux_volume_flag }} \
    {{ if container_cmd == "podman" { "--userns keep-id:uid=" + uid + ",gid=" + gid + " --user " + uid + ":" + gid } else { "" } }} \
    postgres:{{ version }} \
    postgres -c config_file=/etc/postgresql/config/postgresql.conf

jaeger:
  {{container_cmd}} run --rm -d --name jaeger \
    -e COLLECTOR_OTLP_ENABLED=true \
    -p 16686:16686 \
    -p 4317:4317 \
    -p 4318:4318 \
    jaegertracing/all-in-one:latest

stop-containers:
  @for c in pg_exporter_postgres jaeger; do \
        {{container_cmd}} stop $c 2>/dev/null || true; \
  done

# Test against all PostgreSQL versions (14-19)
test-all-pg:
    #!/usr/bin/env bash
    set -euo pipefail

    VERSIONS=(14 15 16 17 18 19)
    FAILED=()

    echo "🚀 Starting all PostgreSQL versions..."
    for v in "${VERSIONS[@]}"; do
        PORT="54${v}"
        {{container_cmd}} rm -f pg${v} >/dev/null 2>&1 || true
        {{container_cmd}} run -d --name pg${v} \
            -e POSTGRES_PASSWORD=postgres \
            -e POSTGRES_USER=postgres \
            -p ${PORT}:5432 \
            postgres:${v}-alpine \
            postgres \
            -c shared_preload_libraries=pg_stat_statements \
            -c pg_stat_statements.track=all \
            -c pg_stat_statements.max=10000 >/dev/null 2>&1 || true
    done

    echo "⏳ Waiting for PostgreSQL instances to be ready..."
    sleep 5

    for v in "${VERSIONS[@]}"; do
        PORT="54${v}"
        timeout 30 bash -c "until {{container_cmd}} exec pg${v} pg_isready -U postgres >/dev/null 2>&1; do sleep 1; done" || true
        PGPASSWORD=postgres psql -h localhost -p "${PORT}" -U postgres -d postgres \
            -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;" >/dev/null
    done

    echo ""
    for v in "${VERSIONS[@]}"; do
        PORT="54${v}"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo "🐘 Testing PostgreSQL ${v} (port ${PORT})"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

        # Container-based tests start their own PostgreSQL; pin it to the same major.
        if PG_EXPORTER_DSN="postgresql://postgres:postgres@localhost:${PORT}/postgres" \
           PG_EXPORTER_TEST_POSTGRES_TAG="${v}" \
           cargo test --quiet 2>&1 | tail -5; then
            echo "✅ PostgreSQL ${v} passed"
        else
            echo "❌ PostgreSQL ${v} failed"
            FAILED+=("${v}")
        fi
        echo ""
    done

    echo "🧹 Cleaning up containers..."
    for v in "${VERSIONS[@]}"; do
        {{container_cmd}} stop pg${v} >/dev/null 2>&1 || true
        {{container_cmd}} rm pg${v} >/dev/null 2>&1 || true
    done

    if [ ${#FAILED[@]} -eq 0 ]; then
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo "✅ All PostgreSQL versions passed!"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    else
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo "❌ Failed versions: ${FAILED[*]}"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        exit 1
    fi

# Test against specific PostgreSQL version
test-pg version:
    #!/usr/bin/env bash
    set -euo pipefail

    PORT="54{{version}}"
    echo "🐘 Starting PostgreSQL {{version}} on port ${PORT}..."
    {{container_cmd}} rm -f pg{{version}} >/dev/null 2>&1 || true
    {{container_cmd}} run -d --name pg{{version}} \
        -e POSTGRES_PASSWORD=postgres \
        -e POSTGRES_USER=postgres \
        -p ${PORT}:5432 \
        postgres:{{version}}-alpine \
        postgres \
        -c shared_preload_libraries=pg_stat_statements \
        -c pg_stat_statements.track=all \
        -c pg_stat_statements.max=10000

    echo "⏳ Waiting for PostgreSQL to be ready..."
    sleep 3
    timeout 30 bash -c "until {{container_cmd}} exec pg{{version}} pg_isready -U postgres >/dev/null 2>&1; do sleep 1; done"
    PGPASSWORD=postgres psql -h localhost -p "${PORT}" -U postgres -d postgres \
        -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;" >/dev/null

    echo "🧪 Running tests..."
    PG_EXPORTER_DSN="postgresql://postgres:postgres@localhost:${PORT}/postgres" \
        PG_EXPORTER_TEST_POSTGRES_TAG="{{version}}" cargo test

    echo "🧹 Cleaning up..."
    {{container_cmd}} stop pg{{version}} && {{container_cmd}} rm pg{{version}}

# Test TLS collector with SSL-enabled PostgreSQL
test-tls version="16":
    #!/usr/bin/env bash
    set -e

    SCRIPT_DIR="tests"
    PG_PORT="${PG_PORT:-5433}"

    echo "🔐 Starting SSL-enabled PostgreSQL {{version}} on port ${PG_PORT}..."
    PG_VERSION={{version}} PG_PORT=${PG_PORT} "${SCRIPT_DIR}/start-ssl-postgres.sh"

    # Ensure cleanup happens even if tests fail
    cleanup() {
        echo "🧹 Cleaning up SSL PostgreSQL container..."
        "${SCRIPT_DIR}/stop-ssl-postgres.sh" || true
    }
    trap cleanup EXIT

    echo "🧪 Running TLS collector tests..."
    PG_EXPORTER_DSN="postgresql://postgres:postgres@localhost:${PG_PORT}/postgres?sslmode=require" \
        cargo test --test collectors_tests tls -- --nocapture

    echo "🧪 Running TLS integration tests..."
    PG_EXPORTER_DSN="postgresql://postgres:postgres@localhost:${PG_PORT}/postgres?sslmode=require" \
        cargo test --test tls_metrics_integration -- --nocapture

    echo "✅ TLS tests completed successfully!"

# Validate Grafana dashboard
validate-dashboard:
  @./scripts/validate-dashboard.sh

# Run all validations (tests + dashboard)
validate-all: test validate-dashboard
  @echo "✅ All validations passed!"

# Run local Prometheus + Grafana stack against exporter
metrics target="host.containers.internal:9432" image="pg-exporter-metrics-stack" name="metrics-stack" prom_volume="prom_data":
  # Build the stack image if it's missing so `just metrics` works without a manual build step
  if ! {{container_cmd}} image inspect "{{image}}" >/dev/null 2>&1; then \
    {{container_cmd}} build -t "{{image}}" grafana; \
  fi
  # Replace any existing container with the same name to avoid conflicts
  {{container_cmd}} rm -f {{name}} 2>/dev/null || true
  # Ensure a persistent volume for Prometheus data
  if ! {{container_cmd}} volume inspect "{{prom_volume}}" >/dev/null 2>&1; then \
    {{container_cmd}} volume create "{{prom_volume}}"; \
  fi
  {{container_cmd}} run -d \
    --name {{name}} \
    --add-host=host.containers.internal:host-gateway \
    -e EXPORTER_TARGET={{target}} \
    -e GF_AUTH_ANONYMOUS_ENABLED=true \
    -e GF_AUTH_ANONYMOUS_ORG_ROLE=Admin \
    -e GF_SECURITY_DISABLE_LOGIN_FORM=true \
    -p 3000:3000 -p 9090:9090 \
    -v {{prom_volume}}:/var/lib/prometheus \
    {{image}}

# On-demand Prometheus + Grafana INSIDE the devcontainer compose network: Prometheus
# scrapes app:9432 by service name (no host networking) and Grafana hot-reloads
# grafana/dashboard.json. Use with the exporter running in `app` (`just watch`).
# Grafana http://localhost:3000, Prometheus http://localhost:9090
metrics-dev:
  @./scripts/metrics-dev up

# Stop the on-demand devcontainer observability stack (Prometheus + Grafana)
metrics-dev-stop:
  @./scripts/metrics-dev down

restart-metrics target="host.containers.internal:9432" image="pg-exporter-metrics-stack" name="metrics-stack" prom_volume="prom_data":
  #!/usr/bin/env bash
  set -euo pipefail

  echo "🛑 Stopping existing metrics-stack container..."
  {{container_cmd}} rm -f {{name}} 2>/dev/null || true

  echo "🔍 Checking if dashboard.json changed..."
  REBUILD=0

  # Check if image exists
  if ! {{container_cmd}} image inspect "{{image}}" >/dev/null 2>&1; then
    echo "📦 Image doesn't exist, will build..."
    REBUILD=1
  else
    # Get image creation time
    IMAGE_TIME=$({{container_cmd}} inspect {{image}} --format='{{{{.Created}}}}' 2>/dev/null || echo "")

    if [ -n "$IMAGE_TIME" ]; then
      # Convert image time to epoch (works with ISO 8601 format)
      IMAGE_EPOCH=$(date -d "$IMAGE_TIME" +%s 2>/dev/null || echo "0")

      # Get dashboard.json modification time
      DASHBOARD_EPOCH=$(stat -c %Y grafana/dashboard.json 2>/dev/null || echo "0")

      if [ "$DASHBOARD_EPOCH" -gt "$IMAGE_EPOCH" ]; then
        echo "📊 Dashboard changed ($(date -d @$DASHBOARD_EPOCH '+%Y-%m-%d %H:%M:%S') > $(date -d @$IMAGE_EPOCH '+%Y-%m-%d %H:%M:%S')), rebuilding image..."
        REBUILD=1
      else
        echo "✅ Dashboard unchanged, reusing existing image"
      fi
    else
      echo "⚠️  Could not get image time, rebuilding to be safe..."
      REBUILD=1
    fi
  fi

  if [ "$REBUILD" -eq 1 ]; then
    echo "🔨 Building Grafana stack image..."
    {{container_cmd}} build -t "{{image}}" grafana
  fi

  if ! {{container_cmd}} volume inspect "{{prom_volume}}" >/dev/null 2>&1; then
    echo "📦 Creating Prometheus volume..."
    {{container_cmd}} volume create "{{prom_volume}}"
  fi

  echo "🚀 Starting metrics-stack container..."
  {{container_cmd}} run -d \
    --name {{name}} \
    --add-host=host.containers.internal:host-gateway \
    -e EXPORTER_TARGET={{target}} \
    -e GF_AUTH_ANONYMOUS_ENABLED=true \
    -e GF_AUTH_ANONYMOUS_ORG_ROLE=Admin \
    -e GF_SECURITY_DISABLE_LOGIN_FORM=true \
    -p 3000:3000 -p 9090:9090 \
    -v {{prom_volume}}:/var/lib/prometheus \
    {{image}}

  echo "✅ Metrics stack restarted!"
  echo "🌐 Grafana: http://localhost:3000"
  echo "📊 Prometheus: http://localhost:9090"
