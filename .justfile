# Get the current user's UID and GID
uid := `id -u`
gid := `id -g`
container_cmd := `if [ -n "${PG_EXPORTER_CONTAINER_CMD:-}" ]; then echo "${PG_EXPORTER_CONTAINER_CMD}"; elif command -v podman >/dev/null 2>&1; then echo "podman"; else echo "docker"; fi`

default: test
  @just --list

# Test suite
test: clippy fmt
  @echo "🧪 Checking PostgreSQL..."
  @if ! {{container_cmd}} ps --filter "name=pg_exporter_postgres" --format "{{{{.Names}}}}" | grep -q "pg_exporter_postgres"; then \
    echo "🚀 PostgreSQL container not running, starting it..."; \
    just postgres; \
    echo "⏳ Waiting for PostgreSQL to be ready..."; \
    sleep 3; \
    timeout 30 bash -c 'until psql -h localhost -p 5432 -U postgres -d postgres -c "SELECT 1" &>/dev/null; do sleep 1; done' || (echo "❌ PostgreSQL failed to start" && exit 1); \
    echo "✅ PostgreSQL is ready"; \
  else \
    echo "✅ PostgreSQL container is already running"; \
  fi
  @echo "🧪 Running setup check..."
  @if [ -f scripts/setup-local-test-db.sh ]; then \
    scripts/setup-local-test-db.sh || (echo "❌ Test database setup failed. Fix the issues above before running tests." && exit 1); \
  fi
  @echo "🔧 Using local test database (overriding .envrc)..."
  PG_EXPORTER_DSN="postgresql://postgres:postgres@localhost:5432/postgres" cargo test -- --nocapture

# Run only the replication topology integration test (primary+replica via testcontainers)
test-replica:
    #!/usr/bin/env bash
    set -euo pipefail

    docker_host="${DOCKER_HOST:-}"

    if [[ -z "${docker_host}" ]]; then
        if [[ -S /var/run/docker.sock ]]; then
            docker_host="unix:///var/run/docker.sock"
        elif [[ -n "${XDG_RUNTIME_DIR:-}" && -S "${XDG_RUNTIME_DIR}/podman/podman.sock" ]]; then
            docker_host="unix://${XDG_RUNTIME_DIR}/podman/podman.sock"
        elif [[ -S "/run/user/$(id -u)/podman/podman.sock" ]]; then
            docker_host="unix:///run/user/$(id -u)/podman/podman.sock"
        else
            echo "❌ No Docker/Podman socket found for testcontainers" >&2
            echo "Set DOCKER_HOST, e.g.:" >&2
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
  cargo clippy --all-targets --all-features

# Formatting check
fmt:
  cargo fmt --all -- --check

# Coverage report
coverage:
  CARGO_INCREMENTAL=0 RUSTFLAGS='-Cinstrument-coverage' LLVM_PROFILE_FILE='coverage-%p-%m.profraw' cargo test
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

# Check if on develop branch
check-develop:
    #!/usr/bin/env bash
    current_branch=$(git branch --show-current)
    if [[ "$current_branch" != "develop" ]]; then
        echo "❌ Not on develop branch (currently on: $current_branch)"
        echo "Switch to develop branch first: git checkout develop"
        exit 1
    fi
    echo "✅ On develop branch"

# Check if tag already exists for a given version
check-tag-not-exists version:
    #!/usr/bin/env bash
    set -euo pipefail
    version="{{version}}"

    git fetch --tags --quiet

    if git rev-parse -q --verify "refs/tags/${version}" >/dev/null 2>&1; then
        echo "❌ Tag ${version} already exists!"
        exit 1
    fi

    echo "✅ No tag exists for version ${version}"

_bump bump_kind: check-develop check-clean clean update test
    #!/usr/bin/env bash
    set -euo pipefail

    bump_kind="{{bump_kind}}"

    cleanup() {
        status=$?
        if [ $status -ne 0 ]; then
            echo "↩️  Restoring version files after failure..."
            git checkout -- Cargo.toml Cargo.lock >/dev/null 2>&1 || true
        fi
        exit $status
    }
    trap cleanup EXIT

    previous_version=$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[0].version')
    echo "ℹ️  Current version: ${previous_version}"

    echo "🔧 Bumping ${bump_kind} version..."
    cargo set-version --bump "${bump_kind}"
    new_version=$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[0].version')
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

    echo "🧹 Running clean build..."
    cargo clean

    echo "🧪 Running tests with new version (via just test)..."
    just test

    git add .
    git commit -m "bump version to ${new_version}"
    git push origin develop
    echo "✅ Version bumped and pushed to develop"

# Bump version and commit (patch level)
bump:
    @just _bump patch

# Bump minor version
bump-minor:
    @just _bump minor

# Bump major version
bump-major:
    @just _bump major

# Internal function to handle the merge and tag process
_deploy-merge-and-tag:
    #!/usr/bin/env bash
    set -euo pipefail

    new_version=$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[0].version')
    echo "🚀 Starting deployment for version $new_version..."

    # Double-check tag doesn't exist (safety check)
    echo "🔍 Verifying tag doesn't exist..."
    git fetch --tags --quiet
    if git rev-parse -q --verify "refs/tags/${new_version}" >/dev/null 2>&1; then
        echo "❌ Tag ${new_version} already exists on remote!"
        echo "This should not happen. The tag may have been created in a previous run."
        exit 1
    fi

    # Ensure develop is up to date
    echo "🔄 Ensuring develop is up to date..."
    git pull origin develop

    # Switch to main and merge develop
    echo "🔄 Switching to main branch..."
    git checkout main
    git pull origin main

    echo "🔀 Merging develop into main..."
    if ! git merge develop --no-edit; then
        echo "❌ Merge failed! Please resolve conflicts manually."
        git checkout develop
        exit 1
    fi

    # Create signed tag
    echo "🏷️  Creating signed tag $new_version..."
    git tag -s "$new_version" -m "Release version $new_version"

    # Push main and tag atomically
    echo "⬆️  Pushing main branch and tag..."
    if ! git push origin main "$new_version"; then
        echo "❌ Push failed! Rolling back..."
        git tag -d "$new_version"
        git checkout develop
        exit 1
    fi

    # Switch back to develop
    echo "🔄 Switching back to develop..."
    git checkout develop

    echo "✅ Deployment complete!"
    echo "🎉 Version $new_version has been released"
    echo "📋 Summary:"
    echo "   - develop branch: bumped and pushed"
    echo "   - main branch: merged and pushed"
    echo "   - tag $new_version: created and pushed"
    echo "🔗 Monitor release: https://github.com/nbari/pg_exporter/actions"

# Deploy: merge to main, tag, and push everything
deploy: bump _deploy-merge-and-tag

# Deploy with minor version bump
deploy-minor: bump-minor _deploy-merge-and-tag

# Deploy with major version bump
deploy-major: bump-major _deploy-merge-and-tag

# Create & push a test tag like t-YYYYMMDD-HHMMSS (skips publish/release in CI)
# Usage:
#   just t-deploy
#   just t-deploy "optional tag message"
t-deploy message="CI test": check-develop check-clean test
    #!/usr/bin/env bash
    set -euo pipefail

    message="{{message}}"
    ts="$(date -u +%Y%m%d-%H%M%S)"
    tag="t-${ts}"

    echo "🏷️  Creating signed test tag: ${tag}"
    git fetch --tags --quiet

    if git rev-parse -q --verify "refs/tags/${tag}" >/dev/null; then
        echo "❌ Tag ${tag} already exists. Aborting." >&2
        exit 1
    fi

    git tag -s "${tag}" -m "${message}"
    git push origin "${tag}"

    echo "✅ Pushed ${tag}"
    echo "🧹 To remove it:"
    echo "   git push origin :refs/tags/${tag} && git tag -d ${tag}"

# Watch for changes and run
watch:
  cargo watch -x 'run -- --collector.vacuum --collector.activity --collector.locks --collector.database --collector.stat --collector.replication --collector.index --collector.statements --collector.exporter --collector.tls -v'

# get metrics curl
curl:
  curl -s 0:9432/metrics

# Run a live pgbench workload for local metrics testing
workload duration="60" clients="5" scale="10" db="pgbench_test":
    #!/usr/bin/env bash
    set -euo pipefail

    duration="{{duration}}"
    clients="{{clients}}"
    scale="{{scale}}"
    db="{{db}}"

    duration="${duration#duration=}"
    clients="${clients#clients=}"
    scale="${scale#scale=}"
    db="${db#db=}"

    if ! command -v pgbench >/dev/null 2>&1; then
        echo "❌ pgbench not found in PATH"
        echo "Install postgresql-contrib (or equivalent) to use this recipe."
        exit 1
    fi

    if ! psql -h localhost -p 5432 -U postgres -d postgres -c "SELECT 1" >/dev/null 2>&1; then
        echo "❌ PostgreSQL is not reachable on localhost:5432"
        echo "Start it first with: just postgres"
        exit 1
    fi

    echo "🔧 Ensuring pgbench dataset exists (scale=${scale})..."
    ./scripts/setup-local-test-db.sh --pgbench --pgbench-scale "${scale}"

    echo "🚀 Running pgbench workload against ${db} for ${duration}s with ${clients} clients..."
    pgbench -h localhost -p 5432 -U postgres -c "${clients}" -T "${duration}" "${db}"

# Create table churn and run a manual vacuum for vacuum-related collector testing
vacuum-workflow scale="20" rounds="5" sample_mod="5" db="pgbench_test" table="pgbench_accounts":
    #!/usr/bin/env bash
    set -euo pipefail

    scale="{{scale}}"
    rounds="{{rounds}}"
    sample_mod="{{sample_mod}}"
    db="{{db}}"
    table="{{table}}"

    scale="${scale#scale=}"
    rounds="${rounds#rounds=}"
    sample_mod="${sample_mod#sample_mod=}"
    db="${db#db=}"
    table="${table#table=}"

    bash ./scripts/run-vacuum-workflow.sh \
        --scale "${scale}" \
        --rounds "${rounds}" \
        --sample-mod "${sample_mod}" \
        --db "${db}" \
        --table "${table}"

# Create churn and wait for PostgreSQL autovacuum to clean it up without manual VACUUM
autovacuum-workflow scale="20" rounds="5" sample_mod="5" timeout="180" poll="5" naptime="5s" db="pgbench_test" table="pgbench_accounts":
    #!/usr/bin/env bash
    set -euo pipefail

    scale="{{scale}}"
    rounds="{{rounds}}"
    sample_mod="{{sample_mod}}"
    timeout="{{timeout}}"
    poll="{{poll}}"
    naptime="{{naptime}}"
    db="{{db}}"
    table="{{table}}"

    scale="${scale#scale=}"
    rounds="${rounds#rounds=}"
    sample_mod="${sample_mod#sample_mod=}"
    timeout="${timeout#timeout=}"
    poll="${poll#poll=}"
    naptime="${naptime#naptime=}"
    db="${db#db=}"
    table="${table#table=}"

    bash ./scripts/run-autovacuum-workflow.sh \
        --scale "${scale}" \
        --rounds "${rounds}" \
        --sample-mod "${sample_mod}" \
        --timeout "${timeout}" \
        --poll "${poll}" \
        --naptime "${naptime}" \
        --db "${db}" \
        --table "${table}"

postgres version="latest":
  mkdir -p db/log/postgres
  {{container_cmd}} run --rm -d --name pg_exporter_postgres \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_HOST_AUTH_METHOD=trust \
    -e PGDATA=/db/data/{{ version }} \
    -p 5432:5432 \
    -v $(pwd)/db:/db \
    -v $(pwd)/db/config/postgres:/etc/postgresql/config \
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

# Test against all PostgreSQL versions (14-18)
test-all-pg:
    #!/usr/bin/env bash
    set -euo pipefail

    VERSIONS=(14 15 16 17 18)
    FAILED=()

    echo "🚀 Starting all PostgreSQL versions..."
    for v in "${VERSIONS[@]}"; do
        PORT="54${v}"
        {{container_cmd}} run -d --name pg${v} \
            -e POSTGRES_PASSWORD=postgres \
            -e POSTGRES_USER=postgres \
            -p ${PORT}:5432 \
            postgres:${v}-alpine >/dev/null 2>&1 || true
    done

    echo "⏳ Waiting for PostgreSQL instances to be ready..."
    sleep 5

    for v in "${VERSIONS[@]}"; do
        PORT="54${v}"
        timeout 30 bash -c "until {{container_cmd}} exec pg${v} pg_isready -U postgres >/dev/null 2>&1; do sleep 1; done" || true
    done

    echo ""
    for v in "${VERSIONS[@]}"; do
        PORT="54${v}"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo "🐘 Testing PostgreSQL ${v} (port ${PORT})"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

        if PG_EXPORTER_DSN="postgresql://postgres:postgres@localhost:${PORT}/postgres" \
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
    PORT="54{{version}}"
    echo "🐘 Starting PostgreSQL {{version}} on port ${PORT}..."
    {{container_cmd}} run -d --name pg{{version}} \
        -e POSTGRES_PASSWORD=postgres \
        -e POSTGRES_USER=postgres \
        -p ${PORT}:5432 \
        postgres:{{version}}-alpine

    echo "⏳ Waiting for PostgreSQL to be ready..."
    sleep 3
    timeout 30 bash -c "until {{container_cmd}} exec pg{{version}} pg_isready -U postgres >/dev/null 2>&1; do sleep 1; done"

    echo "🧪 Running tests..."
    PG_EXPORTER_DSN="postgresql://postgres:postgres@localhost:${PORT}/postgres" cargo test

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
