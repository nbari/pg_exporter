# Get the current user's UID and GID
uid := `id -u`
gid := `id -g`

default: test
  @just --list

# Test suite
test: clippy fmt
  cargo test -- --nocapture

# Linting
clippy:
  cargo clippy --all-targets --all-features -- -D warnings

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

# Bump version and commit (patch level)
bump: check-develop check-clean update clean test
    #!/usr/bin/env bash
    echo "🔧 Bumping patch version..."
    cargo set-version --bump patch
    new_version=$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[0].version')
    echo "📝 New version: $new_version"

    git add .
    git commit -m "bump version to $new_version"
    git push origin develop
    echo "✅ Version bumped and pushed to develop"

# Bump minor version
bump-minor: check-develop check-clean update clean test
    #!/usr/bin/env bash
    echo "🔧 Bumping minor version..."
    cargo set-version --bump minor
    new_version=$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[0].version')
    echo "📝 New version: $new_version"

    git add .
    git commit -m "bump version to $new_version"
    git push origin develop
    echo "✅ Version bumped and pushed to develop"

# Bump major version
bump-major: check-develop check-clean update clean test
    #!/usr/bin/env bash
    echo "🔧 Bumping major version..."
    cargo set-version --bump major
    new_version=$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[0].version')
    echo "📝 New version: $new_version"

    git add .
    git commit -m "bump version to $new_version"
    git push origin develop
    echo "✅ Version bumped and pushed to develop"

# Internal function to handle the merge and tag process
_deploy-merge-and-tag:
    #!/usr/bin/env bash
    new_version=$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[0].version')
    echo "🚀 Starting deployment for version $new_version..."

    # Switch to main and merge develop
    echo "🔄 Switching to main branch..."
    git checkout main
    git pull origin main

    echo "🔀 Merging develop into main..."
    git merge develop

    # Create and push tag
    echo "🏷️  Creating tag v$new_version..."
    git tag -a "$new_version" -m "Release version $new_version"

    # Push everything
    echo "⬆️  Pushing main branch and tags..."
    git push origin main
    git push origin "$new_version"

    # Switch back to develop
    echo "🔄 Switching back to develop..."
    git checkout develop

    echo "✅ Deployment complete!"
    echo "🎉 Version $new_version has been released"
    echo "📋 Summary:"
    echo "   - develop branch: bumped and pushed"
    echo "   - main branch: merged and pushed"
    echo "   - tag $new_version: created and pushed"

# Deploy: merge to main, tag, and push everything
deploy: bump _deploy-merge-and-tag

# Deploy with minor version bump
deploy-minor: bump-minor _deploy-merge-and-tag

# Deploy with major version bump
deploy-major: bump-major _deploy-merge-and-tag

# Watch for changes and run
watch:
  cargo watch -x 'run -- --collector.vacuum --collector.activity -v'

# get metrics curl
curl:
  curl -s 0:9432/metrics

postgres version="latest":
  mkdir -p db/log/postgres
  podman run --rm -d --name pg_exporter_postgres \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_HOST_AUTH_METHOD=trust \
    -e PGDATA=/db/data/{{ version }} \
    -p 5432:5432 \
    -v $(pwd)/db:/db \
    -v $(pwd)/db/config/postgres:/etc/postgresql/config \
    --userns keep-id:uid={{ uid }},gid={{ gid }} \
    --user {{ uid }}:{{ gid }} \
    --userns keep-id:uid=999,gid=999 \
    --user 999:999 \
    postgres:{{ version }} \
    postgres -c config_file=/etc/postgresql/config/postgresql.conf

jaeger:
  podman run --rm -d --name jaeger \
    -e COLLECTOR_OTLP_ENABLED=true \
    -p 16686:16686 \
    -p 4317:4317 \
    -p 4318:4318 \
    jaegertracing/all-in-one:latest

stop-containers:
  @for c in pg_exporter_postgres jaeger; do \
        podman stop $$c 2>/dev/null || true; \
  done
