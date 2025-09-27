test: clippy fmt
  cargo test -- --nocapture

clippy:
  cargo clippy --all -- -W clippy::all -W clippy::nursery -D warnings

fmt:
  cargo fmt --all -- --check

coverage:
  CARGO_INCREMENTAL=0 RUSTFLAGS='-Cinstrument-coverage' LLVM_PROFILE_FILE='coverage-%p-%m.profraw' cargo test
  grcov . --binary-path ./target/debug/deps/ -s . -t html --branch --ignore-not-existing --ignore '../*' --ignore "/*" -o target/coverage/html
  firefox target/coverage/html/index.html
  rm -rf *.profraw

update:
  cargo update

clean:
  cargo clean

check-main:
  @if [ "$(git branch --show-current)" != "main" ]; then echo "Error: Not on main branch"; exit 1; fi

bump: check-main
  cargo set-version --bump patch
  git add .
  git commit -m "bump version to $(cargo metadata --no-deps --format-version 1 | jq -r '.packages[0].version')"
  gbump -t
  git push
  git push --tags

release: update clean test bump
  # podman pull clux/muslrust:stable
  podman run -v $PWD:/volume --rm -t clux/muslrust:stable cargo build --release
