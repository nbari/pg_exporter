test: clippy fmt
  cargo test -- --nocapture

clippy:
  cargo clippy --all-targets --all-features -- -D warnings

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

bump: update clean test
  cargo set-version --bump patch
  git add .
  git commit -m "bump version to $(cargo metadata --no-deps --format-version 1 | jq -r '.packages[0].version')"

release: bump
  # podman pull clux/muslrust:stable
  podman run -v $PWD:/volume --rm -t clux/muslrust:stable cargo build --release
