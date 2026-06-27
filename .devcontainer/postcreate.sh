#!/usr/bin/env bash
set -euo pipefail

# One-time provisioning for the pg_exporter dev container (DevPod postCreateCommand):
# system deps, Rust components, and the mise-managed toolchain (just, postgres
# client). The postgres service runs as a compose sibling and is reached over the
# network at postgres:5432 (see compose.yaml), so the app needs no container runtime
# of its own.

export PATH="$HOME/.local/bin:$HOME/.local/share/mise/shims:$PATH"

# Absolute repo root, independent of the caller's CWD (postcreate.sh lives in
# .devcontainer/). Used to invoke repo scripts directly instead of via `mise run`,
# which can resolve paths from an unexpected CWD under MISE_CONFIG_FILE.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Apply optional git identity/signing forwarded from the host by scripts/dev-up.
sh "$REPO_ROOT/.devcontainer/configure-git.sh"

# 1. The cargo/rustup/mise volume mounts can be created as root by the runtime, so
#    take ownership up front before vscode writes into them.
sudo mkdir -p \
    "$HOME/.local/bin" "$HOME/.local/share" "$HOME/.cache" "$HOME/.config"
sudo chown -R "$(id -u):$(id -g)" \
    "$HOME/.local" "$HOME/.cache" "$HOME/.config" \
    /home/vscode/.cargo /home/vscode/.rustup 2>/dev/null || true

# 2. System dependencies (rustls build, no OpenSSL needed; psql/pgbench come from
#    the mise setup-postgres-client task below). zsh is provided by the common-utils
#    devcontainer feature; make it the default shell for the slick prompt + dotfiles.
sudo apt-get update
sudo apt-get install -y \
    build-essential ca-certificates curl delta dnsutils fd-find fzf git gnupg iputils-ping jq \
    libbz2-dev libcap2-bin libffi-dev liblzma-dev libnss3-tools libreadline-dev libsqlite3-dev \
    libssl-dev luarocks make netcat-openbsd openssh-client pkg-config rsync \
    tmux unzip wget xz-utils yq zip zlib1g-dev

command -v zsh >/dev/null 2>&1 && sudo chsh -s "$(command -v zsh)" vscode || true

# 3. mise: installs the toolchain from mise.toml (just, cargo tools, slick, etc.).
#    Be resilient: a single optional tool (e.g. a pipx/cargo package) must not brick
#    the whole workspace. Try the full install, retry once, then fall back to
#    installing the essentials so `just test` always works.
if ! command -v mise >/dev/null 2>&1; then
    curl -fsSL https://mise.run | sh
fi
mise trust --yes
if ! mise install; then
    echo "mise install failed; retrying once..." >&2
    if ! mise install; then
        echo "mise install still failing; installing essential tools individually." >&2
        # These are required for `just test`; the rest are best-effort.
        mise install just uv python || true
        mise install || true
    fi
fi
# Remove tools no longer in mise.toml (notably a previously mise-managed `rust`),
# so the interactive shell's cargo/rustc resolve to the image toolchain below
# rather than stale mise shims.
mise prune --yes || true
mise reshim || true

# Ensure the essential tools are actually present (the rest are best-effort).
export PATH="$HOME/.local/bin:$HOME/.local/share/mise/shims:$PATH"
if ! command -v just >/dev/null 2>&1; then
    echo "ERROR: 'just' is not available after mise install; cannot continue." >&2
    exit 1
fi

# 4. Rust is provided by the base image (devcontainers/rust), not mise. Add the
#    components the project needs (clippy/rustfmt/rust-analyzer) to the image
#    toolchain. Use the image's rustup explicitly so this never hits a mise shim.
IMAGE_RUSTUP="$(command -v rustup || echo /usr/local/cargo/bin/rustup)"
case "$IMAGE_RUSTUP" in
*/.local/share/mise/*) IMAGE_RUSTUP=/usr/local/cargo/bin/rustup ;;
esac
"$IMAGE_RUSTUP" component add rustfmt clippy rust-analyzer

# Make the mise shims available to login/non-login shells.
sudo tee /etc/profile.d/mise.sh >/dev/null <<'EOF'
export PATH="$HOME/.local/bin:$HOME/.local/share/mise/shims:$PATH"
EOF
grep -qxF 'export PATH="$HOME/.local/bin:$HOME/.local/share/mise/shims:$PATH"' ~/.bashrc 2>/dev/null ||
    echo 'export PATH="$HOME/.local/bin:$HOME/.local/share/mise/shims:$PATH"' >>~/.bashrc
grep -qxF 'export PATH="$HOME/.local/bin:$HOME/.local/share/mise/shims:$PATH"' ~/.zshenv 2>/dev/null ||
    echo 'export PATH="$HOME/.local/bin:$HOME/.local/share/mise/shims:$PATH"' >>~/.zshenv

# 5. Install the PostgreSQL client (psql + pgbench). Call the script directly with
#    an absolute path (not `mise run`) so it is independent of the task CWD.
#    setup-postgres-client is required; setup-tig is an optional git browser.
bash "$REPO_ROOT/scripts/install-postgres-client.sh"
mise run setup-tig || echo "setup-tig failed (optional, continuing)." >&2

# 6. Warm the cargo cache.
cargo fetch || true

# 7. Dotfiles (chezmoi). Opt-in via DEVPOD_DOTFILES (forwarded by scripts/dev-up);
#    defaults to the personal devpod dotfiles repo. Brings in shell config, the
#    slick prompt wiring, zinit, mise activation, nvim config, etc.
#
#    This whole step is best-effort: it must NEVER abort postCreate (which would
#    leave a half-provisioned workspace). We run it in a subshell with `set +e` and
#    retry the chezmoi install, so a transient network hiccup doesn't skip dotfiles.
apply_dotfiles() {
    set +e
    dotfiles_repo="${DEVPOD_DOTFILES:-https://github.com/nbari/dotfiles-devpod.git}"
    [ "$dotfiles_repo" != "" ] || {
        echo "No dotfiles repo configured; skipping."
        return 0
    }

    if ! command -v chezmoi >/dev/null 2>&1 && [ ! -x "$HOME/.local/bin/chezmoi" ]; then
        for attempt in 1 2 3; do
            sh -c "$(curl -fsSL get.chezmoi.io)" -- -b "$HOME/.local/bin" && break
            echo "chezmoi install attempt ${attempt} failed; retrying..." >&2
            sleep 3
        done
    fi

    chezmoi_bin="$(command -v chezmoi || echo "$HOME/.local/bin/chezmoi")"
    if [ ! -x "$chezmoi_bin" ]; then
        echo "chezmoi not available; skipping dotfiles (continuing)." >&2
        return 0
    fi

    "$chezmoi_bin" init --apply --force "$dotfiles_repo" ||
        echo "chezmoi dotfiles apply failed (continuing). Re-run later: chezmoi init --apply --force ${dotfiles_repo}" >&2
}
# Run in a subshell so the `set +e` above stays contained to this best-effort step.
(apply_dotfiles)

# Dotfiles may write git config after the first setup pass. Re-apply the forwarded
# identity/signing config last so commit signing stays stable.
sh "$REPO_ROOT/.devcontainer/configure-git.sh"

echo "✓ postCreate complete: toolchain ready (just, rustfmt, clippy, psql, pgbench, slick)."
echo "  PostgreSQL is at \$PG_HOST:\$PG_PORT (postgres:5432). Run: just test"
