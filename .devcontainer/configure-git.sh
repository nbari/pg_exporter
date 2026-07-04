#!/usr/bin/env sh
set -eu

# Apply git identity/signing forwarded by scripts/dev-up. These are optional so
# contributors can use their own dotfiles or git config without this script
# overwriting anything unless the corresponding environment variable is present.

if [ "${GIT_USER_NAME:-}" != "" ]; then
    git config --global --replace-all user.name "$GIT_USER_NAME"
fi

if [ "${GIT_USER_EMAIL:-}" != "" ]; then
    git config --global --replace-all user.email "$GIT_USER_EMAIL"
fi

signing_key="${GIT_SIGNING_KEY:-}"
if [ "$signing_key" = "" ]; then
    signing_key="$(git config --global --get user.signingkey 2>/dev/null || true)"
fi

if [ "$signing_key" != "" ]; then
    git config --global --replace-all gpg.format ssh
    # DevPod may install `gpg.ssh.program=devpod-ssh-signature`, but Git's SSH
    # commit signing invokes helpers with OpenSSH `ssh-keygen -Y` style flags.
    # The forwarded SSH agent already exposes the signing key, so force Git to
    # use the native OpenSSH signer instead of DevPod's incompatible helper.
    git config --global --replace-all gpg.ssh.program "$(command -v ssh-keygen || printf '%s' ssh-keygen)"
    git config --global --replace-all user.signingkey "$signing_key"
    git config --global --replace-all commit.gpgsign true

    allowed_signers="$HOME/.config/git/allowed_signers"
    mkdir -p "$(dirname "$allowed_signers")"
    printf '%s %s\n' "${GIT_USER_EMAIL:-$(git config --global --get user.email 2>/dev/null || printf '%s' '*')}" "$signing_key" >"$allowed_signers"
    git config --global --replace-all gpg.ssh.allowedSignersFile "$allowed_signers"

    # DevPod re-injects `gpg.ssh.program=devpod-ssh-signature` into the GLOBAL
    # config on every connect, after this script has run. Pin the native signer
    # at the repository level as well: local config always wins and DevPod
    # never rewrites it.
    if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
        git config --replace-all gpg.ssh.program "$(command -v ssh-keygen || printf %s ssh-keygen)"
    fi
fi
