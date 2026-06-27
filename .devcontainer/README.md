# pg_exporter DevPod / Dev Containers workspace

A **portable contributor setup** based on [DevPod](https://devpod.sh) and the
[Dev Containers](https://containers.dev) spec. The default `scripts/dev-up` path is
optimized for a local rootless podman/Docker host, with Linux Atomic
(e.g. fedora-atomic) as the main target. A portable config is also provided for
Docker/remote providers that should not load local-only Podman and 1Password mounts.

## Design: one self-contained compose stack

The devcontainer is **Docker Compose-based**: a single compose project
(`compose.yaml`) defines the **app** dev container *and* the **postgres** service it
needs. `devpod up` brings both up together, so the environment is fully
self-contained — no host-side database or dependency management.

```
   host: podman (or docker) + devpod + docker compose v2
     │  devpod up .
     ▼
 ┌──────────── compose project "pg_exporter" ────────────┐
 │  app  (service "app", user vscode)                    │
 │   └─ rust + mise (just, postgres client, cargo-edit)  │
 │                                                       │
 │  postgres:5432  (pg_stat_statements preloaded)        │
 └───────────────────────────────────────────────────────┘
```

Inside the project network the app reaches the database **by name** at
`postgres:5432` (container-to-container). There is **no host port-forward** in the
path, which means:

- no rootless-podman/`pasta` bulk-`COPY` stalls (client-side `pgbench` just works),
- no SELinux `:Z` juggling (handled by `security_opt: label=disable` + a named
  volume for the data),
- the app container needs **no container runtime of its own** — it only talks to
  postgres over the network.

So `just test` and the full local workflow run out of the box, with no podman in the
toolbox, no `:Z`, and no `PGBENCH_INIT_STEPS` override.

## Files

| File | Purpose |
| --- | --- |
| `compose.yaml` | The stack: `app` + `postgres` (pg_stat_statements preloaded). |
| `compose.podman.yaml` | Local override: `userns_mode: keep-id` so the bind-mounted workspace stays editable as `vscode` under rootless podman. |
| `devcontainer.json` | Local compose-based devcontainer (`compose.yaml` + podman override). |
| `devcontainer.portable.json` | Portable compose-based devcontainer (`compose.yaml` only). |
| `init-db.sql` | First-boot SQL: `CREATE EXTENSION pg_stat_statements`. |
| `postcreate.sh` | One-time provisioning: system deps, rustup components, `mise install`, dotfiles. |
| `post-start.sh` | Every start: wait for postgres, seed the test DB. |
| `../scripts/dev-ssh` | Host helper for entering or running commands in `/workspaces/pg_exporter`. |

The host entrypoint `../scripts/dev-up` wraps `devpod up` with the right flags
(`--ssh-config`, git identity forwarding, and `DEVPOD_DOTFILES`).

The toolchain is declared in [`../mise.toml`](../mise.toml) (just, rust, cargo-edit,
the [slick](https://github.com/nbari/slick) prompt, tree-sitter, and the postgres
client task). Neovim is installed via a devcontainer feature, and `postcreate.sh`
applies your dotfiles with [chezmoi](https://chezmoi.io) (repo from `DEVPOD_DOTFILES`,
default `https://github.com/nbari/dotfiles-devpod.git`) so the shell, prompt, and
nvim config match your host. `zsh` is the default shell.

## Usage

### Local (Linux / macOS / fedora-atomic)

```bash
scripts/dev-up               # build + start the stack, exec-ready
scripts/dev-ssh              # shell in as vscode, in /workspaces/pg_exporter
# inside the container:
just test                    # runs against the postgres service (no host DB needed)
```

`scripts/dev-up` runs `devpod up . --ide none --id pg-exporter --ssh-config
"$HOME/.ssh/devpod"`, forwards your git identity and optional `DEVPOD_DOTFILES`,
and uses the host 1Password SSH agent when present. Plain `devpod up .` works too
for the local config.

If you manage identity in `.envrc`, load it before starting DevPod:

```bash
export GIT_USER_NAME="nbari"
export GIT_USER_EMAIL="nbari@tequila.io"
export GIT_SIGNING_KEY="ssh-ed25519 ..."
scripts/dev-up
```

When those variables are present, the workspace configures:

```bash
git config --global user.name "$GIT_USER_NAME"
git config --global user.email "$GIT_USER_EMAIL"
git config --global gpg.format ssh
git config --global gpg.ssh.program "$(command -v ssh-keygen)"
git config --global user.signingkey "$GIT_SIGNING_KEY"
git config --global commit.gpgsign true
```

Or in VS Code: **Dev Containers: Reopen in Container**.

#### SSH config (`--ssh-config`)

DevPod writes its managed SSH host entries to a **dedicated file** (`~/.ssh/devpod`)
instead of editing your main `~/.ssh/config`. Add this line **once** to
`~/.ssh/config` so `ssh` and VS Code Remote-SSH can resolve the DevPod hosts:

```
Include ~/.ssh/devpod
```

Override the path with `DEVPOD_SSH_CONFIG=/path/to/file scripts/dev-up`.
`--ssh-config` is a `devpod up` option in DevPod v0.6.x; use `scripts/dev-ssh` or
`devpod ssh pg-exporter --workdir /workspaces/pg_exporter` to enter the workspace.

> **Local-focused.** This config targets a local container runtime (rootless
> podman / Docker Desktop). `compose.podman.yaml` applies `userns_mode: keep-id`
> and binds the host 1Password agent, which are local-only — running this exact
> config against a remote (docker) DevPod provider would not work as-is. Use
> `devcontainer.portable.json` for that path.

### Portable / remote provider

Use the portable config when the provider should not load local-only rootless
Podman or 1Password settings:

```bash
devpod up . --devcontainer-path .devcontainer/devcontainer.portable.json \
  --ide none --id pg-exporter
devpod ssh pg-exporter --workdir /workspaces/pg_exporter
```

The portable config still starts `app` + `postgres`; it only omits
`compose.podman.yaml`.

## Dashboards (Prometheus + Grafana) — on-demand

The devcontainer can run the full observability stack so you can iterate on
`grafana/dashboard.json` end-to-end. Prometheus and Grafana are defined in
`compose.yaml` behind the **`observability` compose profile**, so a plain
`devpod up` (which starts only `app` + `postgres`) stays lean — they are **not**
started by default.

```bash
# inside the container: run the exporter so app:9432 has data
just watch

# on the host: bring up Prometheus + Grafana (joins the devcontainer network)
just metrics-dev          # -> Grafana http://localhost:3000, Prometheus :9090
just metrics-dev-stop     # stop them when done
```

Why this is the agnostic approach:

- Prometheus scrapes the exporter by **service name** (`app:9432`) over the shared
  compose network — no `host.containers.internal`, host-gateway, or `127.0.0.1`
  forwarding tricks. It behaves identically on Linux, macOS, and fedora-atomic.
- Grafana provisions its datasource (`http://prometheus:9090`) and a **file-based
  dashboard provider** that **bind-mounts `grafana/dashboard.json`**
  (`updateIntervalSeconds: 10`), so editing the JSON **hot-reloads in Grafana within
  ~10s — no image rebuild, no restart**.

`just metrics-dev` (via `scripts/metrics-dev`) discovers the project name DevPod
created and attaches Prometheus/Grafana to that exact network. Note Grafana/Prometheus
publish host ports `3000`/`9090`, so stop the host-side bundled `just metrics` first if
it is running (it uses the same ports).

## How the database connection is wired

`compose.yaml` sets these in the `app` service, and the tooling honors them
(defaulting to `localhost` outside the container, so the host workflow is unchanged):

| Env | In-container value |
| --- | --- |
| `PG_HOST` | `postgres` |
| `PG_PORT` | `5432` |
| `PG_EXPORTER_DSN` | `postgresql://postgres:postgres@postgres:5432/postgres` |

## Notes

- **Multi-arch:** the base images (`devcontainers/rust`, `postgres`) are multi-arch,
  so Apple Silicon (arm64) and Linux (amd64/arm64) all work natively. Named volumes
  for the DB data and `target/` also make macOS builds fast.
- **Replication test:** the testcontainers-based `replica_topology` test needs a
  Docker/Podman socket; without one in the app container it **skips gracefully**, and
  the rest of the suite runs. To run it, expose a socket to the app container
  (e.g. a docker-outside-of-docker setup) — see `.github/workflows/test.yml` for the
  CI configuration.
