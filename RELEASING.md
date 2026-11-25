# Releasing

This project releases with one command, `just release`, and follows one rule:

> **The commit that is released is exactly the commit CI tested, and everything
> published is built from it in one workflow run.**

The flow is the one used by [cron-when](https://github.com/nbari/cron-when/blob/main/RELEASING.md),
which documents the reasoning in full. This file covers what is specific to `pg_exporter`:
the whole release is one workflow, [`release.yml`](.github/workflows/release.yml), a few
[local actions](.github/actions) and the `bump` and `release` recipes in the
[`.justfile`](.justfile). No tag is ever pushed by hand.

- [Release contract](#release-contract)
- [Daily release](#daily-release)
- [What a release publishes](#what-a-release-publishes)
- [When something fails](#when-something-fails)
- [How the workflow is built](#how-the-workflow-is-built)
- [Settings](#settings)
- [Verifying a release](#verifying-a-release)

## Release contract

1. **Exact commit.** The release builds the commit `develop` points at, and the guard
   refuses it unless the `CI OK` check succeeded on that exact SHA. `CI OK` covers
   formatting, Clippy, the test suite on every supported PostgreSQL major, both feature
   builds, the Linux packages, the container image and the crate.
2. **Tag last.** The `X.Y.Z` tag is created by publishing the GitHub release, after every
   build succeeded. There is never a tag to delete or move.
3. **Versions only go up.** The version must be untagged and above the latest release.
4. **One compiler.** The guard resolves Rust `stable` to an exact version once; every
   Rust job of the run installs that version, and the release notes end with
   `Built with Rust X.Y.Z`.
5. **One build.** The archives, the packages and the container image all hold the same
   two binaries (x86_64 and aarch64), built once in this run.
6. **Not atomic.** The release publishes to GitHub, crates.io and the container registry.
   Each step is safe to repeat, which is what makes a failure recoverable; see
   [When something fails](#when-something-fails).

## Daily release

Work, including dependency updates, lands on `sandbox`.

1. Finish the `## [X.Y.Z]` section of [CHANGELOG.md](CHANGELOG.md); it becomes the release
   notes.
2. As the last commit of the work, bump the version with `just bump` (patch),
   `just bump-minor` or `just bump-major`. The bump refreshes `Cargo.lock`, runs
   `just test` locally and commits `Cargo.toml`, `Cargo.lock` and the dashboard's version
   tag. It refuses to run on `develop` or `main`. Commits must be signed.
3. Push `sandbox`. When **Test & Build** is green there, fast-forward `develop` to that
   same commit and push. `CI OK` is attached to the commit, so the run that passed on
   `sandbox` already satisfies the guard.
4. Run `just release`. It starts the Release workflow on `develop`, follows it, waits for
   `develop`'s own Test & Build run, then moves `main` to the released commit.

```sh
just bump
git push origin sandbox
# wait for Test & Build on sandbox
git push origin sandbox:develop
just release
```

Keep `develop` fast-forward only. If you squash or rebase `sandbox` after its CI passed,
the commit changes and CI has to run again.

The workflow never pushes `main` itself: a protected `main` refuses every update while a
`CI OK` check of the commit is pending or failing, which is exactly the state of
`develop`'s own run right after the push. The recipe waits for it and then pushes from the
operator's machine.

## What a release publishes

For version `X.Y.Z`:

- **GitHub release** `X.Y.Z` (this creates the tag), with:
  - `pg_exporter-X.Y.Z-x86_64-unknown-linux-musl.tar.gz` and
    `pg_exporter-X.Y.Z-aarch64-unknown-linux-musl.tar.gz` (static binaries)
  - `pg_exporter-X.Y.Z-1.x86_64.rpm` and `pg_exporter-X.Y.Z-1.aarch64.rpm`
  - `pg-exporter_X.Y.Z-1_amd64.deb` and `pg-exporter_X.Y.Z-1_arm64.deb`
  - `pg_exporter-X.Y.Z.crate`, the package published to crates.io
  - `SHA256SUMS` for all of them, plus a build-provenance attestation for every file
- **crates.io**: `pg_exporter X.Y.Z`
- **Container image** `ghcr.io/nbari/pg_exporter` for `linux/amd64` and `linux/arm64`,
  tagged `X.Y.Z`, and also `X.Y`, `X` and `latest` when this is the highest release of
  that line. The image is pushed before the release as `sha-<commit>` only; the version
  tags are added to that same digest afterwards.

The release notes are the version's section of `CHANGELOG.md`. Without one, they fall
back to the commit subjects since the previous release.

## When something fails

Fix the cause and use **Re-run failed jobs** on the run. Every step is safe to repeat:
finished jobs are not run again, the Rust version stays the one the guard resolved, a
draft release left by a failed run is replaced, an already published release is kept (and
re-checked), a version crates.io already has is skipped, and tagging the image again
points the tags at the same digest. (Re-running the whole run, or the guard alone,
resolves Rust again and fails on a version that is already tagged.)

- **Before the tag exists** nothing is released. Re-run, or fix the code on `sandbox`,
  advance `develop` and run `just release` again. An image tagged `sha-<commit>` may be
  left in the registry; no version tag points at it.
- **After the tag exists** the release is public, and the crates.io and image-tagging
  jobs can still fail. Transient failures are fixed by re-running. A failure that needs a
  code change is released as the next version; never move the tag.
- **If `just release` stopped before moving `main`**, push it yourself once `develop`'s
  Test & Build is done: `git push origin <released-sha>:refs/heads/main`.
- **The guard refuses** when the version is already tagged (you forgot to bump) or below
  the latest release, when `develop` does not contain `main`, when the run is not on
  `develop`, or when `CI OK` has not succeeded on the commit. The message says which.
- **CI is flaky.** Re-run the failed jobs in Test & Build until `CI OK` is green on the
  commit; the guard accepts a successful `CI OK` on it.
- **crates.io refuses the upload.** Trusted Publishing is probably not configured for
  `release.yml` (see [Settings](#settings)); fix it and re-run the failed job.

## How the workflow is built

```
Release workflow (workflow_dispatch on develop)
 guard          develop only; the version is X.Y.Z, above the latest tag and untagged;
                main is an ancestor; the `CI OK` check succeeded on this exact commit;
                Rust `stable` is resolved to an exact version, used by every Rust job
 build          x86_64 and aarch64, each on a native runner: the binary (run once,
                --version must report the crate's version), the .deb and .rpm (checked
                for version, binary and architecture) and the archive
 crate          cargo package and a publish dry run; the .crate is a release file
 image          a multi-platform image made from the two binaries (the `prebuilt` stage of
                the Containerfile), pushed as sha-<commit>, run once, attested
 release        the exact file set is checked, SHA256SUMS, a build-provenance attestation,
                a draft GitHub release, then publish (this creates the tag at the tested
                commit), then a check that the tag is right and the published files,
                downloaded again, equal the built ones
 publish        crates.io (Trusted Publishing, no stored token)
 image-publish  the version tags are added to the image's digest; `X.Y`, `X` and `latest`
                only when this is the highest release of that line
```

Test & Build runs the same local actions on every push: the packages are built, the
`prebuilt` image is built and run, and the crate goes through `cargo package` and a
publish dry run. A break in any of them fails `CI OK` on the commit, before a release can
start.

## Settings

One-time repository settings the flow relies on.

- **`main`**: protected, requires the `CI OK` check, signed commits and linear history,
  no force pushes or deletions, applies to admins, no required pull request. `just release`
  fast-forwards it from the operator's machine.
- **`develop`**: signed commits and linear history, no force pushes or deletions, no
  required check (it receives the commits CI is about to test; `main` is the gate).
- **A tag ruleset** ("Release tags") that lets `X.Y.Z` tags be created but never moved or
  deleted.
- **Workflow permissions**: the repository default is read-only; each job asks for what
  it needs.
- **crates.io Trusted Publishing** for this repository and the workflow file
  `release.yml`, set on the crate's settings page. Keep that file name stable; it is part
  of the identity that signatures and attestations carry.
- **`release.yml` on `main`.** GitHub only dispatches a workflow whose file exists on the
  default branch. `just release` keeps `main` current after every release.
- **Commit signing**: commits on `sandbox` and `develop` must be signed with a key GitHub
  knows as a signing key, because `main` requires verified signatures.

Releasing needs `just`, `gh` (logged in), `jq`, `cargo set-version` (cargo-edit) and a
signing key.

## Verifying a release

```sh
sha256sum --check --ignore-missing SHA256SUMS
gh attestation verify <file> --repo nbari/pg_exporter --signer-workflow nbari/pg_exporter/.github/workflows/release.yml
gh attestation verify oci://ghcr.io/nbari/pg_exporter:X.Y.Z --repo nbari/pg_exporter --signer-workflow nbari/pg_exporter/.github/workflows/release.yml
```

The release notes also say which Rust version built the files and give the image digest.
The tag is created by GitHub and is not signed; the attestations are what tie a file to
the workflow run that built it.
