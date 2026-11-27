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
   refuses it unless the `CI OK` job that completed last for that exact SHA, in any
   Test & Build run or re-run attempt, succeeded. `CI OK` covers formatting, Clippy, the
   test suite on every supported PostgreSQL major, both feature builds, both
   architectures, the Linux packages, the container image and the crate. `just release`
   tells the run which commit it means, so a `develop` that moved in the meantime is
   refused, not released.
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
operator's machine. It asks for `CI OK` itself, not for the whole run: coverage is
reported, not a gate, so a failed coverage job does not keep `main` back.

The guard and the recipe both get that answer from
[`scripts/ci-ok-status`](scripts/ci-ok-status), which reads the `CI OK` jobs of the
Test & Build workflow's own runs, every attempt of them, and goes by the one that
completed last. A failed run that is being re-run therefore still counts as failed until
the new attempt has finished. A check that only carries the name, published by another
workflow or integration, does not count.

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
re-checked), a version crates.io already has is not uploaded again but its checksum is
still compared with the crate this run released, and tagging the image again points the
tags at the same digest. (Re-running the whole run, or the guard alone, resolves Rust
again and fails on a version that is already tagged.)

- **Before the tag exists** nothing is released. Re-run, or fix the code on `sandbox`,
  advance `develop` and run `just release` again. The image is pushed as `sha-<commit>`
  only after both binaries and the crate were built and the image itself was run, so
  what can be left in the registry by a later failure is a tested image that no version
  tag points at.
- **After the tag exists** the release is public, and the crates.io and image-tagging
  jobs can still fail. Transient failures are fixed by re-running. A failure that needs a
  code change is released as the next version; never move the tag.
- **If `just release` stopped before moving `main`**, move it yourself, but only when
  both are true: the Release run succeeded for that commit, and `scripts/ci-ok-status
  nbari/pg_exporter <released-sha>` prints `success`. Then
  `git push origin <released-sha>:refs/heads/main`.
- **The guard refuses** when the version is already tagged (you forgot to bump) or below
  the latest release, when `develop` does not contain `main`, when the run is not on
  `develop`, when `develop` is no longer at the commit `just release` asked for, or when
  the `CI OK` job that completed last for the commit did not succeed. The
  message says which.
- **`just release` says no single release run appeared.** Look at the Actions page
  before doing anything else. The recipe only follows a run that carries exactly the
  title it asked for, on `develop`, for the commit it read; a run it could not match may
  still exist, and if `develop` had moved, its guard will have refused it.
- **CI is flaky.** Re-run the failed jobs in Test & Build until `CI OK` is green on the
  commit. The commit is tested on `sandbox` and again on `develop`; the most recently
  completed `CI OK` decides, for the guard and for the protected `main` alike.
- **crates.io refuses the upload.** Trusted Publishing is probably not configured for
  `release.yml` (see [Settings](#settings)); fix it and re-run the failed job.
- **crates.io already holds this version with other bytes.** Three checks, and what is
  public differs at each. The crate job checks before the image is pushed: nothing is
  public. The release job checks again before it creates the release: the image is
  already in the registry as `sha-<commit>`, with its attestation, but there is no tag,
  no release and no version tag on the image. The publish job compares after the upload:
  the GitHub release is public. In every case bump the version; in the last one also
  yank the crate, since a crate version cannot be replaced.

## How the workflow is built

```
Release workflow (workflow_dispatch on develop)
 guard          develop only, and at the commit `just release` asked for; the version is
                X.Y.Z, above the latest tag and untagged; main is an ancestor; `CI OK`
                is green in the Test & Build job that completed last for this commit;
                Rust `stable` is resolved to an exact version, used by every Rust job
 build          x86_64 and aarch64, each on a native runner: the binary (run once,
                --version must report the crate's version), the .deb (checked for its
                version, binary and architecture), the .rpm (checked by its file name)
                and the archive
 crate          cargo package and a publish dry run; crates.io must not already hold this
                version with other bytes; the .crate is a release file
 image          after build and crate: a multi-platform image made from the two binaries
                (the `prebuilt` stage of the Containerfile), run before it is pushed as
                sha-<commit>, run again from the registry, attested
 release        the exact file set is checked; crates.io is asked once more; SHA256SUMS,
                a build-provenance attestation,
                a draft GitHub release, then publish (this creates the tag at the tested
                commit; it is marked Latest only when it is the highest version), then a
                check that the tag is right and the published files, downloaded again,
                equal the built ones
 publish        crates.io (Trusted Publishing, no stored token), then a check that the
                checksum crates.io reports is that of the released .crate
 image-publish  the version tags are added to the image's digest; `X.Y`, `X` and `latest`
                only when this is the highest release of that line
```

Test & Build runs the same local actions on every push, on the same two runners: both
binaries are built and run, the packages and archives are made, the two-platform
`prebuilt` image is built and exported (not pushed), its index is checked and its amd64
image run, and the crate goes through `cargo package` and a publish dry run. A break in
any of them fails `CI OK` on the commit, before a release can start. What only a release
does is publish: push the image, attest, create the GitHub release, upload to crates.io
and move the image tags.

Every action in the workflows is pinned to a commit. [Dependabot](.github/dependabot.yml)
proposes version updates as pull requests into `sandbox`. Only the tests run on those
pull requests; the full Test & Build, with `CI OK`, runs once the change is on `sandbox`.

Dependabot **security** updates are different: GitHub opens them against the default
branch, `main`, whatever the configuration says. `main` only ever fast-forwards to a
released commit, so such a pull request cannot be merged. Apply the same update on
`sandbox`, release it the normal way, and close the pull request.

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
  default branch (`gh workflow run` otherwise answers `HTTP 404: workflow release.yml not
  found on the default branch`). `just release` keeps `main` current after every release.
  A repository that has never released this way must put the workflow there once, before
  its first `just release`, by fast-forwarding `main` to a commit whose `CI OK` passed:
  `git push origin <sha>:refs/heads/main`.
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
