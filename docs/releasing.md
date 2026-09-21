# Publishing 1.0.0-alpha1

This guide is for release maintainers. For installation, see the
[project guide](../README.md#installation).

Alpha1 is a testing prerelease. Preparation and validation are safe to run
before publication; creating the tag, preparing a GitHub draft and publishing
are separate, deliberate steps below.

## Version and distribution policy

`VERSION`, extension versions and the future Git tag use `1.0.0-alpha1` (tag
`v1.0.0-alpha1`). Native package metadata uses `1.0.0~alpha1`, which sorts before
stable `1.0.0`. The release tooling accepts `X.Y.Z` and the prerelease forms
`X.Y.Z-alphaN`, `X.Y.Z-betaN` and `X.Y.Z-rcN`, with positive sequence numbers.

Alpha releases must be marked **Pre-release** on GitHub. Draft creation sets
this automatically and excludes them from **Latest**; publication rejects a
prerelease flag that disagrees with `VERSION`. Alpha publication distributes
versioned images, downloadable packages and a signed repository archive. Only
stable releases deploy the APT/YUM repository on GitHub Pages, so existing
repository users do not opt into an alpha automatically.

## Release validation

The release pipeline builds the same reviewed source archive for every target:

- EL9 RPMs, including the optional LLVM package: x86_64 and aarch64.
- Debian 13 and Ubuntu 24.04/26.04 DEBs: amd64 and arm64.
- PostgreSQL 18 and 19 preview containers: linux/amd64 and linux/arm64.
- Nix installation checks: Linux and macOS, both CPU architectures.

Every native package is installed in a clean runtime container. Tests first create
`pgwrh_wait` alone and exercise its API without any other extension dependencies.
They then explicitly create `pgwrh CASCADE`, verify version 1.0.0-alpha1 and absence of
upgrade paths for each of `pgwrh`, `pgwrh_ui`, `pgwrh_fdw`, and `pgwrh_wait`, and call both
native libraries. They render the UI and check its embedded htmx, JavaScript,
and CSS assets. Staged installation tests verify the packaged UI setup guide,
role scripts, PostgREST configuration, and htmx license. The UI remains an
optional controller-only extension; packages do not start PostgREST.
They verify that the controller uses `pgwrh_fdw` and that
`postgres_fdw` is not enabled. They also check both installation orders and
confirm that dropping `pgwrh` leaves the wait API usable. The container additionally runs the
documented Compose quickstart with the read-only PostgREST console and repeats setup to check reuse.
The release is also gated on the complete core, wait, and UI suites, including
real PostgREST HTTP tests with no skips, plus FDW regressions and upstream SCRAM
TAP tests. These run against the same source archive used by every build. The repository test
uses an ephemeral key to check APT indexes, RPM signatures, and YUM metadata.

## Validate without publishing

Open a pull request or manually run **Release packages** in Actions. These events
build and test only when **prepare_draft** is left unchecked. Before tagging, run `python3 packaging/check-release.py`.
The check requires VERSION, extension controls, installation scripts, Nix, RPM,
DEB, container metadata and Compose references to agree. It also verifies that
the core declares its dependencies and that `pgwrh_wait` and `pgwrh_fdw` declare
none. It also verifies the UI dependency on `pgwrh` and its deployment files.
Only fresh 1.0.0-alpha1 installation scripts are produced; no earlier release or
migration/upgrade scripts are included.

## One-time repository configuration

1. Enable GitHub Pages with **GitHub Actions** as its deployment source.
2. In the `release` environment, provide `PACKAGE_SIGNING_KEY`: an ASCII-armored
   private GPG signing key usable noninteractively, and the variable
   `PACKAGE_SIGNING_FINGERPRINT`: its full fingerprint. Use a dedicated release
   signing key. The workflow imports it into a temporary keyring and removes
   that keyring when finished. Never put the key in the source repository.
3. Permit the workflow token to publish GHCR packages. After first publication,
   ensure the `pgwrh` container package is public so anonymous pulls work.

GitHub environment protection rules, if configured by the repository owner,
apply normally. Building or merging the packaging changes does not publish them.

## Prepare a draft before publishing

1. Review [the 1.0.0-alpha1 release notes](releases/1.0.0-alpha1.md) and a successful validation
   run on the intended commit. Keep the version tag fixed once artifacts are built.
2. Create and push `v1.0.0-alpha1` at that commit. This alone does not publish anything.
3. Run **Release packages** on that tag with **prepare_draft** checked. The workflow
   rejects branch runs in this mode, runs every test and build, signs repositories
   and checksums, verifies the checksums/signature, and attaches artifacts to a
   **draft prerelease** on GitHub using the checked-in release notes. An existing
   public release or a draft with the wrong prerelease status is rejected. The
   `release` environment and its signing key are required.
4. Download the draft source, packages and repository archive. Verify
   `SHA256SUMS.asc` using the independently confirmed signing fingerprint, then
   run `sha256sum --check SHA256SUMS`. Inspect package names, architectures, the
   release notes and the complete Actions results. Images from this run remain
   workflow artifacts; GHCR and Pages are untouched.

Manual validation with **prepare_draft** unchecked needs no production signing
key. The repository smoke test uses a disposable signing key, checks signed APT
package discovery against `VERSION`, and verifies RPM and YUM signatures.

## Publish

Keep **Pre-release** checked and **Set as the latest release** unchecked when
publishing the reviewed alpha draft through GitHub. The `release: published` event reruns
all validation and builds before signing and uploading the following:

- Source tarball, DEBs, signed RPMs, public signing key and signed SHA256SUMS.
  Complete outputs, including SRPMs and debug artifacts, remain workflow artifacts.
- Tested images at `ghcr.io/mkleczek/pgwrh:1.0.0-alpha1-pg18`, with a combined AMD64/ARM64
  manifest and individual architecture tags. No image is rebuilt between its
  quickstart test and publication within that run.
- A signed APT/YUM repository archive attached to the release for separate
  static hosting. Alpha publication leaves the stable GitHub Pages repository
  untouched; stable releases also deploy their repository there.

The public run rebuilds artifacts from the same tag; it does not promote the
exact draft bytes. Base OS repositories are resolved at build time, so bytes
may differ. The public run replaces draft attachments with the artifacts it
actually tested. A failed run leaves the GitHub release visible, but blocks
artifact publication until checks pass; inspect Actions before announcing it.

The workflow does not upload packages to upstream PGDG. Each repository
publication currently contains this release only. DEB `.buildinfo` files record
build dependencies, and the Nix dependency graph is locked.

## Build a signed repository elsewhere

On a Linux host with Python 3, `apt-ftparchive` (apt-utils), RPM/rpmsign,
`createrepo_c`, and GnuPG, import the signing key into a dedicated GNUPGHOME and run:

```sh
python3 packaging/repository/build.py downloaded-artifacts public-repository \
  --version "$(cat VERSION)" --key FULL_SIGNING_KEY_FINGERPRINT
```

The output directory must not already exist. Upload its contents to an HTTPS
static host. It contains `apt/dists`, `apt/pool`, `rpm/el9`, and `pgwrh.asc`.
The builder never uploads anything and does not alter input packages.

## Request upstream PGDG packaging

Once alpha1 is published and its native builds have passed, a packaging request
can point maintainers to the immutable tag, source archive, license, dependency
list, DEB recipe and RPM spec. State clearly that this is an opt-in testing
prerelease and that database upgrades are not yet supported. Acceptance and
repository placement are decisions for the PGDG maintainers; the project
release does not depend on that process. Keep using the project's release assets
for field testing while the request is reviewed.

## Validation evidence

Use the successful **Release packages** run for the exact release commit as the
release record. Local checks and earlier runs do not replace the native matrix.
[Historical packaging results](development/packaging-validation.md) are retained
as development notes, separate from the release procedure.

## Development preparation for PostgreSQL 19

Current development builds include independent FDWs for 18 and 19 Beta 3 and
use pg_background 2.0.3 on both. The expanded matrix produces DEBs, container
images (`-pg18` and `-pg19`) and Nix bundles. PostgreSQL 19 RPM publication is
waiting for PGDG's pg_background package; the corresponding spec is ready.
Follow the [version checklist](development/postgres-versions.md) before targeting
PostgreSQL 19 final. Published alpha1 artifacts are not replaced by these changes.
