# Publishing 0.3.0

The release pipeline builds the same reviewed source archive for every target:

- EL9 RPMs, including the optional LLVM package: x86_64 and aarch64.
- Debian 13 and Ubuntu 24.04/26.04 DEBs: amd64 and arm64.
- PostgreSQL 18 container: linux/amd64 and linux/arm64.
- Nix installation checks: Linux and macOS, both CPU architectures.

Every native package is installed in a clean runtime container. Tests first create
`pgwrh_wait` alone and exercise its API without any other extension dependencies.
They then explicitly create `pgwrh CASCADE`, verify version 0.3.0 and absence of
upgrade paths for each of `pgwrh`, `pgwrh_fdw`, and `pgwrh_wait`, and call both
native libraries. They verify that the controller uses `pgwrh_fdw` and that
`postgres_fdw` is not enabled. They also check both installation orders and
confirm that dropping `pgwrh` leaves the wait API usable. The container additionally runs the
three-node Compose example and repeats setup to check reuse. The repository test
uses an ephemeral key to check APT indexes, RPM signatures, and YUM metadata.

## Validate without publishing

Open a pull request or manually run **Release packages** in Actions. These events
build and test only. Before tagging, run `python3 packaging/check-release.py`.
The check requires VERSION, extension controls, installation scripts, Nix, RPM,
DEB, container metadata and Compose references to agree. It also verifies that
the core declares its dependencies and that `pgwrh_wait` and `pgwrh_fdw` declare
none. No 0.2.2 release or migration/upgrade scripts are produced.

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

## Release

Create `v0.3.0` from the reviewed commit and publish its GitHub release. The
`release: published` event runs the complete validation matrix. Only after all
checks pass does it sign and publish:

- Source tarball, DEBs, signed RPMs, public signing key, and signed SHA256SUMS
  attached to the release. Complete build outputs, including SRPMs and debug
  artifacts, are also retained as workflow artifacts.
- The tested images under `ghcr.io/mkleczek/pgwrh:0.3.0-pg18`, with a combined
  AMD64/ARM64 manifest and individual architecture tags. No image is rebuilt
  between the demo test and publication.
- Signed APT and YUM repositories to GitHub Pages. A repository archive is also
  attached to the release for alternative static hosting.

The workflow does not upload packages to the upstream PGDG repositories.
Their acceptance is a separate process. Each repository publication currently
contains this release only, matching the single-version 0.3.0 policy. Before a
future release, decide how to retain older packages and update the SQL-version
policy. Base OS package repositories are resolved at build time; `.buildinfo`
files record DEB build dependencies, and the Nix dependency graph is locked.

## Build a signed repository elsewhere

On a Linux host with Python 3, `apt-ftparchive` (apt-utils), RPM/rpmsign,
`createrepo_c`, and GnuPG, import the signing key into a dedicated GNUPGHOME and run:

```sh
python3 packaging/repository/build.py downloaded-artifacts public-repository \
  --key FULL_SIGNING_KEY_FINGERPRINT
```

The output directory must not already exist. Upload its contents to an HTTPS
static host. It contains `apt/dists`, `apt/pool`, `rpm/el9`, and `pgwrh.asc`.
The builder never uploads anything and does not alter input packages.

## Validation performed while preparing this packaging

On 2026-09-17, installed-extension checks passed for Nix on macOS ARM64 and Linux
ARM64, DEBs on Ubuntu 24.04 (AMD64 and ARM64), Ubuntu 26.04 and Debian 13 (ARM64),
and EL9 RPMs on ARM64 (with LLVM) and AMD64 (without LLVM). The NixOS module was
also evaluated for an x86_64 Linux host. The ARM64 Compose image passed fresh
initialization and repeated setup, with identical row counts, sums, and content
checksums on both replicas. Signed APT discovery and RPM/YUM signature checks
passed with an ephemeral test key. Workflow syntax and shell steps passed actionlint.

Standalone wait installation, both installation orders, and continued wait API
operation after removing the core passed on macOS ARM64 Nix, the ARM64 container,
Ubuntu 24.04 ARM64 DEBs, and EL9 ARM64 RPMs with LLVM. The 51-test wait suite
passed, as did the explicit core/wait coexistence reinstallation check and fresh
Compose setup. The release guard rejected an injected dependency on `pgwrh`.

All 54 core tests passed with controller and shard connections using `pgwrh_fdw`
and no `postgres_fdw` extension installed. The macOS ARM64 Nix check and EL9
ARM64 RPM build with LLVM passed with the reduced dependency set; the built
RPM's metadata no longer requires `postgresql18-contrib`.

The local emulated x86_64 LLVM 21 linker segfaulted on a trivial standalone input
as well as during RPM bitcode indexing. Its native x86_64 CI validation remains
required; the release workflow uses native runners and does not bypass this
check. The remaining architectures in the matrix are likewise validated by CI
before publication. These local results do not imply that a release was published.
