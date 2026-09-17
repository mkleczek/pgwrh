# Historical packaging validation


The results below were recorded with the 0.3.0 development version before
the 1.0.0 version bump. The release workflow must validate the 1.0.0 artifacts
before publication.

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
