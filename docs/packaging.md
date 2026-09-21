# Building and packaging pgwrh

This guide is for source installations and package maintainers. For prebuilt
packages and database activation, see [native installation](packages.md). The
[component overview](../README.md#components) explains each extension's role.

## Build from source

The root Makefile builds four PostgreSQL extensions by default: `pgwrh`,
`pgwrh_ui`, `pgwrh_wait`, and `pgwrh_fdw`. It delegates to a separate Makefile
in each extension directory: `pgwrh/`, `pgwrh_ui/`, `pgwrh_wait/`, and
`pgwrh_fdw/`. Each owns its control file, SQL scripts, and any native sources.
All tests live under `test/`. Building from a release archive never fetches Git
history or other dependencies.

The combined build requires matching PostgreSQL 18 or 19 development files, PGXS,
libpq, a C compiler, GNU Make, a POSIX shell, `sha384sum` (coreutils) or
`shasum`, and the standard text utilities used to assemble the pgwrh SQL script,
plus the TLS/GSSAPI development libraries used by the selected PostgreSQL
installation.

Python 3 is needed by the packaging checks and integration tests, including the
RPM/DEB test phases. It is not needed to build `pgwrh_ui` or embed its assets.
Select one PostgreSQL installation for all components:

```sh
make -j4 PG_CONFIG=/usr/pgsql-18/bin/pg_config
make install PG_CONFIG=/usr/pgsql-18/bin/pg_config DESTDIR=/tmp/pgwrh-package
```

`PG_CONFIG` determines PostgreSQL's library and extension directories. `DESTDIR`
is prepended only during installation; never replace the runtime PostgreSQL
prefix with the RPM build root. Command-line compiler/linker overrides propagate
to recursive make. Installation creates directories but does not start a server,
create extensions, edit configuration, or enable preloading. Omit `DESTDIR` to
install directly into the selected PostgreSQL installation; that requires write
access to its directories. Then follow [database
activation](packages.md#configure-postgresql-and-enable-extensions).

## Build package artifacts locally

Docker BuildKit exports packages to a local directory. The Dockerfiles run
packaging checks, install the resulting package, and exercise both native
libraries against a temporary PostgreSQL cluster before exporting artifacts. Run
from the repository root:

```sh
docker build -f packaging/deb/Dockerfile \
  --build-arg BASE_IMAGE=ubuntu:24.04 \
  --output type=local,dest=.build/packages/noble .

docker build -f packaging/deb/Dockerfile \
  --build-arg BASE_IMAGE=ubuntu:26.04 \
  --output type=local,dest=.build/packages/resolute .

docker build -f packaging/deb/Dockerfile \
  --build-arg BASE_IMAGE=debian:trixie-slim \
  --output type=local,dest=.build/packages/trixie .

docker build -f packaging/rpm/Dockerfile \
  --output type=local,dest=.build/packages/el9 .
```

Use `--platform linux/amd64` or `--platform linux/arm64` to select the target;
prefer native builders for release validation. For RPMs without LLVM, pass
`--build-arg WITH_LLVM=0`. DEB versions include the distribution codename to
prevent ambiguity between artifacts for different distributions.

For a native Debian build, copy `packaging/deb/debian` to `debian` in an
unpacked release archive, install its declared build dependencies using `apt-get
build-dep .`, then run `dpkg-buildpackage -us -uc -b`. The source uses standard
debhelper packaging; `packaging/deb/debian/source/format` also supports a `3.0
(quilt)` source package when the corresponding `pgwrh_1.0.0~alpha1.orig.tar.gz` is
placed in the parent directory. See [RPM integration](#pgdg-rpm-integration) for
native RPM builds.

Building these artifacts does not publish them to PGDG or any package
repository.

For prereleases, upstream tags and SQL scripts use `1.0.0-alpha1`, while DEB and
RPM versions use `1.0.0~alpha1` to sort before final `1.0.0`. The RPM spec's
`upstream_version` macro keeps source and SQL filenames separate from its
package `Version`. The release checker verifies both representations.

## PGDG RPM integration

The complete spec is [`packaging/rpm/pgwrh.spec`](../packaging/rpm/pgwrh.spec).
It follows the [PGDG packaging
repository](https://github.com/pgdg-packaging/pgdg-rpms), using the conventions
in its [pg_cron
spec](https://github.com/pgdg-packaging/pgdg-rpms/blob/e866357bcca63bc9c659d11571ab98b277746860/rpm/redhat/main/non-common/pg_cron/main/pg_cron.spec),
[pg_background
spec](https://github.com/pgdg-packaging/pgdg-rpms/blob/e866357bcca63bc9c659d11571ab98b277746860/rpm/redhat/main/non-common/pg_background/main/pg_background.spec),
and [pglogical
spec](https://github.com/pgdg-packaging/pgdg-rpms/blob/e866357bcca63bc9c659d11571ab98b277746860/rpm/redhat/main/non-common/pglogical/main/pglogical.spec).
These references were checked on 2026-09-16. This is an upstream packaging
recipe; publication in the PGDG repository is a separate maintainer action.

The spec builds the following architecture-specific packages:

| Package | Contents |
| --- | --- |
| `pgwrh_18` | All four extensions, two shared libraries, installation SQL, licenses, and documentation |
| `pgwrh_18-llvmjit` | LLVM bitcode and indexes for both shared libraries; requires exactly the same version/release of `pgwrh_18` |

Standard RPM tooling also generates debug packages where enabled. The release
uses the `1PGDG%{?dist}` convention. Extension files are installed below
`/usr/pgsql-18`, independent of whether the architecture normally uses
`/usr/lib64`. The main package requires `postgresql18-server`,
`postgresql18-libs`, and `pg_background_18 >= 2.0.3`. The bundled FDW replaces the
stock `postgres_fdw`, so `postgresql18-contrib` is not required.

The spec accepts the macros used by PGDG's build system:

| Macro | Default | Purpose |
| --- | --- | --- |
| `pgmajorversion` | `18` | PostgreSQL major version: `18` or `19`; other majors are rejected |
| `pginstdir` | `/usr/pgsql-18` | Versioned PostgreSQL installation prefix |
| `llvm` | `1` | Set to `0` to omit the LLVM subpackage and pass `with_llvm=no` to every build/install/check invocation |

For local builds on a supported Fedora/RHEL-family system, first configure the
PGDG repository, then install `rpm-build`, `dnf-plugins-core`, and
`pgdg-srpm-macros`. On RHEL derivatives, the normal PGDG prerequisites also
apply: enable CRB/CodeReady Builder as appropriate and disable the
distribution's PostgreSQL module where one exists. Install the spec's build
dependencies with:

```sh
sudo dnf builddep --define 'pgmajorversion 18' packaging/rpm/pgwrh.spec
```

The source archive must include the reorganized extension directories and
`test/check-install.py`. `Source0` names the eventual `v1.0.0-alpha1` release archive;
that tag has not been published as part of this change. To build a development
snapshot, export the desired commit with the release-compatible archive prefix:

```sh
mkdir -p "$HOME/rpmbuild/SOURCES"
git archive --format=tar.gz --prefix=pgwrh-1.0.0-alpha1/ \
  --output="$HOME/rpmbuild/SOURCES/pgwrh-1.0.0-alpha1.tar.gz" HEAD
rpmbuild -ba --define 'pgmajorversion 18' packaging/rpm/pgwrh.spec
```

With Jujutsu, replace `HEAD` with the commit ID of the intended change, obtained
using `jj log -r @ --no-graph -T commit_id`; Git's `HEAD` may point at its
parent. Use a distinct snapshot release number when distributing unreleased
builds. The spec checks that its version matches all four extension control
files before building. Each extension ships only its `1.0.0-alpha1` installation
script; no upgrade scripts or earlier installable versions are included.

To build without LLVM, pass the same setting to dependency resolution and RPM:

```sh
sudo dnf builddep --define 'pgmajorversion 18' --define 'llvm 0' packaging/rpm/pgwrh.spec
rpmbuild -ba --define 'pgmajorversion 18' --define 'llvm 0' packaging/rpm/pgwrh.spec
```

The `%check` phase runs the existing packaging suite, including combined,
SQL-only, and standalone installation/uninstallation into temporary directories.
It neither starts PostgreSQL nor writes into the system installation. The RPM
has no service-management or database-modifying scriptlets: installation does
not restart PostgreSQL, enable preloading, or issue `CREATE EXTENSION`.

Earlier build results are retained in [historical packaging
validation](development/packaging-validation.md).

Shipping multiple extension control files does not activate all extensions in
every database. `pgwrh_wait` and `pgwrh_fdw` can each be created independently.
pgwrh depends on `pgwrh_fdw` and `pg_background`. Both controller and shard
connections use `pgwrh_fdw`; the stock `postgres_fdw` extension is not required.
When building with `WITH_FDW=0`, provide `pgwrh_fdw` separately in the target
PostgreSQL installation. The test staging target includes the bundled FDW's SQL
and library so integration tests use the implementation being developed.
Preloading `pgwrh_wait` remains an explicit server configuration step; it must
happen before relying on the wait API.

Release archives must contain `pgwrh/`, `pgwrh_ui/`, `pgwrh_wait/`, and
`pgwrh_fdw/`, together with the root Makefile. Include `test/` to run the
verification suites. No submodule initialization or separate pgwrh_fdw release
download is required. Archive a reviewed release commit, including its subtree,
rather than assembling sources from independent checkouts at package-build time.

## Build variants

Both `WITH_FDW` and `WITH_LSN_WAIT` default to `1` in the PGXS build. Set either
to `0` to omit that component. To install only the SQL extensions, `pgwrh` and
`pgwrh_ui` (provide the required PostgreSQL 18 FDW separately):

```sh
make WITH_FDW=0 WITH_LSN_WAIT=0 install PG_CONFIG=/path/to/pg_config
```

`NO_PGXS=1` defaults both native components to `0` and supports staged SQL-only
installation and uninstallation. Explicitly requesting a native component with
`NO_PGXS=1` fails. The [Nix package](nix.md) builds the complete PostgreSQL 18
bundle. Use the same component options when building, installing, and
uninstalling. Run `make clean` before changing the PostgreSQL installation used
for compilation.

## PostgreSQL 19 preview packages

From this development checkout, select the major explicitly:

```sh
docker build -f packaging/deb/Dockerfile --build-arg PG_MAJOR=19 \
  --build-arg 'PGDG_COMPONENTS=main 19' --output type=local,dest=dist .
docker build -f packaging/container/Dockerfile --build-arg PG_MAJOR=19 \
  --build-arg POSTGRES_IMAGE=postgres:19beta3-trixie -t pgwrh:pg19-preview .
```

For a direct Debian build, copy `packaging/deb/debian` to `debian`, then run
`python3 packaging/deb/select-major.py 19 debian` before installing build
dependencies. The RPM spec accepts `--define 'pgmajorversion 19'`; its container
accepts `PG_MAJOR=19` and `PGDG_TESTING=1`. RPM publication for 19 is waiting for
PGDG's `pg_background_19 >= 2.0.3` package. Both majors require `pg_background`
2.0.3 or newer. See the [release checklist](development/postgres-versions.md).

## Verification

```sh
make test-packaging PG_CONFIG=/path/to/postgresql-18/bin/pg_config
make test-fdw PG_CONFIG=/path/to/postgresql-18/bin/pg_config
```

`test-packaging` requires only Python 3's standard library. It cleans build
outputs, performs a parallel staged installation, checks all four extensions and
both libraries, and verifies that uninstall removes the payload while leaving
unrelated files intact. It also checks each native component independently, both
SQL-only installation paths, and standalone installation from each extension
directory. Native LLVM installation uses paths without spaces because PGXS
splits paths while creating nested bitcode directories. Separate SQL-only and
combined non-LLVM cases check staging paths containing spaces. The suite never
installs into system PostgreSQL.

`test-fdw` runs the context integration suite, retained SQL/isolation
regressions, and exported-symbol audit against private clusters. SCRAM TAP tests
additionally need matching PostgreSQL source test modules and Perl dependencies;
see [FDW tests](development/testing.md#foreign-data-wrapper). Run `make
test-wait` with the dependencies and runtime paths described in [wait
tests](development/testing.md#replication-visibility) to exercise the
apply-watermark component.
