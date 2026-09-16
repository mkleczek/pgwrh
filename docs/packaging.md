# Building one package

The root Makefile builds three SQL extensions by default: `pgwrh`, `pgwrh_wait`,
and `pgwrh_fdw`. It delegates to a separate Makefile in each extension directory:
`pgwrh/`, `pgwrh_wait/`, and `pgwrh_fdw/`. Each owns its control file, SQL scripts,
and any native sources. All tests live under `test/`. Building from a release
archive never fetches Git history or other dependencies.

The combined build currently requires PostgreSQL 18 development files, PGXS,
libpq, a C compiler, GNU Make, and the standard text utilities used to assemble
the pgwrh SQL script. Select one PostgreSQL installation for all components:

```sh
make -j4 PG_CONFIG=/usr/pgsql-18/bin/pg_config
make install PG_CONFIG=/usr/pgsql-18/bin/pg_config DESTDIR=/tmp/pgwrh-package
```

`PG_CONFIG` determines PostgreSQL's library and extension directories. `DESTDIR`
is prepended only during installation; never replace the runtime PostgreSQL
prefix with the RPM build root. Command-line compiler/linker overrides propagate
to recursive make. Installation creates directories but does not start a server,
create extensions, edit configuration, or enable preloading.

## PGDG RPM integration

The complete spec is [`packaging/rpm/pgwrh.spec`](../packaging/rpm/pgwrh.spec).
It follows the [PGDG packaging repository](https://github.com/pgdg-packaging/pgdg-rpms),
using the conventions in its
[pg_cron spec](https://github.com/pgdg-packaging/pgdg-rpms/blob/e866357bcca63bc9c659d11571ab98b277746860/rpm/redhat/main/non-common/pg_cron/main/pg_cron.spec),
[pg_background spec](https://github.com/pgdg-packaging/pgdg-rpms/blob/e866357bcca63bc9c659d11571ab98b277746860/rpm/redhat/main/non-common/pg_background/main/pg_background.spec),
and [pglogical spec](https://github.com/pgdg-packaging/pgdg-rpms/blob/e866357bcca63bc9c659d11571ab98b277746860/rpm/redhat/main/non-common/pglogical/main/pglogical.spec).
These references were checked on 2026-09-16. This is an upstream packaging recipe;
publication in the PGDG repository is a separate maintainer action.

The spec builds the following architecture-specific packages:

| Package | Contents |
| --- | --- |
| `pgwrh_18` | All three extensions, two shared libraries, installation/upgrade SQL, licenses, and documentation |
| `pgwrh_18-llvmjit` | LLVM bitcode and indexes for both shared libraries; requires exactly the same version/release of `pgwrh_18` |

Standard RPM tooling also generates debug packages where enabled. The release
uses the `1PGDG%{?dist}` convention. Extension files are installed below
`/usr/pgsql-18`, independent of whether the architecture normally uses `/usr/lib64`.
The main package requires `postgresql18-server`, `postgresql18-libs`,
and `pg_background_18 >= 1.6`. The bundled FDW replaces the stock `postgres_fdw`,
so `postgresql18-contrib` is not required.

The spec accepts the macros used by PGDG's build system:

| Macro | Default | Purpose |
| --- | --- | --- |
| `pgmajorversion` | `18` | PostgreSQL major version; other majors are rejected because the bundled native code targets PostgreSQL 18 |
| `pginstdir` | `/usr/pgsql-18` | Versioned PostgreSQL installation prefix |
| `llvm` | `1` | Set to `0` to omit the LLVM subpackage and pass `with_llvm=no` to every build/install/check invocation |

For local builds on a supported Fedora/RHEL-family system, first configure the
PGDG repository, then install `rpm-build`, `dnf-plugins-core`, and
`pgdg-srpm-macros`. On RHEL derivatives, the normal PGDG prerequisites also apply:
enable CRB/CodeReady Builder as appropriate and disable the distribution's
PostgreSQL module where one exists. Install the spec's build dependencies with:

```sh
sudo dnf builddep --define 'pgmajorversion 18' packaging/rpm/pgwrh.spec
```

The source archive must include the reorganized extension directories and
`test/check-install.py`. `Source0` names the eventual `v0.2.2` release archive;
that tag has not been published as part of this change. To build a development
snapshot, export the desired commit with the release-compatible archive prefix:

```sh
mkdir -p "$HOME/rpmbuild/SOURCES"
git archive --format=tar.gz --prefix=pgwrh-0.2.2/ \
  --output="$HOME/rpmbuild/SOURCES/pgwrh-0.2.2.tar.gz" HEAD
rpmbuild -ba --define 'pgmajorversion 18' packaging/rpm/pgwrh.spec
```

With Jujutsu, replace `HEAD` with the commit ID of the intended change, obtained
using `jj log -r @ --no-graph -T commit_id`; Git's `HEAD` may point at its parent.
Use a distinct snapshot release number when distributing unreleased builds.
The spec checks that its version matches `pgwrh/pgwrh.control` before building.

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

Validated on AlmaLinux 9 (aarch64) with PGDG PostgreSQL 18.6: both the default
LLVM build and the `llvm=0` build produced binary and source RPMs, with all nine
packaging checks passing in each build. Installing the default RPMs resolved
`pg_background_18` 2.0.3. A private PostgreSQL cluster with `pgwrh_wait` preloaded
loaded all three bundled extensions and executed the FDW's native
connection-inspection function. Activate the bundle explicitly with
`CREATE EXTENSION pgwrh CASCADE; CREATE EXTENSION pgwrh_wait;`.

Shipping multiple extension control files does not activate all extensions in
every database. `pgwrh_wait` and `pgwrh_fdw` can each be created independently.
pgwrh depends on `pgwrh_fdw` and `pg_background`. Both controller and shard
connections use `pgwrh_fdw`; the stock `postgres_fdw` extension is not required.
When building with `WITH_FDW=0`, provide `pgwrh_fdw` separately in the target
PostgreSQL installation. The test staging target includes the bundled FDW's
SQL and library so integration tests use the implementation being developed.
Preloading `pgwrh_wait` remains an explicit server
configuration step; it must happen before relying on the wait API.

Release archives must contain `pgwrh/`, `pgwrh_wait/`, and `pgwrh_fdw/`,
together with the root Makefile. Include `test/` to run the verification suites.
No submodule initialization or separate pgwrh_fdw release download is required.
Archive a reviewed release commit, including its subtree, rather than assembling
sources from independent checkouts at package-build time.

## Build variants

Both `WITH_FDW` and `WITH_LSN_WAIT` default to `1` in the PGXS build. Set either
to `0` to omit that component. For the existing PostgreSQL 16/17 SQL-only build:

```sh
make WITH_FDW=0 WITH_LSN_WAIT=0 install PG_CONFIG=/path/to/pg_config
```

`NO_PGXS=1` defaults both native components to `0` and supports staged SQL-only
installation and uninstallation. Explicitly requesting a native component with
`NO_PGXS=1` fails. The older locked Nix package selects both SQL-only flags.
Use the same component options when building, installing, and uninstalling.
Run `make clean` before changing the PostgreSQL installation used for compilation.

## Verification

```sh
make test-packaging PG_CONFIG=/path/to/postgresql-18/bin/pg_config
make test-fdw PG_CONFIG=/path/to/postgresql-18/bin/pg_config
```

`test-packaging` requires only Python 3's standard library. It cleans build
outputs, performs a parallel staged installation, checks all three extensions
and both libraries, and verifies that uninstall removes the payload while leaving
unrelated files intact. It also checks each native component independently,
both SQL-only installation paths, and standalone installation from each extension
directory. Native LLVM installation uses paths without spaces because PGXS
splits paths while creating nested bitcode directories. Separate SQL-only and
combined non-LLVM cases check staging paths containing spaces. The suite never
installs into system PostgreSQL.

`test-fdw` runs the context integration suite, retained SQL/isolation regressions,
and exported-symbol audit against private clusters. SCRAM TAP tests additionally
need matching PostgreSQL source test modules and Perl dependencies; see
`pgwrh_fdw/README.md`. Run `make test-wait` with the dependencies and runtime paths
described in [LSN waiting](lsn-wait.md) to exercise the apply-watermark component.
