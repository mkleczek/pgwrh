# Building one package

The root Makefile builds three SQL extensions by default: `pgwrh`, `pgwrh_wait`,
and `pgwrh_fdw`. It builds the wait library through the root PGXS definitions and
delegates the FDW library to `fdw/Makefile`. The FDW is a normal source directory
in release archives; building never fetches Git history or other dependencies.

The combined build currently requires PostgreSQL 18 development files, PGXS,
libpq, a C compiler, GNU Make, and the standard text utilities used to assemble
the pgwrh SQL script. Select one PostgreSQL installation for both components:

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

One source archive, one build, and one staged install can supply a single
architecture-specific `pgwrh_18` RPM. The following is an excerpt for a packager,
not a complete RPM specification or a claim of PGDG acceptance:

```spec
%global pgmajorversion 18
%global pgprefix /usr/pgsql-%{pgmajorversion}

%build
%{__make} %{?_smp_mflags} PG_CONFIG=%{pgprefix}/bin/pg_config

%install
%{__make} install PG_CONFIG=%{pgprefix}/bin/pg_config DESTDIR=%{buildroot}

%files
%{pgprefix}/share/extension/pgwrh*.control
%{pgprefix}/share/extension/pgwrh*--*.sql
%{pgprefix}/lib/pgwrh_fdw.so
%{pgprefix}/lib/pgwrh_wait.so
```

When the selected PGXS build enables LLVM, also package the generated
`lib/bitcode/pgwrh_fdw/`, `lib/bitcode/pgwrh_wait/`, and their `.index.bc` files
under that PostgreSQL prefix. Include the root license, `fdw/LICENSE`,
`fdw/COPYRIGHT`, and the relevant documentation. See `fdw/LICENSING.md` for the
FDW's retained PostgreSQL notices and licensing. The RPM specification supplies
the appropriate PostgreSQL build/runtime dependencies and the `pg_background`
dependency used by pgwrh; the native code makes this an architecture-specific
package. Debug packages are handled by the distribution's normal RPM tooling.

Shipping multiple extension control files does not activate all extensions in
every database. `pgwrh_wait` and `pgwrh_fdw` can each be created independently.
pgwrh depends on `pgwrh_fdw` as well as `postgres_fdw` and `pg_background`.
When building with `WITH_FDW=0`, provide `pgwrh_fdw` separately in the target
PostgreSQL installation. The test staging target includes the bundled FDW's
SQL and library so integration tests use the implementation being developed.
Preloading `pgwrh_wait` remains an explicit server
configuration step; it must happen before relying on the wait API.

Release archives must contain `fdw/` as well as the core SQL and native sources.
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
unrelated files intact. It also checks each native component independently and
both SQL-only installation paths, using
staging directories containing spaces. It never installs into system PostgreSQL.

`test-fdw` runs the context integration suite, retained SQL/isolation regressions,
and exported-symbol audit against private clusters. SCRAM TAP tests additionally
need matching PostgreSQL source test modules and Perl dependencies; see
`fdw/README.md`. Run `make test-wait` with the dependencies and runtime paths
described in [LSN waiting](lsn-wait.md) to exercise the apply-watermark component.
