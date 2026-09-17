# Native PostgreSQL 18 packages

The release build produces one package containing `pgwrh`, `pgwrh_fdw`, and
`pgwrh_wait`, all at version 0.3.0. Packages install files and dependencies;
they do not create extensions, change server configuration, or restart services.
Only fresh installation is supported.

## Install release assets

Download the asset matching the operating system and CPU from the GitHub release.
Configure the official [PGDG APT](https://www.postgresql.org/download/linux/ubuntu/)
or [PGDG YUM](https://www.postgresql.org/download/linux/redhat/) repository first.
Then use the package manager so dependencies are resolved automatically:

```sh
# Debian/Ubuntu, from the directory containing the downloaded package:
sudo apt install ./postgresql-18-pgwrh_0.3.0-1+*_*.deb

# EL9 (RHEL, Rocky Linux, AlmaLinux):
sudo dnf install ./pgwrh_18-0.3.0-1PGDG.el9."$(uname -m)".rpm
```

The RPM's optional `pgwrh_18-llvmjit` package supplies LLVM bitcode. The Debian
package builds without LLVM bitcode; PostgreSQL can use both native libraries
without it. Debug packages are for debugging and are not needed to install pgwrh.

On subscribers using the wait API, append `pgwrh_wait` to the existing
`shared_preload_libraries` setting and restart PostgreSQL. Configure logical
replication, including `wal_level = logical` on publishers and worker/slot
capacity appropriate to the number of shards. See
[PostgreSQL's configuration guide](https://www.postgresql.org/docs/18/logical-replication-config.html).
Then, as a database administrator:

```sql
CREATE EXTENSION pgwrh CASCADE;
CREATE EXTENSION pgwrh_wait;
```

The first command also creates `pgwrh_fdw` and `pg_background`.
The second enables the optional wait API. For a subscriber needing only that API,
run only `CREATE EXTENSION pgwrh_wait;`; it has no extension dependencies, although
the native package still includes the full bundle and installs its dependencies.
Cluster membership and shard placement are configured separately.

## Build release artifacts locally

Docker BuildKit exports packages to a local directory. The Dockerfiles run
packaging checks, install the resulting package, and exercise both native
libraries against a temporary PostgreSQL cluster before exporting artifacts.
Run from the repository root:

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

For a native Debian build, copy `packaging/deb/debian` to `debian` in an unpacked
release archive, install its declared build dependencies using `apt-get build-dep .`,
then run `dpkg-buildpackage -us -uc -b`. The source uses standard debhelper
packaging; `packaging/deb/debian/source/format` also supports a `3.0 (quilt)` source
package when the corresponding `pgwrh_0.3.0.orig.tar.gz` is placed in the parent
directory. See [RPM packaging](packaging.md) for native RPM builds.

Building these artifacts does not publish them to PGDG or any package repository.
