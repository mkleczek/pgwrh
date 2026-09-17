# Native PostgreSQL 18 packages

The package contains all four [pgwrh extensions](../README.md#components) at
version 1.0.0-alpha1, plus their documentation and console setup files. It installs
files and dependencies; enabling extensions and configuring PostgreSQL are
separate steps. Only fresh installation is supported.

## Install release assets

After release publication, download the asset matching the operating system and
CPU from the [GitHub
release](https://github.com/mkleczek/pgwrh/releases/tag/v1.0.0-alpha1). To build
packages before publication, see [local package
builds](packaging.md#build-package-artifacts-locally). Configure the official
[PGDG APT](https://www.postgresql.org/download/linux/ubuntu/) or [PGDG
YUM](https://www.postgresql.org/download/linux/redhat/) repository first. Then
use the package manager so dependencies are resolved automatically:

```sh
# Debian/Ubuntu, from the directory containing the downloaded package:
sudo apt install ./postgresql-18-pgwrh_1.0.0~alpha1-1+*_*.deb

# EL9 (RHEL, Rocky Linux, AlmaLinux):
sudo dnf install ./pgwrh_18-1.0.0~alpha1-1PGDG.el9."$(uname -m)".rpm
```

The RPM's `pgwrh_18-llvmjit` companion package is optional. Debug packages are
also optional and are not needed for normal operation.

DEB/RPM use `1.0.0~alpha1` so package managers order the alpha before final
`1.0.0`. PostgreSQL reports extension version `1.0.0-alpha1`. Package ordering
does not imply a supported database upgrade: this alpha supports fresh
installations only.

## Install from the signed project repository

**The alpha is installed from release assets, as above.** Prerelease publication
does not update the stable project repository. The endpoints below become
available after a stable release publishes to GitHub Pages; they do not provide
alpha1. They are the project's repositories; configure PGDG separately for
PostgreSQL and `pg_background`. The alpha includes a signed repository archive
for [separate hosting](releasing.md#build-a-signed-repository-elsewhere); substitute
that host's base URL to opt into it.

On Debian 13 or Ubuntu 24.04/26.04:

```sh
sudo install -d -m 755 /etc/apt/keyrings
curl -fsSL https://mkleczek.github.io/pgwrh/pgwrh.asc | sudo tee /etc/apt/keyrings/pgwrh.asc >/dev/null
. /etc/os-release
echo "deb [signed-by=/etc/apt/keyrings/pgwrh.asc] https://mkleczek.github.io/pgwrh/apt $VERSION_CODENAME main" | sudo tee /etc/apt/sources.list.d/pgwrh.list
sudo apt update
sudo apt install postgresql-18-pgwrh
```

On EL9, save this as `/etc/yum.repos.d/pgwrh.repo`:

```ini
[pgwrh]
name=pgwrh for PostgreSQL 18
baseurl=https://mkleczek.github.io/pgwrh/rpm/el9/$basearch
enabled=1
gpgcheck=1
repo_gpgcheck=1
gpgkey=https://mkleczek.github.io/pgwrh/pgwrh.asc
```

Then run `sudo dnf install pgwrh_18`. The release includes the public key and a
signed checksum manifest for verifying downloaded release assets as well. After
database activation, `psql -X -d your_database -f docs/check-installation.sql`
from a source checkout reports missing prerequisites for the full bundle,
including the optional wait API.

## Configure PostgreSQL and enable extensions

Enable `pgwrh` in the controller and each replica database. In logical
replication, the source is the publisher and the receiver is a subscriber; see
[cluster concepts](overview.md) for these roles.

On subscribers using the optional `pgwrh_wait` visibility API, append
`pgwrh_wait` to the existing `shared_preload_libraries` setting and restart
PostgreSQL. Configure logical replication, including `wal_level = logical` on
publishers and worker/slot capacity appropriate to the number of shards. See
[PostgreSQL's configuration
guide](https://www.postgresql.org/docs/18/logical-replication-config.html).
Then, as a database administrator:

```sql
CREATE EXTENSION pgwrh CASCADE;
-- Optional, after preloading pgwrh_wait and restarting PostgreSQL:
CREATE EXTENSION pgwrh_wait;
```

The first command also creates `pgwrh_fdw` and `pg_background`. The second
enables the optional wait API. For a subscriber needing only that API, run only
`CREATE EXTENSION pgwrh_wait;`; it has no extension dependencies, although the
native package still includes the full bundle and installs its dependencies.
Cluster membership and shard placement are configured separately.

Enable the optional console only in the controller database, after enabling
`pgwrh`:

```sql
CREATE EXTENSION pgwrh_ui;
```

Run PostgREST separately and follow the [console deployment
guide](../pgwrh_ui/README.md) for access roles and web-service configuration.
