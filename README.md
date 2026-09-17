# pgwrh

An extension implementing sharding for PostgreSQL based on logical replication and the bundled pgwrh_fdw.
The goal is to scale **_read queries_** overcoming main limitation of traditional setups based on streaming replication and hot standbys:
lack of sharding and large storage requirements.

See [Architecture](https://github.com/mkleczek/pgwrh/wiki/Architecture) for more information on inner workings.

Start with the [tested quickstart](docs/containers.md) for a controller, two replicas,
and a read-only browser console.
See the [1.0.0 release notes](docs/releases/1.0.0.md) for the validation matrix and
limitations, and [controller recovery](docs/recovery.md) for backup and restore.

# Features

## Horizontal Scalability and High Availability
### No need for rebalancing
Setting up and maintaining a highly available cluster of sharded storage servers is inherently tricky, especially during changes to cluster topology.
Adding a new replica often requires rebalancing (ie. reorganizing data placement among replicas).

_pgwrh_ minimizes the need to copy data by utilizing _Weighted Randezvous Hashing_ algorithm to distribute shards among replicas.
Within each availability zone, adding a host preserves the relative WRH ranking
of existing hosts. Changes to AZ shares or the requested copy count can also move
copies between zones.
### Data redundancy
_pgwrh_ maintains requested level of redundancy of shard data.

Administrator can specify:
* the percentage of replicas to host each shard
* the minimum number of copies of any shard (regardless of the percentage setting above)

So it is possible to implement policies like: _"Shards X, Y, Z should be distributed among 20% of replicas in the cluster, but in no fewer than 2 copies"_.
### Availability zones
Replicas can be assigned to _availability zones_. By default, shard copies are
distributed evenly across them. Versioned [AZ affinity policies](docs/az-affinity.md)
can prefer particular zones while enforcing a minimum number of copies surviving
any single AZ failure.

### Zero downtime reconfiguration of cluster topology
Changing cluster topology very often requires lengthy process of data copying and indexing.
Exposing replicas that do not have necessary indexes created imposes a risk of downtimes due to long queries causing exhaustion of connection pools. 

_pgwrh_ makes sure the cluster can operate without disruptions and that not-yet-ready replicas are isolated from query traffic.

## Sharding policy flexibility and storage tiering
_pgwrh_ does not dictate how data is split into shards. It is possible to implement _any_ sharding policy by utilizing PostgreSQL partitioning.
_pgwrh_ will distribute _leaves_ of partition hierarchy among replicas.
It is also possible to specify different levels of redundancy for different subtrees of partitioning hierarchy.

Thanks to this it is possible to have more replicas maintain _hot_ data and have _cold_ data storage requirements minimized.

## Remote shard aggregation

Replicas keep local shards attached through rollout while preparing their remote
replacements. After an atomic handoff, they can combine complete remote subtrees
behind native partitioned shield views. See [the protocol and tradeoffs](docs/remote-shard-aggregation.md).

## Ease of deployment and cluster administration

The optional [pgwrh_ui controller console](pgwrh_ui/README.md) provides cluster
overview, placement previews, rollout diagnostics and replica management through
external PostgREST and bundled htmx. Install it only in the controller database.

## SQL API with PostgreSQL 18 extensions

pgwrh's management API is implemented in SQL/PLpgSQL. Version 1.0.0 also requires
its bundled native `pgwrh_fdw` extension and `pg_background`. The optional
`pgwrh_wait` API provides replication visibility barriers. See
[LSN waiting](docs/lsn-wait.md) and the [FDW documentation](pgwrh_fdw/README.md).

## Based on built-in PostgreSQL facilities - no need for custom query parser/planner
Contrary to other PostgreSQL sharding solutions that implement a query parser and interpreter to direct queries to
the right replicas, _pgwrh_ uses PostgreSQL partitioning and the bundled pgwrh_fdw,
which is derived from PostgreSQL's postgres_fdw.

PostgreSQL query planner and executor - while still somewhat limited - have capabilities to distribute computing among
multiple machines by:
* _pushing down_ filtering and aggregates (see https://www.postgresql.org/docs/current/runtime-config-query.html#GUC-ENABLE-PARTITIONWISE-AGGREGATE)
* skip execution of unnecessary query plan nodes (see https://www.postgresql.org/docs/current/runtime-config-query.html#GUC-ENABLE-PARTITION-PRUNING)

# Installation

The complete **1.0.0** bundle targets **PostgreSQL 18**. All four extensions share
version 1.0.0. This release supports fresh installation only, with no migration
or upgrade scripts for earlier installations.

| Environment | Installation guide |
| --- | --- |
| Local trial on Linux, macOS or Windows with Docker | [Ready-to-run Compose cluster](docs/containers.md) |
| RHEL/Rocky/AlmaLinux 9 | [RPM packages](docs/packages.md) |
| Debian 13, Ubuntu 24.04/26.04 | [DEB packages](docs/packages.md) |
| Nix or NixOS | [Complete PostgreSQL bundle and NixOS module](docs/nix.md) |

Binary packages and container references become available when the 1.0.0
release workflow publishes them. Before publication, the linked guides describe
local builds. Every distribution includes `pgwrh`, `pgwrh_ui`, `pgwrh_fdw`, and `pgwrh_wait`;
packages resolve `pg_background` as a dependency, and the container/Nix bundle
includes it. Controller and shard connections use the bundled `pgwrh_fdw`;
PostgreSQL's stock `postgres_fdw` extension is not required.

## Build from source

Use the 1.0.0 source archive or release tag. A complete build requires PostgreSQL
18 development files, a C compiler, GNU Make, a POSIX shell, `sha384sum` (coreutils)
or `shasum`, and the TLS/GSSAPI development
libraries used by the selected PostgreSQL installation:

```sh
make -j4 PG_CONFIG=/path/to/postgresql18/bin/pg_config
sudo make install PG_CONFIG=/path/to/postgresql18/bin/pg_config
```

Install `pg_background` with the cookie-protected v2 API (1.6 or newer).
Append `pgwrh_wait` to `shared_preload_libraries`, preserving existing entries,
and restart PostgreSQL before using the wait API. As a database administrator:

```sql
CREATE EXTENSION pgwrh CASCADE;
CREATE EXTENSION pgwrh_wait;
```

The second command enables the optional wait API. `pgwrh_wait` can also be used
independently with built-in logical replication; it does not require `pgwrh`,
`pgwrh_fdw`, or `pg_background` to be enabled in the database.

The [native package guide](docs/packages.md) covers logical replication settings
and database activation. To diagnose the selected database from this checkout:

```sh
psql -X -d your_database -f docs/check-installation.sql
```

The check verifies the full bundle, including the optional wait API, without
changing configuration. It does not verify cluster membership or shard placement.
See [packaging](docs/packaging.md)
for staged installation and build variants, and [releasing](docs/releasing.md)
for artifact publication.

# Repository layout

```text
pgwrh/          SQL extension: control file, SQL sources, and Makefile
pgwrh_ui/       Optional SQL controller console served by external PostgREST
pgwrh_wait/     Replication wait extension: control file, SQL, C sources, and Makefile
pgwrh_fdw/      Foreign data wrapper: sources, control file, SQL, docs, and Makefile
test/
  pgwrh/        Controller and replica integration tests
  pgwrh_wait/   Replication wait tests
  pgwrh_fdw/    FDW integration, SQL, isolation, and TAP tests
  check-install.py
Makefile        Combined build, install, clean, and test entry points
flake.nix       Complete PostgreSQL 18 bundle, extension package, and NixOS module
flake.lock
shell.nix       PostgreSQL 18 integration-test environment
nix/            Supporting Nix expressions
packaging/      RPM, DEB, container, and signed repository build tooling
examples/compose/  Controller and two-replica demonstration
docs/           Project documentation
```

Run `make` and `make install` from the repository root to build and install all
four extensions. Each extension can also be built independently with
`make -C pgwrh`, `make -C pgwrh_ui`, `make -C pgwrh_wait`, or `make -C pgwrh_fdw`. Build products and
staged test extensions live under `.build/`; PGXS object files and libraries
remain next to their extension sources.

The root test targets are `test-pgwrh`, `test-wait`, `test-fdw`,
`test-fdw-tap`, and `test-packaging`. For controller/replica tests, use the Nix
environment, which stages the extensions and provides PostgreSQL and Python:

```sh
nix-shell --run 'pgwrh-test test/pgwrh -q'
```

See [packaging](docs/packaging.md), [LSN waiting](docs/lsn-wait.md), and the
[FDW README](pgwrh_fdw/README.md) for suite-specific dependencies and commands.

# Quickstart

From the unpacked source archive or repository checkout, with Docker Compose and curl installed:

```sh
bash examples/compose/quickstart.sh
```

The script starts a controller, two replicas and PostgREST, commits a four-shard
rollout, verifies that both replicas return the same 100 rows, and checks the
console and its bundled assets. Open <http://localhost:13000/rpc/index?group_id=demo>.
The console is read-only; the demo uses fixed local credentials and loopback ports.

Before the 1.0.0 image is published, build it locally first:

```sh
docker build -f packaging/container/Dockerfile -t pgwrh:1.0.0-local .
PGWRH_IMAGE=pgwrh:1.0.0-local bash examples/compose/quickstart.sh
```

See [the container guide](docs/containers.md) for querying replicas, restarting
or removing the demo, and choosing different ports. The executed SQL in
[seed.sql](examples/compose/seed.sql) and the rollout in
[bootstrap.sh](examples/compose/bootstrap.sh) are the working API example.
For deployment beyond the local demo, follow [native installation](docs/packages.md),
[AZ placement](docs/az-affinity.md), and [controller UI setup](pgwrh_ui/README.md).
