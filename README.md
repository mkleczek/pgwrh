# pgwrh

pgwrh scales PostgreSQL read queries by distributing **shards**—the leaf
partitions of a table—across replicas. Each replica stores an assigned subset of
the data and can query other replicas for the remaining shards, so it can serve
queries over the complete table without storing a full copy.

Applications write to the **controller**, the PostgreSQL database that holds the
source data and manages shard placement. **Replicas** receive their assigned
shards through asynchronous logical replication. For reads that must observe a
known committed write, [`pgwrh_wait`](docs/lsn-wait.md) can wait until the relevant
subscription has applied changes through a specified **log sequence number
(LSN)** before the read takes its snapshot.

Start with the [local quickstart](#quickstart), then read [cluster concepts and
rollouts](docs/overview.md).

**1.0.0-alpha1 is a testing prerelease.** Try it with disposable or recoverable
data and share feedback before 1.0.0. APIs and configuration may change, and an
in-place upgrade to later releases is not promised. See the [alpha release
notes](docs/releases/1.0.0-alpha1.md) for limitations and useful test scenarios.

The development branch also prepares **PostgreSQL 19 Beta 3** support. See the
[version and preview guide](docs/development/postgres-versions.md); published
alpha1 artifacts remain PostgreSQL 18 builds.

## Components

The pgwrh 1.0.0-alpha1 distribution contains four PostgreSQL 18 extensions:

| Extension | Purpose | Where to enable it |
| --- | --- | --- |
| `pgwrh` | Manages shard placement, replication and configuration rollouts | Controller and replicas |
| `pgwrh_fdw` | Foreign data wrapper (FDW) for queries and connections between databases | Enabled automatically by `CREATE EXTENSION pgwrh CASCADE` |
| `pgwrh_wait` | Lets a read wait until a specified write has been replicated | Optional, on subscribers serving reads that need this guarantee |
| `pgwrh_ui` | Browser console for monitoring and managing the cluster | Optional, controller only |

The development bundle uses `pg_background` 2.0.3 to run background tasks on
both PostgreSQL majors.
Packages install it as a dependency; the container and Nix bundle include it.
The console uses **PostgREST**, a separate web service that connects to the
controller database. See [console setup](pgwrh_ui/README.md).

`pgwrh_fdw` and `pgwrh_wait` can also be used independently of the core.
PostgreSQL's stock `postgres_fdw` extension is not required.

## Quickstart

Install Docker with Compose and curl. From an unpacked source archive or
repository checkout, run:

```sh
bash examples/compose/quickstart.sh
```

The script starts a controller, two replicas and a read-only console,
distributes four example shards, and verifies that both replicas return the same
100 rows. When it prints **Quickstart verified**, open [the
console](http://localhost:13000/rpc/index?group_id=demo).

The default image is `ghcr.io/mkleczek/pgwrh:1.0.0-alpha1-pg18`. If it has not yet been
published, build and select a local image first:

```sh
docker build -f packaging/container/Dockerfile -t pgwrh:1.0.0-alpha1-local .
PGWRH_IMAGE=pgwrh:1.0.0-alpha1-local bash examples/compose/quickstart.sh
```

The demo uses fixed local credentials and loopback ports. See [the container
guide](docs/containers.md) for querying replicas, choosing ports, and stopping
or removing the demo.

## Placement and read scaling

- **Partitioning:** use PostgreSQL partitioning to define shards. Different
  parts of a partition hierarchy can have different replication policies,
  allowing more copies of frequently read data.
- **Redundancy:** specify the percentage of replicas that should store each
  shard and a minimum copy count. For example, keep a shard on 20% of replicas,
  with at least two copies.
- **Availability zones:** spread copies across zones, prefer selected zones,
  and require a minimum number of copies to survive a single-zone failure.
  See [AZ affinity](docs/az-affinity.md).
- **Placement changes:** preview a configuration, prepare its replicas, and
  commit it after readiness checks pass. Existing copies remain available
  during preparation. See [rollouts](docs/overview.md#configuration-and-rollouts).
- **Remote reads:** queries can combine local and remote shards. When a remote
  replica holds an entire partition subtree, pgwrh can query it as a unit.
  See [remote shard aggregation](docs/remote-shard-aggregation.md).

Adding replicas can move shard copies. Placement favors retaining existing
copies, but changes to copy counts, zone preferences or available hosts can
require additional copying. Replication redundancy does not replace controller
backups or a PostgreSQL high-availability plan.

## Installation

The **1.0.0-alpha1** bundle targets **PostgreSQL 18**. All four extensions share version
1.0.0-alpha1. This release supports fresh installation only; it includes no upgrade
scripts for earlier installations.

| Environment | Guide |
| --- | --- |
| Linux, macOS or Windows with Docker | [Compose cluster and container](docs/containers.md) |
| RHEL/Rocky/AlmaLinux 9 | [RPM packages](docs/packages.md) |
| Debian 13, Ubuntu 24.04/26.04 | [DEB packages](docs/packages.md) |
| Nix or NixOS | [PostgreSQL bundle and NixOS module](docs/nix.md) |
| Source build | [Build requirements and installation](docs/packaging.md) |

Release downloads, image tags and signed repositories are available after the
release workflow publishes them. The guides also describe local builds.
Installing extension files and enabling extensions in a database are separate
steps; follow your guide's server settings and database activation instructions.

From a source checkout, check an installation with:

```sh
psql -X -d your_database -f docs/check-installation.sql
```

This checks the full bundle, including the optional wait API, without changing
configuration. It does not verify cluster membership or shard placement.

## Operations and limitations

Remote reads use [per-source SCRAM credentials](docs/credentials.md), with
credential rotation managed independently of placement rollouts.

Writes go to the controller. Use [replication visibility
barriers](docs/lsn-wait.md) when a read must observe a known write. Queries
spanning replicas do not have a single cluster-wide snapshot.

Schema changes require operator coordination; version 1.0.0-alpha1 has no coordinated
schema-change rollout facility. Placement rollouts do not make schema changes
atomic across the cluster. Read the [recovery guide](docs/recovery.md) before
deploying and the [release notes](docs/releases/1.0.0-alpha1.md) for supported targets
and limits.

For source layout, tests and implementation details, see the [contributor
documentation](docs/development/README.md).
