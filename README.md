# pgwrh

An extension implementing sharding for PostgreSQL based on logical replication and postgres_fdw.
The goal is to scale **_read queries_** overcoming main limitation of traditional setups based on streaming replication and hot standbys:
lack of sharding and large storage requirements.

See [Architecture](https://github.com/mkleczek/pgwrh/wiki/Architecture) for more information on inner workings.

:warning: **WIP**: README might be incomplete and contain mistakes in usage instructions (as the API is still changing)

# Documentation

- [Operator Guide](docs/operator-guide.md) - deployment, rollouts, monitoring, and day-2 operations.
- [Contributor Architecture](docs/contributor-architecture.md) - internals, control flow, and design rationale.

# Features

## Horizontal Scalability and High Availability
### No need for rebalancing
Setting up and maintaining a highly available cluster of sharded storage servers is inherently tricky, especially during changes to cluster topology.
Adding a new replica often requires rebalancing (ie. reorganizing data placement among replicas).

_pgwrh_ minimizes the need to copy data by utilizing _Weighted Rendezvous Hashing_ algorithm to distribute shards among replicas.
Adding replicas never requires moving data between existing ones.
### Data redundancy
_pgwrh_ maintains requested level of redundancy of shard data.

Administrator can specify:
* the percentage of replicas to host each shard
* the minimum number of copies of any shard (regardless of the percentage setting above)

So it is possible to implement policies like: _"Shards X, Y, Z should be distributed among 20% of replicas in the cluster, but in no fewer than 2 copies"_.
### Availability zones
Replicas can be assigned to _availability zones_ and _pgwrh_ ensures shard copies are distributed evenly across all of them.

### Zero downtime reconfiguration of cluster topology
Changing cluster topology very often requires lengthy process of data copying and indexing.
Exposing replicas that do not have necessary indexes created imposes a risk of downtimes due to long queries causing exhaustion of connection pools. 

_pgwrh_ makes sure the cluster can operate without disruptions and that not-yet-ready replicas are isolated from query traffic.

## Sharding policy flexibility and storage tiering
_pgwrh_ does not dictate how data is split into shards. It is possible to implement _any_ sharding policy by utilizing PostgreSQL partitioning.
_pgwrh_ will distribute _leaves_ of partition hierarchy among replicas.
It is also possible to specify different levels of redundancy for different subtrees of partitioning hierarchy.

Thanks to this it is possible to have more replicas maintain _hot_ data and have _cold_ data storage requirements minimized.

## Ease of deployment and cluster administration


## Pure SQL/PGSQL
This makes it easy to use _pgwrh_ in cloud environments that limit possibilities of custom extension installation.

***
_Caveat_ at the moment _pgwrh_ requires _pg_background_ to operate as it needs a way to execute SQL commands
outside current transaction (_CREATE/ALTER SUBSCRIPTION_ must not be executed in transaction).

## Based on built-in PostgreSQL facilities - no need for custom query parser/planner
Contrary to other PostgreSQL sharding solutions that implement a query parser and interpreter to direct queries to
the right replicas, _pgwrh_ reuses built-in PostgreSQL features: partitioning and postgres_fdw.

PostgreSQL query planner and executor - while still somewhat limited - have capabilities to distribute computing among
multiple machines by:
* _pushing down_ filtering and aggregates (see https://www.postgresql.org/docs/current/runtime-config-query.html#GUC-ENABLE-PARTITIONWISE-AGGREGATE)
* skip execution of unnecessary query plan nodes (see https://www.postgresql.org/docs/current/runtime-config-query.html#GUC-ENABLE-PARTITION-PRUNING)

# Installation

## Prerequisites

| Name | Version |
| :---- | :---: |
| PostgreSQL | 16+ |
| pg_background | 1.2+ |

Additional requirement:
- All participating shard hosts must use the same database name. `pgwrh` uses libpq multi-host connection strings for cross-replica load balancing and those strings carry one shared `dbname`.

## Extension installation

Clone the Git repository.
```sh
git clone https://github.com/mkleczek/pgwrh.git
```
Install the extension.
```sh
cd pgwrh
make install
```
Create extension in PostgreSQL database.
```sh
psql -c "CREATE EXTENSION pgwrh CASCADE"
```

# Usage

For complete, SQL-accurate runbooks and architecture details, use:
- [Operator Guide](docs/operator-guide.md)
- [Contributor Architecture](docs/contributor-architecture.md)

## Quick Start (current SQL API)

### 1. Create cluster on master

```pgsql
SELECT pgwrh.create_replica_cluster('c01');
```

### 2. Configure sharded tables on master

```pgsql
INSERT INTO pgwrh.sharded_table (
    replication_group_id,
    sharded_table_schema,
    sharded_table_name,
    replication_factor
)
VALUES
    ('c01', 'test', 'my_data', 50),
    ('c01', 'test', 'my_data_2024', 100);
```

### 3. Add replicas on master

```pgsql
CREATE ROLE c01_replica;
GRANT SELECT ON ALL TABLES IN SCHEMA test_shards TO c01_replica;

CREATE USER c01r01 PASSWORD 'c01r01Password' REPLICATION IN ROLE c01_replica;
SELECT pgwrh.add_replica('c01', 'c01r01', 'replica01.cluster01.myorg', 5432);
```

### 4. Configure each replica

```pgsql
SELECT pgwrh.configure_controller(
    host => 'master.cluster01.myorg',
    port => '5432',
    username => 'c01r01',
    password => 'c01r01Password'
);
```

### 5. Roll out and commit on master

```pgsql
SELECT pgwrh.start_rollout('c01');
SELECT pgwrh.commit_rollout('c01');
```

### 6. Update host weight (example)

```pgsql
SELECT pgwrh.set_replica_weight('c01', 'default', 'c01r01', 200);
SELECT pgwrh.start_rollout('c01');
SELECT pgwrh.commit_rollout('c01');
```
