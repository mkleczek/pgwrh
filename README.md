# pgwrh

An extension implementing sharding for PostgreSQL based on logical replication and postgres_fdw.
The goal is to scale **_read queries_** overcoming main limitation of traditional setups based on streaming replication and hot standbys:
lack of sharding and large storage requirements.

See [Architecture](https://github.com/mkleczek/pgwrh/wiki/Architecture) for more information on inner workings.

:warning: **WIP**: readme might be incomplete and contain mistakes in usage instrutions (as the API is still changing)

# Features

## Horizontal Scalability and High Availability
### No need for rebalancing
Setting up and maintaining a highly available cluster of sharded storage servers is inherently tricky, especially during changes to cluster topology.
Adding a new replica often requires rebalancing (ie. reorganizing data placement among replicas).

_pgwrh_ minimizes the need to copy data by utilizing _Weighted Randezvous Hashing_ algorithm to distribute shards among replicas.
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

## On master server

### Create your sharded table partitioning hierarchy

The below example would create a two-level partition hierarchy for `test.my_table`:
* First level by dates in `col3` (split by year)
* Second level by hash on `col2`
```pgsql
CREATE SCHEMA IF NOT EXISTS test;

CREATE TABLE test.my_data (col1 text, col2 text, col3 date) PARTITION BY RANGE (col3);
CREATE TABLE test.my_data_2023 PARTITION OF parent FOR VALUES FROM (make_date(2023, 1, 1)) TO (make_date(2024, 1, 1));
CREATE TABLE test.my_data_2024 PARTITION OF parent FOR VALUES FROM (make_date(2024, 1, 1)) TO (make_date(2025, 1, 1));
CREATE TABLE test.my_data_2025 PARTITION OF parent FOR VALUES FROM (make_date(2025, 1, 1)) TO (make_date(2026, 1, 1));

CREATE SCHEMA IF NOT EXISTS test_shards;
DO$$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT
            format('CREATE TABLE test_shards.my_data_%1$s_%2$s PARTITION OF test.my_data_%1$s (PRIMARY KEY (col1)) FOR VALUES WITH (MODULUS 16, REMAINDER %2$s)', year, rem) stmt
        FROM generate_series(2023, 2025) year, generate_series(0, 15) rem
    LOOP
        EXECUTE r.stmt;
    END LOOP;
END$$;
```

That gives 48 (16 * 3) shards in total.

**Note** that there are no specific requirements for the partitioning hierarchy and any partitioned table can be sharded - the above is only for illustration purposes.

### Create a replica cluster

Example:
```pgsql
SELECT pgwrh.create_replica_cluster('c01');
```

### Configure roles and user accounts for replicas

(Optional) Create a role for you cluster replicas and grant rights to SELECT from shards.
```pgsql
CREATE ROLE c01_replica;

GRANT SELECT ON ALL TABLES IN SCHEMA test_shards TO c01_replica;
```

Create account for each replica.
```pgsql
CREATE USER c01r01 PASSWORD 'c01r01Password' REPLICATION IN ROLE c01_replica;
```

## On every replica

Make sure `pgwrh` extension is installed.

### Configure connection to master server

Call `configure_controller` function providing username and password of this replica account created on master.
```pgsql
SELECT configure_controller(
    host => 'master.myorg',
    port => '5432',
    username => 'cr01r01', -- same as above
    password => 'c01r01Password' -- same as above
);
```

## Create and deploy replica cluster configuration

### Specify what tables to replicate

Example below would configure distribution of every partition of `test.my_data` to half (50%) of replicas,
except partitions of `test.my_data_2024` which will be copied to all (100%) replicas.
```pgsql
WITH st(schema_name, table_name, replication_factory) AS (
    VALUES
        ('test', 'my_data', 50),
        ('test', 'my_data_2024', 100)
)
INSERT INTO pgwrh.sharded_table (replication_group_id, sharded_table_schema, sharded_table_name, replication_factor)
SELECT
    'c01', schema_name, table_name, replication_factor
FROM
    st;
```

### Configure replicas
Add replica to configuration:
```pgsql
SELECT pgwrh.add_replica('c01', 'c01r01', 'replica01.cluster01.myorg', 5432);
```

### Start deployment
```pgsql
SELECT pgwrh.start_rollout('c01');
```

New configuration is now visible to connected replicas which will start data replication.

### Commit configuration
Once all replicas confirmed configuration changes, execute:
```pgsql
SELECT pgwrh.commit_rollout('c01');
```
(this will fail if some replicas are not reconfigured yet)

### Add more replicas
```pgsql
CREATE USER c01r02 PASSWORD 'c01r02Password' REPLICATION IN ROLE c01_replica;
CREATE USER c01r03 PASSWORD 'c01r03Password' REPLICATION IN ROLE c01_replica;
CREATE USER c01r04 PASSWORD 'c01r04Password' REPLICATION IN ROLE c01_replica;

select pgwrh.add_replica(
       _replication_group_id := 'c01',
       _host_id := 'c01r02',
       _host_name := 'replica02.cluster01.myorg',
       _port := 5432);
select pgwrh.add_replica(
       _replication_group_id := 'c01',
       _host_id := 'c01r03',
       _host_name := 'replica03.cluster01.myorg',
       _port := 5432,
       _weight := 70);
select pgwrh.add_replica(
       _replication_group_id := 'c01',
       _host_id := 'c01r04',
       _host_name := 'replica04.cluster01.myorg',
       _port := 5432);
```
It is possible to adjust the number of shards assigned to replicas by setting replica weight:
```pgsql
SELECT pgwrh.set_replica_weight('c01', 'c01r04', 200);
```

To deploy new configuration:
```pgsql
SELECT pgwrh.start_rollout('c01');
```
And then:
```pgsql
SELECT pgwrh.commit_rollout('c01');
```
