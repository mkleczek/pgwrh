# Cluster concepts and rollouts

pgwrh distributes table partitions across PostgreSQL databases to scale reads.
The [component overview](../README.md#components) introduces the extensions;
this guide explains the terms used by the configuration and operations guides.

## Controller, replicas and shards

The **controller** holds the application's source tables and accepts writes. It
also stores the desired cluster configuration and replica readiness reports.
Source code and some SQL names call this database the **master**.

A **replica** is a PostgreSQL database that receives assigned data through
logical replication. In PostgreSQL replication terminology, the source is a
**publisher** and a receiving database is a **subscriber**. A **subscription**
connects a subscriber to the data published by its source. A **replication
slot** on the publisher tracks a subscriber's progress so PostgreSQL can retain
the log data that subscriber still needs.

A **shard** is a leaf of a PostgreSQL partition hierarchy: a partition that is
not itself partitioned. The **root table** is the top of that hierarchy. A
**subtree** is a partitioned table together with all its descendant partitions.
For example, an `events` root might contain yearly partitions, each split into
monthly leaf shards.

Replicas store local copies of assigned shards and access the others remotely.
Query the managed root table on a replica to include both local and remote data.
See [remote shard aggregation](remote-shard-aggregation.md) for how complete
subtrees can be queried remotely and for partition-change restrictions.

## Database connections

The controller and every replica can have their own database name. On the
controller, pass the replica's database name as `_dbname` to
`pgwrh.add_replica`. On that replica, pass the controller's database name as
`dbname` to `pgwrh.configure_controller`.

For example, with an existing replication group `readers` and replication login
`replica_a`, register a replica whose database is `read_a` from controller
database `app`:

```sql
-- Run in the controller database app.
SELECT pgwrh.add_replica('readers', 'replica_a', 'replica-a', 5432,
                        _dbname := 'read_a');

-- Run in replica database read_a after enabling pgwrh.
SELECT pgwrh.configure_controller('controller', '5432', 'replica_a', 'replace-me',
                                 dbname := 'app');
```

Use the controller login's actual password and grant it access to the source
shards before starting replication. Database selection applies both to the
replica's configuration connection and its logical subscription. Remote reads
use each destination replica's registered database; managed schema and table
names remain the same across nodes.

Both parameters are optional and default to `current_database()` on the database
where the function runs. Existing calls therefore retain the same-name setup.
The new parameters come after the existing parameters, so named arguments are
convenient when setting just the database name.

The registered endpoint is `(host_name, port, dbname)`, allowing separate replica
databases on one PostgreSQL server. Those replicas share server resources and a
failure domain. In the console, the **Database** field defaults to the controller
database name and the replica list displays each registered database.

## Replication groups and placement

A **replication group** associates replicas with the tables and placement
policies they use. The SQL API also calls a group a replica cluster, as in
`pgwrh.create_replica_cluster`. A registered replica is a **shard host** in the
configuration tables.

A placement policy determines how many copies of each shard to keep and which
hosts may store them. A replication factor is a percentage of eligible hosts; a
minimum copy count provides a floor. Table policies can be inherited through the
partition hierarchy. Hosts can belong to **availability zones (AZs)** so copies
can be spread across failure domains. See [AZ affinity](az-affinity.md) for zone
preferences and survival requirements.

Placement and query routing are separate choices. Excluding a host from a
pending placement changes where copies will be stored after a rollout. Marking a
host offline changes whether replicas may select it for remote queries; it does
not delete its data or stop its assigned replication.

## Configuration and rollouts

The **current configuration** describes the committed placement. A **pending
configuration** is an editable draft of the next placement. Once started, a
rollout prepares that draft as the **target configuration** while replicas
retain the copies needed by the current configuration.

The SQL API identifies configuration versions as `FLIP` and `FLOP`. These are
reused labels, not release numbers or a chronological history. Inspect
`pgwrh.replication_group.current_version` and `target_version` to interpret
them; use `pgwrh.next_pending_version(group_id)` when editing the next
configuration. That function creates the pending configuration if needed; it is
not a read-only status query.

A typical rollout proceeds as follows, with management calls on the controller:

1. Edit the pending configuration and review
   `pgwrh.preview_shard_placement(group_id, version)`. The preview uses the current
   source partition tree; started rollouts use saved assignments.
2. Call `pgwrh.start_rollout(group_id)` to begin preparation. Replicas copy
   newly assigned shards, prepare indexes and configure remote access.
3. Check readiness for the target version. The console's Rollout screen shows
   blockers. The SQL views `pgwrh.missing_subscribed_shard`,
   `pgwrh.missing_connected_local_shard` and `pgwrh.missing_ready_remote_shard`
   report missing replication, local access and remote access readiness.
4. Call `pgwrh.commit_rollout(group_id)` when ready. The call checks readiness
   again before accepting the configuration. Some replica cleanup and optional
   aggregation can continue afterward.

To abandon a rollout, call `pgwrh.rollback_rollout(group_id)`. Replicas restore
current routes before copies needed by the abandoned target can be released.
Wait for their acknowledgements before starting another rollout. An unavailable
replica can delay commit or rollback cleanup; a timeout does not make its data
safe to remove.

The [AZ example](az-affinity.md#example-prefer-az-a) shows pending policy
changes. For a complete runnable setup, use the [Compose
quickstart](containers.md). Its [seed SQL](../examples/compose/seed.sql) and
[rollout script](../examples/compose/bootstrap.sh) demonstrate the SQL API.

## Read consistency and recovery

Readiness means a replica has met the rollout's requirements. It is not a
promise that every new write is already visible there or that the host is
reachable now. Logical replication remains asynchronous. Use [LSN
waiting](lsn-wait.md) where a read must observe a known write.

The controller needs its own backup and availability plan. pgwrh does not elect
a replacement controller, and shard replicas do not replace a source backup. See
[controller backup and recovery](recovery.md).
