# Remote shard aggregation

A pgwrh replica can query a complete table while storing only some of its
shards. It reads locally assigned shards from its own database and uses
`pgwrh_fdw` to query other replicas for the remaining data. See [cluster
concepts](overview.md) for replicas, shards, partition roots and subtrees.

**Remote shard aggregation** lets a replica query a complete partition subtree
on another replica as a unit. For example, if all twelve monthly shards of a
year are remote and have the same eligible destinations, pgwrh can query the
year's partitioned table instead of routing each month separately. This groups
remote access; it does not change shard placement or precompute SQL aggregates.

## When aggregation is possible

Aggregation is automatic when all leaves of a subtree are remote, share the same
eligible destinations, and every destination is ready to serve the whole subtree
locally. A subtree containing a local shard or an incomplete remote copy remains
split into smaller routes. An empty leaf is still a valid shard; a missing leaf
assignment prevents aggregation.

Query through the managed root table, using partition predicates where possible.
An intermediate partition may be detached while the replica accesses its data
through a remote route, so direct queries against intermediate tables are not a
substitute for querying the root.

## Behavior during rollouts

A replica keeps a usable local copy while either rollout configuration needs it.
If the target placement makes that shard remote, the replica prepares remote
access before giving up the local copy. After commit releases the outgoing copy,
the replica switches from local to remote access in one transaction. If that
switch fails, the local attachment remains available.

Commit checks that remote readers have left retiring sources and that replicas
still reading local copies have prepared remote replacements. It does not mean
that all replicas have finished rearranging their local tables. Aggregation is a
later optimization and can finish after the required shard handoff.

Rollback restores current destinations before releasing copies required by the
abandoned target. An unresponsive reader can delay commit or rollback cleanup.
Use the [rollout readiness checks](overview.md#configuration-and-rollouts); do
not remove old copies to bypass them.

## Moving partitions

Moving a partition between parents or changing partition bounds requires
coordination beyond a placement rollout:

1. Pause queries that depend on the changing hierarchy until replicas converge.
2. Commit the controller's related detach/attach changes in one transaction.
3. Clone the current placement by inserting into
   `pgwrh.replication_group_config_clone`, then
   start and commit a rollout using the new hierarchy.
4. Confirm replicas have converged before resuming affected queries.

The new placement uses each leaf's nearest configured ancestor, including its
replication factor and the expression used to calculate a shard's placement key.
Changing the controller's partition hierarchy alone does not refresh saved
placement assignments. Rolling back placement does not undo the controller's
partition DDL.

A hierarchy change is not atomic across the cluster. During convergence, an old
remote route can reference a parent whose contents have already changed on
another replica. Partition keys and column definitions must remain compatible;
version 1.0.0-alpha1 does not coordinate general schema changes.

## Limits

Logical replication and remote aggregation do not provide a cluster-wide query
snapshot. Use [replication visibility barriers](lsn-wait.md) for reads that must
observe a known write; those barriers do not create a shared snapshot either.

Partitioned unique constraints that prohibit foreign partitions remain
unsupported. Primary keys on individual local leaves are supported.
