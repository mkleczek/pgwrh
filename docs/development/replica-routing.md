# Replica routing and shard handoff

This contributor guide describes the routing implementation. For operational
behavior and limitations, see [remote shard
aggregation](../remote-shard-aggregation.md). A shield is a view that exposes a
managed partition tree to remote readers; a slot is an attachment point in the
replica query tree. Here, "master" means the controller and WRH means weighted
rendezvous hashing; see the [placement algorithm](placement.md).

Placement remains leaf-based. The master computes WRH assignments and publishes
effective destinations through the existing FDW API. Each replica prepares its
foreign tables, maintains its local copies, and selects maximal remote subtrees.
Shield views select from the original partitioned tables, preserving
PostgreSQL's partition pruning. There is no additional drain phase.

## Prepare, attach, and release

A replica keeps a usable local leaf attached while either rollout configuration
requires that copy. It prepares and analyzes the target foreign leaf
independently of attachment. `prepared_remote_shards` reports its logical leaf
identity, actual virtual server, actual target set, and mapped user.
`connected_remote_shards` reports the same identity and destination for routes
that are actually reachable from query roots. A prepared foreign table does not
claim to be an active route.

Before commit, the master requires:

| Target placement | Required replica state |
| --- | --- |
| Local | Local copy ready and connected |
| Remote, still served locally | Local copy connected and target foreign replacement prepared |
| Remote, currently served remotely | Active remote route uses the target server and credentials |

`missing_ready_remote_shard` expresses the commit requirement.
`missing_connected_remote_shard` remains a diagnostic of active remote routes.
Prepared replacements cannot exempt a reader still querying the old remote
source. The target and credential checks reject routes containing a destination
outside the retained configuration or using another configuration's credentials.
A nonempty subset is sufficient, allowing offline targets to be excluded.

Each logical remote node keeps a stable `pgwrh_shard_...` virtual server and a
foreign table in `<schema>_remote`. Actual `pgwrh_target_...` servers identify a
replica endpoint, database and credential user and are shared across shards.
Credential or endpoint changes create new actual targets. Virtual mappings are
empty; actual target mappings hold credentials. Actual targets grant PUBLIC
USAGE to match their PUBLIC mappings; applications still need relation access.
As with any server USAGE grant, a role with permission to create foreign tables
can use these read-only credentials for relations accessible to the remote
reader. The controller connection also uses `pgwrh_fdw`, as an ordinary server
without virtual members. The stock `postgres_fdw` extension is not required.

Remote-to-remote changes call `pgwrh_fdw_set_members` in the existing
synchronous worker transaction. It waits for transactions using the old route,
including bindings retained across savepoint rollback. The worker commits before
the existing reporting step can acknowledge the new targets. No attachment
change, rollout revision or additional reconciliation phase is needed for this
switch. Existing statistics describe the same logical data and survive
membership changes. A retained foreign table must have its desired targets
before analysis or reattachment. Local-to-remote and remote-to-local attachment
changes retain their existing locks.

The assignment API carries `shard_server_members text[]`, comma-separated
`host` and `port` lists, and `dbnames text[]`. Entries align by position, including
repetitions. `dbnames` replaces the development API's scalar `dbname`: a native
array preserves commas and other punctuation inside database names. Missing,
empty or differently sized endpoint lists cannot produce a ready route.
`pgwrh_target_servers` accepts this database array as its fourth argument.

Repeated entries in the aligned member/host/port/database lists become actual
server `load_balance_weight` values, preserving `same_zone_multiplier`. Reuse of
an active or idle connection still takes priority over weights. Target sets with
identical members share the FDW's transaction routing decision and join
pushdown. Detached stable foreign tables remain while their logical nodes exist;
target cleanup preserves every target referenced by an owned virtual server.

`shard_host.dbname` supplies each destination database for both assignment and
controller readiness checks. The `replica_controller` server and the logical
subscription instead use `configure_controller(..., dbname := ...)`.
Subscription discovery is scoped to the current database because PostgreSQL's
subscription catalog spans the whole server. Replicas in sibling databases can
share generated login credentials; each grants its local replica role and
retires that membership without dropping a login still used by a sibling.

After commit releases an outgoing copy, the replica replaces the local leaf with
its prepared foreign leaf in one transaction. Query-root locking prevents
readers from seeing a partial replacement. If the transaction fails, the local
attachment survives. Subscription removal, truncation, and index cleanup wait
until that leaf is detached. A slow handoff therefore retains data instead of
exposing a gap.

Only a later reconciliation pass can aggregate the replacement foreign leaves.
This separates the required local-to-remote handoff from the optional
optimization.

## Aggregation and serving trees

The replica computes descendants from structured controller metadata rather than
from the mutable physical partition hierarchy. A parent is eligible only when:

- its complete logical subtree has leaf assignments;
- every descendant is remote, with no remaining original local attachment;
- all descendants have the same canonical actual target set, including endpoint,
  database and credential identity; virtual server names, ordering and weights
  do not affect this equality;
- every destination member advertises the complete parent as locally ready.

Replicas report `serving_subtrees` only for complete native partition trees
whose leaves are connected, subscribed, and indexed. Keeping current local
attachments through rollout preserves these serving trees while readers change
destinations. Subscription readiness alone cannot admit an aggregate over
detached local copies.

Highest eligible ancestors replace their descendant foreign routes atomically
per root. Root relation identities remain unchanged. Empty or incomplete
internal nodes block aggregation; an empty leaf is still a valid shard. Detached
intermediate objects are rebuilt from logical metadata when a subtree must
expand.

Parent foreign-table analysis finishes within the serialized sync pass. State
reporting shares that synchronization lock, so an acknowledgement cannot
overtake an unfinished attachment change or parent shield query.

## Partition moves and bounds

Replicas reconcile both parent identity and partition bounds. Moving an existing
year between `fresh` and `archival`, including advancing their bounds,
reattaches the existing slots and local or foreign tables in one transaction per
query root. Local data and subscriptions survive the move; remote aggregates
expand or regroup according to the resulting leaf destinations. Query through
the managed root with partition predicates; an original intermediate table can
be detached while its foreign aggregate occupies the routing slot.

Commit the controller's detach/attach DDL in one transaction. Placement
snapshots change through the existing rollout: clone the current configuration
into the next version using `replication_group_config_clone`, then start and
commit that rollout normally. The snapshot uses each leaf's new nearest
configured ancestor, including its replication factor and sharding-key
expression. Structural DDL alone does not resnapshot placement. Rolling back a
placement rollout does not undo the controller's partition DDL.

This is eventual reconciliation, not a cluster-wide atomic schema switch.
Quiesce queries that depend on the changing hierarchy until replicas converge:
an old aggregate route can still reference a parent whose contents have changed
remotely. The local attachment transaction and membership locks do not
coordinate that DDL across replicas. Partition keys and column definitions must
remain compatible.

## Rollback

Rollback restores current destinations while retaining the abandoned
configuration's copies, indexes, and credentials. Readers must acknowledge
restored remote routes; replicas holding new local copies may instead prepare
their restored foreign replacements. The master then releases the abandoned
configuration, and the same atomic local-to-remote handoff cleans up its copies.
Both rollback unlock modes follow this rule. Another rollout waits for rollback
acknowledgements.

## Tradeoffs and limits

Commit certifies that remote readers have left retiring sources and that
retained local readers have usable replacements. It does not certify that every
replica has already adopted the final physical tree. Some attachment work and
aggregation therefore occur after commit. Unresponsive readers still block
commit or rollback cleanup; a timeout does not authorize deleting their serving
data.

The master continues to calculate placement. Moving placement to replicas or
transporting configuration and feedback with logical replication is independent
future work. Preparation and serving-tree feedback use the FDW configuration
transport.

As before, logical replication does not provide a cluster-wide query snapshot.
Cross-replica transactional consistency is outside this handoff protocol. Native
partitioned unique constraints that prohibit foreign partitions remain
unsupported; primary keys on individual local leaves are supported.

## Tests and installation

`pgwrh-test` runs isolated PostgreSQL integration tests. Coverage includes
prepared but detached replacements, stale destinations, delayed readers, failed
attachment transactions, concurrent reads, rollback, maximal subtree selection,
nested RANGE, LIST and HASH layouts, empty leaves, endpoint failover, restart,
credential rotation, and generic prepared-query partition pruning through native
shields.

`test/pgwrh/test_database_names.py` covers distinct controller and replica names,
quoted names, replicas sharing a server, reads through each target, replication,
handoff and credential rotation. Backup/restore tests retain registered database
names, and the routing tests cover list alignment and aggregation identity.

Version 1.0.0 is the only installable version of all four bundled extensions.
Install the release files on every node and initialize a fresh database with
`CREATE EXTENSION pgwrh CASCADE`. No migration or upgrade scripts are provided.
Worker execution uses pg_background's cookie-protected v2 API; the integration
shell uses the pg_background package supplied by Nixpkgs.
