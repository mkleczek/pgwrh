# Local-first remote shard aggregation

Placement remains leaf-based. The master computes WRH assignments and publishes
effective destinations through the existing FDW API. Each replica prepares its
foreign tables, maintains its local copies, and selects maximal remote subtrees.
Shield views select from the original partitioned tables, preserving PostgreSQL's
partition pruning. There is no additional drain phase.

## Prepare, attach, and release

A replica keeps a usable local leaf attached while either rollout configuration
requires that copy. It prepares and analyzes the target foreign leaf independently
of attachment. `prepared_remote_shards` reports its logical leaf identity, actual
virtual server, actual target set, and mapped user. `connected_remote_shards` reports the same
identity and destination for routes that are actually reachable from query roots.
A prepared foreign table does not claim to be an active route.

Before commit, the master requires:

| Target placement | Required replica state |
| --- | --- |
| Local | Local copy ready and connected |
| Remote, still served locally | Local copy connected and target foreign replacement prepared |
| Remote, currently served remotely | Active remote route uses the target server and credentials |

`missing_ready_remote_shard` expresses the commit requirement.
`missing_connected_remote_shard` remains a diagnostic of active remote routes.
Prepared replacements cannot exempt a reader still querying the old remote source.
The target and credential checks reject routes containing a destination outside
the retained configuration or using another configuration's credentials. A
nonempty subset is sufficient, allowing offline targets to be excluded.

Each logical remote node keeps a stable `pgwrh_shard_...` virtual server and a
foreign table in `<schema>_remote`. Actual `pgwrh_target_...` servers identify a
replica endpoint, database and credential user and are shared across shards.
Credential or endpoint changes create new actual targets. Virtual mappings are
empty; actual target mappings hold credentials. Actual targets grant PUBLIC
USAGE to match their PUBLIC mappings; applications still need relation access.
As with any server USAGE grant, a role with permission to create foreign tables
can use these read-only credentials for relations accessible to the remote reader.
The controller connection also uses `pgwrh_fdw`, as an ordinary server without
virtual members. The stock `postgres_fdw` extension is not required.

Remote-to-remote changes call `pgwrh_fdw_set_members` in the existing synchronous
worker transaction. It waits for transactions using the old route, including
bindings retained across savepoint rollback. The worker commits before the
existing reporting step can acknowledge the new targets. No attachment change,
rollout revision or additional reconciliation phase is needed for this switch.
Existing statistics describe the same logical data and survive membership changes.
A retained foreign table must have its desired targets before analysis or reattachment.
Local-to-remote and remote-to-local attachment changes retain their existing locks.

Repeated entries in the existing aligned member/host/port lists become actual
server `load_balance_weight` values, preserving `same_zone_multiplier`. Reuse of
an active or idle connection still takes priority over weights. Target sets with
identical members share the FDW's transaction routing decision and join pushdown.
Detached stable foreign tables remain while their logical nodes exist; target
cleanup preserves every target referenced by an owned virtual server.

After commit releases an outgoing copy, the replica replaces the local leaf with
its prepared foreign leaf in one transaction. Query-root locking prevents readers
from seeing a partial replacement. If the transaction fails, the local attachment
survives. Subscription removal, truncation, and index cleanup wait until that leaf
is detached. A slow handoff therefore retains data instead of exposing a gap.

Only a later reconciliation pass can aggregate the replacement foreign leaves.
This separates the required local-to-remote handoff from the optional optimization.

## Aggregation and serving trees

The replica computes descendants from structured controller metadata rather than
from the mutable physical partition hierarchy. A parent is eligible only when:

- its complete logical subtree has leaf assignments;
- every descendant is remote, with no remaining original local attachment;
- all descendants have the same canonical actual target set, including endpoint,
  database and credential identity; virtual server names, ordering and weights
  do not affect this equality;
- every destination member advertises the complete parent as locally ready.

Replicas report `serving_subtrees` only for complete native partition trees whose
leaves are connected, subscribed, and indexed. Keeping current local attachments
through rollout preserves these serving trees while readers change destinations.
Subscription readiness alone cannot admit an aggregate over detached local copies.

Highest eligible ancestors replace their descendant foreign routes atomically per
root. Root relation identities remain unchanged. Empty or incomplete internal nodes
block aggregation; an empty leaf is still a valid shard. Detached intermediate
objects are rebuilt from logical metadata when a subtree must expand.

Parent foreign-table analysis finishes within the serialized sync pass. State
reporting shares that synchronization lock, so an acknowledgement cannot overtake
an unfinished attachment change or parent shield query.

## Rollback

Rollback restores current destinations while retaining the abandoned configuration's
copies, indexes, and credentials. Readers must acknowledge restored remote routes;
replicas holding new local copies may instead prepare their restored foreign
replacements. The master then releases the abandoned configuration, and the same
atomic local-to-remote handoff cleans up its copies. Both rollback unlock modes
follow this rule. Another rollout waits for rollback acknowledgements.

## Tradeoffs and limits

Commit certifies that remote readers have left retiring sources and that retained
local readers have usable replacements. It does not certify that every replica has
already adopted the final physical tree. Some attachment work and aggregation
therefore occur after commit. Unresponsive readers still block commit or rollback
cleanup; a timeout does not authorize deleting their serving data.

The master continues to calculate placement. Moving placement to replicas or
transporting configuration and feedback with logical replication is independent
future work. This change adds preparation and serving-tree feedback to the existing
FDW contract without changing the configuration transport.

As before, logical replication does not provide a cluster-wide query snapshot.
Cross-replica transactional consistency is outside this handoff protocol. Native
partitioned unique constraints that prohibit foreign partitions remain unsupported;
primary keys on individual local leaves are supported.

## Tests and upgrade

`pgwrh-test` runs isolated PostgreSQL integration tests. Coverage includes prepared
but detached replacements, stale destinations, delayed readers, failed attachment
transactions, concurrent reads, rollback, maximal subtree selection, nested RANGE,
LIST and HASH layouts, empty leaves, endpoint failover, restart, credential rotation,
and generic prepared-query partition pruning through native shields.

Version 0.2.2 includes an upgrade from 0.2.1. Install the new extension scripts on all
nodes, create the new dependency with `CREATE EXTENSION IF NOT EXISTS pgwrh_fdw`,
then update the master and replicas with `ALTER EXTENSION pgwrh UPDATE`.
The bundled pgwrh_fdw dependency remains version 0.1.0. Upgrade the controller
first so assignments contain aligned member and endpoint lists. Legacy route
reports remain accepted while replicas replace their old host-set foreign tables
through the existing attachment reconciliation. Reconnect sessions that loaded
an older pgwrh_fdw library before relying on its membership locking.
The upgrade regression builds 0.2.1 from its release tag and checks existing rows and
root OIDs after upgrade. The upgrade also moves worker execution to pg_background's
cookie-protected v2 API. Use pg_background 1.6–1.x during the pgwrh upgrade, then
move to pg_background 2.x after every node has upgraded. The integration shell pins
pg_background 1.9.2 so it can still start the released pgwrh 0.2.1 for this test.
