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
identity, actual virtual server, and each actual target's mapped username.
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
outside the retained configuration or using a source's inactive credentials.
A nonempty subset is sufficient, allowing offline targets to be excluded.
Both reports encode `shard_server_targets` as a JSON object mapping actual target
server names to their mapped usernames. The controller checks each pair against
the configuration's destinations for the reporting source and active credential
generation. A known target paired with another source's username is invalid. Empty objects, old target arrays and scalar-user reports
cannot satisfy readiness. Passwords are never included in these reports.

Each logical remote node keeps a stable `pgwrh_shard_...` virtual server and a
foreign table in `<schema>_remote`. Actual `pgwrh_target_...` servers identify a
replica endpoint, database and credential user and are shared across shards.
Credential or endpoint changes create new actual targets. Virtual mappings are
empty; actual target mappings hold credentials. Target mappings are PUBLIC,
but target servers are created without PUBLIC USAGE. The virtual server owner
authorizes target access; applications need ordinary relation permissions and
do not need additional server grants. Creating another foreign table requires
server USAGE as well as permission to create objects in its schema.
The controller connection also uses `pgwrh_fdw`, as an ordinary server
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
`host` and `port` lists, `dbnames text[]`, and `shard_server_users text[]`.
Entries align by position, including repetitions. `dbnames` replaces the
development API's scalar `dbname`: a native array preserves commas and other
punctuation inside database names. `shard_server_users` replaces the scalar
`shard_server_user`, permitting different usernames for different destinations.
Missing, empty or differently sized lists cannot produce a ready route.
`pgwrh_target_servers` accepts the database array as its fourth argument and the
username array as its fifth.

Repeated entries in the aligned member/host/port/database/username lists become actual
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

## Credential feeds and rotation

The controller generates one random password and login per source replica and
credential generation. The source uses that identity at every other member in
its group. A destination receives only SCRAM-SHA-256 verifiers for incoming
sources. The controller keeps the reusable passwords in `source_credential`;
these tables and the internal `replica_credentials` view are not public feeds.

| Controller view | Columns | Replica use |
| --- | --- | --- |
| `local_credentials` | `username`, `password` | Install incoming logins; `password` contains a SCRAM verifier |
| `remote_credentials` | `member_role`, `username`, `password` | Configure this source's outbound mappings to each destination |
| `credential_state` | `generation`, `username` | Identify the active generation this source must acknowledge |

Replicas read these through the corresponding `fdw_*` foreign tables. The feeds
use `CURRENT_ROLE`: a source receives only its own reusable passwords, and a
destination receives only its incoming verifiers. Self-connections and other
groups are excluded. Preparing, active and retiring credentials remain in both
credential feeds until the controller releases a generation. Mappings select by
destination member and assigned username.

`pgwrh_fdw_scram_verifier(text)` uses PostgreSQL's native SCRAM builder, including
SASLprep, fresh random salts and the controller's `scram_iterations` setting.
Verifiers are generated once and persisted per source, generation and target
PostgreSQL server. Distinct physical targets have different salts. Databases on
one server share its role catalog and therefore its verifier for the same
source login; each database grants and retires its own local replica-role
membership. Register such siblings with the same canonical host name and port.
Aliases for one server are not detected automatically.

`rotate_credentials(group_id)` creates a fresh UUID generation independently of
FLIP/FLOP. Rotation is group-wide and allows one operation in progress:

1. **Prepare:** generate source secrets and target verifiers. Keep routing with
   the active generation until every destination reports its incoming logins.
2. **Switch:** mark the old generation retiring and publish the new generation
   in assignments. Sources create new actual targets and update their virtual
   memberships through the existing transaction-drain protocol.
3. **Retire:** each source reports `replica_state.credential_generation` only
   after its reachable remote routes and applicable local-to-remote replacements
   use its new username. Every source must acknowledge before the controller
   removes old credentials and destinations retire old login memberships.

A generation acknowledgement is explicit even for all-local or empty replicas;
stale or empty routing reports cannot acknowledge a newly activated generation.
Detached foreign tables hidden behind an active aggregate do not block retirement.
They must acquire current targets before later analysis or attachment.
This is a managed-routing guarantee, not proof that a credential has no possible
users. Privileged SQL can address detached foreign tables directly. Roles granted
server USAGE and schema CREATE can also create foreign tables using the PUBLIC
target mappings. Retirement removes incoming
login memberships on the destinations; it does not terminate existing sessions.

Topology start, commit and rollback never create or delete credentials. Both
current and target placements use the active credential generation; rollback
cannot restore a previous password. Ordinary topology readiness still checks
actual destinations and active source usernames. Adding a replica provisions it
for every retained generation without regenerating existing credentials or salts.

`credential_rotation` and `missing_credential_installation` expose progress
without secrets. Offline members and long transactions delay rotation; no timeout
retires their credentials. Rotation state, source secrets and verifier salts are
extension configuration data included in controller backups. See the
[operator procedure](../credentials.md) for scheduling and authentication setup.

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
configuration's copies and indexes. Credential rotation proceeds independently. Readers must acknowledge
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
handoff without changing credentials. Backup/restore tests retain registered database
names, and the routing tests cover list alignment and aggregation identity.
`test/pgwrh/test_credential_protocol.py` exercises the built-in per-source
provider with real SCRAM authentication on separate servers and sibling databases.
It covers scoped feeds, local verifier installation, outbound mappings, every
destination, delayed installation and readers, and repeated independent rotations.
`test/pgwrh/test_credential_lifecycle.py` covers topology commit and rollback,
existing read transactions, joining replicas and replica restart during rotation.
Backup/restore tests preserve both preparing and switching generations. These are
functional lifecycle tests.

Version 1.0.0 is the only installable version of all four bundled extensions.
Install the release files on every node and initialize a fresh database with
`CREATE EXTENSION pgwrh CASCADE`. No migration or upgrade scripts are provided.
Worker execution uses pg_background's cookie-protected v2 API; the integration
shell uses the pg_background package supplied by Nixpkgs.
