# PostgreSQL major upgrade: ownership and reconciliation

Validated on 2026-09-29 against commit `579e27013`, PostgreSQL 18.6 → 19 Beta 3,
and pg_background 2.0.3 on macOS arm64. The original observations below preceded
the registry/supervisor fix. The same four maintenance paths now pass with the
fix, without the test-only marker repair used in the original investigation.

## Original failure evidence

The finding was confirmed. Both real `pg_upgrade` operations complete, but the
subsequent pgwrh reconciliation contract fails. These are separate failures from
PostgreSQL's preservation of logical replication state.

| Path | Observed result |
| --- | --- |
| Upgrade replica in place | Owned markers fall from 53 to 0. `owned_server` and remote-route reporting become empty; the next rollout has two missing remote shards and cannot commit. |
| Upgrade controller in place | Owned markers fall from 7 to 0. `start_rollout()` reaches `sync_publications()` and fails with SQLSTATE `42710`, publication already exists. |
| Replace replica with a fresh PostgreSQL 19 node | Passes: new subscription/slot, copied data, working routes, and a subsequent committed rollout. |
| Replace controller by logical restore on PostgreSQL 19 and rebuild replicas | Passes: reads continue on the fenced old replicas while fresh subscribers copy from the recovered controller, then the rebuilt cluster completes another rollout. |

The read oracle compares typed rows, including numeric, array and JSON values,
using new connections throughout maintenance. It explicitly demands successful
reads while the old server is stopped, during `pg_upgrade`, and after restart.
For replica maintenance, `shard_host.online = false` drains that member from
reachable peer routes first. Every shard has a second physical copy. This is a
planned-maintenance result, not a claim that queries survive losing their only
copy or a connection to the PostgreSQL process being stopped.

The upgraded replica retains its subscription table states (`r`) and replication
origin positions before subscriptions are re-enabled. Subscription OIDs change,
so origin comparisons use the subscription name and numeric LSN rather than
`pg_<oid>` or LSN text. The controller retains its logical slot names/plugins.
Subsequent INSERT/UPDATE/DELETE operations converge on every replica and each
local copy. Server logs must contain no additional table-copy worker starts
after either in-place upgrade.

The original tests saved object identities and restored only their dependency
markers as a causal control. That made the blocked rollouts succeed without
rebuilding replicas or replacing their subscriptions. The passing tests no
longer use that control; repair comes from production code and the registry.

## Daemon qualification

The upgraded replica has no sync daemon during the five-second startup
observation window, despite its configured 0.1-second refresh interval.
However, a real rollout's replicated ping **does restart it** through
`make_sure_daemon_started_on_ping_trigger()`. Once running, it reports the empty
remote-route state and reconciliation remains broken until ownership is repaired.

Thus the restart defect is a missing autonomous startup guarantee. The daemon
is not necessarily absent forever. A failed controller `start_rollout()` also
rolls its ping back, so that particular failure cannot be relied on to wake it.

## Why this happens

`pgwrh/src/common.sql` inserts normal (`deptype = 'n'`) dependencies on the
extension, then reads them through `owned_obj`, `owned_server`, and
`is_dependent_object()`. Binary-upgrade dumping does not reconstruct these
hand-written dependencies for the recreated objects.

On the controller, `publication-sync.sql` uses that predicate to decide whether
a table already has a managed publication. Existing publication names therefore
collide with its CREATE commands. The `sync_publications_on_release` trigger
also invokes this function on subscriber reports, so the code can encounter
the same error before an operator explicitly requests another rollout.

On replicas, `remote_shard`, `remote_server_route`, index ownership and managed
descendant discovery depend on those views/predicates. The server-update and
server/schema-cleanup branches in `sync.sql` also join the owned-object views.
The regression checks those command branches are empty after marker loss, in
addition to exercising the empty report and rejected commit. It does not take a
second peer down to claim a separate routing-failover result. Existing foreign
tables and server options still support reads, concealing the management failure.

## Implemented ownership and supervision

`pgwrh.managed_object` stores object kind, schema and name and is registered
with `pg_extension_config_dump`. `owned_obj`, `owned_server` and
`is_dependent_object()` resolve live catalog OIDs from that table, so existing
reconciliation consumers all use durable ownership. Registration records the
identity and its derived normal dependency in one transaction. Event triggers
follow renames/schema moves and remove dropped identities, including rollback,
to avoid adopting a later object with a reused name. Missing restored objects
are tolerated; publication synchronization can recreate excluded publications.
The bootstrap ping publication's seeded row is excluded from config COPY.

`repair_managed_objects()` regenerates only missing registry-owned markers,
serializing against registrations and other repairs. Native catalog locks
prevent inserting dependencies for concurrently dropped objects; a conflicting
DDL transaction causes a retryable error instead of a stale marker. Run it after an upgrade
or logical restore before treating DROP protection as restored. The supervisor
also repairs controller databases, and each daemon repairs before starting
reconciliation. The explicit post-upgrade step is documented in
[recovery.md](../recovery.md).

A native `pgwrh` worker, loaded through `shared_preload_libraries`, discovers
connectable non-template databases and runs bounded, sequential checks. It
executes only the extension's own privileged entry point, under its superuser
owner. PostgreSQL restarts the launcher after failure. The launcher is disabled
in binary-upgrade mode, and waits for recovery completion on standbys.
`sync_daemon_config` durably records enabled state, refresh interval and
application name. Empty controller settings never enroll a replica daemon;
explicit stops remain stopped across pings/restarts. The existing per-database
advisory lock prevents duplicate daemons.

The current alpha deliberately has no migration script or version bump. Fresh
installations have the registry before being upgraded between PostgreSQL
versions; a marker-only database that has already lost its markers needs its
original ownership inventory to recover.

The [PostgreSQL logical-replication upgrade prerequisites](https://www.postgresql.org/docs/18/logical-replication-upgrade.html)
require PostgreSQL 17+ for migration of slots and subscriber state. The publisher
test pauses management writers, catches slots up to a fixed WAL position and
disables subscriptions before shutdown. The replacement-controller test follows
the existing [logical recovery procedure](../recovery.md), including publication
exclusion, readiness quarantine and fresh replicas; it does not attempt to reuse
old subscription state after a logical restore.

## Running the regressions

After staging both builds as described in [testing.md](testing.md), run:

```sh
python3 -m pytest test/pgwrh/mixed_versions/test_upgrade.py -v --tb=short
```

All four tests now pass. Each in-place test writes `pg_upgrade.log` and
`evidence.json` in its pytest temporary directory. Subscription ready states,
origin progress and controller slots are checked before replication resumes;
copy-worker logs and subsequent DML check that no initial re-copy occurred.
The focused ownership suite also exercises a logical restore with different
OIDs, idempotent repair, restricted registry writes, and DROP RESTRICT/CASCADE.
