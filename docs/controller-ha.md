# Controller high availability

The controller holds the source data. To keep pgwrh replication running when
that server fails, give the controller a **physical streaming standby** and keep
its logical replication slots synchronized there. After promotion, the pgwrh
replicas can reconnect and continue using their existing subscriptions.

A physical controller standby contains the whole controller database cluster.
It is different from a pgwrh replica, which receives assigned shards through
logical replication. Shard redundancy does not replace controller HA.

This guide uses PostgreSQL's native failover slots, available since PostgreSQL
17. pgwrh currently tests PostgreSQL 18 and the PostgreSQL 19 preview; it does
not yet support 17. Use the same PostgreSQL major and the same extension builds
on both controllers. Your HA manager remains responsible for failure detection,
promotion, fencing the old primary, and directing application writes to the new
primary. pgwrh does not elect controllers or prevent split brain.

## Configure the controller pair

The example uses database `app`, primary `controller-a`, standby `controller-b`,
and physical replication slot `controller_b`. Substitute your addresses and
size the resource limits for your cluster.

Install the pgwrh bundle and pg_background on both machines before taking a
base backup. Physical replication copies database contents and extension
catalogs, but does not install shared libraries or operating-system packages.

On both controllers, set these PostgreSQL settings and restart:

```conf
wal_level = logical
max_replication_slots = 32
max_wal_senders = 32
hot_standby = on
```

These are example capacities. Allow for every pgwrh subscription, concurrent
table-copy slots, physical standbys, and backup connections. Keep standby limits
at least as large as the primary's. Retain your other required server settings
from the [installation guide](packages.md#configure-postgresql-and-enable-extensions).

Create a dedicated physical replication login on the primary, using `psql` as
an administrator:

```sql
CREATE ROLE controller_stream LOGIN REPLICATION;
\password controller_stream
GRANT CONNECT ON DATABASE app TO controller_stream;
```

Permit both the physical replication connection and the slot synchronization
connection in the primary's `pg_hba.conf`. For a standby at `10.20.0.12`:

```conf
hostssl replication controller_stream 10.20.0.12/32 scram-sha-256
hostssl app         controller_stream 10.20.0.12/32 scram-sha-256
```

Reload the primary after changing HBA rules. The example assumes working TLS
with trusted certificates. Put the password in the PostgreSQL OS user's
protected passfile on the standby, with entries for both `replication` and
`app`, and mode `0600`. Keep this account separate from pgwrh replica logins.
Prepare reciprocal HBA rules, certificates and passfiles before allowing the
two controller hosts to exchange primary and standby roles.

Provision the standby through your HA manager or a base backup. For a manual
setup, run this on `controller-b` as the PostgreSQL OS user, targeting a **new,
empty** data directory:

```sh
pg_basebackup \
  --dbname='host=controller-a port=5432 user=controller_stream sslmode=verify-full' \
  --pgdata=/path/to/new/standby-data --wal-method=stream \
  --create-slot --slot=controller_b --write-recovery-conf
```

`--create-slot` is for a new slot; do not recreate one already managed by your
HA system. Configure the resulting standby, including its generated
`postgresql.auto.conf`, so the effective settings are:

```conf
primary_conninfo = 'host=controller-a port=5432 user=controller_stream dbname=app application_name=controller_b sslmode=verify-full'
primary_slot_name = 'controller_b'
hot_standby_feedback = on
sync_replication_slots = on
```

The database name is needed for slot synchronization even though physical
streaming copies the whole cluster. Start the standby and confirm that it is
streaming. Then configure the primary and reload it:

```conf
synchronized_standby_slots = 'controller_b'
```

This holds logical delivery behind the standby's durable WAL position. If that
standby is unavailable, logical delivery can stall; monitor this dependency.
It does **not** by itself make application commits durable on the standby.

If acknowledged writes must survive losing the primary, also configure
synchronous physical replication on the primary:

```conf
synchronous_standby_names = 'FIRST 1 (controller_b)'
synchronous_commit = on
```

Here `controller_b` is the standby's `application_name`; in
`synchronized_standby_slots` it is the physical **slot name**. They happen to
match in this example. With one synchronous standby, losing it blocks commit
acknowledgements. Plan that availability tradeoff with your HA manager, and do
not let application sessions override the durability setting if you require
this guarantee. See PostgreSQL's [replication
settings](https://www.postgresql.org/docs/18/runtime-config-replication.html).

## Connect pgwrh replicas to the primary

Use a stable endpoint that your HA manager directs exclusively to the current
primary, or supply both controller addresses when initially configuring each
pgwrh replica:

```sql
SELECT pgwrh.configure_controller(
    host := 'controller-a,controller-b',
    port := '5432,5432',
    username := 'replica_a',
    password := 'replace-with-this-replicas-controller-password',
    dbname := 'app'
);
```

Register the replica and grant its login access to the source shards as described
in the [cluster guide](overview.md). Both controller nodes must accept that
login after promotion. Configure TLS for both the controller FDW connection and
the logical subscription according to your deployment's connection policy.

In development builds, both the subscription and the `replica_controller` FDW
use `target_session_attrs=primary`. A reachable standby is skipped when making
a connection. The published alpha1 FDW does not set this option automatically;
on each existing replica add it once, if absent:

```sql
ALTER SERVER replica_controller OPTIONS (ADD target_session_attrs 'primary');
```

If the option already exists, use `SET` instead of `ADD`. Primary selection
does not fence an old primary that is still writable, and it does not move
existing connections. The old primary must be stopped or otherwise isolated
from applications and all subscribers before promotion.

`configure_controller` creates a subscription; do not call it again to redirect
an existing replica. When changing addresses, update **both** the FDW server
(`ALTER SERVER ... OPTIONS (SET host ..., SET port ...)`) and the existing
subscription (`ALTER SUBSCRIPTION ... CONNECTION ...`), preserving its database,
credentials, security options and `target_session_attrs=primary`.

## Check readiness before promotion

Slot synchronization is asynchronous. A configured standby is not necessarily
ready yet. Check and monitor every required slot, including replicas currently
disconnected from the controller.

On **each pgwrh replica**, record the subscription's slot name and confirm
`subfailover` is true:

```sql
SELECT subname, subslotname, subfailover
FROM pg_subscription
WHERE subname = 'pgwrh_replica_subscription';
```

Combine the names from all replicas. On the candidate controller standby, put
that complete list into this check (the names below are placeholders):

```sql
WITH required(slot_name) AS (
    VALUES ('replica_a_slot'), ('replica_b_slot')
)
SELECT r.slot_name,
       COALESCE(s.synced AND NOT s.temporary
                AND s.invalidation_reason IS NULL, false) AS failover_ready
FROM required r
LEFT JOIN pg_replication_slots s USING (slot_name)
ORDER BY r.slot_name;
```

Every required slot must return `true`. A missing slot deliberately returns
`false`; querying only the slots already present could hide a missing replica.
Keep this inventory outside the primary so it remains available after a crash.

For a planned switchover, pause rollouts and wait for table copies to finish.
On each replica this should return no rows:

```sql
SELECT r.srrelid::regclass, r.srsubstate
FROM pg_subscription_rel r
JOIN pg_subscription s ON s.oid = r.srsubid
WHERE s.subname = 'pgwrh_replica_subscription' AND r.srsubstate <> 'r';
```

During a failure with an unfinished table copy, additional table-sync slots
can be required. Follow PostgreSQL's [complete failover readiness
procedure](https://www.postgresql.org/docs/18/logical-replication-failover.html)
to collect them. The steady-state check above is not sufficient for that case.
Do not assume a zero-row result from an incomplete inventory proves readiness.

## Switch over or recover from a failure

For a planned switchover:

1. Pause application writes, placement rollouts and credential changes. Wait for
   existing transactions and table copies to finish. Check the slots above.
2. Disable each pgwrh subscription with
   `ALTER SUBSCRIPTION pgwrh_replica_subscription DISABLE`. Record
   `SELECT pg_current_wal_flush_lsn()` on the old primary and wait until
   `pg_last_wal_replay_lsn()` on the standby reaches it.
3. Fence and stop the old primary using your HA procedure, keeping the standby
   streaming until shutdown finishes. Promote the verified standby.
4. Review `synchronous_standby_names` and `synchronized_standby_slots` on the new
   primary: they must describe its actual downstream standbys, not the old
   topology. An obsolete entry can block commits or logical delivery. Rebuild
   physical redundancy before reopening writes if your durability policy requires
   it; running temporarily without a standby is an explicit degraded-mode choice.
5. Switch the application and console endpoints. If using a stable primary
   endpoint or the multi-host configuration above, pgwrh's connection settings
   can stay unchanged. Otherwise update both connections on every replica.
6. Enable the subscriptions and verify new writes reach every assigned shard.
   Check `pg_stat_subscription`, subscription errors, and the rollout readiness
   views before resuming placement or credential changes.

After an abrupt failure, fence the failed primary before promotion and apply
the same slot, WAL-continuity and topology checks. Subscriptions left enabled
retry their connections; their configured endpoints must select the new primary.
Do not recreate subscriptions or slots just to make errors disappear: a new
slot cannot reconstruct an unknown gap. If required slots or WAL are missing,
use the [replica rebuild procedure](recovery.md#recover-service).

Rejoin the old controller only after your HA system has rewound or rebuilt it
as a standby and configured its physical slot and slot synchronization. Verify
readiness again before allowing another failover.

## Monitoring and tested scope

Monitor physical replication lag, slot validity and retained WAL on the
controller pair, plus logical replication progress and errors on each pgwrh
replica. Budget WAL storage for the longest tolerated outage. Finite
`max_slot_wal_keep_size` limits disk growth but can invalidate lagging slots;
unlimited retention can fill the disk. `hot_standby_feedback` can also delay
catalog cleanup. PostgreSQL documents the [slot synchronization
requirements](https://www.postgresql.org/docs/18/logicaldecoding-explanation.html#LOGICALDECODING-REPLICATION-SLOTS-SYNCHRONIZATION).

The [integration tests](development/testing.md#controller-failover) exercise a
real base backup, physical streaming, automatic slot synchronization, planned
shutdown and abrupt failure, multi-host reconnection, pending and new data
changes, and a new shard rollout after promotion. They do not certify an HA
manager's election/fencing, network partitions, cross-major failover, or promotion
midway through initial table copy. LSN barriers guarantee visibility, not
durability across an arbitrary failover; see [the wait guide](lsn-wait.md).
