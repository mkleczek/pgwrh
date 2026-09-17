# Controller backup and recovery

Back up the controller's application data and pgwrh metadata together. Replicas
are derived copies; they do not replace a controller backup. Keep PostgreSQL
configuration, credentials, TLS files, role definitions and the matching 1.0.0
extension packages alongside your database recovery plan.

## Before an incident

Use PostgreSQL physical backups and WAL archiving for point-in-time recovery
where required. Test restores on an isolated host. Record the replication group,
current and target versions, replica endpoints and any rollout in progress.
Schedule logical backups as an additional portability and configuration check:

```sh
# PGHOST, PGPORT and PGUSER select the source controller; use a protected .pgpass.
umask 077
pg_dumpall --roles-only --file=roles.sql
pg_dump --format=custom --file=controller.dump --dbname=your_database
```

Freeze role changes and configuration management while taking the pair. The
database dump uses a consistent snapshot, but roles are captured separately.
Protect the files: role dumps can include password hashes, and database dumps
can include connection credentials. Do not use `--no-owner` or `--no-acl` if you
intend to preserve ownership and grants.

The extension marks configuration tables for `pg_dump`, including AZ policies,
pending/current configurations, snapshot seeds and assignments, member reports,
and group deletion locks. The dump does **not** preserve usable replication slots,
live workers, replication origins or continuity with existing replica data.

## Logical restore into a fresh controller

Fence the old controller first: stop application writes, management jobs and
replica daemons, and prevent clients or replicas from connecting to the new host.
Never run two writable controllers for the same cluster. Keep the restored
controller isolated until recovery checks and replica rebuilding are complete.

1. Install PostgreSQL 18 and the same pgwrh 1.0.0 extension files and dependencies.
   Apply the [server settings](packages.md), including wait preloading if used.
2. Inspect `roles.sql`. Remove only the `CREATE ROLE` statement for a bootstrap
   administrator that already exists on the destination; retain its applicable
   `ALTER ROLE` statements. Restore other roles and memberships before the database.
   Resolve tablespace paths separately if your database uses custom tablespaces.
3. Point the `PG*` connection variables at the new host and run:

```sh
psql -X --set=ON_ERROR_STOP=1 --dbname=postgres --file=roles.sql
createdb --template=template0 restored_controller
pg_restore --exit-on-error --single-transaction --dbname=restored_controller \
  --section=pre-data --no-publications --no-subscriptions controller.dump
pg_restore --exit-on-error --single-transaction --dbname=restored_controller \
  --data-only --disable-triggers controller.dump
pg_restore --exit-on-error --single-transaction --dbname=restored_controller \
  --section=post-data --no-publications --no-subscriptions controller.dump
psql -X --set=ON_ERROR_STOP=1 --dbname=restored_controller \
  -c 'SELECT pgwrh.sync_publications();'
psql -X --set=ON_ERROR_STOP=1 --dbname=restored_controller \
  --file=docs/recovery-quarantine.sql
```

Run the restore as a PostgreSQL superuser. Extension tables have circular
references and lifecycle triggers that normally clone and lock configurations.
The separate **data-only** pass disables those triggers while loading the saved
rows and enables them again on success. Each pass is transactional. If any pass
fails, discard this new restore database and retry from a fresh database after
fixing the cause; do not resume traffic against a partial restore.

The publication flags are intentional. Creating the extension already creates
its ping publication; restoring its dumped definition again would collide.
`sync_publications()` recreates pgwrh's shard publications from the saved
assignments, including their internal dependency tracking. The procedure excludes
**all** dumped publications and subscriptions. If this database also owns
unrelated logical replication, inspect `pg_restore --list controller.dump` and
restore only those unrelated entries with a reviewed list before reconnecting
their consumers. Do not blindly restore pgwrh-managed entries.

The quarantine script marks saved hosts offline and clears saved readiness
reports. A report from before the backup is not evidence that a replica is ready
for the restored controller. Do not commit an interrupted rollout on that basis.

## Recover service

For a **logical restore**, rebuild replicas from the recovered controller into
fresh databases with the same extension versions. Reuse the intended replica
identities and roles from the recovered configuration, supply current secrets,
and run `pgwrh.configure_controller(...)` on each fresh replica. Bring each
endpoint online only when it belongs to the rebuilt replica. Reconciliation must
recreate subscriptions, copy assigned shards, build indexes and report readiness.
Never reattach an old data directory by merely pointing it at the new controller.

For an isolated **replica failure** with a healthy controller, mark its
`pgwrh.shard_host.online` false, keep it out of query traffic and rebuild it in a
fresh database. Review replication slots on the controller before dropping any:
remove a stale slot only after fencing its old consumer. If other replicas cannot
serve the required shards, keep reads paused until the replacement is ready.

For a **physical controller failover**, use a tested PostgreSQL HA procedure that
preserves the needed logical slots and WAL continuity. pgwrh does not elect or
fence controllers. If continuity cannot be established, follow the replica
rebuild path above instead of trusting pre-failover reports.

For an **interrupted rollout**, inspect `pgwrh.replication_group` and the
`missing_subscribed_shard`, `missing_connected_local_shard` and
`missing_ready_remote_shard` views for both current and target versions. Resume
reconciliation and obtain new reports before choosing either
`pgwrh.commit_rollout(group_id)` or `pgwrh.rollback_rollout(group_id)`. Rollback
retains target resources until replicas restore current routes; wait for that
acknowledgement before cleaning anything up. Do not edit snapshot or lock tables
to bypass readiness checks.

Before admitting traffic, compare application row counts and checksums against
the restored controller, confirm required shards and indexes on every serving
replica, verify roles and UI grants, and exercise the application's consistency
checks. Use [LSN barriers](lsn-wait.md) when reads must observe a known write.

## Regression coverage

`test/pgwrh/test_backup_restore.py` performs real `pg_dump`, `pg_dumpall` and
`pg_restore` operations into fresh PostgreSQL clusters for committed, pending
and in-flight configurations. It compares every extension configuration table,
AZ policies, assignment seeds, application data, publications, role membership
and UI grants, and checks that lock and rollout protections still apply. It
also executes the quarantine script. The tests do not claim automatic failover
or reuse of existing replication slots after a logical restore.

```sh
nix develop .#tests --command python3 -m pytest test/pgwrh/test_backup_restore.py -q
```
