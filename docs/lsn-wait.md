# Waiting for logical apply visibility

`pgwrh_wait` lets a read wait until a logical-replication subscriber has applied
a known write from its publisher. It supports PostgreSQL **18** and **19 Beta 3**
(preview) with the built-in `pgoutput` replication plugin and can be used independently of pgwrh.
For publisher, subscriber and subscription terminology, see [cluster
concepts](overview.md).

A **log sequence number (LSN)** identifies a position in the publisher's
write-ahead log (WAL). The application supplies an LSN as a **watermark**: the
read waits until the relevant subscription has committed through that position.
A **snapshot** determines which committed rows a query can see, so the wait must
finish before the read acquires its snapshot. See [token
requirements](#token-and-replication-requirements) for obtaining a usable
watermark.

## Installation

Install the [bundle](../README.md#installation), or build only this extension
from the repository root against the PostgreSQL installation that will load it:

```sh
make -C pgwrh_wait PG_CONFIG=/path/to/postgresql18/bin/pg_config
make -C pgwrh_wait PG_CONFIG=/path/to/postgresql18/bin/pg_config install
```

On the controller (publisher) issuing commit tokens and every subscriber that
will serve guarded reads, add `pgwrh_wait` to `shared_preload_libraries`,
preserving other entries, and restart PostgreSQL. Then install the SQL API in
each participating database:

```sql
CREATE EXTENSION pgwrh_wait;
```

This installs the wait API independently, without `pgwrh`, `pgwrh_fdw`, or
`pg_background`. Its functions use the `pgwrh` schema; sharing that schema does
not require the `pgwrh` extension.

A server restart is required; loading the library only in a client session is
insufficient. Preloading also ensures that token capture observes every commit
from the start of a writer session. Creating the extension without preloading
is allowed, but its functions report a prerequisite error. Before using
transaction settings to wait, call `pgwrh.applied_lsn(subscription_name)` in a
separate health-check transaction. PostgreSQL accepts unknown custom settings,
so a successful `SET` alone cannot confirm that the wait extension is available.

Version 1.0.0-alpha1 supports fresh installation only. Dropping the extension removes
its SQL API; removing it from `shared_preload_libraries` requires a server
restart.

## Snapshot-safe transaction barrier

Send the following commands on the actual connection that will execute the read.
Substitute a publisher watermark obtained **after the write committed**:

```sql
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL pgwrh.read_after_subscription = 'pgwrh_replica_subscription';
SET LOCAL pgwrh.wait_timeout_ms = '5s';
SET LOCAL pgwrh.read_after_lsn = '0/12345678';
SELECT * FROM my_table;
COMMIT;
```

`SET LOCAL pgwrh.read_after_lsn` does not return until the configured
subscription has committed through that watermark. It waits without acquiring a
transaction snapshot. A subsequent statement can then acquire a snapshot
containing those commits. Read Committed, Repeatable Read and Serializable are
supported. This is a lower bound on visibility, not a global snapshot or an
upper bound on which later writes are visible.

Use an explicit top-level transaction and send the watermark before any query,
including `SELECT 1`, `set_config`, cursor creation, or snapshot import. Setting
it too late, in a subtransaction, through a function, or as a session default
errors. Repeating or increasing the watermark before the first snapshot is
allowed. Transaction completion restores the settings automatically.

The default subscription is `pgwrh_replica_subscription`; the default timeout is
10 seconds. A timeout of zero performs one immediate progress check. A timeout
or cancellation raises an error, so callers must roll back. There is no fallback
to stale reads. The timeout bounds progress waiting; ordinary catalog/DDL lock
acquisition is additionally governed by `lock_timeout` and `statement_timeout`.
The subscription cannot be dropped or reconfigured concurrently until the
waiting transaction ends. In particular, enable a disabled subscription
**before** starting a wait that needs new progress.

This guards reads on this subscriber only. With foreign tables, every actual
remote transaction must execute the same barrier before its snapshot, including
connections opened after reconnects. Stock `postgres_fdw` does not propagate
these settings. A coordinator-side wait alone is insufficient. The bundled
[`pgwrh_fdw`](../pgwrh_fdw/README.md) extension provides configurable
propagation; the receiving extension implements waiting. Packaging the FDW does
not convert existing foreign servers or configure propagation automatically.

## SQL API

On the controller, `pgwrh.last_commit_lsn() RETURNS pg_lsn` returns the exact
**commit-end LSN** of the current physical session's last successful WAL-logged
transaction. It returns NULL before a qualifying commit. Read-only transactions,
rollbacks and failed commits preserve the previous value. Inside a writing
transaction, it still returns the previous committed transaction's token;
subtransaction completion does not issue a new token. Other sessions' writes
cannot change it. Ordinary users can call it with `USAGE` on the `pgwrh` schema.

On a replica:

```sql
SELECT pgwrh.applied_lsn('pgwrh_replica_subscription');
SELECT pgwrh.wait_for_lsn('pgwrh_replica_subscription', '0/12345678', 5000);
-- Run the read as a separate, subsequent Read Committed statement.
SELECT * FROM my_table;
```

`applied_lsn(text)` returns the last monitored publisher LSN, or NULL before the
monitor initializes. It is diagnostic and does not wait or certify table sync
readiness. `wait_for_lsn(text, pg_lsn, integer DEFAULT 10000)` returns void on
success and rejects NULL arguments, a zero LSN, or a negative timeout.

A SQL function is called after its statement's snapshot has been taken. Thus the
SQL wait only protects a **subsequent Read Committed statement**. Combining the
wait and a read in one SELECT, CTE, join or RLS policy does not refresh that
snapshot. The SQL wait rejects Repeatable Read and Serializable; use the
transaction-setting barrier for those isolation levels. Both APIs are callable
by ordinary users; normal table access privileges still apply.

## Token and replication requirements

An LSN is scoped to a publisher database/replication history, not a globally
unique token. The application or routing layer must associate the token with the
correct subscription and publisher. Do not compare unrelated publishers or reuse
tokens after rebuilding a topology from a different history.

Capture the token **after a successful COMMIT on the same physical controller
connection**, before that connection commits another WAL-logged transaction:

```sql
BEGIN;
INSERT INTO my_table (id, value) VALUES (42, 'updated');
COMMIT;
SELECT pgwrh.last_commit_lsn();  -- Save this pg_lsn as the application's token.
```

An autocommit write followed by the SELECT on the same connection also works.
Do not retrieve the token in `RETURNING`, a CTE, or a SELECT before COMMIT: the
current transaction has no commit boundary yet. Check that COMMIT succeeded;
after a rollback the function may still return a token for an older write.

Keep the physical connection checked out until token retrieval finishes. Session
pooling or a dedicated connection can provide this guarantee. Ordinary
transaction or statement pooling can release or switch the server connection
at COMMIT, so a subsequent SELECT may return NULL or another client's token.
Use a connection mode that pins the backend across both commands. Reconnecting
loses the session's token; checking out a previously used backend does not reset
its commit history.

The application carries the saved token to the replica and supplies it to
`pgwrh.wait_for_lsn` or the snapshot-safe transaction barrier above. Replica
reads need no further controller query. The monitor advances on applied
commits, **not keepalives or received WAL**. When the subscription receives the
application's transaction, its commit-end token is reachable even if the
subscription becomes idle immediately afterward. No later heartbeat is needed
to compensate for unrelated WAL. Sampling `pg_current_wal_insert_lsn()` after
COMMIT can overshoot that boundary and cause such an idle wait to time out.

An exact token does not force publication. If table/row filters exclude the
entire transaction, the subscription has no corresponding apply commit. Include
a published marker in the writing transaction, or arrange a later heartbeat
transaction included in every participating subscription. Until an included
transaction covers the token, waiting can still time out. The guarantee covers
only the rows and columns actually replicated.

All tables in a subscription must have completed initial synchronization before
a wait is accepted. `copy_data=false` requires the administrator to have already
established a valid baseline. The guarantee covers the subscribed rows and
columns only. Two-phase subscriptions, including their pending state, are
rejected. Ordinary and streamed transactions (`streaming=off`, `on`, and
`parallel`) are supported; aborts do not publish an in-flight watermark.

Manual origin advancement, previously skipped transactions, local divergence,
conflicts that PostgreSQL resolves by skipping changes, table truncation outside
replication, and changing the publisher behind an existing subscription can
invalidate a read-your-writes claim. These are not repaired by an LSN barrier. A
currently pending subscription skip is rejected. Administrators must rebuild a
valid baseline before using the barrier after such operations.

## Monitoring, capacity and restarts

Active waits appear as `Extension / PgwrhWaitForLSN` in `pg_stat_activity`.
After a server restart, the monitor rebuilds its state as replication workers
initialize. `applied_lsn` can return NULL before initialization; the wait APIs
wait for progress subject to their timeout.

If an apply worker stops immediately after committing, reported progress can lag
until the next applied commit or a server restart. A heartbeat transaction can
allow the monitor to catch up.

`pgwrh.max_tracked_subscriptions` defaults to 256 and requires a server restart
to change. It limits the number of distinct subscriptions tracked across all
databases between server restarts. Dropped subscriptions continue to count;
recreating a subscription with the same name requires a new entry. Allow for
this when sizing the limit. Exhaustion does not stop replication, but readers of
untracked subscriptions receive a capacity error. Increase the setting and
restart to clear the tracked entries.

A successful wait guarantees visibility at that time, not crash durability or
survival across failover. Durability follows PostgreSQL's replication and
synchronous-commit settings. Re-establish a valid replication baseline after
recovery before relying on old tokens.
