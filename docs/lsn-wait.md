# Waiting for logical apply visibility

`pgwrh_wait` is an optional C component shipped in the pgwrh repository. It
supports PostgreSQL **18** and built-in logical replication with `pgoutput`.
It has no dependency on a patched FDW. The pgwrh core can be used without the wait API, but version 0.3.0 still
requires the bundled PostgreSQL 18 FDW.

## Installation

Build against the PostgreSQL installation that will load the library:

```sh
make PG_CONFIG=/path/to/postgresql18/bin/pg_config
make PG_CONFIG=/path/to/postgresql18/bin/pg_config install
```

On every subscriber that will serve guarded reads, add `pgwrh_wait` to
`shared_preload_libraries`, preserving other entries, and restart PostgreSQL.
Then install the SQL API in each database:

```sql
CREATE EXTENSION pgwrh_wait;
```

This installs the wait API independently, without `pgwrh`, `pgwrh_fdw`, or
`pg_background`. Its functions use the `pgwrh` schema; sharing that schema does
not require the `pgwrh` extension.

The library must be preloaded in **both apply workers and reader backends**.
`LOAD` and `session_preload_libraries` cannot initialize this shared monitor.
Creating the extension without preloading is allowed, but its functions report
an explicit prerequisite error. PostgreSQL accepts unknown custom GUCs as
placeholders, so a `SET` alone cannot detect an entirely absent library. Verify
the deployment by calling `pgwrh.applied_lsn(subscription_name)` in a separate
health-check transaction before using the GUC protocol.

To omit only the wait component, use `make WITH_LSN_WAIT=0 install`.
`pgwrh_wait` shares version 0.3.0 with pgwrh and is a separate optional extension
whose functions live in the `pgwrh` schema. Only fresh installation scripts are
provided. Dropping it removes its SQL functions; removing a preloaded library
requires a server restart.

## Snapshot-safe transaction barrier

Send the following commands on the actual connection that will execute the
read. Substitute a publisher watermark obtained **after the write committed**:

```sql
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL pgwrh.read_after_subscription = 'pgwrh_replica_subscription';
SET LOCAL pgwrh.wait_timeout_ms = '5s';
SET LOCAL pgwrh.read_after_lsn = '0/12345678';
SELECT * FROM my_table;
COMMIT;
```

`SET LOCAL pgwrh.read_after_lsn` does not return until the configured subscription
has committed through that watermark. It waits without acquiring a transaction
snapshot. A subsequent statement can then acquire a snapshot containing those
commits. Read Committed, Repeatable Read and Serializable are supported. This
is a lower bound on visibility, not a global snapshot or an upper bound on
which later writes are visible.

Use an explicit top-level transaction and send the watermark before any query,
including `SELECT 1`, `set_config`, cursor creation, or snapshot import. Setting
it too late, in a subtransaction, through a function, or as a session default
errors. Repeating or increasing the watermark before the first snapshot is
allowed. Transaction completion restores the GUCs automatically; assign hooks
never perform a wait during rollback or cleanup.

The default subscription is `pgwrh_replica_subscription`; the default timeout
is 10 seconds. A timeout of zero performs one immediate progress check. A
timeout or cancellation raises an error, so callers must roll back. There is
no fallback to stale reads. The timeout bounds progress waiting; ordinary
catalog/DDL lock acquisition is additionally governed by `lock_timeout` and
`statement_timeout`. A shared subscription lock is retained until transaction
end to prevent concurrent drop or reconfiguration. In particular, enable a
disabled subscription **before** starting a wait that needs new progress.

This guards reads on this subscriber only. With foreign tables, every actual
remote transaction must execute the same barrier before its snapshot, including
connections opened after reconnects. Stock `postgres_fdw` does not propagate
these settings. A coordinator-side wait alone is insufficient. The bundled
[`pgwrh_fdw`](../pgwrh_fdw/README.md) extension provides configurable propagation;
the receiving extension implements waiting. Packaging the FDW does not convert
existing foreign servers or configure propagation automatically.

## SQL API

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

A SQL function is called after its statement's snapshot has been taken. Thus
the SQL wait only protects a **subsequent Read Committed statement**. Combining
the wait and a read in one SELECT, CTE, join or RLS policy does not refresh that
snapshot. The SQL wait rejects Repeatable Read and Serializable; use the GUC
barrier for those isolation levels. Both APIs are callable by ordinary users;
normal table access privileges still apply.

## Token and replication requirements

An LSN is scoped to a publisher database/replication history, not a globally
unique token. The application or routing layer must associate the token with
the correct subscription and publisher. Do not compare unrelated publishers
or reuse tokens after rebuilding a topology from a different history.

Capture a token after COMMIT; an LSN sampled inside the writing transaction or
a row's transaction ID does not identify its commit boundary. Sampling
`pg_current_wal_insert_lsn()` on the publisher after COMMIT provides a
conservative bound but may include WAL beyond the last published commit. The
component advances on applied commits, **not keepalives or received WAL**.
An idle or filtered subscription may therefore need a later published
heartbeat transaction to reach the bound. Arrange for a heartbeat table in
every participating subscription, and commit a heartbeat after obtaining the
token (or run periodic heartbeats). Token issuance and heartbeat scheduling
are outside this component.

All tables in a subscription must have completed initial synchronization
before a wait is accepted. `copy_data=false` requires the administrator to have
already established a valid baseline. The guarantee covers the subscribed
rows and columns only. Two-phase subscriptions, including their pending state,
are rejected. Ordinary and streamed transactions (`streaming=off`, `on`, and
`parallel`) are supported; aborts do not publish an in-flight watermark.

Manual origin advancement, previously skipped transactions, local divergence,
conflicts that PostgreSQL resolves by skipping changes, table truncation outside
replication, and changing the publisher behind an existing subscription can
invalidate a read-your-writes claim. These are not repaired by an LSN barrier.
A currently pending subscription skip is rejected. Administrators must rebuild
a valid baseline before using the barrier after such operations.

## Implementation and restart behavior

The preloaded library registers a transaction callback in every backend and
filters for logical apply workers, excluding table synchronization workers and
ordinary sessions. PRE_COMMIT captures the worker's own
`replorigin_session_origin_lsn` and reserves a shared hash entry. Only
XACT_EVENT_COMMIT publishes it, after `ProcArrayEndTransaction` has removed the
applying transaction. The post-commit path does no allocation, catalog access,
SQL execution, or error reporting. Aborted/prepared transactions do not publish.
Reader waits use a condition variable and recheck progress under an LWLock;
cancellation always removes the reader from the wait queue. Active waits appear
as `Extension / PgwrhWaitForLSN` in `pg_stat_activity`; elapsed time uses a
monotonic clock.

Progress is keyed by database OID and subscription OID. Parallel apply uses
the committing worker's own commit LSN, not the concurrently changing shared
origin position. PostgreSQL's leader waits for each parallel transaction's
completion before proceeding past its commit, preserving commit order.

For a new shared entry, the leader's origin-setup transaction captures recovered
origin progress before connecting upstream or launching parallel workers. Its
commit callback initializes the monitor. Entries are never removed until server
restart, so a replacement worker retains an existing callback-confirmed position
instead of trusting a live origin that an old parallel worker might still be
advancing. If a worker dies after committing but before publishing its callback,
the monitor can conservatively lag until the next applied commit or server
restart. Ordinary readers never use origin progress as evidence of visibility:
during live apply it can run ahead of ProcArray removal.

Shared state is rebuilt after server restart. Successful waits guarantee
visibility at that time, not crash durability or survival across failover.
Durability follows PostgreSQL's replication and synchronous-commit settings.

`pgwrh.max_tracked_subscriptions` is a postmaster setting, default 256, for the
number of database/subscription identities tracked over the server's lifetime.
Entries remain until restart so dropped/recreated names cannot inherit a
watermark. Size this for subscription churn. Exhaustion does not stop apply;
untracked subscribers report an explicit capacity error to readers. Increase
the setting and restart to reclaim the table.

This relies on PostgreSQL 18 internal worker structures and callback ordering.
The build rejects other major versions until the implementation and tests have
been audited for them. Relevant upstream code:

- [CommitTransaction and ProcArray ordering](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/access/transam/xact.c)
- [Apply commit handling and origin setup](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/replication/logical/worker.c)
- [Parallel apply commit ordering](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/replication/logical/applyparallelworker.c)
- [Snapshot requirements for SET](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/tcop/pquery.c)

## Tests

Install PostgreSQL 18 development files, `pg_background`, and the Python tools
in `test/pgwrh_wait/requirements.txt`. Then:

```sh
make PG_CONFIG=/path/to/postgresql18/bin/pg_config test-stage
PGWRH_TEST_BIN_DIR=/path/to/postgresql18/bin python3 -m pytest test/pgwrh_wait -v
```

Tests use temporary real publisher/subscriber clusters and PostgreSQL 18's
`extension_control_path`, so installing pgwrh into the system directories is
unnecessary. `PGWRH_TEST_CONTROL_PATH` and `PGWRH_TEST_LIBRARY_PATH` optionally
add directories for externally staged dependencies. Tests need permission to
bind local ports. No existing server or data directory is used.
