# Replication visibility monitor internals

`pgwrh_wait` observes logical replication commits and lets readers wait for a
publisher log sequence number (LSN). Read the [user contract](../lsn-wait.md)
before changing the monitor or its transaction ordering.

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

Progress is keyed by database OID and subscription OID. Parallel apply uses the
committing worker's own commit LSN, not the concurrently changing shared origin
position. PostgreSQL's leader waits for each parallel transaction's completion
before proceeding past its commit, preserving commit order.

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
untracked subscribers report an explicit capacity error to readers. Increase the
setting and restart to reclaim the table.

This relies on PostgreSQL 18 internal worker structures and callback ordering.
The build rejects other major versions until the implementation and tests have
been audited for them. Relevant upstream code:

- [CommitTransaction and ProcArray ordering](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/access/transam/xact.c)
- [Apply commit handling and origin setup](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/replication/logical/worker.c)
- [Parallel apply commit ordering](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/replication/logical/applyparallelworker.c)
- [Snapshot requirements for SET](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/tcop/pquery.c)
