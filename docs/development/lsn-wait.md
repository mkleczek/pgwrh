# Replication visibility monitor internals

`pgwrh_wait` observes logical replication commits and lets readers wait for a
publisher log sequence number (LSN). Read the [user contract](../lsn-wait.md)
before changing the monitor or its transaction ordering.

The preloaded library also captures writer tokens in backend-local state.
`src/commit.c` copies PostgreSQL's `XactLastCommitEnd` at `XACT_EVENT_COMMIT`
only when `GetTopTransactionIdIfAny()` is valid: `RecordTransactionCommit` then
emitted a commit record, and the callback runs after ProcArray removal. A plain
read normally leaves the core value alone, but no-XID transactions that emit
maintenance WAL can also overwrite it without a commit record. Keeping our own
value preserves the last actual commit across those reads as well as rollbacks.
The callback performs no allocation, catalog access or SQL. PREPARE, abort and
subtransaction events do not update the token; two-phase subscriptions remain
unsupported. `last_commit_lsn()` is volatile and parallel unsafe because the
value belongs to this backend. It requires preloading so that commits before
the first function call are observed, and returns NULL before the first
qualifying commit. Token capture does not depend on an active subscription.

`test/pgwrh_wait/test_commit_lsn.py` covers session lifetime, concurrent writers,
asynchronous commit, rollback/failed commit, subtransactions and WAL without an
XID. The idle-subscriber test compares the token to the monitored apply commit
end and contrasts it with an overshooting global WAL position. General wait
tests use exact tokens without compensating heartbeats; filtered transactions
retain coverage of the published-marker requirement.

The preloaded library registers a transaction callback in every backend and
filters for leader and parallel apply workers, excluding table and sequence
synchronization workers and ordinary sessions. PRE_COMMIT captures the worker's own
origin LSN (`replorigin_session_origin_lsn` on 18,
`replorigin_xact_state.origin_lsn` on 19) and reserves a shared hash entry. Only
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

This relies on PostgreSQL 18/19 internal worker structures and callback ordering.
`src/compat.h` adapts origin state, shared-hash initialization, LSN soft-error
parsing and table-readiness enumeration. The monitor and wait logic stay shared.
PostgreSQL 19 sequence synchronization neither publishes table-read watermarks
nor blocks a wait once all subscription tables are ready. The origin-setup commit
still precedes the upstream connection, and COMMIT callbacks still follow
ProcArray removal in `REL_19_BETA3`. Other majors are rejected until audited.
Relevant upstream code:

- [Commit record and XactLastCommitEnd handling on PostgreSQL 18](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/access/transam/xact.c)
- [Commit record and XactLastCommitEnd handling on PostgreSQL 19 Beta 3](https://github.com/postgres/postgres/blob/REL_19_BETA3/src/backend/access/transam/xact.c)
- [CommitTransaction and ProcArray ordering](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/access/transam/xact.c)
- [Apply commit handling and origin setup](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/replication/logical/worker.c)
- [Parallel apply commit ordering](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/replication/logical/applyparallelworker.c)
- [Snapshot requirements for SET](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/tcop/pquery.c)
