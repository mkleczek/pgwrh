# pgwrh_fdw

An independent PostgreSQL 18 foreign-data wrapper that propagates selected
custom configuration parameters when it starts remote transactions. It forks
PostgreSQL's `contrib/postgres_fdw`; pgwrh is not a dependency.

The PostgreSQL 18.3 source is pinned in [UPSTREAM.md](UPSTREAM.md). The extension
retains upstream query, modification, connection, and transaction behavior
when `transaction_parameters` is absent. Stock `postgres_fdw` and `pgwrh_fdw`
can run together in the same database and backend.

The fork is licensed under **AGPL-3.0-only**, with PostgreSQL's original notices
and permissions preserved. See [LICENSING.md](LICENSING.md).

The first release is **0.1.0**, tagged `v0.1.0`.

## Build and install

You need PostgreSQL 18 server development headers, PGXS, libpq, a C compiler,
and Make. Select the installation explicitly if you have several versions:

```sh
make PG_CONFIG=/path/to/postgresql-18/bin/pg_config
make PG_CONFIG=/path/to/postgresql-18/bin/pg_config install
```

The install command needs write access to that PostgreSQL installation.
Builds for other major versions are rejected. Both the SQL extension version
and the library's module version are `0.1.0`. A fresh `CREATE EXTENSION pgwrh_fdw`
uses the single `pgwrh_fdw--0.1.0.sql` installation script, including all
connection-management functions. There are no upgrade scripts for this first
release; future releases will add them when their SQL definitions need changes.

The former `1.0`/`1.1`/`1.2` install chain was inherited during development and
was never a pgwrh_fdw release. There is no migration path from those development
snapshots. Existing experimental installations need a planned recreation of
the extension and its dependent foreign objects; installing the new files
alone does not change their catalog version. No existing objects are removed
automatically.

## Configuration

```sql
CREATE EXTENSION pgwrh_fdw;

CREATE SERVER replica_a FOREIGN DATA WRAPPER pgwrh_fdw OPTIONS (
    host 'localhost', dbname 'replica',
    transaction_parameters 'pgwrh.read_after_lsn,app.request_id'
);
CREATE USER MAPPING FOR CURRENT_USER SERVER replica_a
    OPTIONS (user 'application_user', password 'replace-me');

IMPORT FOREIGN SCHEMA public LIMIT TO (items)
    FROM SERVER replica_a INTO public;

BEGIN;
SET LOCAL pgwrh.read_after_lsn = '0/12345678';
SET LOCAL app.request_id = 'request-42';
SELECT * FROM items;
COMMIT;
```

These are opaque strings. Neither example parameter needs a defining extension:
PostgreSQL accepts custom parameter placeholders. A receiving extension can
instead define a typed parameter or intercept the explicit `SET LOCAL` utility
command to implement application-specific behavior.

`transaction_parameters` is a **foreign-server option only**. It accepts a
nonempty comma-separated list; whitespace around names is ignored. Names are
case-insensitive and normalized to lowercase. Version one accepts ASCII
identifier components matching `[a-z_][a-z0-9_]*`, separated by one or more
individual dots, with a maximum total length of 63 bytes. Quoted identifiers,
empty elements, duplicates, and undotted/core GUC names are rejected. The
`postgres_fdw.*` and `pgwrh_fdw.*` namespaces are excluded. In particular, this
cannot override the FDW's search path, timezone, encoding, or transfer settings.

Omit the option, or `ALTER SERVER ... OPTIONS (DROP transaction_parameters)`,
to disable propagation. An empty option string is an error.

## Frozen transaction context

The first attempt to start a remote transaction on a server with this option
captures the effective values of **all supported custom GUCs visible to the
current local role**. Only each server's explicit list is sent remotely. The
capture lives until the local top-level transaction ends, including across
savepoint rollback and caught exceptions.

Every participating server uses values from that same capture, even if first
accessed later or configured with a different list. A connection to a server
without the option does not trigger capture. Planning with `use_remote_estimate`
can trigger it, including during planning/execution of a prepared statement.

**Later local changes are ignored by propagation.** This includes `SET`,
`SET LOCAL`, `set_config()`, function-local settings, and restoration on
function return or rollback. They still have their ordinary local effects.
If first foreign access occurs inside a function with its own settings, those
effective settings are captured. Establish context before planning or foreign
access and keep it fixed for the transaction.

For example, after server A receives request ID `first`, changing the local
ID to `second` leaves both A and a newly accessed server B using `first`.
Commit or roll back the top-level transaction to capture a new context.

| Situation | Behavior |
| --- | --- |
| Listed GUC did not exist at capture | Error, even if defined later |
| Registered GUC has a default | Its effective value is captured |
| Value is empty | Explicit `SET LOCAL name = ''` |
| Value is the string `DEFAULT` | Sent as a quoted string, not the SQL keyword |
| Placeholder left empty by RESET/transaction end | Empty string is propagated |
| GUC was not visible to the capturing role | It is unavailable in that capture |
| Later local role cannot examine a selected GUC | Error; visibility is checked again |
| Receiver rejects a type, value, reserved prefix, or permission | Error; dependent remote work cannot proceed |

The receiver applies its ordinary privileges and validation. The extension does
not elevate the mapped remote role or bypass parameter ACLs. A missing remote
custom parameter normally becomes a PostgreSQL placeholder; a receiving
extension must be loaded beforehand if its validation/hook is required.

## Remote lifecycle and savepoints

For each new remote transaction, on a fresh or cached/reconnected connection:

1. Resolve the server's list against the frozen context.
2. Start the remote transaction with upstream isolation semantics.
3. Execute and await each safely quoted `SET LOCAL`, in list order.
4. Create mirrored savepoints, then allow EXPLAIN, scans, or other remote work.

Settings are applied at the remote top level **before mirrored savepoints**.
If first access is inside a savepoint, rolling back that savepoint retains the
remote settings and the original local capture. No “initialized once” flag is
needed for a surviving remote transaction. Each subsequent top-level transaction
gets a fresh capture and fresh `SET LOCAL` commands.

A failed remote initialization leaves the connection marked incomplete. It is
discarded on further access or transaction cleanup. Catching the original error
does not make a partially configured connection usable; further access or
commit can report a connection error. A replacement must complete initialization
again. Values themselves can appear in PostgreSQL statement/error logs, just as
with ordinary `SET` commands.

## Development and tests

No Python packages are required: tests use Python 3's standard library and the
selected installation's libpq. Run as an ordinary OS user, not root:

```sh
export PG_CONFIG=/path/to/postgresql-18/bin/pg_config
python3 test/test_context.py
python3 tools/run-upstream.py
python3 tools/check-symbols.py
```

These commands build and stage the extension in `.build/stage`, start private
temporary PostgreSQL clusters, and stop them on completion. They need permission
to open local sockets. No installation into system PostgreSQL is required.
The tests print the temporary cluster/log location and retain it for inspection.
Use `make clean` (and `make -C test clean`) before switching PostgreSQL builds.

The integration suite covers independent participants, context freezing,
connection reuse, reconnects, planning, prepared reads/writes, async scans,
savepoints, SQL quoting, defaults, missing values, permissions, propagation
failure recovery, and coexistence in both load orders. A test-only receiving
utility hook raises an error if the propagated request ID arrives after the
remote transaction acquires a snapshot.

`tools/run-upstream.py` runs the retained main FDW and query-cancellation SQL
tests plus the `eval_plan_qual` isolation tests. For the retained SCRAM TAP test,
also provide matching PostgreSQL source test modules and Perl's `IPC::Run`:

```sh
PG_SOURCE=/path/to/postgresql-18.3 python3 tools/run-tap.py
```

See [docs/validation.md](docs/validation.md) for versions and actual results;
CI builds the pinned PostgreSQL release and runs the same checks on Linux.

## Limits

This propagates transaction context; it does not interpret LSNs, inspect
subscriptions, wait for replication, establish a global snapshot, or implement
distributed atomic commit. Different remote participants retain upstream FDW
snapshot semantics. Arbitrary later GUC synchronization, remote changes made
by user code, and parallel-worker context transfer are outside version one.
Upstream `postgres_fdw` does not provide parallel-aware foreign scans; async
execution is supported and tested. Compatibility with other PostgreSQL major
versions or untested minors is not claimed.

The fork and our modifications use [AGPL-3.0-only](LICENSE); inherited PostgreSQL
material retains its [original license](COPYRIGHT). See [LICENSING.md](LICENSING.md)
for attribution and source-offer details, and
[docs/design.md](docs/design.md) for the implementation and maintenance audit.
