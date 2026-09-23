# pgwrh_fdw

`pgwrh_fdw` is a PostgreSQL 18/19 **foreign data wrapper (FDW)**: it lets local
foreign tables access tables in other PostgreSQL databases. It can pass selected
transaction settings to remote databases and choose among several eligible
servers. It is included in [pgwrh](../README.md#components) and can also be used
independently.

This guide covers standalone configuration. pgwrh manages its own foreign
servers; for cluster operations, start with [cluster
concepts](../docs/overview.md).

The extension is based on PostgreSQL's `postgres_fdw`. Without the
`transaction_parameters` and `members` options described below, it retains that
wrapper's query, connection and transaction behavior. Both wrappers can be
enabled in the same database.

Version **1.0.0-alpha1** is shared by all four extensions in the pgwrh distribution.
The fork is licensed under **AGPL-3.0-only**, with PostgreSQL's original notices
preserved. See [LICENSING.md](LICENSING.md).

## Build and install

Install the [pgwrh bundle](../README.md#installation), or run the commands below
from the repository root to build and install just this extension.

You need PostgreSQL 18 or 19 server development headers, PGXS, libpq, a C compiler,
and Make. Select the installation explicitly if you have several versions:

```sh
make -C pgwrh_fdw PG_CONFIG=/path/to/postgresql-18/bin/pg_config
make -C pgwrh_fdw PG_CONFIG=/path/to/postgresql-18/bin/pg_config install
```

The install command needs write access to that PostgreSQL installation. Only
PostgreSQL 18 and PostgreSQL 19 Beta 3 (preview) are supported by this checkout.
`PG_CONFIG` selects the matching source tree under `18/` or `19/`. Version 1.0.0-alpha1 supports fresh installation; there are
no migration or upgrade scripts. Existing experimental installations need a
planned recreation of the extension and dependent foreign objects. Installing
new files alone does not update extensions already enabled in a database.

## Configuration

The SQL helper `pgwrh_fdw_scram_verifier(password text)` returns a freshly salted
SCRAM verifier using PostgreSQL's native implementation and `scram_iterations`
setting. pgwrh uses it to provision [managed source identities](../docs/credentials.md).
Persist its result when the same verifier must be installed again; each call
generates a new salt.

The example assumes a remote `public.items` table and a remote
`application_user` role permitted to read it. Choose a local schema without a
conflicting `items` table. `app.request_id` is an example application setting;
passing it does not itself add remote application behavior.

```sql
CREATE EXTENSION pgwrh_fdw;

CREATE SERVER replica_a FOREIGN DATA WRAPPER pgwrh_fdw OPTIONS (
    host 'localhost', dbname 'replica'
);
CREATE USER MAPPING FOR CURRENT_USER SERVER replica_a
    OPTIONS (user 'application_user', password 'replace-me');

IMPORT FOREIGN SCHEMA public LIMIT TO (items)
    FROM SERVER replica_a INTO public;

ALTER SERVER replica_a OPTIONS (ADD transaction_parameters 'app.request_id');

BEGIN;
SET LOCAL app.request_id = 'request-42';
SELECT * FROM items;
COMMIT;
```

Configuration parameters are also called **GUCs** in PostgreSQL. The FDW sends
their values as strings. PostgreSQL accepts undefined custom parameters as
placeholders, so successful propagation does not prove that a receiver acts on
them. For replication waits, install and configure the receiving [`pgwrh_wait`
extension](../docs/lsn-wait.md) before relying on a replication watermark (a log
position the read must wait for).

`transaction_parameters` is a **foreign-server option only**. It accepts a
nonempty comma-separated list; whitespace around names is ignored. Names are
case-insensitive and normalized to lowercase. Names must use ASCII identifier
components matching `[a-z_][a-z0-9_]*`, separated by one or more individual
dots, with a maximum total length of 63 bytes. Quoted identifiers, empty
elements, duplicates, and undotted/core GUC names are rejected. The
`postgres_fdw.*` and `pgwrh_fdw.*` namespaces are excluded. In particular, this
cannot override the FDW's search path, timezone, encoding, or transfer settings.

Omit the option, or `ALTER SERVER ... OPTIONS (DROP transaction_parameters)`, to
disable propagation. An empty option string is an error.

## Virtual servers

A server with `members 'replica_a,replica_b'` delegates connections to ordinary
pgwrh_fdw servers. It requires an empty user mapping; credentials and
transaction settings come from the selected member's mapping and server. Routing
prefers an existing connection to an eligible member, so overlapping virtual
servers can share one remote transaction. Among equally reusable targets,
selection is proportional to the actual server's positive integer
`load_balance_weight` (default 1). Selection remains fixed for the local
transaction. Initial connection failures can try another eligible target before
acquiring a remote transaction; established bindings and context/query errors
never fail over. Virtual servers with identical member sets share one selection
for the same effective user, regardless of member order, including across
statements and savepoints. Each shard can therefore have its own stable virtual
server name. `pgwrh_fdw_set_members('shard_server', ARRAY['replica_a',
'replica_b'])` waits for users of the old routing configuration before changing
membership. The update becomes ready to report after its transaction commits.
See [virtual servers](docs/virtual-servers.md) for configuration, access checks,
option ownership and error behavior.

Virtual servers with a common member can push eligible SELECT joins to that
member, including joins between different shard servers. Identical member sets
also allow repeated shard references across pushed joins, separate scans and
partitionwise joins because all aliases share a transaction binding. See
[virtual-server routing](docs/virtual-servers.md) for transaction and plan
rules.

## Local lookup joins

Small filtered local tables can drive remote INNER and SEMI joins through
parameterized `unnest` relations. Output-only lookup values stay local; shard
routing skips irrelevant execution connections. Ordinary joins and `IN`/`EXISTS`
need no manual arrays. Condition types and operators follow postgres_fdw's
shippability rules, including extension types configured on the server; shard
pruning additionally requires compatible partition-key equality.
See [automatic lookup joins](docs/lookup-joins.md) for
supported inputs, the enable setting, runtime bounds/fallback and a reproducible
`EXPLAIN ANALYZE` example.

## Frozen transaction context

The first attempt to start a remote transaction on a server with
`transaction_parameters` captures the effective values of **all supported custom
GUCs visible to the current local role**. Only each server's explicit list is
sent remotely. The capture lives until the local top-level transaction ends,
including across savepoint rollback and caught exceptions.

Every participating server uses values from that same capture, even if first
accessed later or configured with a different list. A connection to a server
without the option does not trigger capture. Planning with `use_remote_estimate`
can trigger it, including during planning/execution of a prepared statement.

**Later local changes are ignored by propagation.** This includes `SET`, `SET
LOCAL`, `set_config()`, function-local settings, and restoration on function
return or rollback. They still have their ordinary local effects. If first
foreign access occurs inside a function with its own settings, those effective
settings are captured. Establish context before planning or foreign access and
keep it fixed for the transaction.

For example, after server A receives request ID `first`, changing the local ID
to `second` leaves both A and a newly accessed server B using `first`. Commit or
roll back the top-level transaction to capture a new context.

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
extension must be loaded beforehand if its behavior is required.

## Remote lifecycle and savepoints

The FDW sends the selected settings in the configured order at the start of each
remote transaction, before queries, remote estimates or savepoints. This also
applies when reusing or reconnecting a connection.

Settings are applied at the remote top level **before mirrored savepoints**. If
first access is inside a savepoint, rolling back that savepoint retains the
remote settings and the original local capture. Each subsequent top-level
transaction gets a fresh capture and fresh `SET LOCAL` commands.

If remote initialization fails, catching the error does not make the connection
usable. Roll back the local transaction before retrying. Values can appear in
PostgreSQL statement and error logs, just as with ordinary `SET` commands.

## Limits

This propagates transaction context; it does not interpret LSNs, inspect
subscriptions, wait for replication, establish a global snapshot, or implement
distributed atomic commit. Different remote participants retain upstream FDW
snapshot semantics. Arbitrary later GUC synchronization, remote changes made by
user code, and parallel-worker context transfer are outside version 1.0.0-alpha1.
Upstream `postgres_fdw` does not provide parallel-aware foreign scans; async
execution is supported and tested. Compatibility with other PostgreSQL major
versions or untested minors is not claimed.

The fork and our modifications use [AGPL-3.0-only](18/LICENSE); inherited
PostgreSQL material retains its [original license](18/COPYRIGHT) (also [PostgreSQL 19](19/COPYRIGHT)). See
[LICENSING.md](LICENSING.md) for attribution and source-offer details.

## Contributor documentation

See [test instructions](../docs/development/testing.md#foreign-data-wrapper),
[implementation and maintenance](docs/design.md), [upstream
provenance](UPSTREAM.md), and [historical validation](docs/validation.md).
