# Design and upstream maintenance

This contributor guide describes how `pgwrh_fdw` propagates transaction settings
and how to maintain its PostgreSQL fork. Read the [user contract](../README.md)
first; [virtual-server internals](virtual-server-internals.md) covers routing.

## Contract

Capture all supported custom parameters on the first configured remote
transaction initialization attempt, then select each server's explicit subset
from that immutable capture. Capturing only a server's list on its first use
would let later participants see different values. Capturing every visible
custom parameter avoids that inconsistency without intercepting arbitrary SET,
function GUC stacks, or `set_config()` calls. Unlisted values are never sent.

The capture uses `GetConfigOption`, not display-oriented show hooks. Registered
GUC defaults and empty placeholders have their normal effective values. Names
are deliberately restricted to a small, documented ASCII grammar. Missing or
inaccessible values cannot silently fall back to the remote default.

## Lifecycle integration

`GetConnection()` retains upstream connection-cache, pending async request,
catalog invalidation, and dead-connection retry logic. Both its ordinary path
and reconnect path call `begin_remote_xact()` with the selected cache entry.
At remote depth zero, that function resolves the configured values, starts the
transaction, and synchronously applies `SET LOCAL` before returning or creating
any savepoint. Thus remote-estimate EXPLAIN, execution initialization, prepared
modifications, ANALYZE, IMPORT, and other callers share the same gate.

Connection setup performs upstream session SET commands before a transaction;
it does not execute a snapshot-taking SELECT. `PQescapeIdentifier` and
`PQescapeLiteral` use the actual connection's encoding/quoting behavior. Each
SET goes through upstream `do_sql_command`, which waits for `PGRES_COMMAND_OK`.
No query is bundled ahead of its completion and no `SELECT set_config()` is used.

`changing_xact_state` remains armed from START until all settings succeed.
Once START succeeds, `xact_depth` is one. A propagation error cannot trigger
the upstream depth-zero reconnect retry or return a partially configured
connection. Upstream incomplete-state rejection/abort cleanup discards it.
Failure before a mirrored savepoint can therefore require top-level rollback;
a caught exception is not permission to use partial remote context.

## Savepoints and ownership

The frozen hash table is owned by `TopTransactionContext`, not the allocating
subtransaction. Its pointer is reset by an independent terminal transaction
callback, including error paths before a remote connection becomes usable.
The complete capture is published only after it is built. No subtransaction
callback resets it.

Applying SET at depth one before creating `s2`, `s3`, etc. keeps settings outside
every mirrored savepoint. Rollback preserves them, including first access in
nested subtransactions. If local rollback restores a different local value,
the frozen value still governs newly participating remote servers. No separate
per-connection propagation flag can become stale after rollback.

## Coexistence audit

* SQL extension, wrapper, handler, validator, connection inspection/disconnect
  functions, installation script, library and module magic name use
  `pgwrh_fdw`.
* The application-name GUC and reserved GUC prefix use `pgwrh_fdw`; the upstream
  `postgres_fdw.application_name` remains independent.
* SQL C entry points and their `pg_finfo_` exports are renamed. Every shared
  helper/global declared in the FDW header is prefixed through `namespace.h`.
  Hidden visibility prevents accidental ELF interposition; a symbol audit checks
  the export allowlist. PostgreSQL's loader-required `Pg_magic_func` and
  `_PG_init` retain their prescribed names and are resolved per module.
* ConnectionHash, cursor/prepared-statement counters, options, shippability
  cache, callbacks, and custom wait-event IDs are module-local. Wait-event and
  diagnostic cache names are renamed. No hooks into stock postgres_fdw are used.
* Both load orders are tested in separate fresh backends, with distinct remote
  PIDs, application names, connection lists, and disconnect effects.

## Upstream strategy

pgwrh_fdw shares SQL extension and module version `1.0.0-alpha1` with the pgwrh release.
This is the only installable version, with no upgrade scripts.
The initial SQL install script directly defines the final upstream function
signatures rather than replaying postgres_fdw's historical upgrades. The C
entry point `pgwrh_fdw_get_connections_1_2` retains its upstream API suffix;
that suffix is not a pgwrh_fdw release version. Future upstream SQL changes
must be adapted to pgwrh_fdw's installation script.

`upstream/postgres_fdw` retains the unmodified filtered PostgreSQL history.
Functional jj changes keep that standalone layout. `fdw_base_18` bookmarks one
aggregate with all functional changes as its parents. Git subtree imports the
patched aggregate under `pgwrh_fdw/`; test relocation and repository build
adaptations follow in a separate change.

The import helper advances only the pristine upstream bookmark and its
provenance tag. Duplicate the functional changes and aggregate onto the new
baseline, resolve conflicts there, and test before importing the reviewed
aggregate into pgwrh. The same graph can be copied to another PostgreSQL major,
with compatibility fixes isolated from directory moves. See [upstream
maintenance](../UPSTREAM.md) for the update and porting workflow.

Keep pristine upstream history, mechanical namespace/PGXS changes, and behavior
changes separate. Preserve the upstream PostgreSQL notices; pgwrh_fdw additions
and modifications are AGPL-3.0-only, as documented in LICENSING.md. For each
release merge, audit connection initialization and
retry, snapshot-taking callers, transaction/subtransaction callbacks, exported
symbols, new GUCs/SQL objects, and transfer settings. Port upstream fixes before
claiming the new release tested. Run integration, upstream SQL/isolation, TAP,
and export checks on PostgreSQL 18, including Linux for ELF coexistence.
