# pgwrh_ui

A SQL/PLpgSQL extension that serves a controller console through external
PostgREST and bundled htmx. Install it **only in the controller database**.
It neither installs anything on replicas nor opens connections to them.
Installation adds functions and media types in `pgwrh_ui`; it does not alter
pgwrh's tables, views, triggers, roles, or reporting protocol.

## Screens

- **Overview:** replica groups, configuration phase, current shard counts and
  readiness blockers, with links to the relevant group screens.
- **Replicas:** AZ, endpoint, current/next weight, assigned copies, reported
  local and remote readiness, active slots and confirmed WAL lag.
- **Placement:** additions, retained copies and retirements relative to the
  current snapshot. Editable drafts call `pgwrh.preview_shard_placement`;
  started rollouts use saved assignments. Filter by shard, replica or AZ.
- **Rollout:** subscription, local-attachment and remote-route blockers using
  the controller's existing readiness rules. Filter and page through blockers.

Operators can register a replica using an existing non-superuser LOGIN
REPLICATION role, set its pending weight, exclude/reinclude it in the next
placement, enable/disable routing for maintenance, and start, commit or roll
back a rollout. Replica installation, credentials, grants, connection setup,
group creation and table policies remain managed through the existing SQL API.

Exclusion edits the draft's host weights; it does not unregister the member.
Maintenance changes `shard_host.online`; replicas learn the routing change on
their next synchronization and continue replicating their assigned shards.
Changing either setting is distinct from deleting replica data or terminating
queries already using the host.

## Install

Requirements: PostgreSQL 18 with pgwrh 1.0.0, and external PostgREST with custom
media handlers (tested with PostgREST 15). No pgwrh_wait installation or preload
setting is required by the UI. The UI adds no native library dependency.

The repository's root build includes pgwrh_ui. To install only this extension
into an existing PostgreSQL installation, run from the repository root:

```sh
make -C pgwrh_ui PG_CONFIG=/path/to/pg_config
make -C pgwrh_ui install PG_CONFIG=/path/to/pg_config
```

The build uses a POSIX shell and `sha384sum` (coreutils) or `shasum` to verify
and embed the vendored assets. Python is needed only for tests. Runtime needs
SQL, PostgREST and the browser. `NO_PGXS=1` is supported. There are no build-time or
runtime CDN requests. The htmx version and digest are pinned in `vendor/README.md`.

As a database administrator connected to the **controller database**:

```sql
CREATE EXTENSION pgwrh_ui;
```

Packages install this guide, `readonly.sql`, `operator.sql`, `postgrest.conf`,
and `HTMX-LICENSE` under `$(pg_config --sharedir)/pgwrh_ui`. For PGDG RPMs this
is `/usr/pgsql-18/share/pgwrh_ui`; for DEBs and the container it is
`/usr/share/postgresql/18/pgwrh_ui`. Nix includes the same directory in its
PostgreSQL bundle. The JavaScript and CSS are embedded in the installation SQL.
PostgREST remains an external service and is not started by package installation.

Create the dedicated access roles once, using the supplied scripts. The examples
below use a source checkout; with an installed package, replace `pgwrh_ui/`
with the installed support directory above:

```sh
psql -X -v ON_ERROR_STOP=1 "$CONTROLLER_ADMIN_URI" -f pgwrh_ui/readonly.sql
psql -X -v ON_ERROR_STOP=1 "$CONTROLLER_ADMIN_URI" -f pgwrh_ui/operator.sql
```

The second script is optional. Roles are deployment configuration and are not
created or dropped by the extension. A viewer can read every group on the
controller; an operator can additionally manage every group. Grant only these
endpoint privileges, not access to all functions or controller tables. Endpoints
use SECURITY DEFINER with fixed search paths; their owner must be trusted and
able to invoke pgwrh's management API, including its privileged rollout work.
UI endpoints require a fresh database session without a temporary namespace,
preventing direct SQL callers from shadowing relations used by existing core APIs.

Create a non-superuser PostgREST login and grant the allowed web roles:

```sql
CREATE ROLE pgwrh_ui_authenticator LOGIN NOINHERIT;
GRANT pgwrh_ui_viewer, pgwrh_ui_operator TO pgwrh_ui_authenticator;
```

Configure its authentication using your PostgreSQL deployment's normal method
(password, peer or certificates), then start the external server:

```sh
export PGRST_DB_URI='postgresql://pgwrh_ui_authenticator@localhost/controller_database'
postgrest pgwrh_ui/postgrest.conf
```

Open **http://127.0.0.1:3000/rpc/index**. The example defaults to viewer access.
For a local administrative console, set `PGRST_DB_ANON_ROLE=pgwrh_ui_operator`
while retaining the loopback bind. A remotely exposed deployment should supply
authenticated roles via PostgREST JWTs or an authenticated reverse proxy. This
MVP does not implement login, password storage, or a browser token manager.

Expose only the `pgwrh_ui` schema. Keep browser requests on the same origin and
have a reverse proxy preserve the external `Host`. Mutation endpoints check
`Origin`, `Sec-Fetch-Site`, and the console's custom request header. Ordinary
reads use GET; management always uses POST. A proxy may mount the console at a
path prefix as long as it preserves the `/rpc/` route structure: asset and page
links are relative to that directory.

After extension installation or function changes, reload PostgREST's schema
cache (`NOTIFY pgrst, 'reload schema'`) or restart PostgREST. This release has
fresh-install SQL only; there is no upgrade script for an earlier pgwrh_ui.
Dropping `pgwrh_ui` removes its objects without changing controller configuration.

## State and concurrency

Read-only page requests never create a draft. Replica actions create/edit the
existing pgwrh pending configuration. The single draft belongs to the group;
there are no separate drafts per browser or operator.

Every management form carries a fingerprint of the configuration, membership,
lock seeds and source partition tree. Mutations serialize on the existing
replication-group row, check that fingerprint, then call the core API. Stale
forms return HTTP 409 with an explanation. Replica state reports do not
invalidate forms. Edits made outside the UI are detected on the next submission;
external SQL clients still need their normal transaction/locking discipline.

Commit and rollback require explicit confirmation. Core readiness checks run
again inside `commit_rollout`; disabling the button is only a convenience.
Rollback retains target copies until replicas acknowledge current routes.
Validation failures roll back the entire action and return an HTML error.

Polling refreshes only status panels every ten seconds, pauses in hidden tabs,
and preserves open forms and confirmation checkboxes. Connection failures show
a persistent banner until a successful request. Filtered placement and blocker
results are rendered in pages of 100 rows; placement counts cover the entire
group. Preview computation still evaluates the group's placement policy.

## Monitoring limits

The MVP uses the controller's existing reports. There is no per-replica report
timestamp, heartbeat, durable error log or history, so the UI explicitly says
that report age is unavailable. The displayed query time is when the controller
was read, not when a replica last reported. Zero blockers does not prove live
reachability; absent slot metrics are shown as unavailable.

Confirmed WAL lag is the publisher WAL position minus a slot's confirmed flush
position, aggregated to the largest gap across the member's logical slots in
this database. It is a byte distance, not elapsed time or a read-consistency
guarantee. The UI does not estimate transfer bytes, disk capacity or progress
percentages without the required telemetry. Controller failure also makes this
controller-hosted console unavailable.

## Tests

```sh
nix-shell nix/ui-tests.nix --run 'make test-ui'
```

Alternatively, stage extensions with `make testgres-ext`, provide the testgres
and pytest Python dependencies, set `PGWRH_TEST_BIN_DIR`/`PG_BIN` to PostgreSQL
18 with pg_background, and run `python3 -m pytest test/pgwrh_ui`.
Set `POSTGREST_BIN` or put `postgrest` on PATH to include the HTTP tests; they are
skipped when no binary is available. All tests use disposable local databases.

Coverage includes installation/removal, read-only rendering, escaped names,
frozen assignments, infeasible previews, permissions, replica operations,
concurrent/stale submissions, commit/rollback checks, HTTP content negotiation,
origin checks and form-encoded requests. `make test-packaging` checks staged
install/uninstall, including SQL-only and standalone extension builds.
