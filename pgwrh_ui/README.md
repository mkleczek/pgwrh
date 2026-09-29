# pgwrh_ui

`pgwrh_ui` is an optional browser console for monitoring and managing a pgwrh
cluster. Install it **only in the controller database**. **PostgREST** runs as a
separate web service and connects the browser to that database. No console
installation is needed on replicas.

See [cluster concepts](../docs/overview.md) for replication groups, availability
zones (AZs), placement and rollouts. The [Compose
quickstart](../docs/containers.md) includes a read-only console if you want to
try it before configuring a deployment.

## Screens

- **Overview:** replication groups, configuration phase, current shard counts and
  readiness blockers, with links to the relevant group screens.
- **Replicas:** AZ, endpoint, current/next weight, assigned copies, reported
  local and remote readiness, active slots and confirmed WAL lag.
- **Placement:** additions, retained copies and retirements relative to the
  current placement. Editable drafts call `pgwrh.preview_shard_placement`;
  started rollouts use saved assignments. Filter by shard, replica or AZ.
- **Rollout:** subscription, local-attachment and remote-route blockers using
  the controller's existing readiness rules. Filter and page through blockers.

Operators can register a replica using an existing non-superuser LOGIN
REPLICATION role, set its pending weight, exclude/reinclude it in the next
placement, enable/disable routing for maintenance, and start, commit or roll
back a rollout. Replica installation, credentials, grants, connection setup,
group creation and table policies remain managed through the existing SQL API.

Registration includes a **Database** field, initially set to the controller's
database name. Set it to the replica's actual database name; the replica list
shows it alongside the hostname and port. Configure the replica's connection
back to the controller separately, as shown in
[database connections](../docs/overview.md#database-connections).

Exclusion edits the draft's host weights; it does not unregister the member.
Maintenance changes `shard_host.online`; replicas learn the routing change on
their next synchronization and continue replicating their assigned shards.
Changing either setting is distinct from deleting replica data or terminating
queries already using the host.

## Install

Requirements: PostgreSQL 18 with pgwrh 1.0.0-alpha1, and PostgREST with custom media
handler support. The Compose example pins PostgREST 14.16. The console does not
require the optional `pgwrh_wait` extension.

The repository's root build includes pgwrh_ui. To install only this extension
into an existing PostgreSQL installation, run from the repository root:

```sh
make -C pgwrh_ui PG_CONFIG=/path/to/pg_config
make -C pgwrh_ui install PG_CONFIG=/path/to/pg_config
```

Browser assets are included; no CDN or separate static file service is required.

As a database administrator connected to the **controller database**:

```sql
CREATE EXTENSION pgwrh_ui;
```

Packages install this guide, `readonly.sql`, `operator.sql`, `postgrest.conf`,
and `HTMX-LICENSE` under `$(pg_config --sharedir)/pgwrh_ui`. For PGDG RPMs this
is `/usr/pgsql-18/share/pgwrh_ui`; for DEBs and the container it is
`/usr/share/postgresql/18/pgwrh_ui`. Nix includes the same directory in its
PostgreSQL bundle. PostgREST remains an external service and is not started by
package installation.

Create the dedicated access roles once, using the supplied scripts. The examples
below use a source checkout; with an installed package, replace `pgwrh_ui/` with
the installed support directory above. Set `CONTROLLER_ADMIN_URI` to an
administrator connection URI for the controller database:

```sh
psql -X -v ON_ERROR_STOP=1 "$CONTROLLER_ADMIN_URI" -f pgwrh_ui/readonly.sql
psql -X -v ON_ERROR_STOP=1 "$CONTROLLER_ADMIN_URI" -f pgwrh_ui/operator.sql
```

The second script is optional. Roles are deployment configuration and are not
created or dropped by the extension. A viewer can read every group on the
controller; an operator can additionally manage every group. Grant only these
endpoint privileges, not access to all functions or controller tables. The
extension owner must be a trusted administrator with permission to run pgwrh
management operations. Use fresh database sessions without temporary tables when
calling UI endpoints directly from SQL.

Create a non-superuser PostgREST login and grant viewer access:

```sql
CREATE ROLE pgwrh_ui_authenticator LOGIN NOINHERIT;
GRANT pgwrh_ui_viewer TO pgwrh_ui_authenticator;
```

If you ran `operator.sql` and want management access through this login, also
run:

```sql
GRANT pgwrh_ui_operator TO pgwrh_ui_authenticator;
```

Configure its authentication using your PostgreSQL deployment's normal method
(password, peer or certificates), then start the external server:

```sh
export PGRST_DB_URI='postgresql://pgwrh_ui_authenticator@localhost/controller_database'
postgrest pgwrh_ui/postgrest.conf
```

Open [the console](http://127.0.0.1:3000/rpc/index). The example defaults to
viewer access. For a local administrative console, set
`PGRST_DB_ANON_ROLE=pgwrh_ui_operator` while retaining the loopback bind. A
remotely exposed deployment should supply authenticated roles via PostgREST JWTs
or an authenticated reverse proxy. The console does not provide its own login
page or browser token manager.

Expose only the `pgwrh_ui` schema. Keep browser requests on the same origin and
have a reverse proxy preserve the external `Host`, origin information and
request headers. A proxy may mount the console at a path prefix if it preserves
the `/rpc/` route structure; page and asset links are relative to that
directory.

After extension installation or function changes, reload PostgREST's schema
cache (`NOTIFY pgrst, 'reload schema'`) or restart PostgREST. This release has
fresh-install SQL only; there is no upgrade script for an earlier pgwrh_ui.
Dropping `pgwrh_ui` removes its objects without changing controller
configuration.

## State and concurrency

Read-only page requests never create a draft. Replica actions create/edit the
existing pgwrh pending configuration. The single draft belongs to the group;
there are no separate drafts per browser or operator.

If another operator or SQL client changes the configuration while a form is
open, submission returns a conflict (HTTP 409). Refresh the form, review the new
state and try again. Replica readiness reports do not invalidate open forms.

Commit and rollback require confirmation in the console. Commit checks readiness
again before accepting the configuration. Rollback retains target copies until
replicas acknowledge restored current routes. If validation fails, the action is
rolled back and the console displays an error.

Polling refreshes only status panels every ten seconds, pauses in hidden tabs,
and preserves open forms and confirmation checkboxes. Connection failures show a
persistent banner until a successful request. Filtered placement and blocker
results are rendered in pages of 100 rows; placement counts cover the entire
group. Preview computation still evaluates the group's placement policy.

## Monitoring limits

The console uses the controller's existing reports. There is no per-replica
report timestamp, heartbeat, durable error log or history, so the UI explicitly
says that report age is unavailable. The displayed query time is when the
controller was read, not when a replica last reported. Zero blockers does not
prove live reachability; absent slot metrics are shown as unavailable.

WAL is PostgreSQL's write-ahead log. Confirmed WAL lag is the publisher WAL
position minus a replication slot's confirmed flush position, aggregated to the
largest gap across the member's logical slots in this database. It is a byte
distance, not elapsed time or a read-consistency guarantee. The UI does not
estimate transfer bytes, disk capacity or progress percentages without the
required telemetry. Controller failure also makes this controller-hosted console
unavailable.
