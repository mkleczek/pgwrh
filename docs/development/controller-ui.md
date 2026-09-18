# Controller console internals

The [console guide](../../pgwrh_ui/README.md) covers deployment and use.
`pgwrh_ui` is a SQL/PLpgSQL extension served through PostgREST. It renders pages
using bundled htmx and reads controller-local views without opening replica
connections. Installation adds functions and media types in `pgwrh_ui`; it does
not change the core tables, triggers or replica reporting protocol.

## Build and assets

The build uses a POSIX shell and `sha384sum` (coreutils) or `shasum` to verify
and embed vendored assets in installation SQL. Python is needed only for tests.
`NO_PGXS=1` is supported. The asset version and digest are recorded in the
[vendor manifest](../../pgwrh_ui/vendor/README.md). No CDN is used at build time
or runtime. See [testing](testing.md#controller-console) for the UI suite.

## Permissions and concurrent edits

Endpoints use SECURITY DEFINER with fixed search paths. Their owner must be
trusted and able to invoke privileged core management APIs. Fresh sessions
without temporary namespaces prevent direct SQL callers from shadowing relations
used by those APIs.

Management forms carry a fingerprint of configuration, membership, lock seeds
and the source partition tree. Mutations serialize on the replication-group
row, validate the fingerprint, and invoke the core API. Stale forms return HTTP
409. Replica reports do not invalidate the fingerprint. Core readiness checks
run again inside `commit_rollout`, independently of the browser button state.

Mutation endpoints check `Origin`, `Sec-Fetch-Site` and a custom request header.
Reads use GET; management uses POST. Validation failures roll back the entire
action and return an HTML error.
