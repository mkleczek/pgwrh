# Testing pgwrh

Run commands from the repository root. Tests use disposable PostgreSQL clusters
and need permission to open local sockets. Select PostgreSQL 18 or 19 and install the
dependencies for the chosen suite. These commands are for contributors; database
operators can use the [installation check](../../README.md#installation).

The root targets are `test-pgwrh`, `test-ui`, `test-wait`, `test-fdw`,
`test-fdw-tap` and `test-packaging`. Build products and staged extensions live
under `.build/`; PGXS object files and libraries remain beside their sources.
Default pytest discovery covers `test/`. Run the selected FDW suite with
`make test-fdw`; its two imported source trees contain identically named tests.

Use `nix develop .#tests-18` or `nix develop .#tests-19` for a pinned environment.
Both use `pg_background` 2.0.3. Run `bash test/run-functional.sh` inside it for
all core, wait and HTTP UI tests with skipped tests treated as failures.

## Controller and replicas

The Nix environment stages the extensions and provides PostgreSQL and Python:

```sh
nix-shell --run 'pgwrh-test test/pgwrh -q'
```

## Controller backup and restore

`test/pgwrh/test_backup_restore.py` performs real `pg_dump`, `pg_dumpall` and
`pg_restore` operations into fresh PostgreSQL clusters for committed, pending
and in-flight configurations, plus preparing and switching credential rotations.
It compares every extension configuration table, AZ policies, credential
generations and verifiers, application data, publications, role membership
and UI grants, and checks that lock and rollout protections still apply. It also
executes the quarantine script. The tests do not claim automatic failover or
reuse of existing replication slots after a logical restore.

```sh
nix develop .#tests --command python3 -m pytest test/pgwrh/test_backup_restore.py -q
```

## Controller failover

`test/pgwrh/test_controller_failover.py` creates a controller, a physical standby
from a real base backup, and two pgwrh replicas. It runs on PostgreSQL 18 and 19
as part of the core suite, covering:

- Primary selection for both FDW and logical replication with a reachable
  standby listed first in the connection addresses.
- Automatic logical-slot synchronization and detection of a missing required slot.
- Planned switchover and abrupt controller shutdown, preserving subscription
  identities, slot names and connection settings.
- Pending inserts, updates and deletes, automatic reconnection of an enabled
  subscriber, new writes and a new shard rollout after promotion.

To run just these tests, enter `nix develop .#tests-18` (or `.#tests-19`), then:

```sh
make clean
make testgres-ext
python3 -m pytest test/pgwrh/test_controller_failover.py -q
```

The old primary stays stopped after promotion. These tests do not cover an HA
manager's election or fencing, network partitions, or failover during initial
table copy. See the [operator guide](../controller-ha.md) for configuration,
readiness checks and recovery procedures.

## Replication visibility

Install PostgreSQL 18 or 19 development files, `pg_background` 2.0.3, and the Python tools
in `test/pgwrh_wait/requirements.txt`. Then:

```sh
make PG_CONFIG=/path/to/postgresql18/bin/pg_config test-stage
PGWRH_TEST_BIN_DIR=/path/to/postgresql18/bin python3 -m pytest test/pgwrh_wait -v
```

Tests use temporary real publisher/subscriber clusters and PostgreSQL's
`extension_control_path`, so installing pgwrh into the system directories is
unnecessary. `PGWRH_TEST_CONTROL_PATH` and `PGWRH_TEST_LIBRARY_PATH` optionally
add directories for externally staged dependencies. Tests need permission to
bind local ports. No existing server or data directory is used.

## Controller console

```sh
nix-shell nix/ui-tests.nix --run 'make test-ui'
```

Alternatively, stage extensions with `make testgres-ext`, provide the testgres
and pytest Python dependencies, set `PGWRH_TEST_BIN_DIR`/`PG_BIN` to PostgreSQL
18 or 19 with pg_background 2.0.3, and run `python3 -m pytest test/pgwrh_ui`. Set
`POSTGREST_BIN` or put `postgrest` on PATH to include the HTTP tests; they are
skipped when no binary is available. All tests use disposable local databases.

Coverage includes installation/removal, read-only rendering, escaped names,
frozen assignments, infeasible previews, permissions, replica operations,
concurrent/stale submissions, commit/rollback checks, HTTP content negotiation,
origin checks and form-encoded requests. `make test-packaging` checks staged
install/uninstall, including SQL-only and standalone extension builds.

## Foreign data wrapper

The upstream import and jj directory-move maintenance checks run with:

```sh
nix develop .#tests-18 --command python3 test/pgwrh_fdw/test_upstream_import.py
nix develop .#tests-18 --command python3 test/pgwrh_fdw/test_jj_layout.py
```

No Python packages are required: tests use Python 3's standard library and the
selected installation's libpq. Run as an ordinary OS user, not root:

```sh
export PG_CONFIG=/path/to/postgresql-18/bin/pg_config
make test-fdw
```

These commands build and stage the extension in `pgwrh_fdw/MAJOR/.build/pgwrh_fdw/stage`, start
private temporary PostgreSQL clusters, and stop them on completion. They need
permission to open local sockets. No installation into system PostgreSQL is
required. The tests print the temporary cluster/log location and retain it for
inspection. Use `make clean` before switching PostgreSQL builds.

The integration suite covers independent participants, context freezing,
connection reuse, reconnects, planning, prepared reads/writes, async scans,
savepoints, SQL quoting, defaults, missing values, permissions, propagation
failure recovery, and coexistence in both load orders. A test-only receiving
utility hook raises an error if the propagated request ID arrives after the
remote transaction acquires a snapshot.

`pgwrh_fdw/MAJOR/run-upstream.py` runs the retained main FDW and
query-cancellation SQL tests plus the `eval_plan_qual` isolation tests. For the
retained SCRAM TAP test, also provide matching PostgreSQL source test modules
and Perl's `IPC::Run`:

```sh
PG_SOURCE=/path/to/postgresql-18.3 make test-fdw-tap
```

See [FDW validation](../../pgwrh_fdw/docs/validation.md) for versions and actual
results; CI builds the pinned PostgreSQL release and runs the same checks on
Linux.

## Packaging and Nix

See [packaging verification](../packaging.md#verification) for staged install
and uninstall checks. `nix flake check` runs the installed-package check in a
temporary database. It covers standalone waiting, both extension installation
orders, removal of the core while waiting remains usable, all four extension
versions, and both native libraries. It also checks that the controller uses
`pgwrh_fdw` without activating `postgres_fdw`.
