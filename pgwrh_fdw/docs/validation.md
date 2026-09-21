# Validation record

These historical records cover checks on 2026-09-14 and 2026-09-15 before the
unified 1.0.0 release. Each section states its environment and scope; the initial
Linux CI result does not cover later changes validated only locally.
The current tree ships only `pgwrh_fdw--1.0.0-alpha1.sql` and no upgrade scripts.

## Weighted routing and initial connection failover

Validated locally on 2026-09-15 with PostgreSQL 18.3 on macOS arm64:

* All 99 integration tests (23 context, 76 virtual), plus 26 subtests.
* Both retained upstream SQL suites, `eval_plan_qual` isolation, all seven SCRAM
  TAP assertions, and the unchanged 16-symbol export audit.
* Weight validation, unequal selection, pushed joins, reuse priority, shared pins,
  and weight totals above 32 bits.
* Failed idle reconnects, coordinated failover of cached join plans, exhausted
  candidates, and shared failure state across savepoint rollback.
* Transaction-context errors do not retry, including a receiver hook explicitly
  raising SQLSTATE 08001 after the remote transaction has started.

The weight change passed all 95 integration tests independently before adding
initial connection failover. Both changes are included in the installation script.

## Synchronized virtual membership updates

Validated locally on 2026-09-15 with PostgreSQL 18.3 on macOS arm64:

* Warning-free build and all 90 integration tests (23 context, 67 virtual).
* Both retained upstream SQL suites and `eval_plan_qual` isolation.
* All seven SCRAM TAP assertions.
* The 16-symbol export audit, including the new function and its fmgr entry.
* Fresh installation with the member-update function included, also in
  a custom extension schema, and no other versions or upgrade paths.

The 12 added tests exercise the blocking update API, actual lock waits observed
through `pg_locks`, reader and updater commit/rollback, transaction pins after
savepoint rollback, pushed-join inputs sharing a routing group, cached plans
waiting for catalog changes, and independence of unused sibling aliases.
They also cover planning, ANALYZE, IMPORT, failed acquisition, both directions
of the same-transaction guard, timeout recovery, owner/read-only checks,
identifier quoting, invalid members and normal DDL event-trigger execution.
No pgwrh controller/replica logic or PostgreSQL core files changed. These
changes have not been validated in Linux CI.

## Shared routing for identical member sets

The routing foundation passed all 71 integration tests (23 context and 48
virtual-server tests) locally on PostgreSQL 18.3/macOS arm64 on 2026-09-15.
The build completed without warnings. Tests cover unordered member sets,
shared failures, effective-user separation, mapping replacement, invalidated
connections, savepoint affinity and topology changes. Planner checks include
conflicts introduced by references to another shard in the same routing group.
Remote estimation does not attach unused aliases to a transaction binding.

The completed stack, including repeated-reference join pushdown, passed:

* All 78 integration tests (23 context and 55 virtual-server tests).
* Both upstream SQL suites and `eval_plan_qual` isolation.
* All seven retained SCRAM TAP assertions.
* The unchanged 14-symbol dynamic export audit and whitespace check.

The added planning tests inspect remote SQL and execute repeated joins, separate
scans, async Append, outer joins with aggregation and partitionwise joins with
generic pruning. They also exercise cached plans across transactions and
topology changes from another session, and independently bound groups whose
current member lists become equal. Joins between different groups retain the
conservative outside-reference check. Cross-server writes and row-locking joins
remain local. These changes have not been validated in Linux CI.

## Joins across virtual servers

Validated locally on 2026-09-15 with the pinned PostgreSQL 18.3 runtime on macOS
arm64. The coordinated-routing foundation passed 46 integration tests before
join planning was added. The completed join stack passed:

* All 63 integration tests (23 context tests and 40 virtual-server tests).
* Both upstream SQL suites and `eval_plan_qual` isolation.
* All seven retained SCRAM TAP assertions.
* The dynamic export audit with the same 14 prescribed symbols.

Tests check actual remote SQL and results for two- and three-way joins, outer
and semi joins, aggregation, async Append, and partitionwise joins with generic
parameter pruning. They also cover target mapping privileges, view owners,
role changes, cached plans across topology changes in another session, caught
group-acquisition errors, transaction pins after savepoint rollback, and
repeated references inside and outside a proposed remote join.

Known limits are documented in [virtual servers](virtual-servers.md):
cross-server writes and row-locking joins remain local; partial joins with
outside references to their virtual inputs are declined; an already-prepared
remote join may need explicit replanning after a later incompatible transaction
binding. These changes have not been validated in Linux CI.

## Virtual servers and connection reuse

Validated locally on 2026-09-15 using the pinned PostgreSQL 18.3 runtime on
macOS arm64. The standalone routing change passed 38 integration tests (23
existing context tests and 15 virtual-server tests), both upstream SQL suites,
the isolation suite, and the export audit before adding connection preference.

The completed stack passed:

* All 45 integration tests, including seven additional connection-reuse tests.
* Both upstream SQL suites and `eval_plan_qual` isolation.
* All seven retained SCRAM TAP assertions.
* The dynamic export audit with the same 14 prescribed symbols.

Overlapping virtual servers were verified to share one remote backend PID,
including async Append with single-row fetches. Tests also distinguish active
and idle candidates, prevent borrowing a different user mapping's connection,
and execute a generic partition-pruned query with an unreachable unused member.
These new changes have not yet been validated in Linux CI.

## Initial standalone release

The release version and consolidated installation script were validated locally
on PostgreSQL 18.3 (macOS arm64):

* All 23 integration tests passed, including a fresh installation with matching SQL
  and module versions, no inherited extension versions or upgrade paths,
  and working connection-management functions.
* Both upstream SQL suites, the isolation suite, and all seven SCRAM TAP
  assertions passed.
* The dynamic export audit passed with the same 14 prescribed symbols.
* PGXS installation into a fresh staging directory produced the library,
  control file, and exactly one installation SQL script.

## Implementation validation before release versioning

| Check | PostgreSQL 18.1 (Homebrew) | PostgreSQL 18.3 (built from pinned tag) |
| --- | --- | --- |
| PGXS extension build | Pass | Pass |
| 22 context integration tests | Pass | Pass |
| Upstream `pgwrh_fdw` SQL suite | Pass | Pass |
| Upstream `query_cancel` | Pass | Pass |
| Upstream `eval_plan_qual` isolation | Pass | Pass |
| Upstream SCRAM TAP (7 assertions) | Not run | Pass |
| Dynamic export allowlist (14 symbols) | Pass | Pass |

The 18.3 runtime was built from `REL_18_3`, commit
`62d6c7d3df6287f1bd83199c1a746e50d31571a0`, using:

```sh
./configure --prefix=/temporary/install/path --without-icu --without-readline
make -j8
make install
make -C contrib/postgres_fdw install
```

The SCRAM test used the matching 18.3 source's Perl test modules and locally
available Perl 5.40 / IPC::Run dependencies. The test extension is staged without
modifying system PostgreSQL. See [test instructions](../../docs/development/testing.md#foreign-data-wrapper)
for reusable commands.

Investigation resolved these initial test failures:

* The OS sandbox denied socket creation; tests were run with local-socket access.
* Upstream loopback connections initially used `/tmp` instead of the private test
  socket directory. Passing PGHOST to the test postmaster fixed the harness.
* Five expected-output widths needed mechanical adjustment after shortening
  `postgres_fdw` to `pgwrh_fdw`. No query result or expected plan changed.
* The new invalid-value test initially expected the wrong SQLSTATE. PostgreSQL
  returns `22023` for the tested GUC validation failure and `08000` when rejecting
  the incomplete connection. Assertions now check those codes.

Linux CI (Ubuntu 24.04, PostgreSQL 18.3, assertions enabled) also passed the
build, all 22 integration tests, both upstream SQL suites, the isolation suite,
all seven SCRAM assertions, and the dynamic export audit. This includes both
extension load orders under ELF, in addition to the macOS checks above.

Evidence: [successful CI run 34825897295](https://github.com/mkleczek/pgwrh_fdw/actions/runs/34825897295)
for implementation commit `2230540766d846529f7548555c3f4a48465e0efc`.
No other PostgreSQL version or platform is claimed tested.
