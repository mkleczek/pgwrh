# Validation record

Validated on 2026-09-14, locally on macOS arm64 and in Linux CI.

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

## Initial 0.1.0 release

The release version and consolidated installation script were validated locally
on PostgreSQL 18.3 (macOS arm64):

* All 23 integration tests passed, including a fresh installation with SQL and
  module version `0.1.0`, no inherited extension versions or upgrade paths,
  and working connection-management functions.
* Both upstream SQL suites, the isolation suite, and all seven SCRAM TAP
  assertions passed.
* The dynamic export audit passed with the same 14 prescribed symbols.
* PGXS installation into a fresh staging directory produced the library,
  control file, and exactly one SQL script: `pgwrh_fdw--0.1.0.sql`.

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
modifying system PostgreSQL. See the README for reusable test commands.

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
