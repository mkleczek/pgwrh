# Validation record

Validated locally on 2026-09-14, macOS arm64.

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

CI targets Linux with the same pinned PostgreSQL 18.3 sources and assertions
enabled. Its configured checks do not, by themselves, establish a completed
Linux validation run. No other PostgreSQL version or platform is claimed tested.
