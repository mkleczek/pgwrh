# Lookup-join validation — 2026-09-22–23

Local macOS runs used `nix develop .#tests-18` (PostgreSQL 18.6) and
`.#tests-19` (PostgreSQL 19 Beta 3). The imported source baselines remain those
listed in [UPSTREAM.md](../UPSTREAM.md). All servers were disposable local test
servers; no existing user cluster was changed and nothing was published.

## FDW and source-layout checks

Commands run from the repository root:

```sh
nix develop .#tests-18 --command make test-fdw
nix develop .#tests-19 --command make test-fdw
nix develop .#tests-18 --command bash -c 'python3 test/pgwrh_fdw/test_jj_layout.py && python3 test/pgwrh_fdw/test_upstream_import.py'
nix develop .#tests-19 --command bash -c 'python3 test/pgwrh_fdw/test_jj_layout.py && python3 test/pgwrh_fdw/test_upstream_import.py'
python3 pgwrh_fdw/tools/refresh-layout.py 18 --check
python3 pgwrh_fdw/tools/refresh-layout.py 19 --check
```

The existing aggregates retain all their original parents plus the three new
shared changes (`sqmzztxu`, `yznpkzqo`, `ynssotmk`). Aggregate and directory-move
change identities are unchanged. The new implementation/test files are identical
in both generated directories. Project metadata, topology tests and documentation
are in `opmxnoqn`.

Both majors passed:

| Check | Result per major |
| --- | --- |
| LIMIT suite | 17 tests |
| Context, virtual-server and lookup suite | 122 tests, including 18 lookup tests |
| Upstream SQL regression | `pgwrh_fdw`, `query_cancel` |
| Upstream isolation | `eval_plan_qual` |
| Dynamic exports | Only 18 prescribed exports on 18; 20 on 19; internal helpers hidden |
| jj layout tests | 4 tests |
| Upstream import tests | 3 tests |
| Actual directory moves | Exact matches with the respective aggregate trees |

The lookup suite checks INNER/EXISTS/IN plans, occurrence multiplicity with
`EXCEPT ALL` in both directions, NULLs and retained wide/local-enum outputs,
parallel condition arrays, reversed remote views, per-shard row IDs, all three
partition strategies, mixed leaves and single-leaf partition slots. It also
checks generic plans after local updates, repeated scans without reevaluating
the lookup, runtime row/byte overflow, format-independent scalar conversion,
virtual routing/frozen context/savepoints, cancellation and remote error cleanup,
permissions/RLS/security barriers, unsupported joins/conditions and connection
avoidance. Remote estimation is tested separately from execution counters.
Existing tests cover virtual join-hook behavior and stock postgres_fdw coexistence.

## Managed topology and example

For each major, staging and topology runs use:

```sh
nix develop .#tests-18 --command bash -c 'make testgres-ext && python3 -m pytest test/pgwrh/test_remote_lookup_join.py test/pgwrh/test_virtual_shard_routing.py test/pgwrh/test_remote_shard_aggregation.py -q'
nix develop .#tests-19 --command bash -c 'make testgres-ext && python3 -m pytest test/pgwrh/test_remote_lookup_join.py test/pgwrh/test_virtual_shard_routing.py test/pgwrh/test_remote_shard_aggregation.py -q'
nix develop .#tests-18 --command python3 pgwrh_fdw/18/lookup_join_demo.py
nix develop .#tests-19 --command python3 pgwrh_fdw/19/lookup_join_demo.py
```

Both managed-topology runs passed all 33 tests (18: 481.10s; 19: 213.09s;
elapsed times are observations, not assertions). The new test uses a
real controller and two pgwrh replicas, exercises a mixed local/foreign root,
checks zero execution connections when all matches are local, then verifies
ordinary-plan fallback after co-location collapses the remote subtree.

The example passed on both majors: baseline 22,000 remote rows across two scans,
optimized four remote rows from one scan, and the same four final rows. Generated
SQL performs the join remotely over typed array parameters and returns a stable
lookup occurrence ID. The wide/NULL label remains local. Full EXPLAIN ANALYZE is
printed by the script; a count-only excerpt is in [lookup joins](lookup-joins.md).

No environmental blocker remains for the checks above. These runs do not
validate Linux ELF loading or upstream TAP suites. No timing assertions are
used. Supported inputs, explicit fallbacks and the sequential,
bounded execution model are documented in [lookup joins](lookup-joins.md).
