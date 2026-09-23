# Lookup type and operator validation — 2026-09-23

The shared follow-up `ysvzmywq` uses existing FDW shippability in place of
integer-key and scalar-payload allowlists. It is an additional parent of the
existing PostgreSQL 18 and 19 aggregates (`lvwzssxp`, `mqpxytlz`); both existing
directory moves were refreshed. The original shared implementation changes
remain unchanged. No new aggregate or PostgreSQL 17 adaptation was added.

Local macOS validation used PostgreSQL 18.6 and PostgreSQL 19 Beta 3 from
`nix develop .#tests-18` and `.#tests-19`. All database tests use disposable
servers. The imported source baselines are unchanged.

## FDW, type semantics and source layout

Commands run from the repository root:

```sh
nix develop .#tests-18 --command make test-fdw
nix develop .#tests-19 --command make test-fdw
nix develop .#tests-18 --command bash -c 'python3 pgwrh_fdw/18/test_lookup_join.py && python3 pgwrh_fdw/18/lookup_join_demo.py'
nix develop .#tests-19 --command bash -c 'python3 pgwrh_fdw/19/test_lookup_join.py && python3 pgwrh_fdw/19/lookup_join_demo.py'
nix develop .#tests-18 --command bash -c 'python3 test/pgwrh_fdw/test_jj_layout.py && python3 test/pgwrh_fdw/test_upstream_import.py && python3 pgwrh_fdw/tools/refresh-layout.py 18 --check && python3 pgwrh_fdw/tools/refresh-layout.py 19 --check'
nix develop .#tests-19 --command bash -c 'python3 test/pgwrh_fdw/test_jj_layout.py && python3 test/pgwrh_fdw/test_upstream_import.py'
python3 pgwrh_fdw/tools/refresh-layout.py 18 --check
python3 pgwrh_fdw/tools/refresh-layout.py 19 --check
```

Both majors passed:

| Check | Result per major |
| --- | --- |
| Focused lookup suite | 25 tests, including parameterized type/strategy cases |
| Full context, virtual-server and lookup suite | 129 tests |
| LIMIT suite | 17 tests |
| Upstream SQL regression | `pgwrh_fdw`, `query_cancel` |
| Upstream isolation | `eval_plan_qual` |
| Dynamic exports | 18 prescribed exports on 18; 20 on 19; internal helpers hidden |
| jj layout and import suites | 4 layout tests and 3 import tests |
| Actual directory moves | Exact matches with the respective aggregate trees |
| Selective example | Same four result rows; 22,000 remote rows/two scans reduced to four remote rows/one scan |

The new type matrix checks INNER and SEMI execution against stock postgres_fdw
WHERE shippability for text/varchar/char, UUID, numeric, float/NaN, boolean, bytea,
date/time types, interval, inet, bit strings, JSONB, ranges, multiranges, points
and arrays. Extension-owned enums, domains and composites are rejected without
the server's `extensions` option and accepted when configured. Composite NULL
fields and quoted type names are included. Output-only labels remain absent
from generated remote SQL.

LIKE, regex, array membership and overlap execute remotely when shippable.
Stock collation-provenance restrictions remain enforced. Array tests preserve
empty values, multidimensional shape, nondefault lower bounds, NULL elements
and duplicate occurrences. Generic executions pick up changed arrays; runtime
overflow falls back using the same materialization. Result comparisons use
`EXCEPT ALL` in both directions and explicit remote row counts.

Text, UUID, numeric, date and array keys exercise RANGE, LIST and HASH routing,
including DEFAULT partitions, one remote execution and one skipped connection.
Cross-type/expression/NULL-safe conditions use all destinations when routing
cannot be proved. A citext comparison over text partition bounds proves that
case-insensitive matching visits both ranges instead of dropping a match through
incorrect pruning. Existing tests continue to cover mixed leaves, virtual
servers, permissions, cancellation, savepoints, rescans and fallback behavior.

Initial runs exposed test assumptions that stock postgres_fdw itself rejects
(domain-literal casts and locally derived collation expressions). Those became
explicit fallback coverage or comparisons with an eligible value expression.
The only upstream golden change adds the `Lookup Routing` EXPLAIN property.

## Managed topology

Each major was staged and tested sequentially, since the root staging directory
is shared:

```sh
nix develop .#tests-18 --command bash -c 'make clean && make testgres-ext && python3 -m pytest test/pgwrh/test_remote_lookup_join.py test/pgwrh/test_virtual_shard_routing.py test/pgwrh/test_remote_shard_aggregation.py -q'
nix develop .#tests-19 --command bash -c 'make clean && make testgres-ext && python3 -m pytest test/pgwrh/test_remote_lookup_join.py test/pgwrh/test_virtual_shard_routing.py test/pgwrh/test_remote_shard_aggregation.py -q'
```

Both runs passed all 33 tests and exited successfully (18: 484.94s; 19: 446.01s,
observed elapsed times only). This includes the real controller/replica lookup
test with mixed local/foreign leaves, skipped connections and collapsed-subtree
fallback, plus existing virtual routing, aggregation and transactional handoff
coverage. No environmental blocker remains for the checks listed here.

These checks do not validate Linux ELF loading or upstream TAP suites. No timing
assertions are used. Supported shapes, bounds and fallbacks are documented in
[automatic lookup joins](lookup-joins.md).
