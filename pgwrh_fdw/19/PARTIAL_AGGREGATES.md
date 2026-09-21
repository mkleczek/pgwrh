# Partial aggregate pushdown

With `enable_partitionwise_aggregate = on`, pgwrh_fdw can aggregate each foreign
partition remotely when groups span partitions. PostgreSQL combines the returned
states in a local Finalize Aggregate. Local partitions use PostgreSQL's own
Partial Aggregate. The remote server executes ordinary SQL; no server extension
beyond the existing FDW setup, core patch or new aggregate syntax is required.
The same implementation is shared by the PostgreSQL 18 and 19 bundles.

For example, for sales partitioned by date:

```sql
SET enable_partitionwise_aggregate = on;
EXPLAIN (VERBOSE, ANALYZE)
SELECT country, count(*), sum(amount_integer)
FROM sales GROUP BY country HAVING count(*) > 1000;
```

An eligible foreign scan's Remote SQL contains `count(*)`, `sum(amount_integer)`
and `GROUP BY country`. A local Finalize Aggregate combines all partitions and
applies the aggregate-dependent HAVING clause. A group that fails HAVING on every
individual shard can still satisfy it after combination.

## Supported signatures

| Aggregate | Accepted built-in signatures | State/result |
| --- | --- | --- |
| count | `count(*)`, `count(any)` | bigint |
| sum | smallint, integer | bigint |
| avg | smallint, integer | bigint[count, sum] array; final result numeric |
| sum | real, double precision, money | same as input |
| min, max | smallint, integer, bigint, real, double precision, numeric, text, character, date, time, time with time zone, timestamp, timestamp with time zone, interval, money | same as input |

`count(any)` means PostgreSQL's built-in one-argument count; its argument must
still pass ordinary FDW expression, type and collation checks. Character means
`bpchar`, not the internal `"char"` type. Casts can select one of these signatures
if the cast itself is shippable. Floating-point sums have the usual PostgreSQL
partial-aggregation rounding/order sensitivity; no bitwise identity across
arbitrary reduction orders is promised. Foreign table types and collations must
match their remote columns, as for existing aggregate pushdown.

Eligibility uses fixed built-in function OIDs, then verifies the aggregate
catalog's transition/output types and combine function. Scalar states require no
finalization or serialization. Integer averages require the exact built-in array
transition/combine/final functions and no serialization. A same-named user aggregate (including one installed in
pg_catalog or declared shippable by an extension) is not admitted.

For `avg(smallint)` and `avg(integer)`, the remote SQL synthesizes the native
partial state as `ARRAY[count(x), COALESCE(sum(x), 0::bigint)]`. Both components
retain the original argument and FILTER. `count(x)` excludes NULL inputs; the
zero sum gives empty/all-NULL inputs the valid `{0,0}` state. Core combines these
states with local partitions' native states and performs the usual numeric
finalization. Shards with different row counts are weighted correctly. Ordinary
full average pushdown continues to emit `avg(x)`.

## Restrictions and planning

- Other `avg` signatures, `sum(bigint)`, `sum(numeric)`, `sum(interval)` and all opaque `internal`
  transition states are excluded. Polymorphic min/max, user-defined aggregates,
  aggregate DISTINCT/ORDER BY, ordered-set/variadic aggregates and grouping sets
  also stay on the existing execution path.
- Every aggregate needed by a partial target must be supported, including ones
  used only in HAVING or ORDER BY. Mixed supported/unsupported targets fall back
  together; the FDW never feeds a final result to an incompatible combine function.
- Shippable aggregate FILTER expressions and WHERE clauses are retained remotely.
  A WHERE condition that must run locally before aggregation prevents pushdown.
  Group keys, argument expressions, types and collations retain normal FDW checks.
- HAVING is evaluated only after final combination. Core's partial target includes
  its aggregate inputs, even when they are absent from the SELECT list.
- If core removes redundant group keys from its partial target, this first version
  falls back rather than inventing missing remote GROUP BY expressions. This also
  preserves the empty-input behavior of constant grouping keys.
- Core's partitionwise-aggregation decision controls availability; the FDW does
  not add partial aggregation to unrelated query shapes. Paths compete on cost,
  using the existing FDW cost model and `use_remote_estimate` option. An eligible
  partition can still use a local partial aggregate when that is cheaper.
- Ordinary whole-relation and full partitionwise aggregation retain their existing
  broader signature support. This feature does not change them.

## Reproducible transfer measurement

From the project root, run either major's self-contained loopback experiment:

```sh
nix develop .#tests-18 --command python3 pgwrh_fdw/18/partial_aggregate_demo.py
nix develop .#tests-19 --command python3 pgwrh_fdw/19/partial_aggregate_demo.py
```

It builds a private temporary cluster, creates two foreign partitions containing
500,000 rows each and ten groups each, runs both plans, and checks equal results.
Each JSON report includes Remote SQL and the sum of `Actual Rows * Actual Loops`
at the Foreign Scan nodes. With the default input, the baseline returns 1,000,000
foreign rows and the pushed plan returns 20 partial rows; both return ten final
groups. The script asserts these counts, not an elapsed-time improvement. Its query mixes
count, sum and integer average. This
measures transferred tuples, not wire bytes (the row widths differ).
`--rows 100000` offers a smaller run. Logs remain in the printed temporary path.

`test_feature_partial_aggregate.py` covers every admitted signature, NULLs, empty input and
partitions, mixed local/foreign partitions, FILTER, HAVING-only aggregates, generic
and custom prepared plans, unsupported/shippable-custom aggregate fallback,
redundant grouping, remote estimates, the planner control and full aggregation.
Integer-average cases also cover unequal shard counts, FILTERs on both state
components, integer bounds, duplicate/shared aggregates and exact numeric results.
`managed_test_partial_aggregate.py`, collected by the project's
`test/pgwrh/test_fdw_features.py`, also tests real shard hosts
through pgwrh's managed virtual-server topology, comparing both readers with the
controller's local table and checking actual foreign row counts.

## Upstream basis

The FDW hook/deparsing approach follows the [October 2021 proposal and subsequent
discussion](https://www.postgresql.org/message-id/flat/cf744a8ee4d47bdabe1da9174d4f3dc9%40postgrespro.ru).
The review's warning about name-based selection is addressed by identity and
state checks. The [June 2024 discussion](https://www.postgresql.org/message-id/TY2PR01MB383585CACD74F2106A0563C195CB2%40TY2PR01MB3835.jpnprd01.prod.outlook.com)
distinguishes ordinary states from opaque internal states; the [March 2025
revision](https://www.postgresql.org/message-id/TYRPR01MB13941CEA16574771B1BFD130595A02%40TYRPR01MB13941.jpnprd01.prod.outlook.com)
uses core changes and `PARTIAL_AGGREGATE` syntax to export more states. That broader
design remains outside this implementation. PostgreSQL's existing
[combine/finalize machinery](https://www.postgresql.org/docs/18/xaggr.html#XAGGR-PARTIAL-AGGREGATES)
consumes the admitted scalar results and the synthesized integer-average arrays.
