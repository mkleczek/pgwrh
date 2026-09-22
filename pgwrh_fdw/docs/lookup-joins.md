# Automatic remote lookup joins

`pgwrh_fdw` can materialize a small local lookup and use it as a parameterized
relation in remote PostgreSQL joins. Ordinary INNER and SEMI joins are eligible,
including `IN` and `EXISTS` after PostgreSQL's normal transformations:

```sql
SELECT s.id, s.partition_key, l.label
FROM sharded_table s
JOIN lookup_table l ON l.partition_key_ref = s.partition_key
WHERE l.enabled;

SELECT s.*
FROM sharded_table s
WHERE EXISTS (
  SELECT FROM lookup_table l
  WHERE l.partition_key_ref = s.partition_key AND l.enabled
);
```

No lookup primary key, replicated lookup, composite type, matching partition
layout or SQL rewrite is required. Keep normal table statistics current; the
planner retains ordinary join paths and chooses by estimated cost.
`use_remote_estimate` is optional.

## Execution and retained values

A `Custom Scan (Pgwrh Remote Lookup Join)` consumes the filtered lookup once.
It retains the columns needed by the conditions and output, assigning each input
**occurrence** a stable row number. It sends only condition columns as parallel
typed arrays. Duplicate keys and NULL elements remain aligned. Output-only
lookup columns stay local and can have types unavailable on the remote server.

An INNER request looks like this (column names depend on the query):

```sql
SELECT r1.id, r1.partition_key, l.lookup_rowno
FROM public.remote_shard r1
JOIN ROWS FROM (
  pg_catalog.unnest($1::bigint[]),
  pg_catalog.unnest($2::bigint[])
) AS l(c1, lookup_rowno)
ON r1.partition_key = l.c1;
```

The second array contains stable local row numbers. They remain meaningful when
each shard receives a different subset, and when remote results arrive in a
different order. The coordinator attaches retained columns by that identifier;
it does not match keys again. Two lookup rows with the same key and different
labels produce separate matches. `ROWS FROM` provides PostgreSQL's parallel
unnest semantics while allowing every function name to be schema-qualified.

A SEMI request uses `WHERE EXISTS (SELECT 1 FROM ROWS FROM (...) AS l(...)
WHERE ...)`. It sends no row-number array and returns only shard columns. Remote
PostgreSQL preserves each qualifying shard row once, including separate
occurrences of identical rows. There is no output `DISTINCT` or batching.
Additional conditions must all pass the FDW's expression/type/collation checks;
an unsupported condition declines the complete optimized join, including SEMI.

Normal ForeignScan nodes perform remote execution. They use the existing
connection cache, mappings and effective user, virtual-server selection,
transaction affinity, frozen context, mirrored savepoints, cancellation and
error cleanup. The custom node delays their connection acquisition until after
lookup materialization and routing. Existing FDW participant snapshot semantics
remain unchanged; this does not establish a distributed snapshot.

## Routing and supported inputs

The initial routing key is one `smallint`, `integer` or `bigint` column joined
by strict built-in equality to a lookup column of the same type. Standalone
pgwrh foreign tables are eligible; an ordinary server or a virtual server can
provide the endpoint.

Partitioned inputs support single-column declarative HASH, RANGE and LIST
partitioning with built-in operator families and direct local/foreign leaves.
The one extra layer used by pgwrh's partition slots is supported when each slot
contains one leaf and uses the same scalar key. PostgreSQL's partition bounds
and comparison/hash helpers route lookup values. Shards with no possible match
are skipped before execution connection acquisition. Local leaves join against
the same materialization with local qualification.

More general nested shapes, multicolumn/expression keys, non-pgwrh FDWs within
the tree and custom partition operator families retain ordinary plans. Managed
collapsed remote subtrees carry foreign-table option `lookup_join 'false'` and
also retain ordinary plans. This metadata is set when pgwrh creates their
foreign tables. The option can explicitly disable eligibility on a standalone
foreign table too; its default is true. It does not change ordinary FDW scans.
No remote descendant pruning is inferred from an opaque foreign-table endpoint.

The lookup must be one ordinary local table with an independent local scan.
Local filters and external prepared-statement parameters are supported. RLS,
security-barrier subqueries, differing effective users, correlated/LATERAL
inputs, inherited lookup scans, joins planned inside unflattened subqueries,
OUTER/ANTI joins, writes, row locking/EPQ, volatile expressions, whole-row/system
column references and planner placeholders decline this alternative.
Supported shipped scalar types include integers, booleans, floats, numeric,
text/varchar/char, bytea, date/time/timestamp/interval, UUID, JSON/JSONB and bit
strings, subject to the FDW's semantic checks. User-defined condition types are
excluded; user-defined output-only lookup types can stay local.

The node offers no ordering, parallel or asynchronous execution contract.
Destinations run sequentially. LIMIT, sorting and aggregation remain above the
join; partial aggregate pushdown (issue #7) is outside this feature. A future
remote ordering or aggregation implementation must revisit which retained
columns need to move remotely.

## Settings and runtime bounds

| Setting | Default | Meaning |
| --- | --- | --- |
| `pgwrh_fdw.enable_lookup_join` | `on` | Offer the custom path when planning. |
| `pgwrh_fdw.lookup_join_max_rows` | `10000` | Maximum lookup occurrences for the optimized execution. |
| `pgwrh_fdw.lookup_join_max_memory` | `8MB` | Maximum accounted row/index/payload bytes; accepted range 1kB–64MB. |
| Foreign-table option `lookup_join` | `true` | Allow this endpoint to participate. pgwrh sets false for collapsed subtrees. |

The row and byte bounds are checked against actual execution data, including
retained output values, spool copies, row/routing indexes, array construction
workspace and encoded payloads. Accounting is conservative. It is not a hard
limit on total backend memory: the spillable spool uses `work_mem`, and executor
and temporary conversion allocations have their own overhead.

If estimates exceed a bound, the path is not offered. If actual data exceeds a
bound, execution finishes materializing the lookup into a spillable tuplestore
and uses ordinary shard scans with a local nested-loop join over that store.
The decision is made before any remote request or output. The lookup is never
executed again to implement fallback. This fallback favors correctness and can
be expensive for large inputs; its local work can grow with lookup rows times
shard rows. Smaller bounds are a way to exercise fallback, not a batching knob.
Empty lookups execute no remote scans. Unchanged rescans reuse the lookup;
changed executor parameters rebuild it. Every prepared execution consumes its
current local rows and parameter values, rather than embedding values in a plan.

The enable setting affects planning. To compare a previously cached generic
plan after toggling it, use `DISCARD PLANS` or prepare the query again. Runtime
bounds are consulted even for an already cached plan.

## EXPLAIN and a reproducible example

`EXPLAIN VERBOSE` shows generated SQL on the foreign children. Both optimized
and saved fallback child plans are visible; `ANALYZE` identifies which ran.
Custom properties show the join method, number of condition columns, lookup
rows and accounted bytes, remote executions, rows returned remotely, skipped
shards, and whether overflow selected local execution. Counts do not expose
parameter contents. Lookup rows/bytes describe the current materialization;
execution and skip counters accumulate across rescans.

Run a complete disposable example from the repository root:

```sh
nix develop .#tests-18 --command python3 pgwrh_fdw/18/lookup_join_demo.py
# Or select tests-19 and pgwrh_fdw/19/lookup_join_demo.py.
```

It creates two foreign ranges with 11,000 rows each, including duplicate shard
rows, and a lookup with a wide output-only label, NULLs and duplicate keys.
Only key 1 is enabled among matching keys. It prints both plans for this SQL:

```sql
SELECT s.id, s.k, l.label
FROM items s JOIN lookup l ON s.k = l.k
WHERE l.enabled;
```

The ordinary plan reads both foreign ranges. The optimized plan requests only
the matching range and joins remotely, returning four matches; the label stays
local. The tests assert result multisets and execution/row counts, never timing.

On PostgreSQL 18.6 and 19 Beta 3, the example's ordinary plan returned 11,000
rows from each of two ForeignScans (22,000 total) to produce four final rows.
The optimized EXPLAIN ANALYZE showed:

```text
Custom Scan (Pgwrh Remote Lookup Join) (actual rows=4.00 loops=1)
  Lookup Join: Remote INNER
  Lookup Condition Columns: 1
  Lookup Execution: Parameterized unnest
  Lookup Rows: 3
  Remote Executions: 1
  Remote Rows: 4
  Skipped Shards: 1
```

The three materialized occurrences include two key-1 rows with different labels
and one NULL key. Only the two matching occurrences are sent to the first shard.
The generated request returns `r4.k, r4.id, l.lookup_rowno`; its parameters are
`integer[]` keys and `bigint[]` row IDs. The retained label is absent from both.
Saved fallback scans and the second optimized foreign scan show `never executed`.
See the [validation record](lookup-join-validation.md) for commands and scope.

Focused tests: `python3 pgwrh_fdw/MAJOR/test_lookup_join.py`. They also run through
`make test-fdw`, alongside existing virtual-server and transaction-context tests.
The real managed-topology check is `test/pgwrh/test_remote_lookup_join.py`.
