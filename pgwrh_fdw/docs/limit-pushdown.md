# LIMIT pushdown through append paths

`pgwrh_fdw` propagates a final query limit to eligible foreign inputs of
`Append` and `MergeAppend`. The optimization is independent of the table's
columns, partitioning scheme and remote indexes. It requires no PostgreSQL
core patch or additional extension.

For example, a query over foreign partitions:

```sql
SELECT * FROM records
WHERE category = ANY ($1)
ORDER BY created_at DESC, id DESC
LIMIT $2;
```

can send the same filtering, ordering and limit to each remote input while
retaining the global limit and merge. A shard's first N qualifying rows suffice
to find the global first N. The remote planner sees an SQL `LIMIT`, allowing it
to choose a suitable index or bounded sort. This does not depend on which index
access method is installed on the remote server.

## Supported paths

The final limit must be a positive constant or an external prepared-statement
parameter. Generic prepared plans keep the parameter as a parameter; planner
row estimates never become execution bounds. `NULL`, zero and negative runtime
values retain PostgreSQL's normal limit semantics.

The traversal supports nested append and merge-append paths and scalar,
nonvolatile projections. Ordered inputs must already have the required ordering.
Local partitions and inputs from other FDWs remain unchanged. Runtime partition
pruning and async foreign scans retain their existing metadata and behavior.
An ineligible input does not prevent safe siblings from receiving a bound.

The optimization stops at sorts (including sorts the planner would insert
implicitly), joins, aggregates, window functions, subquery scans and custom
paths. It leaves partial and parameterized join paths alone. Remote inputs
with local filters are never truncated: those filters might discard rows after
the remote limit and produce too few results. Queries with row locks, set-returning
targets, volatile targets, `OFFSET` (including `OFFSET 0`), or `WITH TIES` do not
receive this optimization. Limits computed by expressions or initplans are
also left alone. PostgreSQL's existing whole-query FDW pushdown is unaffected.

## Planning and inspection

The extension chains `create_upper_paths_hook` at `UPPERREL_FINAL`. It copies
eligible path branches underneath an existing `LimitPath` and marks its own
simple foreign paths for SQL limit deparsing. It does not mutate shared paths,
add truncated alternatives to base-relation path lists, or change the FDW API.

The enclosing plan retains its existing cost and row estimates, which already
account for consuming a prefix. This deliberately avoids guessing savings from
a different remote plan. It does not generate new ordering strategies or push
sorts through append paths. Consequently, a local sort above the append remains
a barrier even when a different plan might admit remote top-K execution.

Use `EXPLAIN (VERBOSE)` and inspect each foreign scan's `Remote SQL` for the
filter, ordering and `LIMIT`. Use `EXPLAIN (ANALYZE, VERBOSE)` to check pruning
and scans that were never executed. A limit stopping consumption of older
partitions is distinct from partition pruning; this feature does not extend
the predicates PostgreSQL can use for pruning.

The optimization is enabled by default and can be disabled for subsequently
planned statements:

```sql
SET pgwrh_fdw.enable_limit_pushdown = off;
```

This planner setting does not invalidate existing cached plans. Reprepare a
statement when comparing the two settings. No preload setting is required:
the hook is installed when the FDW library loads, and all other planner hooks
are chained.

The shared `test_limit_pushdown.py` suite checks remote SQL and actual results
on both PostgreSQL 18 and 19. `make test-fdw` runs it before the existing FDW
integration, regression, isolation and symbol-export checks.
