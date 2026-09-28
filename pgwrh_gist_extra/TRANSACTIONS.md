# Partitioned transaction searches

Install `pgwrh_gist_extra` and `pg_trgm` on each data node. Load the planner hook
in every backend that plans local index scans, for example with
`session_preload_libraries = 'pgwrh_gist_extra'`. A coordinator with local
partitions needs it too. Installing the extension loads it in the installation
session; subsequent sessions need `LOAD` or preloading.

Partition first by transaction date ranges, then by `HASH(account)`. This index
on each physical data partition combines independent filtering and ordering
features:

```sql
CREATE INDEX ON transactions USING gist (
    transaction_date pgwrh_gist_date_order_ops,
    id pgwrh_gist_int8_order_ops,
    account pgwrh_gist_text_ops(attno=3),
    description gist_trgm_ops,
    transaction_key(transaction_date, id)
);
```

`transaction_key` is an optional application function, such as the example in
[PAGING.md](PAGING.md). It is not supplied or interpreted by the extension.
The date and ID should be `NOT NULL`; the pair must uniquely identify a result
row for this cursor scheme. For a coordinator containing foreign partitions,
create the indexes on the physical remote tables and any local partitions.

```sql
SELECT transaction_date, id, account, description
FROM transactions
WHERE transaction_date >= $1::date
  AND transaction_date < $2::date
  AND transaction_date <= $3::date
  AND account = ANY($4::text[])
  AND account ||= $4::text[]
  AND description ILIKE $5::text
  AND transaction_key(transaction_date, id)
        < transaction_key($3::date, $6::bigint)
ORDER BY transaction_date DESC, id DESC
LIMIT $7;
```

For the first page omit the cursor predicate and its redundant date bound.
For subsequent pages pass the last row's date and ID. The native date bounds
permit date partition pruning. Native `ANY` retains whatever account partition
pruning PostgreSQL can perform; `||=` supplies the GiST array index condition.
Generic plans do not gain whole-array execution pruning from this extension.
The hash-aware array cache filters values for an individual local leaf's GiST
scan; it does not avoid connecting to remote leaves.

Ordinary `ORDER BY` is preserved in the query. PostgreSQL can use ordered
Append across date ranges and Merge Append across hash partitions, with a
`pgwrh GiST ordered scan` over an ordered Index Scan at each eligible local
leaf. The planner remains free to choose a different plan according to costs.

For remote leaves use `pgwrh_fdw`, matching extension versions on both ends,
and a server `extensions` option containing `pgwrh_gist_extra,pg_trgm`. Eligible
remote scans receive ordinary `ORDER BY` and the existing generic LIMIT
pushdown. Keep every filter shippable to permit per-leaf LIMIT pushdown.
The SQL example's immutable SQL cursor function is inlined into built-in
expressions; a non-inlined application function needs its own shipping setup.
This example uses trigram-indexable `ILIKE`. It does not add shipping support
for the stable, threshold-dependent trigram similarity `%` operator.

Run `make test-gist-integration` to exercise mixed local and remote leaves,
actual remote plans, date pruning, array changes, generic prepared statements,
and complete pagination across dates and full-range bigint IDs. These tests
also run in the PostgreSQL 18/19 functional CI matrix.
