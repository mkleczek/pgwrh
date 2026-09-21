# pgwrh_gist_extra

Additional operators and operator classes for PostgreSQL GiST indexes.
The extension provides text-array membership filters and exact column ordering
on top of `btree_gist`. It can be installed independently of pgwrh and supports PostgreSQL
18 and 19 preview.

```sql
CREATE EXTENSION pgwrh_gist_extra CASCADE;

CREATE TABLE transactions(account text, description text);
CREATE INDEX transactions_accounts ON transactions
    USING gist (account pgwrh_gist_text_ops);

SELECT * FROM transactions
WHERE account ||= ARRAY['account-1', 'account-2'];
```

`CASCADE` installs the prerequisite `btree_gist`. The core pgwrh extensions do
not require or automatically enable this extension. Its files are included in
the development bundle; published alpha1 artifacts predate this addition.

## Operators

| Operator | Meaning for a non-NULL text value |
| --- | --- |
| `value ||= text[]` | True if any element equals the value; false for an empty array; otherwise NULL if the array contains NULL |
| `value &&= text[]` | False if an element differs; true for an empty array; otherwise NULL if the array contains NULL |

Both operators are strict: a NULL value or NULL array produces NULL. NULL
array elements follow SQL `ANY`/`ALL` three-valued logic. Strictness means that
a NULL scalar with an empty array still produces NULL, unlike native `ANY`/`ALL`.
The opclass also supports the usual text comparison operators.

For partitioned tables, retain a native predicate alongside the GiST predicate
when it is useful for partition pruning:

```sql
WHERE account = ANY ($1::text[])
  AND account ||= $1::text[]
```

The optional opclass setting `attno` identifies the indexed attribute's
one-based position within the index. The implementation uses it to
filter array elements against a matching single-column hash partition key.
The scan owns its cached array, refreshes it by contents, and releases replaced
values on rescans. Hash filtering maps attached-table columns by name and uses
the partition key's hash function and matching collation; unsupported keys are
left unchanged. The default opclass does not enable partition-bound filtering.
It does not add PostgreSQL-native SAOP pruning.

## Exact column ordering

The `pgwrh_gist_{int2,int4,int8,date,timestamp,timestamptz}_order_ops` opclasses
allow ordinary `ORDER BY column` through a custom scan over GiST. They preserve
the full supported value range, including bigint extremes and temporal infinities.
No application cursor encoding or declared minimum/maximum is needed.
See [ORDERING.md](ORDERING.md) for loading the planner hook, index definitions,
supported orderings, and fallback behavior. Use these opclasses explicitly;
existing indexes keep their existing behavior.

Array filters, ordering and cursor predicates are independent. An application can
use an indexable scalar expression for composite pagination; [PAGING.md](PAGING.md)
shows a date/bigint example. [TRANSACTIONS.md](TRANSACTIONS.md) combines these
features with date-range and account-hash partitioning and remote LIMIT pushdown.

For FDW queries, install matching versions on both sides and include
`pgwrh_gist_extra` in the server's `extensions` option to permit operator shipping.
Do not use the old `btree_gist_extra` installation alongside this extension in
the same schema: the operator names overlap.

## Build and test

```sh
make -C pgwrh_gist_extra PG_CONFIG=/path/to/pg_config
make -C pgwrh_gist_extra install PG_CONFIG=/path/to/pg_config
make test-gist
make test-gist-integration
```

The root build includes the extension by default. Use `WITH_GIST_EXTRA=0` to
exclude it, or build this directory alone. `NO_PGXS=1` excludes native modules.
The extension follows the pgwrh bundle version and supplies fresh-installation
scripts only; no migration from `btree_gist_extra` is provided.

## Source

Imported from [mkleczek/btree_gist_extra](https://github.com/mkleczek/btree_gist_extra)
at `160266499f3b83477b6e624a52ac67dd036b2148`, without its Git history.
The original GPL version 3 license is retained in [LICENSE](LICENSE).
The monorepo is the maintenance source for `pgwrh_gist_extra`.
