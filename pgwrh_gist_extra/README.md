# pgwrh_gist_extra

Additional operators and operator classes for PostgreSQL GiST indexes.
The current implementation provides text-array membership filters on top of
`btree_gist`. It can be installed independently of pgwrh and supports PostgreSQL
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
| `value ||= text[]` | True if any non-NULL array element equals the value; false for an empty array |
| `value &&= text[]` | True if every element is non-NULL and equals the value; true for an empty array |

Both operators are strict: a NULL value or NULL array produces NULL. NULL
array elements otherwise behave as nonmatches, so the operators are not exact
three-valued replacements for SQL `ANY`/`ALL` in arbitrary expressions.
The opclass also supports the usual text comparison operators.

For partitioned tables, retain a native predicate alongside the GiST predicate
when it is useful for partition pruning:

```sql
WHERE account = ANY ($1::text[])
  AND account ||= $1::text[]
```

The optional opclass setting `attno` identifies the indexed attribute's
one-based position within the index. The imported implementation uses it to
filter array elements against a matching single-column hash partition key.
This experimental optimization and its cache require further hardening; the
default opclass does not enable partition-bound filtering. It does not add
PostgreSQL-native SAOP pruning or ordinary GiST `ORDER BY` support.

For FDW queries, install matching versions on both sides and include
`pgwrh_gist_extra` in the server's `extensions` option to permit operator shipping.
Do not use the old `btree_gist_extra` installation alongside this extension in
the same schema: the operator names overlap.

## Build and test

```sh
make -C pgwrh_gist_extra PG_CONFIG=/path/to/pg_config
make -C pgwrh_gist_extra install PG_CONFIG=/path/to/pg_config
make test-gist
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
