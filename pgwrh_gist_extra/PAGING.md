# Date and bigint cursors

An application can define a numeric cursor expression with the same
order as `(date, id)`. This example supports the whole bigint range, finite dates and
date infinities. Both arguments are strict: a NULL argument produces NULL.
The pair must be unique within the query's result for deterministic pagination.

GiST ordering and array filtering are independent of this representation. This
function is an application example, not part of the extension API.

```sql
CREATE FUNCTION transaction_key(date, bigint) RETURNS numeric
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
RETURN (CASE WHEN $1 = '-infinity'::date THEN -2147483648::numeric
             WHEN $1 = 'infinity'::date THEN 2147483647::numeric
             ELSE ($1 - DATE '2000-01-01')::numeric END)
       * 18446744073709551616::numeric + $2::numeric;

CREATE INDEX transactions_cursor ON transactions USING gist
    (transaction_key(transaction_date, id));

SELECT * FROM transactions
WHERE transaction_date >= $1
  AND transaction_date <= $2
  AND transaction_key(transaction_date, id)
        < transaction_key($2, $3)
ORDER BY transaction_date DESC, id DESC
LIMIT $4;
```

The cursor expression can be another key in the same multicolumn GiST index
as the ordering, account and description keys. btree_gist's numeric comparisons
make it indexable. Keep the native date predicates for partition pruning.

The key uses the date's day ordinal multiplied by `2^64`, then adds the signed
ID. The smallest ID on the next day sorts exactly one unit after the largest
ID on the previous day. Date infinities receive the extreme integer day
positions, leaving all their ID distinctions intact. All arithmetic is numeric;
the packed value is never converted to float8 or used as a GiST distance.

This replaces smaller application-specific radices that are unsafe for
unrestricted bigint IDs. It does not turn native row comparisons into GiST
index conditions or infer date bounds from the packed expression.
