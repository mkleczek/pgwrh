# Exact GiST ordering

The `pgwrh_gist_{int2,int4,int8,date,timestamp,timestamptz}_order_ops`
operator classes reuse btree_gist's fixed-size lower/upper keys and comparison
support. They add ordering operators without introducing another access method
or changing GiST pages. Use these classes on the ordering columns of a
multicolumn GiST index; account and trigram columns retain their own classes.

The internal `<#` operator returns an exact float8 ordering component. Its
smallint selector is `1` for ascending or `-1` for descending. For bigint and
timestamps, a second term (`2` or `-2`) completes the ordering:

```sql
SELECT id FROM transactions
ORDER BY id <# (-1)::smallint, id <# (-2)::smallint;
```

For a signed 64-bit value, the components are its signed high 32 bits followed
by its unsigned low 32 bits. Every component is exactly representable in
float8, and their lexicographic order is the original integer order. Negating
both components reverses that order, including at the minimum bigint value.
No sentinel date, subtraction, ID bound, or conversion of the full value to
float8 is involved. Timestamp components use PostgreSQL's integer microseconds;
date uses its integer day count. Infinity representations also preserve order.

At an internal entry the high component uses the lower or upper bound. A range
crossing a high-word boundary uses the whole unsigned low-word domain as its
conservative bound. This is required even when the low-word operator is used
on its own. At leaves both components are exact, so ordering rechecks and a
tie-repair sort are unnecessary. GiST's normal heap visibility and predicate
rechecks remain in effect.

The operator classes use integer support functions for date/time keys as well:
their fixed-size physical representations and comparisons are identical. This
avoids calendar subtraction and distance overflow at infinities. The reused
storage and support-function contracts are tested on PostgreSQL 18 and 19.

GiST emits NULL ordering values last. A native-order adapter must therefore
either request NULLS LAST or prove that a requested NULLS FIRST column cannot
contain NULLs. Merely labeling an existing IndexPath with native pathkeys is
incorrect: its child plan must retain its real `<#` ordering expressions.
