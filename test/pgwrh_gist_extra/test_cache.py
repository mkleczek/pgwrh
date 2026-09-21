"""Rescans must not retain executor-owned arrays or grow the scan cache."""

import pytest


def test_partition_array_cache_rescans(node):
    node.execute("""
        CREATE TABLE items(account text NOT NULL, seq integer) PARTITION BY HASH(account);
        CREATE TABLE p0 PARTITION OF items FOR VALUES WITH (MODULUS 2, REMAINDER 0);
        CREATE TABLE p1 PARTITION OF items FOR VALUES WITH (MODULUS 2, REMAINDER 1);
        INSERT INTO items SELECT 'account-' || i, i FROM generate_series(1,300) i;
        CREATE INDEX ON items USING gist (account pgwrh_gist_text_ops(attno=1));
        SET enable_seqscan=off;
        SET enable_bitmapscan=off;
        SET enable_memoize=off;
        CREATE FUNCTION cache_bytes() RETURNS bigint VOLATILE LANGUAGE sql AS
        $$SELECT coalesce(sum(total_bytes), 0)::bigint FROM pg_backend_memory_contexts
          WHERE name = 'pgwrh GiST array values'$$;
    """)
    # Each array is constructed at execution time, often at a reused address.
    # Both leaf scans keep their own fn_extra through 2000 nested-loop rescans.
    rows = node.execute("""
        SELECT n, x.seq, cache_bytes() FROM generate_series(1,2000) n
        CROSS JOIN LATERAL (
            SELECT seq FROM items
            WHERE account ||= ARRAY['account-' || (n % 300 + 1), 'missing-' || n]
            OFFSET 0
        ) x
        ORDER BY n
    """)
    assert [(n, seq) for n, seq, _ in rows] == [
        (n, n % 300 + 1) for n in range(1, 2001)]
    sizes = [size for _, _, size in rows]
    assert min(sizes) > 0
    assert max(sizes) < 128 * 1024


def test_toasted_array_and_changed_lengths(node):
    node.execute("""
        CREATE TABLE items(account text);
        INSERT INTO items VALUES ('a'), ('b'), ('c');
        CREATE INDEX ON items USING gist(account pgwrh_gist_text_ops);
        CREATE TABLE queries(id integer, accounts text[]);
        INSERT INTO queries VALUES (1, ARRAY['a']), (2, ARRAY[]::text[]),
            (3, NULL), (4, ARRAY['b',NULL,'b']);
        INSERT INTO queries SELECT 5, array_agg(md5(i::text)) || ARRAY['c']
          FROM generate_series(1,5000) i;
        SET enable_seqscan=off;
        SET enable_bitmapscan=off;
        SET enable_memoize=off;
    """)
    rows = node.execute("""
        SELECT q.id, x.account FROM queries q CROSS JOIN LATERAL
            (SELECT account FROM items WHERE account ||= q.accounts OFFSET 0) x
        ORDER BY q.id, x.account
    """)
    assert rows == [(1, 'a'), (4, 'b'), (5, 'c')]


def test_hash_filter_maps_attached_columns_by_name(node):
    node.execute("""
        CREATE TABLE items(d date, account text, seq integer) PARTITION BY RANGE(d);
        CREATE TABLE m PARTITION OF items FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')
          PARTITION BY HASH(account);
        CREATE TABLE p0(seq integer, d date, account text);
        CREATE TABLE p1(account text, seq integer, d date);
        ALTER TABLE m ATTACH PARTITION p0 FOR VALUES WITH (MODULUS 2, REMAINDER 0);
        ALTER TABLE m ATTACH PARTITION p1 FOR VALUES WITH (MODULUS 2, REMAINDER 1);
        INSERT INTO items SELECT '2025-01-01', 'account-' || i, i FROM generate_series(1,100) i;
        CREATE INDEX ON items USING gist(seq, account pgwrh_gist_text_ops(attno=2));
        SET enable_seqscan=off;
        SET enable_bitmapscan=off;
    """)
    for i in range(1, 20):
        array = f"ARRAY['account-{i}', 'account-{i+50}', NULL]"
        assert node.execute(f'SELECT seq FROM items WHERE account ||= {array} ORDER BY seq') == \
            node.execute(f'SELECT seq FROM items WHERE account = ANY({array}) ORDER BY seq')


def test_invalid_index_attribute_option(node):
    node.execute("""
        CREATE TABLE items(a text, b text);
        INSERT INTO items VALUES ('a', 'b');
        CREATE INDEX ON items USING gist(a pgwrh_gist_text_ops(attno=2), b);
        SET enable_seqscan=off;
    """)
    with pytest.raises(Exception, match='attno option must match'):
        node.execute("SELECT * FROM items WHERE a ||= ARRAY['a','b']")


def test_array_null_truth_values(node):
    assert node.execute("""
        SELECT bool_and((a ||= b) IS NOT DISTINCT FROM (a = ANY(b))),
               bool_and((a &&= b) IS NOT DISTINCT FROM (a = ALL(b)))
        FROM (VALUES ('a'::text), ('b'::text)) v(a)
        CROSS JOIN (VALUES (ARRAY['a','b']), (ARRAY['a',NULL]),
            (ARRAY[NULL]::text[]), (ARRAY[]::text[]), (NULL::text[])) q(b)
    """) == [(True, True)]
    # The operators are deliberately strict: GiST assumes strict search operators.
    assert node.execute("SELECT NULL::text ||= ARRAY[]::text[], NULL::text &&= ARRAY[]::text[]") == [
        (None, None)]
