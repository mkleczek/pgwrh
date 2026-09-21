"""An exact order, including values which collapse to the same float8."""
import pytest


@pytest.mark.parametrize('typ,opclass,values,parts', [
    ('smallint', 'int2', ['-32768', '-1', '0', '1', '32767'], 1),
    ('integer', 'int4', ['-2147483648', '-1', '0', '1', '2147483647'], 1),
    ('bigint', 'int8', ['-9223372036854775808', '-9007199254740993', '-4294967297',
                      '-4294967296', '-1', '0', '1', '4294967295', '4294967296',
                      '9007199254740992', '9007199254740993', '9223372036854775807'], 2),
    ('date', 'date', ["'-infinity'", "'4713-01-01 BC'", "'1999-12-31'", "'2000-01-01'",
                     "'2026-01-01'", "'5874897-12-31'", "'infinity'"], 1),
    ('timestamp', 'timestamp', ["'-infinity'", "'1999-12-31 23:59:59.999999'",
                              "'2000-01-01'", "'2500-01-01 12:00:00.000001'",
                              "'2500-01-01 12:00:00.000002'", "'infinity'"], 2),
    ('timestamptz', 'timestamptz', ["'-infinity'", "'1999-12-31 23:59:59.999999+00'",
                                  "'2000-01-01+00'", "'2500-01-01 12:00:00.000001+00'",
                                  "'2500-01-01 12:00:00.000002+00'", "'infinity'"], 2),
])
def test_exact_ordering_keys(node, typ, opclass, values, parts):
    node.execute(f'CREATE TABLE items(id serial, k {typ})')
    node.execute('INSERT INTO items(k) VALUES ' + ','.join(f'(({v})::{typ})' for v in values))
    node.execute('INSERT INTO items(k) VALUES (NULL)')
    # Make inner pages and exercise inserts after the index was built.
    node.execute('INSERT INTO items(k) SELECT k FROM items CROSS JOIN generate_series(1,200)')
    node.execute(f'CREATE INDEX items_order ON items USING gist(k pgwrh_gist_{opclass}_order_ops)')
    node.execute('INSERT INTO items(k) SELECT k FROM items LIMIT 100')
    node.execute('SET enable_seqscan=off; SET enable_bitmapscan=off')
    for direction, sign in [('ASC', 1), ('DESC', -1)]:
        ordering = ', '.join(f'k <# ({part*sign})::smallint' for part in range(1, parts+1))
        expected = node.execute(f'SELECT k::text AS value FROM items ORDER BY k {direction} NULLS LAST')
        actual = node.execute(f'SELECT k::text FROM items ORDER BY {ordering}')
        assert actual == expected
        plan = '\n'.join(row[0] for row in node.execute(
            f'EXPLAIN (COSTS OFF) SELECT k FROM items ORDER BY {ordering} LIMIT 5'))
        assert 'Index Scan using items_order' in plan or 'Index Only Scan using items_order' in plan
        assert 'Sort' not in plan
    # Bounds for a low-word-only scan must still be conservative.
    if parts == 2:
        for sign in [1, -1]:
            expr = f'k <# ({sign*2})::smallint'
            indexed = node.execute(f'SELECT {expr} FROM items ORDER BY {expr}')
            node.execute('SET enable_indexscan=off; SET enable_indexonlyscan=off')
            sequential = node.execute(f'SELECT {expr} FROM items ORDER BY {expr}')
            assert indexed == sequential
            node.execute('SET enable_indexscan=on; SET enable_indexonlyscan=on')


def test_ordering_opclass_keeps_filters_and_fetch(node):
    node.execute("""
        CREATE TABLE items(k bigint NOT NULL);
        INSERT INTO items SELECT 9007199254740900 + i FROM generate_series(1,3000) i;
        CREATE INDEX items_order ON items USING gist(k pgwrh_gist_int8_order_ops);
        SET enable_seqscan=off;
        SET enable_bitmapscan=off;
    """)
    node.execute('VACUUM ANALYZE items')
    assert node.execute("""SELECT k FROM items WHERE k >= 9007199254740992
        ORDER BY k <# (-1)::smallint, k <# (-2)::smallint LIMIT 2""") == [
        (9007199254743900,), (9007199254743899,)]
