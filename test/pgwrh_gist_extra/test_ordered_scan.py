import json

import pytest


def plan(node, query, analyze=False):
    value = node.execute('EXPLAIN (FORMAT JSON, COSTS OFF' +
                         (', ANALYZE, TIMING OFF' if analyze else '') + ') ' + query)[0][0]
    return (json.loads(value) if isinstance(value, str) else value)[0]['Plan']


def walk(tree):
    yield tree
    for child in tree.get('Plans', []):
        yield from walk(child)


def setup(node, nullable=False):
    node.execute(f"""
        CREATE TABLE items(d date {'NULL' if nullable else 'NOT NULL'}, id bigint NOT NULL, payload text);
        INSERT INTO items SELECT '2026-01-01'::date + (i % 7),
            CASE WHEN i % 2 = 0 THEN 9007199254740992 + i ELSE -9007199254740992 - i END,
            'payload-' || i FROM generate_series(1,4000) i;
        CREATE INDEX items_order ON items USING gist
            (d pgwrh_gist_date_order_ops, id pgwrh_gist_int8_order_ops);
        ANALYZE items;
        SET enable_seqscan=off;
        SET enable_bitmapscan=off;
    """)


@pytest.mark.parametrize('ordering', ['d DESC, id DESC', 'd, id', 'd DESC, id', 'id DESC, d'])
def test_native_column_ordering(node, ordering):
    setup(node)
    query = f"SELECT d, id, upper(payload) AS label FROM items WHERE d >= DATE '2026-01-02' ORDER BY {ordering} LIMIT 30"
    node.execute('SET pgwrh_gist_extra.enable_ordered_scan=off')
    expected = node.execute(query)
    node.execute('SET pgwrh_gist_extra.enable_ordered_scan=on')
    assert node.execute(query) == expected
    tree = plan(node, query, True)
    assert any(n.get('Custom Plan Provider') == 'pgwrh GiST ordered scan' for n in walk(tree))
    assert not any(n['Node Type'] in ('Sort', 'Incremental Sort') for n in walk(tree))
    assert any('Order By' in n and '<#' in str(n['Order By']) for n in walk(tree))


def test_nulls_and_unsupported_paths_fall_back(node):
    setup(node, True)
    node.execute("INSERT INTO items VALUES (NULL, 9223372036854775807, 'null')")
    for ordering in ['d DESC, id DESC', 'd NULLS FIRST, id', 'payload', 'd DESC NULLS LAST, id DESC']:
        query = f'SELECT d, id FROM items ORDER BY {ordering} LIMIT 10'
        node.execute('SET pgwrh_gist_extra.enable_ordered_scan=off')
        expected = node.execute(query)
        node.execute('SET pgwrh_gist_extra.enable_ordered_scan=on')
        assert node.execute(query) == expected
        custom = any(n.get('Custom Plan Provider') == 'pgwrh GiST ordered scan' for n in walk(plan(node, query)))
        assert custom == (ordering == 'd DESC NULLS LAST, id DESC')
    tree = plan(node, 'SELECT * FROM items ORDER BY id DESC LIMIT 1 FOR UPDATE')
    assert not any(n.get('Custom Plan Provider') == 'pgwrh GiST ordered scan' for n in walk(tree))


def test_generic_parameters_and_lateral_rescans(node):
    setup(node)
    node.execute("""
        SET plan_cache_mode=force_generic_plan;
        PREPARE page(date, bigint, integer) AS
          SELECT d, id FROM items WHERE d >= $1 AND id < $2 ORDER BY d DESC, id DESC LIMIT $3;
    """)
    for day, limit in [('2026-01-01', 1), ('2026-01-05', 20), ('2026-01-08', 5)]:
        query = f"EXECUTE page('{day}', 9223372036854775807, {limit})"
        assert node.execute(query) == node.execute(
            f"SELECT d,id FROM items WHERE d >= '{day}' AND id < 9223372036854775807 "
            f"ORDER BY d DESC, id DESC LIMIT {limit}")
        assert any(n.get('Custom Plan Provider') == 'pgwrh GiST ordered scan' for n in walk(plan(node, query)))
    query = """SELECT q.d, x.id FROM
        (VALUES (DATE '2026-01-02'), (DATE '2026-01-04'), (DATE '2026-01-06')) q(d)
        CROSS JOIN LATERAL (SELECT id FROM items WHERE d >= q.d
            ORDER BY d DESC, id DESC LIMIT 3) x ORDER BY q.d, x.id"""
    actual = node.execute(query)
    node.execute('SET pgwrh_gist_extra.enable_ordered_scan=off')
    assert actual == node.execute(query)
