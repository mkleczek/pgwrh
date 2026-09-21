from test.pgwrh_gist_extra.test_ordered_scan import plan, walk


def custom(tree):
    return any(n.get('Custom Plan Provider') == 'pgwrh GiST ordered scan' for n in walk(tree))


def test_partial_index_and_scroll_cursor(node):
    node.execute("""
        CREATE TABLE items(id bigint NOT NULL, enabled boolean, label text);
        INSERT INTO items SELECT 9007199254740992 + i, i % 2 = 0, 'item-' || i
            FROM generate_series(1,200) i;
        CREATE INDEX ON items USING gist(id pgwrh_gist_int8_order_ops) WHERE enabled;
        SET enable_seqscan=off;
        SET enable_sort=off;
    """)
    query = 'SELECT id, upper(label), items::text, tableoid::regclass::text FROM items WHERE enabled ORDER BY id DESC LIMIT 10'
    assert custom(plan(node, query))
    assert not custom(plan(node, query.replace('WHERE enabled', '')))
    expected = node.execute(query)
    node.execute('SET pgwrh_gist_extra.enable_ordered_scan=off')
    assert node.execute(query) == expected
    node.execute('SET pgwrh_gist_extra.enable_ordered_scan=on')
    node.execute('BEGIN')
    node.execute('DECLARE page SCROLL CURSOR FOR ' + query)
    assert node.execute('FETCH 5 FROM page') == expected[:5]
    assert node.execute('FETCH BACKWARD 2 FROM page') == [expected[3], expected[2]]
    assert node.execute('FETCH FORWARD 3 FROM page') == expected[3:6]
    node.execute('ROLLBACK')


def test_index_only_projection_and_rls(node):
    node.execute("""
        CREATE TABLE items(d date NOT NULL, id bigint NOT NULL);
        INSERT INTO items SELECT DATE '2500-01-01' + i % 3, 9007199254740992 + i
          FROM generate_series(1,200) i;
        CREATE INDEX ON items USING gist(d pgwrh_gist_date_order_ops, id pgwrh_gist_int8_order_ops);
        CREATE ROLE reader;
        GRANT SELECT ON items TO reader;
        ALTER TABLE items ENABLE ROW LEVEL SECURITY;
        CREATE POLICY only_some ON items TO reader USING (id % 3 = 0);
        SET enable_seqscan=off;
        SET enable_sort=off;
    """)
    node.execute('VACUUM ANALYZE items')
    query = 'SELECT d,id,id + 1 FROM items ORDER BY d DESC,id DESC LIMIT 17'
    tree = plan(node, query, True)
    assert custom(tree)
    assert any(n['Node Type'] == 'Index Only Scan' for n in walk(tree))
    node.execute('SET ROLE reader')
    actual = node.execute(query)
    assert custom(plan(node, query))
    node.execute('SET pgwrh_gist_extra.enable_ordered_scan=off')
    assert node.execute(query) == actual
    assert all(row[1] % 3 == 0 for row in actual)
    node.execute('RESET ROLE')
