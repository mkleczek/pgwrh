import pytest


def setup_table(node):
    node.execute("""
        CREATE TABLE items(account text, seq int);
        INSERT INTO items
          SELECT 'account-' || (i % 29), i FROM generate_series(1,1000) i;
        INSERT INTO items VALUES (NULL, 1001);
        CREATE INDEX items_gist ON items USING gist (account pgwrh_gist_text_ops);
        ANALYZE items;
        SET enable_seqscan = off;
        SET enable_bitmapscan = off;
    """)


def test_standalone_installation(node):
    assert node.execute("""SELECT extname FROM pg_extension
        WHERE extname <> 'plpgsql' ORDER BY extname""") == [
        ('btree_gist',), ('pgwrh_gist_extra',)]
    assert node.execute("SELECT extversion FROM pg_extension WHERE extname='pgwrh_gist_extra'") == [
        ('1.0.0-alpha1',)]
    assert node.execute("SELECT * FROM pg_extension_update_paths('pgwrh_gist_extra')") == []
    node.execute('DROP EXTENSION pgwrh_gist_extra')
    assert node.execute("SELECT count(*) FROM pg_extension WHERE extname='btree_gist'") == [(1,)]


def test_scalar_operators(node):
    assert node.execute("""SELECT
        'a' ||= ARRAY['b','a'], 'a' ||= ARRAY['b',NULL],
        'a' ||= ARRAY[]::text[], 'a' ||= NULL::text[],
        'a' &&= ARRAY['a','a'], 'a' &&= ARRAY['a','b'],
        'a' &&= ARRAY[]::text[], 'a' &&= ARRAY['a',NULL]
    """) == [(True, None, False, None, True, False, True, None)]


@pytest.mark.parametrize('array', ["ARRAY['account-1','account-7']", "ARRAY['missing']",
                                  "ARRAY[]::text[]", "ARRAY[NULL,'account-7','account-7']",
                                  'NULL::text[]'])
def test_indexed_any_matches_native_filter(node, array):
    setup_table(node)
    actual = node.execute(f'SELECT seq FROM items WHERE account ||= {array} ORDER BY seq')
    expected = node.execute(f'SELECT seq FROM items WHERE account = ANY({array}) ORDER BY seq')
    assert actual == expected
    # Check a nonempty filter so empty/NULL simplification does not remove the scan.
    plan = node.execute("EXPLAIN (COSTS OFF) SELECT * FROM items "
                        "WHERE account ||= ARRAY['account-1','account-7']")
    assert 'Index Scan using items_gist' in '\n'.join(row[0] for row in plan)


def test_existing_comparisons_and_all_operator(node):
    setup_table(node)
    for predicate in ["account < 'account-2'", "account = 'account-7'",
                      "account &&= ARRAY['account-7','account-7']",
                      "account &&= ARRAY['account-7','account-8']"]:
        node.execute('SET enable_seqscan=off; SET enable_indexscan=on')
        indexed = node.execute('SELECT seq FROM items WHERE ' + predicate + ' ORDER BY seq')
        node.execute('SET enable_seqscan=on; SET enable_indexscan=off')
        sequential = node.execute('SELECT seq FROM items WHERE ' + predicate + ' ORDER BY seq')
        assert indexed == sequential


def test_generic_plan_and_rescans(node):
    setup_table(node)
    node.execute("""
        SET plan_cache_mode=force_generic_plan;
        PREPARE accounts(text[]) AS SELECT seq FROM items WHERE account ||= $1 ORDER BY seq;
    """)
    for array in ["ARRAY['account-1','account-2']", "ARRAY['account-7','account-8']",
                  "ARRAY[]::text[]", "ARRAY['missing']", "ARRAY['account-1','account-2']"]:
        assert node.execute('EXECUTE accounts(' + array + ')') == node.execute(
            f'SELECT seq FROM items WHERE account = ANY({array}) ORDER BY seq')
    # Nested-loop rescans change the array while reusing the same GiST scan state.
    actual = node.execute("""SELECT q.id, x.seq FROM
        (VALUES (1,ARRAY['account-1','account-2']), (2,ARRAY['account-7','account-8']),
                (3,ARRAY['missing'])) q(id, accounts)
        CROSS JOIN LATERAL
          (SELECT seq FROM items WHERE account ||= q.accounts OFFSET 0) x
        ORDER BY q.id, x.seq""")
    expected = node.execute("""SELECT q.id, i.seq FROM
        (VALUES (1,ARRAY['account-1','account-2']), (2,ARRAY['account-7','account-8']),
                (3,ARRAY['missing'])) q(id, accounts)
        JOIN items i ON i.account = ANY(q.accounts) ORDER BY q.id, i.seq""")
    assert actual == expected


def test_hash_partition_option(node):
    node.execute("""
        CREATE TABLE items(account text NOT NULL, seq int) PARTITION BY HASH(account);
        CREATE TABLE p0 PARTITION OF items FOR VALUES WITH (MODULUS 2, REMAINDER 0);
        CREATE TABLE p1 PARTITION OF items FOR VALUES WITH (MODULUS 2, REMAINDER 1);
        INSERT INTO items SELECT 'account-' || (i%29), i FROM generate_series(1,1000) i;
        CREATE INDEX ON items USING gist (account pgwrh_gist_text_ops(attno=1));
        SET enable_seqscan=off;
        SET enable_bitmapscan=off;
    """)
    for array in ["ARRAY['account-1','account-2','account-7','account-8']",
                  "ARRAY['missing','account-4']", "ARRAY['account-1']"]:
        assert node.execute(f'SELECT seq FROM items WHERE account ||= {array} ORDER BY seq') == \
            node.execute(f'SELECT seq FROM items WHERE account = ANY({array}) ORDER BY seq')
