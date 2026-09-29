"""Application-owned cursor, generic GiST features, and real remote partitions."""
import json

import pytest


KEY_SQL = """
CREATE FUNCTION transaction_key(date, bigint) RETURNS numeric
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
RETURN ($1 - DATE '2000-01-01')::numeric * 18446744073709551616::numeric + $2;
"""


def walk(tree):
    yield tree
    for child in tree.get('Plans', []):
        yield from walk(child)


def plan(connection, query):
    value = connection.execute('EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON, COSTS OFF, TIMING OFF) ' + query)[0][0]
    return (json.loads(value) if isinstance(value, str) else value)[0]['Plan']


def create_tree(connection, foreign=False):
    connection.execute(KEY_SQL)
    connection.execute('CREATE TABLE transactions(d date NOT NULL, id bigint NOT NULL, account text NOT NULL, description text) PARTITION BY RANGE(d)')
    for month in range(1, 5):
        connection.execute(f"CREATE TABLE m{month} PARTITION OF transactions FOR VALUES FROM ('2026-0{month}-01') TO ('2026-0{month+1}-01') PARTITION BY HASH(account)")
        for remainder in range(2):
            leaf = f'm{month}h{remainder}'
            bound = f'PARTITION OF m{month} FOR VALUES WITH (MODULUS 2, REMAINDER {remainder})'
            if foreign and month != 1:
                connection.execute(f"CREATE FOREIGN TABLE {leaf} {bound} SERVER replica OPTIONS (table_name '{leaf}')")
            else:
                connection.execute(f'CREATE TABLE {leaf} {bound}')
        if not foreign or month == 1:
            connection.execute(f"""
                CREATE INDEX ON m{month} USING gist
                    (d pgwrh_gist_date_order_ops, id pgwrh_gist_int8_order_ops,
                     account pgwrh_gist_text_ops(attno=3), description gist_trgm_ops,
                     transaction_key(d,id));
                INSERT INTO m{month}
                  SELECT DATE '2026-0{month}-01' + (i % 3),
                    CASE WHEN i % 2 = 0 THEN 9223372036854775807 - i
                         ELSE '-9223372036854775808'::bigint + i END,
                    'account-' || (i % 7),
                    CASE WHEN i % 3 = 0 THEN 'coffee payment ' ELSE 'groceries ' END || i
                  FROM generate_series(0, 999) i;
                ANALYZE m{month};
            """)
    connection.execute('ANALYZE transactions; SET enable_seqscan=off; SET enable_bitmapscan=off; SET enable_sort=off; SET max_parallel_workers_per_gather=0')


@pytest.fixture
def transactions(node):
    node.execute('CREATE DATABASE replica')
    node.execute("""
        CREATE EXTENSION pg_trgm;
        CREATE EXTENSION pgwrh_fdw;
        ALTER DATABASE replica SET session_preload_libraries='pgwrh_gist_extra';
        ALTER DATABASE replica SET enable_seqscan=off;
        ALTER DATABASE replica SET enable_bitmapscan=off;
        ALTER DATABASE replica SET enable_sort=off;
    """)
    with node.node.connect(dbname='replica', autocommit=True) as remote:
        remote.execute('CREATE EXTENSION pgwrh_gist_extra CASCADE; CREATE EXTENSION pg_trgm')
        create_tree(remote)
        node.execute(f"""
            CREATE SERVER replica FOREIGN DATA WRAPPER pgwrh_fdw OPTIONS
              (host '127.0.0.1', port '{node.node.port}', dbname 'replica',
               extensions 'pgwrh_gist_extra,pg_trgm');
            CREATE USER MAPPING FOR CURRENT_USER SERVER replica;
        """)
        create_tree(node, foreign=True)
        yield node, remote


FILTER = """d >= DATE '2026-01-01' AND d < DATE '2026-04-01'
    AND account = ANY (ARRAY['account-0','account-1','account-4'])
    AND account ||= ARRAY['account-0','account-1','account-4']
    AND description ILIKE '%coffee%'
    AND transaction_key(d,id) < transaction_key(DATE '2026-03-31', 9223372036854775807)"""


def test_ordered_partitions_and_remote_limit(transactions):
    local, remote = transactions
    query = 'SELECT d,id,account,upper(description) FROM transactions WHERE ' + FILTER + ' ORDER BY d DESC,id DESC LIMIT 17'
    remote.execute('SET pgwrh_gist_extra.enable_ordered_scan=off')
    expected = remote.execute(query)
    remote.execute('SET pgwrh_gist_extra.enable_ordered_scan=on')
    assert local.execute(query) == remote.execute(query) == expected
    local_tree = plan(remote, query)
    assert any(n.get('Custom Plan Provider') == 'pgwrh GiST ordered scan' for n in walk(local_tree)), local_tree
    assert not any(n['Node Type'] in ('Sort', 'Incremental Sort') for n in walk(local_tree))
    tree = plan(local, query)
    nodes = list(walk(tree))
    assert any(n['Node Type'] == 'Append' for n in nodes)
    assert any(n['Node Type'] == 'Merge Append' for n in nodes)
    assert not any(n['Node Type'] in ('Sort', 'Incremental Sort') for n in nodes)
    assert all(not n.get('Relation Name', '').startswith('m4') for n in nodes)
    assert any(n.get('Relation Name', '').startswith('m1') and n['Actual Loops'] == 0 for n in nodes)
    scans = [n for n in nodes if 'Remote SQL' in n]
    assert scans and all(' ORDER BY ' in n['Remote SQL'] and ' LIMIT ' in n['Remote SQL'] for n in scans)
    # Inspect the actual shipped statement on the replica, not just its local plan.
    for scan in scans:
        remote_plan = plan(remote, scan['Remote SQL'])
        assert any(n.get('Custom Plan Provider') == 'pgwrh GiST ordered scan' for n in walk(remote_plan))
        conditions = ' '.join(n.get('Index Cond', '') for n in walk(remote_plan))
        assert '||=' in conditions and '~~*' in conditions and '18446744073709551616' in conditions


@pytest.mark.parametrize('generic', [False, True])
def test_complete_pages_and_changing_account_arrays(transactions, generic):
    local, remote = transactions
    local.execute('SET plan_cache_mode=' + ('force_generic_plan' if generic else 'force_custom_plan'))
    local.execute("""
        PREPARE page(text[],date,date,date,bigint,int) AS
          SELECT d::text AS day,id,account FROM transactions
          WHERE d >= $2 AND d < $3 AND d <= $4
            AND account = ANY($1) AND account ||= $1
            AND description ILIKE '%coffee%'
            AND transaction_key(d,id) < transaction_key($4,$5)
          ORDER BY d DESC,id DESC LIMIT $6;
    """)
    remote.execute('SET pgwrh_gist_extra.enable_ordered_scan=off')
    for accounts in ["ARRAY['account-0','account-1','account-4']", "ARRAY['account-6',NULL]", "ARRAY[]::text[]"]:
        expected = remote.execute(f"SELECT d::text AS day,id,account FROM transactions WHERE d >= '2026-01-01' AND d < '2026-04-01' AND account = ANY({accounts}) AND description ILIKE '%coffee%' ORDER BY d DESC,id DESC")
        after = ('2026-03-31', 9223372036854775807)
        actual = []
        while True:
            query = f"EXECUTE page({accounts},'2026-01-01','2026-04-01','{after[0]}',({after[1]})::bigint,17)"
            page = local.execute(query)
            if not page:
                break
            actual.extend(page)
            after = page[-1][:2]
        assert actual == expected
    tree = plan(local, "EXECUTE page(ARRAY['account-0','account-1'],'2026-03-01','2026-04-01','2026-03-31',9223372036854775807,17)")
    assert all(not n.get('Relation Name', '').startswith(('m1','m2','m4')) for n in walk(tree))
