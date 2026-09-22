"""Lookup pushdown through a real pgwrh rollout and its partition slots."""
from .pgwrh_testkit import MasterHandle, PgwrhCluster, ReplicaSpec, quote_literal, wait_until


def plans(node):
    yield node
    for child in node.get('Plans', []):
        yield from plans(child)


def test_lookup_join_in_mixed_managed_topology(postgres_node_factory):
    master = MasterHandle(postgres_node_factory('lookup_master'))
    master.execute('''
        CREATE ROLE test_replica;
        CREATE SCHEMA data AUTHORIZATION test_replica;
        CREATE TABLE data.root(id int, k int, value text) PARTITION BY HASH(k);
        CREATE TABLE data.p0 PARTITION OF data.root(PRIMARY KEY(id,k))
          FOR VALUES WITH(MODULUS 2,REMAINDER 0);
        CREATE TABLE data.p1 PARTITION OF data.root(PRIMARY KEY(id,k))
          FOR VALUES WITH(MODULUS 2,REMAINDER 1);
        ALTER TABLE data.p0 OWNER TO test_replica;
        ALTER TABLE data.p1 OWNER TO test_replica;
        INSERT INTO data.root SELECT n,n,'value-'||n FROM generate_series(1,2000) n;
        SELECT pgwrh.create_replica_cluster('g1');
    ''')
    keys = {}
    for owner, other in [('lookup_a', 'lookup_b'), ('lookup_b', 'lookup_a')]:
        keys[owner] = master.query_scalar(f"""SELECT n::text FROM generate_series(1,100) n
            WHERE pgwrh.score(100,n::text,'{owner}') > pgwrh.score(100,n::text,'{other}') LIMIT 1""")
    expression = f"SELECT CASE WHEN $2 = 'p0' THEN '{keys['lookup_a']}' ELSE '{keys['lookup_b']}' END"
    master.execute(f"""INSERT INTO pgwrh.sharded_table
        (replication_group_id, sharded_table_schema, sharded_table_name, replication_factor, sharding_key_expression)
        VALUES('g1','data','root',0,{quote_literal(expression)})""")
    cluster = PgwrhCluster(master, postgres_node_factory)
    cluster.add_replicas([ReplicaSpec('lookup_a'), ReplicaSpec('lookup_b')])
    cluster.deploy(timeout=60)
    cluster.assert_query_results_match('SELECT * FROM data.root ORDER BY id')
    reader = cluster.replicas[0]
    assert reader.query_scalar('SELECT count(*) FROM pgwrh.connected_local_shard') == 1
    assert reader.query_scalar('SELECT count(*) FROM pgwrh.connected_remote_shard') == 1
    local_key = master.query_scalar('SELECT min(k) FROM data.p0')
    remote_key = master.query_scalar('SELECT min(k) FROM data.p1')
    reader.execute(f"""CREATE TABLE public.lookup(k int,label text,enabled bool);
        INSERT INTO public.lookup VALUES({local_key},'local',true),
          ({remote_key},repeat('wide',2000),true),({remote_key},NULL,true);
        ANALYZE public.lookup""")
    query = ('SELECT s.id,s.k,l.label FROM data.root s JOIN public.lookup l ON s.k=l.k '
             'WHERE l.enabled')
    with reader.node.connect() as conn:
        tree = conn.execute('EXPLAIN(ANALYZE,VERBOSE,FORMAT JSON) ' + query)[0][0][0]['Plan']
        custom, = [p for p in plans(tree) if p.get('Custom Plan Provider') == 'Pgwrh Remote Lookup Join']
        assert custom['Remote Executions'] == 1, custom
        assert custom['Remote Rows'] == 2, custom
        assert custom['Lookup Rows'] == 3, custom
        assert all('label' not in p.get('Remote SQL', '') for p in plans(custom))
        conn.execute('SET pgwrh_fdw.enable_lookup_join=off')
        conn.execute('CREATE TEMP TABLE baseline AS ' + query)
        conn.execute('SET pgwrh_fdw.enable_lookup_join=on')
        conn.execute('CREATE TEMP TABLE optimized AS ' + query)
        assert conn.execute('''(TABLE baseline EXCEPT ALL TABLE optimized)
            UNION ALL (TABLE optimized EXCEPT ALL TABLE baseline)''') == []
        conn.commit()
        conn.execute('SELECT pgwrh_fdw_disconnect_all()')
        conn.execute(f'UPDATE public.lookup SET enabled=false WHERE k={remote_key}')
        conn.commit()
        tree = conn.execute('EXPLAIN(ANALYZE,FORMAT JSON) ' + query)[0][0][0]['Plan']
        custom, = [p for p in plans(tree) if p.get('Custom Plan Provider') == 'Pgwrh Remote Lookup Join']
        assert custom['Remote Executions'] == 0, custom
        assert custom['Skipped Shards'] == 1, custom
        assert conn.execute('SELECT count(*) FROM pgwrh_fdw_get_connections()') == [(0,)]

    # Co-locating both leaves produces a collapsed remote subtree on lookup_b.
    master.execute(f"""INSERT INTO pgwrh.sharded_table
        (replication_group_id, sharded_table_schema, sharded_table_name, replication_factor, sharding_key_expression)
        SELECT replication_group_id, sharded_table_schema, sharded_table_name, replication_factor,
               {quote_literal('SELECT ' + quote_literal(keys['lookup_a']))}
        FROM pgwrh.sharded_table
        WHERE version = (SELECT current_version FROM pgwrh.replication_group WHERE replication_group_id='g1')
        ON CONFLICT (replication_group_id, sharded_table_schema, sharded_table_name, version)
        DO UPDATE SET sharding_key_expression = EXCLUDED.sharding_key_expression""")
    cluster.deploy(timeout=60)
    remote_reader = cluster.replicas[1]
    wait_until(lambda: remote_reader.query_scalar("""SELECT EXISTS(SELECT FROM pgwrh.remote_node n
        JOIN pgwrh.reachable_shard r USING(reg_class) WHERE n.node_rel_id=('data','root')::pgwrh.rel_id)"""),
        timeout=30, message='remote lookup test root did not collapse')
    assert remote_reader.query_scalar("""SELECT value FROM pg_foreign_table f,
        LATERAL pgwrh.opts(f.ftoptions) WHERE f.ftrelid='data_remote.root'::regclass AND key='lookup_join'""") == 'false'
    remote_reader.execute('CREATE TABLE public.lookup(k int,label text,enabled bool); '
                          "INSERT INTO public.lookup VALUES(1,'one',true); ANALYZE public.lookup")
    tree = remote_reader.execute('EXPLAIN(VERBOSE,FORMAT JSON) ' + query)[0][0][0]['Plan']
    assert not any(p.get('Custom Plan Provider') == 'Pgwrh Remote Lookup Join' for p in plans(tree))
    assert remote_reader.execute(query) == [(1, 1, 'one')]
