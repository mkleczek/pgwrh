"""Partial states through pgwrh-managed virtual servers and real shard hosts."""
from test.pgwrh.pgwrh_testkit import MasterHandle, PgwrhCluster, ReplicaSpec, quote_literal


def plans(node):
    yield node
    for child in node.get('Plans', []):
        yield from plans(child)


def test_partial_aggregation_in_managed_topology(postgres_node_factory):
    master = MasterHandle(postgres_node_factory('partial_master'))
    master.execute("""
        CREATE ROLE test_replica;
        CREATE SCHEMA data AUTHORIZATION test_replica;
        CREATE TABLE data.root(id int, k int, g int, v int) PARTITION BY HASH(k);
        CREATE TABLE data.p0 PARTITION OF data.root(PRIMARY KEY(id,k))
          FOR VALUES WITH(MODULUS 2,REMAINDER 0);
        CREATE TABLE data.p1 PARTITION OF data.root(PRIMARY KEY(id,k))
          FOR VALUES WITH(MODULUS 2,REMAINDER 1);
        ALTER TABLE data.p0 OWNER TO test_replica;
        ALTER TABLE data.p1 OWNER TO test_replica;
        INSERT INTO data.root SELECT n,n,n%10,CASE WHEN n%7=0 THEN NULL ELSE n%100 END
          FROM generate_series(1,20000) n;
        SELECT pgwrh.create_replica_cluster('g1');
    """)
    keys = {}
    for owner, other in [('partial_a', 'partial_b'), ('partial_b', 'partial_a')]:
        keys[owner] = master.query_scalar(f"""SELECT n::text FROM generate_series(1,100) n
            WHERE pgwrh.score(100,n::text,'{owner}') > pgwrh.score(100,n::text,'{other}') LIMIT 1""")
    expression = f"SELECT CASE WHEN $2 = 'p0' THEN '{keys['partial_a']}' ELSE '{keys['partial_b']}' END"
    master.execute(f"""INSERT INTO pgwrh.sharded_table
        (replication_group_id, sharded_table_schema, sharded_table_name, replication_factor, sharding_key_expression)
        VALUES('g1','data','root',0,{quote_literal(expression)})""")
    cluster = PgwrhCluster(master, postgres_node_factory)
    cluster.add_replicas([ReplicaSpec('partial_a'), ReplicaSpec('partial_b')])
    cluster.deploy(timeout=60)
    cluster.assert_query_results_match('SELECT * FROM data.root ORDER BY id')
    for reader in cluster.replicas:
        assert reader.query_scalar('SELECT count(*) FROM pgwrh.connected_local_shard') == 1
        assert reader.query_scalar('SELECT count(*) FROM pgwrh.connected_remote_shard') == 1
        with reader.node.connect() as conn:
            conn.execute('SET enable_partitionwise_aggregate=on; SET max_parallel_workers_per_gather=0')
            for query in [
                'SELECT count(*),count(v),sum(v),min(v),max(v) FROM data.root',
                'SELECT g,count(*),sum(v) FILTER(WHERE v>40),min(v),max(v) '
                'FROM data.root GROUP BY g HAVING count(*)>1000 ORDER BY g',
            ]:
                tree = conn.execute('EXPLAIN(ANALYZE,VERBOSE,FORMAT JSON) '+query)[0][0][0]['Plan']
                foreign = [p for p in plans(tree) if 'Remote SQL' in p]
                assert len(foreign) == 1, tree
                assert 'count(*)' in foreign[0]['Remote SQL'], tree
                assert ' HAVING ' not in foreign[0]['Remote SQL'], tree
                assert any(p.get('Partial Mode') == 'Finalize' for p in plans(tree)), tree
                assert any(p.get('Partial Mode') == 'Partial' for p in plans(tree)), tree
                assert foreign[0]['Actual Rows'] <= 10, tree
                assert conn.execute(query) == master.execute(query)
            conn.execute('SET enable_partitionwise_aggregate=off')
            tree = conn.execute('EXPLAIN(ANALYZE,VERBOSE,FORMAT JSON) '
                                'SELECT g,count(*) FROM data.root GROUP BY g')[0][0][0]['Plan']
            foreign = [p for p in plans(tree) if 'Remote SQL' in p]
            assert all('count(*)' not in p['Remote SQL'] for p in foreign), tree
            assert sum(p['Actual Rows']*p['Actual Loops'] for p in foreign) > 9000, tree
