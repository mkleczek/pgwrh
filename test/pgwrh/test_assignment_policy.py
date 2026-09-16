from __future__ import annotations

from .pgwrh_testkit import MasterHandle, PgwrhCluster, ReplicaSpec


def test_configuration_clone_preserves_inherited_sharding_keys(postgres_node_factory):
    master = MasterHandle(postgres_node_factory('master'))
    master.execute("""
        CREATE ROLE test_replica;
        CREATE SCHEMA data AUTHORIZATION test_replica;
        CREATE TABLE data.root (id int) PARTITION BY HASH (id);
        CREATE TABLE data.leaf PARTITION OF data.root
            FOR VALUES WITH (MODULUS 1, REMAINDER 0);
        ALTER TABLE data.leaf OWNER TO test_replica;
        SELECT pgwrh.create_replica_cluster('g1');
        INSERT INTO pgwrh.sharded_table
            (replication_group_id, sharded_table_schema, sharded_table_name,
             replication_factor, sharding_key_expression)
        VALUES ('g1', 'data', 'root', 50, 'SELECT ''root policy'''),
               ('g1', 'data', 'leaf', 100, 'SELECT ''leaf policy''');
    """)
    cluster = PgwrhCluster(master, postgres_node_factory)
    cluster.add_replicas([ReplicaSpec('replica')])
    cluster.deploy(timeout=60)

    # An explicit leaf override creates the next configuration and clones all
    # other policies. Neither the inherited key nor the override may be lost.
    master.execute("""INSERT INTO pgwrh.sharded_table
        (replication_group_id, sharded_table_schema, sharded_table_name,
         replication_factor, sharding_key_expression)
        VALUES ('g1', 'data', 'leaf', 100, 'SELECT ''new leaf policy''')""")
    assert master.execute('''SELECT sharded_table_name, replication_factor, sharding_key_expression
        FROM pgwrh.sharded_table JOIN pgwrh.replication_group USING (replication_group_id)
        WHERE version <> current_version ORDER BY sharded_table_name''') == [
        ('leaf', 100, "SELECT 'new leaf policy'"),
        ('root', 50, "SELECT 'root policy'"),
    ]
