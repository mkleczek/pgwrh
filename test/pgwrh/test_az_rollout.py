from __future__ import annotations

from .pgwrh_testkit import MasterHandle, PgwrhCluster, ReplicaSpec, wait_until


def test_affinity_changes_roll_out_and_roll_back_with_live_replicas(postgres_node_factory):
    master = MasterHandle(postgres_node_factory('az_master'))
    master.execute("""
        CREATE ROLE test_replica;
        CREATE SCHEMA data AUTHORIZATION test_replica;
        CREATE TABLE data.root (id int) PARTITION BY HASH (id);
        DO $$ BEGIN
            FOR i IN 0..3 LOOP
                EXECUTE format('CREATE TABLE data.leaf%s PARTITION OF data.root (PRIMARY KEY (id))
                    FOR VALUES WITH (MODULUS 4, REMAINDER %s)', i, i);
                EXECUTE format('ALTER TABLE data.leaf%s OWNER TO test_replica', i);
            END LOOP;
        END $$;
        INSERT INTO data.root SELECT generate_series(1, 32);
        SELECT pgwrh.create_replica_cluster('g1');
        INSERT INTO pgwrh.sharded_table
            (replication_group_id, sharded_table_schema, sharded_table_name, replication_factor)
        VALUES ('g1', 'data', 'root', 0);
        UPDATE pgwrh.replication_group_config SET min_replica_count = 3,
            min_replica_count_after_az_failure = 2 WHERE version = 'FLOP';
    """)
    cluster = PgwrhCluster(master, postgres_node_factory)
    cluster.add_replicas([ReplicaSpec('a1', 'a'), ReplicaSpec('a2', 'a'),
                          ReplicaSpec('b1', 'b'), ReplicaSpec('c1', 'c')])
    cluster.deploy(timeout=60)
    current_assignments = """SELECT table_name, availability_zone, host_id
        FROM pgwrh.shard_assigned_host JOIN pgwrh.replication_group USING (replication_group_id)
        WHERE version = current_version ORDER BY 1, 2, 3"""
    original = master.execute(current_assignments)
    assert len(original) == 12
    assert all(sum(t == table and z == zone for t, z, _ in original) == 1
               for table in {t for t, _, _ in original} for zone in 'abc')

    # The first affinity row creates the pending version, including inherited
    # group constraints. Revise those explicitly to allow two copies in A.
    master.execute("""
        INSERT INTO pgwrh.sharded_table_az_affinity
            (replication_group_id, sharded_table_schema, sharded_table_name, availability_zone, weight)
        VALUES ('g1', 'data', 'root', 'a', 4);
        UPDATE pgwrh.replication_group_config SET min_replica_count_per_availability_zone = 0,
            min_replica_count_after_az_failure = 1
        WHERE version = pgwrh.next_pending_version('g1');
    """)
    assert master.execute(current_assignments) == original
    expected = master.execute("""SELECT table_name, availability_zone, host_id
        FROM pgwrh.preview_shard_placement('g1', pgwrh.next_pending_version('g1'))
        ORDER BY 1, 2, 3""")
    assert all(sum(t == table and z == 'a' for t, z, _ in expected) == 2
               for table in {t for t, _, _ in expected})
    cluster.deploy(commit=False, timeout=60)
    assert master.execute(current_assignments) == original
    cluster.assert_query_results_match('SELECT * FROM data.root ORDER BY id')
    master.rollback_rollout()
    wait_until(lambda: master.query_scalar("""SELECT count(*) FROM pgwrh.replication_group_config_lock
        WHERE rollback_unlock IS NOT NULL""") == 0, timeout=60,
        message='affinity rollback did not finish')
    assert master.execute(current_assignments) == original
    cluster.assert_query_results_match('SELECT * FROM data.root ORDER BY id')

    # Retrying the unchanged pending policy produces the same snapshot.
    cluster.deploy(timeout=60)
    assert master.execute(current_assignments) == expected
    cluster.assert_query_results_match('SELECT * FROM data.root ORDER BY id')
    master.execute("""INSERT INTO pgwrh.sharded_table_az_affinity
        (replication_group_id, sharded_table_schema, sharded_table_name, availability_zone, weight)
        VALUES ('g1', 'data', 'root', 'a', 1)""")
    cluster.deploy(timeout=60)
    assert master.execute(current_assignments) == original
    cluster.assert_query_results_match('SELECT * FROM data.root ORDER BY id')
