from __future__ import annotations

from contextlib import ExitStack

import pytest

from .pgwrh_testkit import MasterHandle, PgwrhCluster, ReplicaSpec, quote_literal, wait_until


@pytest.fixture
def partitioned_master(postgres_node_factory):
    master = MasterHandle(postgres_node_factory('master'))
    master.execute('''
        CREATE ROLE test_replica;
        CREATE SCHEMA data AUTHORIZATION test_replica;
        CREATE TABLE data.root (id int, year int) PARTITION BY RANGE (year);
        CREATE TABLE data.archival PARTITION OF data.root
            FOR VALUES FROM (MINVALUE) TO (2025) PARTITION BY RANGE (year);
        CREATE TABLE data.fresh PARTITION OF data.root
            FOR VALUES FROM (2025) TO (MAXVALUE) PARTITION BY RANGE (year);
        CREATE TABLE data.y2024 PARTITION OF data.archival (PRIMARY KEY (id))
            FOR VALUES FROM (2024) TO (2025);
        CREATE TABLE data.y2025 PARTITION OF data.fresh (PRIMARY KEY (id))
            FOR VALUES FROM (2025) TO (2026);
        CREATE TABLE data.y2026 PARTITION OF data.fresh (PRIMARY KEY (id))
            FOR VALUES FROM (2026) TO (2027);
        ALTER TABLE data.y2024 OWNER TO test_replica;
        ALTER TABLE data.y2025 OWNER TO test_replica;
        ALTER TABLE data.y2026 OWNER TO test_replica;
        INSERT INTO data.root VALUES (1, 2024), (2, 2025), (3, 2026);
        SELECT pgwrh.create_replica_cluster('g1');
        INSERT INTO pgwrh.sharded_table
            (replication_group_id, sharded_table_schema, sharded_table_name, replication_factor)
        VALUES ('g1', 'data', 'root', 100);
    ''')
    return master


def test_reconciles_changed_bounds_without_recreating_local_shards(
    partitioned_master, postgres_node_factory,
):
    master = partitioned_master
    cluster = PgwrhCluster(master, postgres_node_factory)
    cluster.add_replicas([ReplicaSpec('replica')])
    cluster.deploy(timeout=60)
    replica = cluster.replicas[0]
    identity_query = '''SELECT srsubid, srrelid
        FROM pg_subscription_rel
        WHERE srrelid = 'data.y2026'::regclass'''
    identity = replica.execute(identity_query)
    assert len(identity) == 1

    # Both expansion and contraction must update the slot and its local child.
    for upper in (2028, 2027):
        bound = f'FOR VALUES FROM (2026) TO ({upper})'
        with replica.node.connect() as paused:
            paused.execute('SELECT pg_advisory_lock(2895359559)')
            master.execute(f'''ALTER TABLE data.fresh DETACH PARTITION data.y2026;
                ALTER TABLE data.fresh ATTACH PARTITION data.y2026 {bound}''')
            scripts = replica.execute("""SELECT async, transactional, commands
                FROM pgwrh.sync WHERE description LIKE 'Switching query routes%'""")
            assert len(scripts) == 1
            assert scripts[0][:2] == (False, True)
            assert len(scripts[0][2]) == 5  # root lock, two detaches, two attaches
            paused.execute('SELECT pg_advisory_unlock(2895359559)')
        wait_until(lambda: replica.execute('''SELECT bound FROM pgwrh.local_rel
            WHERE rel_id IN (('data', 'y2026')::pgwrh.rel_id,
                             ('data_slot', 'y2026')::pgwrh.rel_id)''') == [(bound,), (bound,)],
            timeout=30, message='slot and local shard bounds did not converge')
        assert replica.execute(identity_query) == identity
        cluster.assert_query_results_match('SELECT * FROM data.root ORDER BY id')
        assert replica.query_scalar("""SELECT count(*) FROM pgwrh.sync
            WHERE description LIKE 'Switching query routes%'""") == 0

    master.execute('INSERT INTO data.root VALUES (4, 2026)')
    wait_until(lambda: replica.query_scalar('SELECT count(*) FROM data.root') == 4,
               timeout=30, message='replication did not continue after rebinding')


def move_year_to_archival(master):
    # One controller transaction exposes only the completed logical tree.
    master.execute('''
        ALTER TABLE data.root DETACH PARTITION data.fresh;
        ALTER TABLE data.root DETACH PARTITION data.archival;
        ALTER TABLE data.fresh DETACH PARTITION data.y2025;
        ALTER TABLE data.archival ATTACH PARTITION data.y2025
            FOR VALUES FROM (2025) TO (2026);
        ALTER TABLE data.root ATTACH PARTITION data.archival
            FOR VALUES FROM (MINVALUE) TO (2026);
        ALTER TABLE data.root ATTACH PARTITION data.fresh
            FOR VALUES FROM (2026) TO (MAXVALUE);
    ''')


def test_reparenting_and_ancestor_bounds_switch_atomically(
    partitioned_master, postgres_node_factory,
):
    master = partitioned_master
    cluster = PgwrhCluster(master, postgres_node_factory)
    cluster.add_replicas([ReplicaSpec('replica')])
    cluster.deploy(timeout=60)
    replica = cluster.replicas[0]
    tree_query = '''SELECT relid::oid, parentrelid::oid, pg_get_expr(c.relpartbound, c.oid)
        FROM pg_partition_tree('data.root') JOIN pg_class c ON c.oid = relid
        ORDER BY relid::oid'''
    original_tree = replica.execute(tree_query)

    with replica.node.connect() as paused:
        paused.execute('SELECT pg_advisory_lock(2895359559)')
        move_year_to_archival(master)
        scripts = replica.execute("""SELECT commands FROM pgwrh.sync
            WHERE description LIKE 'Switching query routes%'""")
        assert len(scripts) == 1
        # The whole root can be switched and rolled back without losing its data.
        with replica.node.connect() as conn:
            conn.execute(';'.join(scripts[0][0]))
            assert conn.execute('SELECT year FROM data.archival ORDER BY year') == [(2024,), (2025,)]
            assert conn.execute('SELECT year FROM data.fresh ORDER BY year') == [(2026,)]
            conn.rollback()
        assert replica.execute(tree_query) == original_tree
        paused.execute('SELECT pg_advisory_unlock(2895359559)')

    wait_until(lambda: replica.execute('SELECT year FROM data.archival ORDER BY year') == [(2024,), (2025,)],
               timeout=30, message='year did not move to archival')
    for table in ('root', 'fresh', 'archival'):
        cluster.assert_query_results_match(f'SELECT * FROM data.{table} ORDER BY id')
    for year in (2024, 2025, 2026):
        cluster.assert_query_results_match(f'SELECT * FROM data.root WHERE year = {year} ORDER BY id')
    assert [row[0] for row in replica.execute(tree_query)] == [row[0] for row in original_tree]
    assert replica.query_scalar("""SELECT count(*) FROM pgwrh.sync
        WHERE description LIKE 'Switching query routes%'""") == 0


def test_moved_year_inherits_archival_placement_and_rebuilds_remote_aggregates(
    partitioned_master, postgres_node_factory,
):
    master = partitioned_master
    keys = {}
    for owner, other in [('replica1', 'replica2'), ('replica2', 'replica1')]:
        keys[owner] = master.query_scalar(f"""SELECT n::text FROM generate_series(1, 100) n
            WHERE pgwrh.score(100, n::text, '{owner}') > pgwrh.score(100, n::text, '{other}')
            LIMIT 1""")
    # Archival years deliberately go to different hosts; fresh years go to both.
    archival_key = (f"SELECT CASE WHEN $2 = 'y2024' THEN '{keys['replica1']}' "
                    f"ELSE '{keys['replica2']}' END")
    master.execute(f"""INSERT INTO pgwrh.sharded_table
        (replication_group_id, sharded_table_schema, sharded_table_name,
         replication_factor, sharding_key_expression)
        VALUES ('g1', 'data', 'archival', 0, {quote_literal(archival_key)}),
               ('g1', 'data', 'fresh', 100, 'SELECT ''fresh''')""")
    cluster = PgwrhCluster(master, postgres_node_factory)
    cluster.add_replicas([ReplicaSpec('replica1'), ReplicaSpec('replica2'), ReplicaSpec('reader')])
    master.execute('''DELETE FROM pgwrh.shard_host_weight
        WHERE host_id = 'reader' AND version <>
            (SELECT current_version FROM pgwrh.replication_group WHERE replication_group_id = 'g1')''')
    cluster.deploy(timeout=60)
    reader = cluster.replicas[-1]
    remote_query = '''SELECT (n.node_rel_id).table_name
        FROM pgwrh.remote_node n JOIN pgwrh.reachable_shard r USING (reg_class) ORDER BY 1'''
    wait_until(lambda: reader.execute(remote_query) == [('archival',), ('fresh',)],
               timeout=60, message='initial tiers did not aggregate')
    identity_query = "SELECT ftrelid, ftserver FROM pg_foreign_table WHERE ftrelid = 'data_remote.fresh'::regclass"
    fresh_identity = reader.execute(identity_query)
    placement_query = '''SELECT table_name, array_agg(host_id ORDER BY host_id)
        FROM pgwrh.shard_assigned_host JOIN pgwrh.replication_group USING (replication_group_id)
        WHERE version = target_version GROUP BY table_name ORDER BY table_name'''
    assert master.execute(placement_query) == [
        ('y2024', ['replica1']), ('y2025', ['replica1', 'replica2']), ('y2026', ['replica1', 'replica2']),
    ]

    with ExitStack() as stack:
        # Keep metadata reads out of the controller DDL, and resume all daemons
        # together with the changed tree and placement available to reconcile.
        paused = [stack.enter_context(replica.node.connect()) for replica in cluster.replicas]
        for conn in paused:
            conn.execute('SELECT pg_advisory_lock(2895359559)')
        move_year_to_archival(master)
        # Structural DDL is observed directly. Placement is resnapshotted through
        # the existing configuration clone/start/commit lifecycle.
        master.execute('''INSERT INTO pgwrh.replication_group_config_clone
            SELECT replication_group_id, current_version, pgwrh.next_version(current_version)
            FROM pgwrh.replication_group WHERE replication_group_id = 'g1' ''')
        master.start_rollout()
        assert master.execute(placement_query) == [
            ('y2024', ['replica1']), ('y2025', ['replica2']), ('y2026', ['replica1', 'replica2']),
        ]
        assert master.execute('''SELECT sharded_table_name FROM pgwrh.shard
            JOIN pgwrh.replication_group USING (replication_group_id)
            WHERE version = target_version AND table_name = 'y2025' ''') == [('archival',)]
        for conn in paused:
            conn.execute('SELECT pg_advisory_unlock(2895359559)')
    master.wait_for_rollout_ready(expected_replicas=3, timeout=60)
    # The previous fresh copy remains available until the normal rollout commit.
    for replica in cluster.replicas[:2]:
        assert replica.query_scalar("SELECT count(*) FROM pgwrh.connected_local_shard WHERE (rel_id).table_name = 'y2025'") == 1
    master.commit_rollout()

    for replica in cluster.replicas:
        wait_until(lambda: replica.query_scalar('SELECT count(*) FROM pgwrh.sync') == 0,
                   timeout=60, message=f'{replica.name} did not finish the partition move')
    assert reader.execute(remote_query) == [('fresh',), ('y2024',), ('y2025',)]
    assert reader.execute(identity_query) == fresh_identity
    for replica, expected in zip(cluster.replicas, [('y2024', 'y2026'), ('y2025', 'y2026'), ()]):
        assert replica.execute('SELECT (rel_id).table_name FROM pgwrh.connected_local_shard ORDER BY 1') == [
            (name,) for name in expected
        ]
    assert reader.execute('''SELECT bound FROM pgwrh.local_rel
        WHERE rel_id IN (('data_remote', 'fresh')::pgwrh.rel_id,
                         ('data_slot', 'fresh')::pgwrh.rel_id)''') == [
        ('FOR VALUES FROM (2026) TO (MAXVALUE)',), ('FOR VALUES FROM (2026) TO (MAXVALUE)',),
    ]
    cluster.assert_query_results_match('SELECT * FROM data.root ORDER BY id')
    # Aggregated readers query through the root: their original intermediate
    # tables are detached while the foreign aggregate occupies the slot.
    for table in ('fresh', 'archival'):
        query = f'SELECT * FROM data.{table} ORDER BY id'
        for replica in cluster.replicas[:2]:
            assert replica.execute(query) == master.execute(query)
    for year in (2024, 2025, 2026):
        cluster.assert_query_results_match(f'SELECT * FROM data.root WHERE year = {year} ORDER BY id')
    master.execute('INSERT INTO data.root VALUES (4, 2025)')
    wait_until(lambda: all(replica.query_scalar('SELECT count(*) FROM data.root WHERE year = 2025') == 2
                           for replica in cluster.replicas),
               timeout=30, message='moved year did not continue replicating and routing writes')
