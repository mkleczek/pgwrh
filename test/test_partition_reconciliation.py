from __future__ import annotations

import pytest

from .pgwrh_testkit import MasterHandle, PgwrhCluster, ReplicaSpec, wait_until


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
