from contextlib import contextmanager, ExitStack

import pytest

from .pgwrh_testkit import MasterHandle, PgwrhCluster, ReplicaSpec, wait_until


@contextmanager
def pause_sync(replicas):
    with ExitStack() as stack:
        connections = [stack.enter_context(r.node.connect()) for r in replicas]
        for conn in connections:
            conn.execute('SELECT pg_advisory_lock(2895359559)')
        try:
            yield connections
        finally:
            for conn in connections:
                conn.execute('SELECT pg_advisory_unlock(2895359559)')


def clone_config(master):
    master.execute('''INSERT INTO pgwrh.replication_group_config_clone
        SELECT replication_group_id, current_version, pgwrh.next_version(current_version)
        FROM pgwrh.replication_group WHERE replication_group_id = 'g1'
        ON CONFLICT DO NOTHING''')


@pytest.fixture
def retirement_cluster(postgres_node_factory):
    master = MasterHandle(postgres_node_factory('master'))
    master.execute('''
        CREATE ROLE test_replica;
        CREATE SCHEMA data AUTHORIZATION test_replica;
        CREATE TABLE data.root (id int, payload text) PARTITION BY RANGE (id);
        CREATE TABLE data.p0 PARTITION OF data.root (PRIMARY KEY (id))
            FOR VALUES FROM (0) TO (100);
        CREATE TABLE data.old PARTITION OF data.root
            FOR VALUES FROM (100) TO (200) PARTITION BY RANGE (id);
        CREATE TABLE data.p1 PARTITION OF data.old (PRIMARY KEY (id))
            FOR VALUES FROM (100) TO (200);
        ALTER TABLE data.p0 OWNER TO test_replica;
        ALTER TABLE data.p1 OWNER TO test_replica;
        INSERT INTO data.root SELECT n, 'row ' || n FROM generate_series(0, 199) n;
        SELECT pgwrh.create_replica_cluster('g1');
        INSERT INTO pgwrh.sharded_table
            (replication_group_id, sharded_table_schema, sharded_table_name, replication_factor)
        VALUES ('g1', 'data', 'root', 0);
    ''')
    cluster = PgwrhCluster(master, postgres_node_factory)
    cluster.add_replicas([ReplicaSpec('source'), ReplicaSpec('reader')])
    master.execute("DELETE FROM pgwrh.shard_host_weight WHERE host_id = 'reader'")
    cluster.deploy(timeout=60)
    return cluster


def settle(cluster):
    for replica in cluster.replicas:
        wait_until(lambda: replica.query_scalar('SELECT count(*) FROM pgwrh.sync') == 0,
                   timeout=60, message=f'{replica.name} did not finish reconciliation')


@pytest.mark.parametrize('operation', ['DROP', 'DETACH'])
@pytest.mark.parametrize('subtree', [False, True])
def test_partition_removal_waits_for_committed_rollout(retirement_cluster, operation, subtree):
    cluster = retirement_cluster
    master = cluster.master
    source, reader = cluster.replicas
    original = source.execute('SELECT * FROM data.root ORDER BY id')
    table, parent = ('old', 'root') if subtree else ('p1', 'old')
    with pause_sync(cluster.replicas):
        if operation == 'DROP':
            master.execute(f'DROP TABLE data.{table}')
        else:
            master.execute(f'ALTER TABLE data.{parent} DETACH PARTITION data.{table}')
        # The saved membership is still current, including the old logical tree.
        assert source.query_scalar("""SELECT count(*) FROM pgwrh.fdw_shard_structure
            WHERE schema_name = 'data' AND table_name = 'p1'""") == 1
    settle(cluster)
    for replica in cluster.replicas:
        assert replica.execute('SELECT * FROM data.root ORDER BY id') == original

    clone_config(master)
    master.start_rollout()
    master.wait_for_rollout_ready(expected_replicas=2, timeout=60)
    for replica in cluster.replicas:
        assert replica.execute('SELECT * FROM data.root ORDER BY id') == original
    master.commit_rollout()
    settle(cluster)
    cluster.assert_query_results_match('SELECT * FROM data.root ORDER BY id')
    assert source.query_scalar("""SELECT count(*) FROM pg_subscription_rel
        WHERE srrelid = 'data.p1'::regclass""") == 0
    assert source.query_scalar("""SELECT count(*) FROM pg_inherits
        WHERE inhrelid = 'data.p1'::regclass""") == 0
    assert source.query_scalar('SELECT count(*) FROM data.p1') == 0
    # A retired leaf must not block later publication changes on this subscription.
    with pause_sync(cluster.replicas):
        master.execute('ALTER TABLE data.root DETACH PARTITION data.p0')
    clone_config(master)
    cluster.deploy(timeout=60)
    settle(cluster)
    assert source.query_scalar("""SELECT count(*) FROM pg_subscription_rel
        JOIN pg_class c ON c.oid = srrelid JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'data' AND c.relname IN ('p0', 'p1')""") == 0
    cluster.assert_query_results_match('SELECT * FROM data.root ORDER BY id')


@pytest.mark.parametrize('unlock', [True, False])
def test_rollback_new_partition_then_rollout_copies_fresh_data(retirement_cluster, unlock):
    cluster = retirement_cluster
    master = cluster.master
    source, reader = cluster.replicas
    with pause_sync(cluster.replicas):
        master.execute('''CREATE TABLE data.p2 PARTITION OF data.root (PRIMARY KEY (id))
            FOR VALUES FROM (200) TO (300);
            ALTER TABLE data.p2 OWNER TO test_replica;
            INSERT INTO data.root SELECT n, 'before rollback' FROM generate_series(200, 249) n''')
    clone_config(master)
    master.start_rollout()
    master.wait_for_rollout_ready(expected_replicas=2, timeout=60)
    with pause_sync([reader]):
        master.rollback_rollout(unlock=unlock)
        # A delayed reader may still query this partition: keep its publication
        # and structure until the rollback acknowledgement releases the snapshot.
        assert master.query_scalar('''SELECT count(*) FROM pgwrh.replication_group_config_lock
            WHERE rollback_unlock IS NOT NULL''') == 1
        assert master.query_scalar("""SELECT count(*) FROM pg_publication
            WHERE pubname = pgwrh.pubname('data', 'p2')""") == 1
        assert source.query_scalar("""SELECT count(*) FROM pgwrh.fdw_shard_structure
            WHERE table_name = 'p2'""") == 1
        master.execute("UPDATE data.p2 SET payload = 'during rollback' WHERE id = 200")
        wait_until(lambda: source.query_scalar('SELECT payload FROM data.p2 WHERE id = 200')
                   == 'during rollback', timeout=30, message='retained copy stopped replicating')
    wait_until(lambda: master.query_scalar('''SELECT count(*) FROM pgwrh.replication_group_config_lock
        WHERE rollback_unlock IS NOT NULL''') == 0, timeout=60, message='rollback did not finish')
    settle(cluster)
    wait_until(lambda: master.query_scalar("""SELECT count(*) FROM pg_publication
        WHERE pubname = pgwrh.pubname('data', 'p2')""") == 0,
        timeout=30, message='released publication was not retired')
    assert source.query_scalar('SELECT count(*) FROM data.root') == 200
    assert source.query_scalar("""SELECT count(*) FROM pg_subscription_rel
        WHERE srrelid = 'data.p2'::regclass""") == 0

    master.execute('''INSERT INTO data.root SELECT n, 'after rollback' FROM generate_series(250, 299) n;
        UPDATE data.p2 SET payload = 'changed while unpublished' WHERE id = 200''')
    # Keep the old publication spelling without any actual table subscription,
    # as can happen after a publication disappears and a refresh removes its rel.
    pubname = master.query_scalar("SELECT pgwrh.pubname('data', 'p2')")
    with pause_sync(cluster.replicas):
        source.execute(f'ALTER SUBSCRIPTION pgwrh_replica_subscription ADD PUBLICATION "{pubname}" WITH (refresh = false)')
        clone_config(master)
        master.start_rollout()
    master.wait_for_rollout_ready(expected_replicas=2, timeout=60)
    master.commit_rollout()
    settle(cluster)
    cluster.assert_query_results_match('SELECT * FROM data.root ORDER BY id')
    master.execute("UPDATE data.p2 SET payload = 'after rerollout' WHERE id = 200")
    wait_until(lambda: all(r.query_scalar('SELECT payload FROM data.root WHERE id = 200')
                           == 'after rerollout' for r in cluster.replicas),
               timeout=30, message='resubscribed partition did not receive later writes')


def test_rerollout_before_subscription_cleanup_keeps_receiving_writes(retirement_cluster):
    cluster = retirement_cluster
    master = cluster.master
    source, _ = cluster.replicas
    with pause_sync(cluster.replicas):
        master.execute('''CREATE TABLE data.p2 PARTITION OF data.root (PRIMARY KEY (id))
            FOR VALUES FROM (200) TO (300);
            ALTER TABLE data.p2 OWNER TO test_replica;
            INSERT INTO data.root VALUES (200, 'initial')''')
    clone_config(master)
    master.start_rollout()
    master.wait_for_rollout_ready(expected_replicas=2, timeout=60)
    with pause_sync(cluster.replicas) as connections:
        master.rollback_rollout()
        # Acknowledge restored current routes while holding the sync locks, so
        # the next rollout starts before either daemon can remove the old copy.
        for conn in connections:
            conn.execute('SELECT pgwrh.report_state()')
            conn.commit()
        assert master.query_scalar('''SELECT count(*) FROM pgwrh.replication_group_config_lock
            WHERE rollback_unlock IS NOT NULL''') == 0
        assert master.query_scalar("""SELECT count(*) FROM pg_publication
            WHERE pubname = pgwrh.pubname('data', 'p2')""") == 1
        master.execute("UPDATE data.p2 SET payload = 'between rollouts' WHERE id = 200")
        wait_until(lambda: source.query_scalar('SELECT payload FROM data.p2 WHERE id = 200')
                   == 'between rollouts', timeout=30, message='reusable copy missed intervening writes')
        master.start_rollout()
    master.wait_for_rollout_ready(expected_replicas=2, timeout=60)
    master.commit_rollout()
    settle(cluster)
    cluster.assert_query_results_match('SELECT * FROM data.root ORDER BY id')
