from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest

from .pgwrh_testkit import MasterHandle, PgwrhCluster, ReplicaSpec, wait_until


@pytest.fixture
def handoff_cluster(postgres_node_factory):
    master = MasterHandle(postgres_node_factory('master'))
    master.execute('''
        CREATE ROLE test_replica;
        CREATE SCHEMA data AUTHORIZATION test_replica;
        CREATE TABLE data.root (id int, value text) PARTITION BY HASH (id);
        CREATE TABLE data.p0 PARTITION OF data.root (PRIMARY KEY (id)) FOR VALUES WITH (MODULUS 2, REMAINDER 0);
        CREATE TABLE data.p1 PARTITION OF data.root (PRIMARY KEY (id)) FOR VALUES WITH (MODULUS 2, REMAINDER 1);
        ALTER TABLE data.p0 OWNER TO test_replica;
        ALTER TABLE data.p1 OWNER TO test_replica;
        INSERT INTO data.root SELECT n, 'row ' || n FROM generate_series(1, 32) n;
        SELECT pgwrh.create_replica_cluster('g1');
        INSERT INTO pgwrh.sharded_table
            (replication_group_id, sharded_table_schema, sharded_table_name, replication_factor)
        VALUES ('g1', 'data', 'root', 0);
    ''')
    cluster = PgwrhCluster(master, postgres_node_factory)
    cluster.add_replicas([ReplicaSpec('source'), ReplicaSpec('destination'), ReplicaSpec('reader')])
    master.execute("DELETE FROM pgwrh.shard_host_weight WHERE host_id <> 'source'")
    cluster.deploy(timeout=60)
    return cluster


def move_to_destination(cluster):
    cluster.master.execute('''
        SELECT pgwrh.set_replica_weight('g1', 'default', 'destination', 100);
        DELETE FROM pgwrh.shard_host_weight
        WHERE host_id <> 'destination' AND version <>
            (SELECT current_version FROM pgwrh.replication_group WHERE replication_group_id = 'g1');
    ''')
    cluster.master.start_rollout()


def wait_prepared(cluster):
    source, destination, _ = cluster.replicas
    wait_until(lambda: destination.query_scalar('SELECT count(*) FROM pgwrh.connected_local_shard') == 2,
               timeout=60, message='destination copies were not attached')
    wait_until(lambda: source.query_scalar('''SELECT count(*) FROM pgwrh.prepared_remote_shard
        WHERE shard_server_user = (SELECT username FROM pgwrh.fdw_credentials
            WHERE username IN (SELECT shard_server_user FROM pgwrh.fdw_shard_assignment) LIMIT 1)''') == 2,
               timeout=60, message='source did not prepare its replacements')


def test_prepared_replacements_allow_commit_only_for_local_readers(handoff_cluster):
    cluster = handoff_cluster
    source, destination, reader = cluster.replicas
    with reader.node.connect() as paused:
        paused.execute('SELECT pg_advisory_lock(2895359559)')
        move_to_destination(cluster)
        wait_prepared(cluster)
        assert source.query_scalar('SELECT count(*) FROM pgwrh.connected_local_shard') == 2
        assert source.query_scalar('SELECT count(*) FROM pgwrh.connected_remote_shard') == 0
        assert source.query_scalar('''SELECT count(*) FROM pgwrh.shard_assignment_r a
            JOIN pgwrh.rel r ON r.rel_id = a.remote_rel_id
            JOIN pg_inherits i ON i.inhrelid = r.reg_class''') == 0
        # Preparing a route must not exempt a reader still connected to the old source.
        cluster.master.execute('''UPDATE pgwrh.replication_group_member
            SET prepared_remote_shards = (SELECT prepared_remote_shards
                FROM pgwrh.replication_group_member WHERE host_id = 'source')
            WHERE host_id = 'reader' ''')
        with pytest.raises(Exception, match='required remote shards'):
            cluster.master.commit_rollout()
        cluster.master.execute("INSERT INTO data.root VALUES (33, 'written during rollout')")
        wait_until(lambda: source.query_scalar('SELECT count(*) FROM data.root') == 33
                   and destination.query_scalar('SELECT count(*) FROM data.root') == 33,
                   timeout=30, message='retained and target copies stopped receiving writes')
        cluster.assert_query_results_match('SELECT * FROM data.root ORDER BY id')
        paused.execute('SELECT pg_advisory_unlock(2895359559)')
    cluster.master.wait_for_rollout_ready(expected_replicas=3, timeout=60)
    assert source.query_scalar('SELECT count(*) FROM pgwrh.connected_local_shard') == 2
    assert cluster.master.query_scalar('''SELECT count(*) FROM pgwrh.missing_connected_remote_shard
        WHERE host_id = 'source' AND version = (SELECT target_version FROM pgwrh.replication_group
            WHERE replication_group_id = 'g1')''') == 2
    with source.node.connect() as paused:
        paused.execute('SELECT pg_advisory_lock(2895359559)')
        for field in ('shard_server_name', 'shard_server_user'):
            with cluster.master.node.connect() as report:
                report.execute(f"""UPDATE pgwrh.replication_group_member
                    SET prepared_remote_shards = (SELECT jsonb_agg(p || jsonb_build_object('{field}', 'stale'))
                        FROM jsonb_array_elements(prepared_remote_shards::jsonb) p)
                    WHERE host_id = 'source' """)
                with pytest.raises(Exception, match='required remote shards'):
                    report.execute("SELECT pgwrh.commit_rollout('g1')")
                report.rollback()
        paused.execute('SELECT pg_advisory_unlock(2895359559)')
    # An unrelated rollout at the same target version must not block this group.
    cluster.master.execute("""
        INSERT INTO pgwrh.replication_group (replication_group_id, current_version, target_version)
        VALUES ('other', 'FLOP', 'FLOP');
        CREATE ROLE other_owner;
        CREATE ROLE other_reader;
        SELECT pgwrh.add_replica('other', 'other_owner', 'localhost', 1);
        SELECT pgwrh.add_replica('other', 'other_reader', 'localhost', 2);
        INSERT INTO pgwrh.sharded_table
            (replication_group_id, sharded_table_schema, sharded_table_name, replication_factor)
        VALUES ('other', 'data', 'root', 0);
        SELECT pgwrh.start_rollout('other');
    """)
    assert cluster.master.query_scalar("SELECT count(*) FROM pgwrh.missing_ready_remote_shard WHERE replication_group_id = 'other'") > 0
    cluster.master.commit_rollout()
    wait_until(lambda: source.query_scalar('SELECT count(*) FROM pgwrh.connected_local_shard') == 0,
               timeout=60, message='source did not replace its local attachments')
    cluster.assert_query_results_match('SELECT * FROM data.root ORDER BY id')


def test_failed_handoff_preserves_attachment_subscription_and_rows(handoff_cluster):
    cluster = handoff_cluster
    source, _, _ = cluster.replicas
    move_to_destination(cluster)
    cluster.master.wait_for_rollout_ready(expected_replicas=3, timeout=60)
    source.execute('''
        CREATE FUNCTION public.reject_detach() RETURNS event_trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF TG_TAG = 'ALTER TABLE' AND NOT EXISTS (
                SELECT 1 FROM pg_inherits WHERE inhparent = 'data_slot.p0'::regclass
            ) THEN RAISE EXCEPTION 'injected handoff failure'; END IF;
        END $$;
        CREATE EVENT TRIGGER reject_detach ON ddl_command_end EXECUTE FUNCTION public.reject_detach();
    ''')
    cluster.master.commit_rollout()
    # Execute a complete pass: the injected failure happens after DETACH, and
    # subsequent cleanup in that pass must not remove the still-attached copy.
    with source.node.connect() as paused:
        paused.execute('SELECT pg_advisory_lock(2895359559)')
        assert paused.execute('SELECT pgwrh.sync_step()') == [(True,)]
        paused.execute('SELECT pg_advisory_unlock(2895359559)')
    assert source.query_scalar('SELECT count(*) FROM pgwrh.connected_local_shard') == 2
    assert source.query_scalar("SELECT count(*) FROM pgwrh.subscribed_local_shard WHERE (rel_id).schema_name = 'data'") == 2
    cluster.assert_query_results_match('SELECT * FROM data.root ORDER BY id')
    source.execute('DROP EVENT TRIGGER reject_detach; DROP FUNCTION public.reject_detach()')
    stop = Event()
    def read_during_handoff():
        while not stop.is_set():
            assert source.query_scalar('SELECT count(*) FROM data.root') == 32
    with ThreadPoolExecutor(max_workers=1) as pool:
        reads = pool.submit(read_during_handoff)
        try:
            wait_until(lambda: source.query_scalar("SELECT count(*) FROM pgwrh.subscribed_local_shard WHERE (rel_id).schema_name = 'data'") == 0,
                       timeout=60, message='detached copies were not cleaned up')
        finally:
            stop.set()
        reads.result()
    cluster.assert_query_results_match('SELECT * FROM data.root ORDER BY id')


@pytest.mark.parametrize('unlock', [True, False])
def test_rollback_retains_new_local_copies_until_readers_return(handoff_cluster, unlock):
    cluster = handoff_cluster
    source, destination, reader = cluster.replicas
    move_to_destination(cluster)
    cluster.master.wait_for_rollout_ready(expected_replicas=3, timeout=60)
    with reader.node.connect() as paused:
        paused.execute('SELECT pg_advisory_lock(2895359559)')
        cluster.master.rollback_rollout(unlock=unlock)
        wait_until(lambda: destination.query_scalar('SELECT count(*) FROM pgwrh.prepared_remote_shard') == 2,
                   timeout=60, message='rollback replacements were not prepared')
        assert destination.query_scalar('SELECT count(*) FROM pgwrh.connected_local_shard') == 2
        assert cluster.master.query_scalar('SELECT count(*) FROM pgwrh.replication_group_config_lock WHERE rollback_unlock IS NOT NULL') == 1
        cluster.assert_query_results_match('SELECT * FROM data.root ORDER BY id')
        with pytest.raises(Exception, match='Rollback has not finished'):
            cluster.master.start_rollout()
        paused.execute('SELECT pg_advisory_unlock(2895359559)')
    wait_until(lambda: cluster.master.query_scalar('SELECT count(*) FROM pgwrh.replication_group_config_lock WHERE rollback_unlock IS NOT NULL') == 0,
               timeout=60, message='rollback did not finish')
    wait_until(lambda: destination.query_scalar("SELECT count(*) FROM pgwrh.subscribed_local_shard WHERE (rel_id).schema_name = 'data'") == 0,
               timeout=60, message='rollback copies were not cleaned up')
    assert source.query_scalar('SELECT count(*) FROM pgwrh.connected_local_shard') == 2
    cluster.assert_query_results_match('SELECT * FROM data.root ORDER BY id')
