"""Functional lifecycle tests using real replicas and ordinary SQL traffic."""
import pytest

from .pgwrh_testkit import ReplicaSpec, wait_until
from .test_credential_protocol import (
    credentials_for, generation_states, rotate, wait_for_local_credentials, wait_rotation,
)
from .test_local_first_handoff import handoff_cluster, move_to_destination


@pytest.mark.parametrize('finish', ['commit', 'rollback'])
@pytest.mark.parametrize('handoff_cluster', [dict(source='a', destination='b', reader='c')], indirect=True)
def test_topology_can_finish_while_credentials_are_preparing(handoff_cluster, finish):
    cluster = handoff_cluster
    source, destination, reader = cluster.replicas
    original = credentials_for(cluster)
    original_generation, = generation_states(cluster)
    with destination.node.connect() as delayed:
        delayed.execute('SELECT pg_advisory_lock(2895359559)')
        pending = rotate(cluster)
        move_to_destination(cluster)
        cluster.master.wait_for_rollout_ready(expected_replicas=3, timeout=60)
        if finish == 'commit':
            cluster.master.commit_rollout()
        else:
            cluster.master.rollback_rollout()
        assert generation_states(cluster) == {original_generation: 'active', pending: 'preparing'}
        assert credentials_for(cluster) == original
        cluster.assert_query_results_match('SELECT * FROM data.root ORDER BY id')
        delayed.execute('SELECT pg_advisory_unlock(2895359559)')
    wait_rotation(cluster, pending)
    wait_until(lambda: cluster.master.query_scalar('''SELECT count(*) FROM pgwrh.replication_group_config_lock
        WHERE rollback_unlock IS NOT NULL''') == 0, timeout=60, message='topology cleanup did not finish')
    cluster.assert_query_results_match('SELECT * FROM data.root ORDER BY id')
    # A subsequent topology rollback must keep the newly rotated credentials.
    current_credentials = credentials_for(cluster)
    move_to_destination(cluster)
    cluster.master.wait_for_rollout_ready(expected_replicas=3, timeout=60)
    cluster.master.rollback_rollout()
    wait_until(lambda: cluster.master.query_scalar('''SELECT count(*) FROM pgwrh.replication_group_config_lock
        WHERE rollback_unlock IS NOT NULL''') == 0, timeout=60, message='second rollback did not finish')
    assert credentials_for(cluster) == current_credentials
    assert generation_states(cluster) == {pending: 'active'}
    cluster.assert_query_results_match('SELECT * FROM data.root ORDER BY id')


def test_rotation_waits_for_existing_reader_transaction(handoff_cluster):
    cluster = handoff_cluster
    _, _, reader = cluster.replicas
    old_generation, = generation_states(cluster)
    with reader.node.connect() as transaction:
        assert transaction.execute('SELECT count(*) FROM data.root') == [(32,)]
        pending = rotate(cluster)
        wait_until(lambda: generation_states(cluster).get(pending) == 'active', timeout=60,
                   message='new verifiers were not installed')
        # The ordinary read transaction pins its old virtual destinations. Its
        # reconciliation worker cannot acknowledge the switch before it ends.
        assert generation_states(cluster)[old_generation] == 'retiring'
        assert transaction.execute('SELECT count(*) FROM data.root') == [(32,)]
        transaction.commit()
    wait_rotation(cluster, pending)
    assert reader.query_scalar('SELECT count(*) FROM data.root') == 32


def test_new_member_and_replica_restart_during_rotation(handoff_cluster):
    cluster = handoff_cluster
    _, destination, reader = cluster.replicas
    with destination.node.connect() as delayed:
        delayed.execute('SELECT pg_advisory_lock(2895359559)')
        pending = rotate(cluster)
        before = cluster.master.execute('''SELECT * FROM pgwrh.target_credential_verifier
            ORDER BY generation, source_role, host_name, port''')
        joining = cluster.add_replica(ReplicaSpec('joining'))
        assert cluster.master.query_scalar("SELECT count(*) FROM pgwrh.source_credential WHERE source_role = 'joining'") == 2
        # Adding an endpoint fills missing verifiers; existing salts stay stable.
        after = cluster.master.execute('''SELECT * FROM pgwrh.target_credential_verifier
            ORDER BY generation, source_role, host_name, port''')
        assert all(row in after for row in before)
        assert generation_states(cluster)[pending] == 'preparing'
        assert joining.query_scalar('SELECT count(*) FROM pgwrh.fdw_credential_state') == 1
        reader.node.restart()
        reader.execute('SELECT pgwrh.start_sync_daemon(0.1)')
        delayed.execute('SELECT pg_advisory_unlock(2895359559)')
    wait_rotation(cluster, pending)
    wait_for_local_credentials(cluster, credentials_for(cluster))
    # The new member can use the committed topology before its placement rollout.
    wait_until(lambda: joining.query_scalar('SELECT count(*) FROM pgwrh.connected_remote_shard') == 2,
               timeout=60, message='joining member did not configure the committed topology')
    cluster.assert_query_results_match('SELECT * FROM data.root ORDER BY id')
