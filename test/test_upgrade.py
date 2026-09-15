from __future__ import annotations

from contextlib import ExitStack
from pathlib import Path
import subprocess

from .pgwrh_testkit import MasterHandle, PgwrhCluster, ReplicaSpec, wait_until


def release_file(path):
    return subprocess.check_output(['git', 'show', f'v0.2.1:{path}'], text=True)


def test_upgrade_preserves_existing_replica_data_and_root_identity(postgres_node_factory):
    # Build the released installation script from its immutable tag; avoid storing
    # a second copy of the extension sources just for this regression.
    sources = ['src/common.sql']
    for side in ('master', 'replica'):
        order = subprocess.check_output(['tsort'], input=release_file(f'src/{side}/deps.txt'), text=True)
        sources.extend(f'src/{side}/{name}.sql' for name in order.split())
    extension_dir = Path(__file__).resolve().parents[1] / '.build/testgres-ext/extension'
    (extension_dir / 'pgwrh--0.2.1.sql').write_text('\n'.join(release_file(path) for path in sources))

    def legacy_node(name):
        # Use the released control file too: the old installation did not
        # depend on pgwrh_fdw, so it must not be preinstalled by the fixture.
        control = extension_dir / 'pgwrh.control'
        current_control = control.read_text()
        try:
            control.write_text(release_file('pgwrh.control'))
            node = postgres_node_factory(name)
        finally:
            control.write_text(current_control)
        assert node.execute("SELECT count(*) FROM pg_extension WHERE extname = 'pgwrh_fdw'") == [(0,)]
        return node

    master = MasterHandle(legacy_node('master'))
    master.load_seed(Path(__file__).with_name('master_non_partitioned_workaround.sql'))
    cluster = PgwrhCluster(master, legacy_node)
    cluster.add_replicas([ReplicaSpec('replica1'), ReplicaSpec('replica2')])
    master.start_rollout()
    wait_until(lambda: master.query_scalar("""SELECT
        (SELECT count(*) FROM pgwrh.missing_connected_local_shard WHERE version =
            (SELECT target_version FROM pgwrh.replication_group WHERE replication_group_id = 'g1')) +
        (SELECT count(*) FROM pgwrh.missing_connected_remote_shard WHERE version =
            (SELECT target_version FROM pgwrh.replication_group WHERE replication_group_id = 'g1'))""") == 0,
        timeout=60, message='released version did not finish rollout')
    master.commit_rollout()
    query = 'SELECT * FROM test.non_partitioned_data ORDER BY id'
    cluster.assert_query_results_match(query)
    roots = [replica.query_scalar("SELECT 'test.non_partitioned_data'::regclass::oid") for replica in cluster.replicas]
    with ExitStack() as stack:
        paused = [stack.enter_context(replica.node.connect()) for replica in cluster.replicas]
        for conn in paused:
            conn.execute('SELECT pg_advisory_lock(2895359559)')
        master.execute("CREATE EXTENSION pgwrh_fdw; ALTER EXTENSION pgwrh UPDATE TO '0.2.2'")
        for replica in cluster.replicas:
            replica.execute("CREATE EXTENSION pgwrh_fdw; ALTER EXTENSION pgwrh UPDATE TO '0.2.2'")
        for conn in paused:
            conn.execute('SELECT pg_advisory_unlock(2895359559)')
    for replica, oid in zip(cluster.replicas, roots):
        wait_until(lambda: replica.query_scalar('SELECT count(*) FROM pgwrh.sync') == 0,
                   timeout=60, message='upgraded replica did not converge')
        assert replica.query_scalar("SELECT 'test.non_partitioned_data'::regclass::oid") == oid
        assert replica.query_scalar("SELECT extversion FROM pg_extension WHERE extname = 'pgwrh'") == '0.2.2'
    cluster.assert_query_results_match(query)
    assert sum(replica.query_scalar('SELECT count(*) FROM pgwrh.remote_node_assignment WHERE level = 0')
               for replica in cluster.replicas) == 1
