"""Concurrent index admission, provenance, recovery, and live replica rollout."""
import pytest

from .conftest import DatabaseNode
from .pgwrh_testkit import wait_until
from .test_daemon_supervisor import daemon_pids
from .test_local_first_handoff import handoff_cluster  # noqa: F401


def enqueue(node, table='items', index='items_value', template='(value)'):
    return node.execute(
        f"SELECT pgwrh.enqueue_index_build('public', '{table}', '{index}', '{template}')"
    )[0][0]


def tasks(node):
    return node.execute('SELECT datid, relid, job_id, pid, starting FROM pgwrh.index_build_tasks()')


def wait_idle(node):
    wait_until(lambda: not tasks(node), timeout=15, message='index reservations were not released')


def wait_build(node, index='items_value'):
    try:
        wait_until(lambda: node.execute(f"""SELECT count(*) FROM pgwrh.index_build_status
            WHERE index_name = '{index}' AND phase = 'waiting for writers before build'""") == [(1,)],
            timeout=15, message='concurrent index did not enter its writer wait')
    except AssertionError:
        raise AssertionError(node.execute('SELECT * FROM pgwrh.index_build_status'))
    return node.execute(f"SELECT pid FROM pgwrh.index_build_status WHERE index_name = '{index}'")[0][0]


def valid(node, index='items_value'):
    return node.execute(f"SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass('public.{index}')") == [(True,)]


def seed(node, table='items'):
    node.execute(f'CREATE TABLE public.{table}(id integer PRIMARY KEY, value text)')
    node.execute(f"INSERT INTO public.{table} VALUES (1, 'original')")


def test_reservations_cover_launch_wait_and_databases(postgres_node_factory):
    server = postgres_node_factory('index_capacity', preload=False)
    server.stop()
    server.append_conf('max_worker_processes = 8')
    server.start()
    server.execute('CREATE DATABASE second')
    second = DatabaseNode(server, 'second')
    second.execute('CREATE EXTENSION pgwrh CASCADE')
    nodes = [server, second]
    for node in nodes:
        for i in range(3):
            seed(node, f'items{i}')
    # Keep launch transactions open. Workers have reserved their slots but
    # cannot read job intent yet and have no pg_stat_progress row.
    with server.connect() as first_launch, second.connect() as second_launch:
        for launch in (first_launch, second_launch):
            assert enqueue(launch, 'items0', 'index0')
            assert not enqueue(launch, 'items0', 'another_index')
            assert enqueue(launch, 'items1', 'index1')
        assert len(tasks(server)) == 4
        assert len({row[0] for row in tasks(server)}) == 2
        assert server.execute('SELECT count(*) FROM pg_stat_progress_create_index') == [(0,)]
        assert not enqueue(server, 'items2', 'index2')
        assert not enqueue(second, 'items2', 'index2')
        first_launch.rollback()
        wait_until(lambda: len(tasks(server)) == 2, timeout=10, message='rollback leaked capacity')
        with server.connect() as writer:
            writer.execute("UPDATE items2 SET value = 'held'")
            assert enqueue(server, 'items2', 'index2')
            pid = wait_build(server, 'index2')
            assert len(tasks(server)) == 3
            assert not enqueue(server, 'items2', 'same_table')
            assert server.execute(f"SELECT current_locker_pid IS NOT NULL FROM pgwrh.index_build_status WHERE pid = {pid}") == [(True,)]
            second_launch.commit()
            writer.commit()
    wait_idle(server)
    assert valid(server, 'index2')
    assert valid(second, 'index0') and valid(second, 'index1')
    assert server.execute("SELECT to_regclass('index0'), to_regclass('index1')") == [(None, None)]


@pytest.mark.parametrize('failure', ['cancel', 'terminate', 'restart'])
def test_interrupted_build_is_owned_invalid_and_retryable(postgres_node_factory, failure):
    node = postgres_node_factory('index_failure', preload=False)
    seed(node)
    with node.connect() as writer:
        writer.execute("UPDATE items SET value = 'held'")
        assert enqueue(node)
        pid = wait_build(node)
        assert not valid(node)
        assert node.execute("""SELECT count(*) FROM pgwrh.managed_object
            WHERE object_kind = 'index' AND schema_name = 'public' AND object_name = 'items_value'""") == [(1,)]
        # The first CIC commit already contains both the index and its registry
        # record. This is the formerly unsafe creation/registration interval.
        assert node.execute("""SELECT count(*) FROM pg_depend WHERE objid = 'items_value'::regclass
            AND refclassid = 'pg_extension'::regclass AND deptype = 'n'""") == [(1,)]
        if failure == 'restart':
            node.stop(['-m', 'immediate'])
            node.start()
        else:
            node.execute(f'SELECT pg_{"cancel" if failure == "cancel" else "terminate"}_backend({pid})')
            wait_idle(node)
            writer.rollback()
    wait_idle(node)
    assert not valid(node)
    if failure == 'cancel':
        assert node.execute('SELECT last_sqlstate FROM pgwrh.index_build_job') == [('57014',)]
    wait_until(lambda: enqueue(node), timeout=15, message='failed build was not retried')
    wait_idle(node)
    assert valid(node)
    assert node.execute('SELECT attempts, last_error, completed_at IS NOT NULL FROM pgwrh.index_build_job') == [(2, None, True)]
    assert node.execute("SELECT count(*) FROM pg_indexes WHERE indexname = 'items_value'") == [(1,)]
    with pytest.raises(Exception, match='other objects depend on it'):
        node.execute('DROP EXTENSION pgwrh')


def test_existing_unrelated_index_is_never_adopted_or_removed(postgres_node_factory):
    node = postgres_node_factory('index_collision', preload=False)
    seed(node)
    node.execute('CREATE INDEX items_value ON items(id)')
    oid = node.execute("SELECT 'items_value'::regclass::oid")
    assert enqueue(node)
    wait_idle(node)
    assert node.execute("SELECT 'items_value'::regclass::oid") == oid
    assert node.execute("SELECT count(*) FROM pgwrh.managed_object WHERE object_name = 'items_value'") == [(0,)]
    assert 'unrelated index' in node.execute('SELECT last_error FROM pgwrh.index_build_job')[0][0]
    assert not enqueue(node)  # persisted backoff, not a hot retry loop
    node.execute('DROP INDEX items_value')
    wait_until(lambda: enqueue(node), timeout=15, message='name collision did not recover')
    wait_idle(node)
    assert valid(node)


def test_failed_launch_releases_capacity(postgres_node_factory):
    node = postgres_node_factory('index_launch_failure', preload=False)
    seed(node)
    node.stop()
    node.append_conf('max_worker_processes = 2')
    node.append_conf('max_logical_replication_workers = 0')
    node.start()
    # Fill both native worker slots with independent pg_background sleepers.
    node.execute("SELECT pgwrh.launch_in_background('SELECT pg_sleep(30)')")
    node.execute("SELECT pgwrh.launch_in_background('SELECT pg_sleep(30)')")
    assert not enqueue(node)
    assert not tasks(node)
    assert node.execute('SELECT last_sqlstate FROM pgwrh.index_build_job') == [('53400',)]
    node.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE backend_type = 'pg_background'")
    wait_until(lambda: enqueue(node), timeout=15, message='launch failure did not recover')
    wait_idle(node)
    assert valid(node)


def test_partitioned_parent_is_not_recursively_indexed(postgres_node_factory):
    node = postgres_node_factory('index_parent', preload=False)
    node.execute('CREATE TABLE items(id integer, value text) PARTITION BY RANGE (id)')
    node.execute('CREATE TABLE leaf PARTITION OF items FOR VALUES FROM (0) TO (10)')
    with pytest.raises(Exception, match='physical shard table'):
        enqueue(node)
    assert enqueue(node, 'leaf', 'leaf_value')
    wait_idle(node)
    assert valid(node, 'leaf_value')
    assert node.execute("SELECT count(*) FROM pg_index WHERE indrelid = 'items'::regclass") == [(0,)]


@pytest.mark.parametrize('interruption', ['daemon', 'cancel', 'restart'])
def test_required_indexes_preserve_live_replication_and_readiness(handoff_cluster, interruption):
    cluster = handoff_cluster
    source = cluster.replicas[0]
    # The new template is declared on the partitioned root and expanded onto
    # its physical leaves. Hold the first scan with a writer, without blocking
    # the logical apply worker's writes to other rows.
    with source.node.connect() as holder:
        holder.execute("LOCK TABLE data.p0 IN ROW EXCLUSIVE MODE")
        cluster.master.execute("""INSERT INTO pgwrh.shard_index_template
            (replication_group_id, index_template_schema, index_template_table_name, index_template_name, index_template)
            VALUES ('g1', 'data', 'root', 'value_idx', '(value)')""")
        cluster.master.start_rollout()
        wait_until(lambda: bool(source.execute("""SELECT pid FROM pg_stat_progress_create_index
            WHERE relid = 'data.p0'::regclass AND phase = 'waiting for writers before build'""")),
            timeout=30, message='required index did not wait for live writer')
        index = source.execute("""SELECT indexrelid::regclass::text FROM pg_index
            WHERE indrelid = 'data.p0'::regclass AND NOT indisvalid""")[0][0]
        assert source.query_scalar(f"SELECT count(*) FROM pgwrh.local_shard_index WHERE index_name = '{index.split('.')[-1]}'") == 0
        assert source.query_scalar("SELECT count(*) FROM pgwrh.ready_serving_subtree WHERE rel_id = ('data', 'root')::pgwrh.rel_id") == 0
        with pytest.raises(Exception, match='required local shards'):
            cluster.master.commit_rollout()
        # An exact row oracle after each publisher transaction proves apply and
        # reads progress while CIC is still active, for INSERT/UPDATE/DELETE.
        for step in range(5):
            cluster.master.execute(f"""INSERT INTO data.root VALUES ({200 + step}, 'new {step}');
                UPDATE data.root SET value = 'updated {step}' WHERE id = {1 + step};
                DELETE FROM data.root WHERE id = {20 + step};""")
            expected = cluster.master.execute('SELECT * FROM data.root ORDER BY id')
            wait_until(lambda: source.execute('SELECT * FROM data.root ORDER BY id') == expected,
                timeout=15, message='logical apply or replica reads stalled during CIC')
            assert source.execute(f"SELECT indisvalid FROM pg_index WHERE indexrelid = '{index}'::regclass") == [(False,)]
        if interruption == 'daemon':
            original = daemon_pids(source.node)
            source.execute(f'SELECT pg_terminate_backend({original[0][0]})')
            wait_until(lambda: bool(daemon_pids(source.node)) and daemon_pids(source.node) != original,
                       timeout=15, message='sync daemon did not recover while indexing')
            assert source.query_scalar("SELECT count(*) FROM pgwrh.index_build_tasks() WHERE relid = 'data.p0'::regclass") == 1
            holder.rollback()
        elif interruption == 'cancel':
            source.execute("""SELECT pg_cancel_backend(pid) FROM pg_stat_progress_create_index
                WHERE relid = 'data.p0'::regclass""")
            wait_until(lambda: source.query_scalar("SELECT count(*) FROM pgwrh.index_build_job WHERE last_sqlstate = '57014'") == 1,
                       timeout=10, message='cancelled build did not record its error')
            assert source.query_scalar(f"SELECT count(*) FROM pgwrh.local_shard_index WHERE index_name = '{index.split('.')[-1]}'") == 0
            with pytest.raises(Exception, match='required local shards'):
                cluster.master.commit_rollout()
            holder.rollback()
        else:
            source.node.stop(['-m', 'immediate'])
            source.node.start()
    cluster.master.wait_for_rollout_ready(expected_replicas=3, timeout=60)
    cluster.master.commit_rollout()
    cluster.assert_query_results_match('SELECT * FROM data.root ORDER BY id')
    assert source.query_scalar("SELECT count(*) FROM pg_index WHERE indrelid IN ('data.p0'::regclass, 'data.p1'::regclass) AND NOT indisvalid") == 0


def test_replaced_table_does_not_inherit_an_admitted_build(postgres_node_factory):
    node = postgres_node_factory('index_table_identity', preload=False)
    seed(node)
    with node.connect() as launch:
        assert enqueue(launch)
        node.execute('DROP TABLE items; CREATE TABLE items(id integer, value text)')
        launch.commit()
    wait_idle(node)
    assert node.execute("SELECT to_regclass('items_value')") == [(None,)]
    assert 'no longer a physical table' in node.execute('SELECT last_error FROM pgwrh.index_build_job')[0][0]


def test_index_worker_entry_points_require_administrator(postgres_node_factory):
    node = postgres_node_factory('index_permissions', preload=False)
    node.execute('CREATE ROLE app LOGIN')
    for sql in ('SELECT pgwrh.launch_index_build(1, 1)',
                'SELECT * FROM pgwrh.index_build_tasks()',
                "SELECT pgwrh.enqueue_index_build('public', 'items', 'idx', '(id)')",
                'SELECT pgwrh.schedule_index_builds()',
                'SELECT pgwrh.register_index_build(1, 1)'):
        with pytest.raises(Exception, match='permission denied'):
            node.execute(sql, username='app')


def test_completed_index_survives_interrupted_bookkeeping(postgres_node_factory):
    node = postgres_node_factory('index_final_commit', preload=False)
    seed(node)
    # Stop between the successful top-level CREATE and final job bookkeeping.
    # Ownership must already be durable, including its DROP protection.
    node.execute('''CREATE OR REPLACE FUNCTION pgwrh.finish_index_build(_job bigint)
        RETURNS void LANGUAGE sql AS 'SELECT pg_advisory_xact_lock(224)' ''')
    with node.connect() as barrier:
        barrier.execute('SELECT pg_advisory_lock(224)')
        assert enqueue(node)
        wait_until(lambda: valid(node), timeout=10, message='concurrent index did not become valid')
        wait_until(lambda: node.execute("SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND objid = 224 AND NOT granted") == [(1,)],
                   timeout=10, message='worker did not reach final bookkeeping')
        oid = node.execute("SELECT 'items_value'::regclass::oid")
        assert node.execute('SELECT completed_at FROM pgwrh.index_build_job') == [(None,)]
        node.execute(f'SELECT pg_terminate_backend({tasks(node)[0][3]})')
        wait_idle(node)
    node.execute('SELECT pgwrh.recover_index_builds()')
    node.execute('SELECT pgwrh.recover_index_builds()')
    assert node.execute('SELECT attempts, completed_at IS NOT NULL FROM pgwrh.index_build_job') == [(1, True)]
    assert node.execute("SELECT 'items_value'::regclass::oid") == oid
    node.execute("""DELETE FROM pg_depend WHERE objid = 'items_value'::regclass
        AND refclassid = 'pg_extension'::regclass AND deptype = 'n'""")
    node.execute('SELECT pgwrh.repair_managed_objects()')
    assert node.execute("""SELECT count(*) FROM pg_depend WHERE objid = 'items_value'::regclass
        AND refclassid = 'pg_extension'::regclass AND deptype = 'n'""") == [(1,)]
    with pytest.raises(Exception, match='other objects depend on it'):
        node.execute('DROP EXTENSION pgwrh')
