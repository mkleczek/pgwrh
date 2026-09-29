"""Real pg_upgrade and node replacement, with reads checked during maintenance.

Ownership and daemon recovery must work without a test-only repair or wake-up.
Infrastructure, replication, and oracle failures are never treated as expected.
"""
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
import json
from pathlib import Path
import subprocess
from threading import Event

import pytest

from ..pgwrh_testkit import (MasterHandle, PgwrhCluster, ReplicaHandle, ReplicaSpec,
                            quote_ident, quote_literal, wait_until)
from ..test_backup_restore import run
from .test_cluster import ROWS, assert_changes_replicate, make_cluster  # noqa: F401


@pytest.fixture
def maintenance_cluster(make_cluster, postgres_installations):
    cluster = make_cluster(18, [18, 18])
    cluster.add_replica(ReplicaSpec('replica3', installation=postgres_installations[18]))
    # Two physical copies per leaf, and one remote subtree per replica. Taking
    # one member offline can therefore preserve full reads on the other two.
    keys = []
    for excluded in ('replica3', 'replica1', 'replica2'):
        keys.append(cluster.master.query_scalar(f"""
            SELECT n::text FROM generate_series(1, 1000) n
            WHERE pgwrh.score(100, n::text, {quote_literal(excluded)}) < ALL (
                SELECT pgwrh.score(100, n::text, host)
                FROM unnest(ARRAY['replica1','replica2','replica3']) host
                WHERE host <> {quote_literal(excluded)}) ORDER BY n LIMIT 1
        """))
    assert all(keys)
    expression = 'SELECT CASE ' + ' '.join(
        f"WHEN $2 LIKE 'items_p{i}_%' THEN {quote_literal(key)}"
        for i, key in enumerate(keys)) + ' END'
    cluster.master.execute(f"""
        INSERT INTO pgwrh.sharded_table
            (replication_group_id, sharded_table_schema, sharded_table_name,
             replication_factor, sharding_key_expression)
        VALUES ('g1', 'data', 'items', 50, {quote_literal(expression)})
        ON CONFLICT (replication_group_id, sharded_table_schema, sharded_table_name, version)
        DO UPDATE SET replication_factor = EXCLUDED.replication_factor,
                      sharding_key_expression = EXCLUDED.sharding_key_expression
    """)
    cluster.deploy(timeout=90)
    wait_until(lambda: all(r.query_scalar('SELECT count(*) FROM pgwrh.connected_local_shard') == 4
                           and r.query_scalar('SELECT count(*) FROM pgwrh.connected_remote_shard') == 2
                           for r in cluster.replicas), timeout=60,
               message='redundant maintenance topology did not converge')
    cluster.assert_query_results_match(ROWS)
    return cluster


class ReadOracle:
    """Every connection checks full typed rows; SQL errors are never retried."""

    def __init__(self, replicas, expected):
        self.replicas = replicas
        self.expected = expected
        self.stop = Event()
        self.count = 0

    def run(self):
        while not self.stop.is_set():
            for replica in self.replicas:
                with replica.node.connect(autocommit=True) as conn:
                    conn.execute("SET statement_timeout = '10s'")
                    assert conn.execute(ROWS) == self.expected, replica.name
            self.count += 1
            self.stop.wait(0.02)

    def progress(self, rounds=3):
        before = self.count

        def advanced():
            if self.task.done():
                self.task.result()
                raise AssertionError('read workload stopped early')
            return self.count >= before + rounds

        wait_until(advanced, timeout=20, interval=0.05,
                   message='read oracle stopped making progress')

    def __enter__(self):
        self.pool = ThreadPoolExecutor(max_workers=1)
        self.task = self.pool.submit(self.run)
        self.progress()
        return self

    def __exit__(self, *exc):
        self.stop.set()
        try:
            self.task.result(timeout=15)
        finally:
            self.pool.shutdown(wait=True)


def markers(node):
    # Read physical pg_depend rows independently of the registry-backed views.
    return node.execute('''SELECT a.type, a.object_names, a.object_args
        FROM pg_depend d JOIN pg_extension e
            ON d.refclassid = 'pg_extension'::regclass AND d.refobjid = e.oid,
        LATERAL pg_identify_object_as_address(d.classid, d.objid, 0) a
        WHERE e.extname = 'pgwrh' AND d.deptype = 'n' AND d.objsubid = 0 AND d.refobjsubid = 0
        ORDER BY 1, 2, 3''')


def inventory(node):
    return node.execute('SELECT * FROM pgwrh.managed_object ORDER BY 1, 2, 3')


def subscriptions(node):
    return node.execute('''
        SELECT s.subname, s.subslotname, n.nspname, c.relname, r.srsubstate
        FROM pg_subscription s JOIN pg_subscription_rel r ON r.srsubid = s.oid
        JOIN pg_class c ON c.oid = r.srrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace ORDER BY 1, 3, 4
    ''')


def origins(node):
    # Subscription OIDs (and therefore pg_<oid> origin names) can change, and
    # PostgreSQL 19 pads LSN text differently. Compare identity and byte offset.
    return node.execute('''SELECT s.subname, r.remote_lsn - '0/0'::pg_lsn
        FROM pg_replication_origin_status r JOIN pg_subscription s
            ON r.external_id = 'pg_' || s.oid::text ORDER BY 1''')


def daemon_count(node):
    return node.execute("""SELECT count(*) FROM pg_stat_activity
        WHERE datname = current_database() AND application_name = 'pgwrh_sync_daemon'
          AND pid IN (SELECT pid FROM pg_locks WHERE locktype = 'advisory'
              AND classid = 0 AND objid = 517384732 AND objsubid = 1 AND granted)""")[0][0]


def copy_starts(node):
    log = (Path(node.base_dir) / 'logs/postgresql.log').read_text()
    return sum('logical replication table synchronization worker' in line
               and 'has started' in line for line in log.splitlines())


def set_subscriptions(node, enabled):
    for (name,) in node.execute('SELECT subname FROM pg_subscription'):
        node.execute(f'ALTER SUBSCRIPTION {quote_ident(name)} {"ENABLE" if enabled else "DISABLE"}')


@contextmanager
def paused_daemons(cluster):
    # Freeze management writes while catching publisher slots up to a fixed LSN.
    # Persist the pause so the supervisor cannot undo it; reads stay live.
    configs = [(r, r.execute('SELECT refresh_seconds, application_name FROM pgwrh.sync_daemon_config WHERE enabled'))
               for r in cluster.replicas]
    try:
        with ExitStack() as stack:
            for replica, _ in configs:
                replica.execute('SELECT pgwrh.stop_sync_daemon()')
                conn = stack.enter_context(replica.node.connect(autocommit=True))
                conn.execute("SET statement_timeout = '20s'")
                conn.execute('SELECT pg_advisory_lock(517384732)')
                conn.execute('SELECT pg_advisory_lock(2895359559)')
            yield
    finally:
        for replica, config in configs:
            if config:
                seconds, app = config[0]
                replica.execute(f'SELECT pgwrh.start_sync_daemon({seconds}, {quote_literal(app)})')


def upgrade(old, new, tmp_path, oracle):
    new.stop()
    old.stop()
    before = oracle.count
    oracle.progress()  # Reads must succeed while the server is actually down.
    command = [str(Path(new.bin_dir) / 'pg_upgrade'),
               '--old-bindir', old.bin_dir, '--new-bindir', new.bin_dir,
               '--old-datadir', old.data_dir, '--new-datadir', new.data_dir,
               '--old-port', str(old.port), '--new-port', str(new.port),
               '--socketdir', str(tmp_path)]
    result = subprocess.run(command, cwd=tmp_path, text=True, capture_output=True, timeout=120)
    (tmp_path / 'pg_upgrade.log').write_text(result.stdout + result.stderr)
    assert result.returncode == 0, result.stdout + result.stderr
    oracle.progress()
    down_reads = oracle.count - before
    # Retain the endpoint, as an in-place operation must. The old node retains
    # the testgres reservation until fixture teardown; release only the spare.
    new.free_port()
    new.port = old.port
    new.append_conf(f'port = {old.port}')
    new.start()
    assert int(new.execute('SHOW server_version_num')[0][0]) // 10000 == 19
    oracle.progress()
    return down_reads


def drain(cluster, replica):
    cluster.master.execute(f"UPDATE pgwrh.shard_host SET online = false WHERE host_id = {quote_literal(replica.name)}")
    peers = [r for r in cluster.replicas if r is not replica]
    wait_until(lambda: all(r.query_scalar(f"""
        SELECT count(*) FROM pgwrh.connected_remote_shard r,
             LATERAL jsonb_object_keys(r.shard_server_targets) member, pg_foreign_server t,
             LATERAL pgwrh.opts(t.srvoptions) p
        WHERE t.srvname = member AND p.key = 'port'
          AND p.value = {quote_literal(str(replica.port))}
    """) == 0 for r in peers), timeout=60, message='peers still route to the drained member')


def pending_rollout(cluster):
    cluster.master.execute("SELECT pgwrh.set_replica_weight('g1', 'default', 'replica2', 101)")


def test_replica_pg_upgrade_preserves_reconciliation(maintenance_cluster, postgres_node_factory,
                                                    postgres_installations, tmp_path):
    cluster = maintenance_cluster
    replica = cluster.replicas[0]
    drain(cluster, replica)
    saved = markers(replica.node)
    saved_inventory = inventory(replica.node)
    before = subscriptions(replica.node)
    assert saved and before and all(row[-1] == 'r' for row in before)
    new = postgres_node_factory('upgraded_replica', installation=postgres_installations[19],
                                install_extension=False)
    set_subscriptions(replica.node, False)
    wait_until(lambda: replica.query_scalar('SELECT count(*) FROM pg_stat_subscription WHERE pid IS NOT NULL') == 0,
               timeout=20, message='subscriber did not stop before origin snapshot')
    old_origins = origins(replica.node)
    assert old_origins
    evidence = {}
    with ReadOracle(cluster.replicas[1:], cluster.master.execute(ROWS)) as oracle:
        evidence['read_rounds_while_down'] = upgrade(replica.node, new, tmp_path, oracle)
        replica.node = new
        assert inventory(new) == saved_inventory
        # pg_upgrade restores subscriptions disabled. Inspect ready state and
        # origin positions BEFORE enabling: a recopy cannot conceal lost state.
        assert subscriptions(new) == before
        assert origins(new) == old_origins
        # With subscriptions still disabled, no ping can conceal startup failure.
        wait_until(lambda: daemon_count(new) == 1 and markers(new) == saved,
                   timeout=20, message='supervisor did not recover daemon and drop protection')
        evidence['markers'] = (len(saved), len(markers(new)))
        evidence['daemon_after_boot'] = daemon_count(new)
        set_subscriptions(new, True)
        assert replica.execute(ROWS) == oracle.expected
        evidence['remote_routes'] = replica.query_scalar('SELECT count(*) FROM pgwrh.connected_remote_shard')
        assert evidence['remote_routes'] == 2
        pending_rollout(cluster)
        cluster.deploy(timeout=60)
        evidence['rollout_gaps'] = cluster.master.rollout_gap_counts()
        assert not any(evidence['rollout_gaps'].values()), evidence
        oracle.progress()
        assert subscriptions(new) == before
        assert replica.execute(ROWS) == oracle.expected
        # Exercise an actual route change on the upgraded replica, beyond an
        # unchanged-placement rollout. It must stop targeting a drained peer.
        cluster.master.execute("UPDATE pgwrh.shard_host SET online = true WHERE host_id = 'replica1'")
        drain(cluster, cluster.replicas[1])
        evidence['peer_drain_reconciled'] = True
        assert replica.execute(ROWS) == oracle.expected
        cluster.master.execute("UPDATE pgwrh.shard_host SET online = true WHERE host_id = 'replica2'")
        oracle.progress()
        evidence['read_rounds'] = oracle.count
    assert_changes_replicate(cluster)
    assert copy_starts(new) == 0, 'upgraded replica performed a new initial copy'
    (tmp_path / 'evidence.json').write_text(json.dumps(evidence, indent=2))


def test_controller_pg_upgrade_preserves_reconciliation(maintenance_cluster, postgres_node_factory,
                                                       postgres_installations, tmp_path):
    cluster = maintenance_cluster
    saved = markers(cluster.master.node)
    saved_inventory = inventory(cluster.master.node)
    before = [subscriptions(r.node) for r in cluster.replicas]
    copies_before = [copy_starts(r.node) for r in cluster.replicas]
    new = postgres_node_factory('upgraded_controller', installation=postgres_installations[19],
                                install_extension=False)
    evidence = {}
    with ReadOracle(cluster.replicas, cluster.master.execute(ROWS)) as oracle, paused_daemons(cluster):
        lsn = cluster.master.query_scalar('SELECT pg_current_wal_insert_lsn()::text')
        wait_until(lambda: cluster.master.query_scalar(f"""SELECT bool_and(confirmed_flush_lsn >= {quote_literal(lsn)}::pg_lsn)
            FROM pg_replication_slots WHERE slot_type = 'logical'""") is True,
            timeout=60, message='logical slots did not catch up before controller upgrade')
        slots = cluster.master.execute('SELECT slot_name, plugin FROM pg_replication_slots ORDER BY 1')
        assert slots
        for replica in cluster.replicas:
            set_subscriptions(replica.node, False)
        evidence['read_rounds_while_down'] = upgrade(cluster.master.node, new, tmp_path, oracle)
        cluster.master.node = new
        assert new.execute('SELECT slot_name, plugin FROM pg_replication_slots ORDER BY 1') == slots
        assert inventory(new) == saved_inventory
        # The documented post-upgrade step is also safe if boot already repaired.
        new.execute('SELECT pgwrh.repair_managed_objects()')
        assert markers(new) == saved
        evidence['markers'] = (len(saved), len(markers(new)))
        for replica, expected in zip(cluster.replicas, before):
            assert subscriptions(replica.node) == expected
            set_subscriptions(replica.node, True)
        pending_rollout(cluster)
        cluster.master.start_rollout()
        oracle.progress()
        evidence['read_rounds'] = oracle.count
    cluster.master.wait_for_rollout_ready(expected_replicas=3, timeout=60)
    cluster.master.commit_rollout()
    cluster.assert_query_results_match(ROWS)
    assert_changes_replicate(cluster)
    assert [copy_starts(r.node) for r in cluster.replicas] == copies_before, 'controller upgrade caused a recopy'
    (tmp_path / 'evidence.json').write_text(json.dumps(evidence, indent=2))


def test_replace_replica_with_new_major(maintenance_cluster, postgres_node_factory,
                                      postgres_installations):
    cluster = maintenance_cluster
    replica = cluster.replicas[0]
    drain(cluster, replica)
    old_slot = replica.query_scalar('SELECT subslotname FROM pg_subscription')
    with ReadOracle(cluster.replicas[1:], cluster.master.execute(ROWS)) as oracle:
        replica.node.stop()
        oracle.progress()
        # A fresh subscriber has a new slot and must copy; old slot removal is
        # safe only after fencing the old consumer, as above.
        cluster.master.execute(f'SELECT pg_drop_replication_slot({quote_literal(old_slot)})')
        replica.node = postgres_node_factory('replacement', installation=postgres_installations[19])
        cluster.master.execute(f"UPDATE pgwrh.shard_host SET port = {replica.port} WHERE host_id = 'replica1'")
        replica.configure_controller(master_port=cluster.master.port)
        wait_until(lambda: replica.query_scalar('SELECT count(*) FROM pgwrh.connected_local_shard') == 4,
                   timeout=90, message='replacement did not copy its assigned shards')
        assert replica.query_scalar('SELECT subslotname FROM pg_subscription') != old_slot
        assert markers(replica.node)
        assert replica.execute(ROWS) == oracle.expected
        cluster.master.execute("UPDATE pgwrh.shard_host SET online = true WHERE host_id = 'replica1'")
        pending_rollout(cluster)
        cluster.deploy(timeout=60)
        oracle.progress()
        cluster.assert_query_results_match(ROWS)
    assert_changes_replicate(cluster)


def test_replace_controller_with_new_major(maintenance_cluster, postgres_node_factory,
                                         postgres_installations, tmp_path):
    original = maintenance_cluster
    rebuilt = None
    try:
        with ReadOracle(original.replicas, original.master.execute(ROWS)) as oracle, paused_daemons(original):
            # Follow the documented logical-restore recovery path. This has no
            # slot continuity, so preserve reads on fenced old replicas while
            # building fresh subscribers against the replacement controller.
            archive = tmp_path / 'controller.dump'
            run(original.master.node, 'pg_dump', '-d', 'postgres', '-Fc', '-f', str(archive))
            roles = run(original.master.node, 'pg_dumpall', '--roles-only', '--no-role-passwords')
            bootstrap = original.master.query_scalar('SELECT quote_ident(current_user)')
            roles_file = tmp_path / 'roles.sql'
            roles_file.write_text(roles.replace(f'CREATE ROLE {bootstrap};', ''))
            for replica in original.replicas:
                set_subscriptions(replica.node, False)
            original.master.node.stop()
            oracle.progress()

            new = postgres_node_factory('replacement_controller', install_extension=False,
                                        installation=postgres_installations[19])
            run(new, 'psql', '-X', '-v', 'ON_ERROR_STOP=1', '-d', 'postgres', '-f', str(roles_file))
            for section in ('pre-data', 'data', 'post-data'):
                flags = ('--data-only', '--disable-triggers') if section == 'data' else (
                    f'--section={section}', '--no-publications', '--no-subscriptions')
                run(new, 'pg_restore', '-d', 'postgres', '--exit-on-error', '--single-transaction',
                    *flags, str(archive))
            new.execute('SELECT pgwrh.sync_publications()')
            new.execute((Path(__file__).resolve().parents[3] / 'docs/recovery-quarantine.sql').read_text())
            assert new.execute(ROWS) == oracle.expected
            assert new.execute('SELECT count(*) FROM pg_replication_slots') == [(0,)]
            rebuilt = PgwrhCluster(MasterHandle(new), postgres_node_factory)
            for old in original.replicas:
                node = postgres_node_factory('rebuilt_' + old.name, installation=postgres_installations[19])
                replica = ReplicaHandle(old.spec, node, old.username, old.password)
                rebuilt.replicas.append(replica)
                new.execute(f'ALTER ROLE {quote_ident(old.username)} PASSWORD {quote_literal(old.password)}')
                new.execute(f"UPDATE pgwrh.shard_host SET port = {node.port} WHERE host_id = {quote_literal(old.name)}")
            new.execute('UPDATE pgwrh.shard_host SET online = true')
            for replica in rebuilt.replicas:
                replica.configure_controller(master_port=new.port)
            rebuilt.master.wait_for_rollout_ready(expected_replicas=3, timeout=90)
            rebuilt.assert_query_results_match(ROWS)
            oracle.progress()
            pending_rollout(rebuilt)
            rebuilt.deploy(timeout=60)
            oracle.progress()
        for replica in original.replicas:
            replica.node.stop(['-m', 'immediate'])
        assert_changes_replicate(rebuilt)
    finally:
        if rebuilt is not None:
            for replica in reversed(rebuilt.replicas):
                replica.node.stop(['-m', 'immediate'])
