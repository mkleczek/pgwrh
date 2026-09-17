"""Interrupt real databases and one replica's controller transport at known phases."""
from contextlib import contextmanager
import select
import socket
import socketserver
from threading import Event, Lock, Thread

import pytest

from .pgwrh_testkit import quote_literal, wait_until
from .test_credential_protocol import generation_states, rotate, wait_rotation
from .test_local_first_handoff import handoff_cluster, move_to_destination, wait_prepared


def crash_and_restart(cluster, victim):
    node = cluster.master.node if victim == 'controller' else cluster.replicas[0].node
    node.stop(['-m', 'immediate'])
    node.start()
    if victim != 'controller':
        node.execute('SELECT pgwrh.start_sync_daemon(0.1)')


def assert_rows(cluster):
    cluster.assert_query_results_match('SELECT id, value FROM data.root ORDER BY id')


@pytest.mark.parametrize('victim', ['controller', 'replica'])
@pytest.mark.parametrize('phase', ['preparing', 'committed', 'rolling_back'])
def test_rollout_resumes_after_database_crash(handoff_cluster, victim, phase):
    cluster = handoff_cluster
    source, destination, reader = cluster.replicas
    # Keep the reader on the old routes before commit, or on the prepared routes
    # during rollback. Restarting another database must not bypass its report.
    with reader.node.connect() as delayed:
        if phase == 'preparing':
            delayed.execute('SELECT pg_advisory_lock(2895359559)')
        move_to_destination(cluster)
        wait_prepared(cluster)
        if phase == 'preparing':
            with pytest.raises(Exception, match='required remote shards'):
                cluster.master.commit_rollout()
        else:
            cluster.master.wait_for_rollout_ready(expected_replicas=3, timeout=60)
            delayed.execute('SELECT pg_advisory_lock(2895359559)')
            if phase == 'committed':
                cluster.master.commit_rollout()
            else:
                cluster.master.rollback_rollout()
                assert destination.query_scalar('SELECT count(*) FROM pgwrh.connected_local_shard') == 2

        versions = cluster.master.execute('SELECT current_version, target_version FROM pgwrh.replication_group')
        crash_and_restart(cluster, victim)
        assert cluster.master.execute('SELECT current_version, target_version FROM pgwrh.replication_group') == versions
        if phase == 'preparing':
            with pytest.raises(Exception, match='required remote shards'):
                cluster.master.commit_rollout()
            assert source.query_scalar('SELECT count(*) FROM pgwrh.connected_local_shard') == 2
        elif phase == 'rolling_back':
            assert destination.query_scalar('SELECT count(*) FROM pgwrh.connected_local_shard') == 2
            with pytest.raises(Exception, match='Rollback has not finished'):
                cluster.master.start_rollout()
        assert_rows(cluster)
        delayed.execute('SELECT pg_advisory_unlock(2895359559)')

    if phase == 'preparing':
        cluster.master.wait_for_rollout_ready(expected_replicas=3, timeout=60)
        cluster.master.commit_rollout()
    wait_until(lambda: cluster.master.query_scalar('''SELECT count(*)
        FROM pgwrh.replication_group_config_lock WHERE rollback_unlock IS NOT NULL''') == 0,
        timeout=60, message='crashed rollout did not finish cleanup')
    retained, removed = (source, destination) if phase == 'rolling_back' else (destination, source)
    wait_until(lambda: retained.query_scalar('SELECT count(*) FROM pgwrh.connected_local_shard') == 2
               and removed.query_scalar('SELECT count(*) FROM pgwrh.connected_local_shard') == 0,
               timeout=60, message='crashed rollout did not converge to its chosen placement')
    assert_rows(cluster)


@pytest.mark.parametrize('victim', ['controller', 'replica'])
@pytest.mark.parametrize('phase', ['preparing', 'retiring', 'retired'])
def test_credential_rotation_resumes_after_database_crash(handoff_cluster, victim, phase):
    cluster = handoff_cluster
    _, destination, reader = cluster.replicas
    old, = generation_states(cluster)
    with destination.node.connect() as delayed, reader.node.connect() as slow_reader:
        delayed.execute('SELECT pg_advisory_lock(2895359559)')
        pending = rotate(cluster)
        wait_until(lambda: cluster.master.query_scalar(f'''SELECT count(*)
            FROM pgwrh.missing_credential_installation WHERE generation = {quote_literal(pending)}
            AND member_role <> 'destination' ''') == 0,
            timeout=60, message='other replicas did not install the pending credentials')
        slow_reader.execute('SELECT pg_advisory_lock(2895359559)')
        if phase != 'preparing':
            delayed.execute('SELECT pg_advisory_unlock(2895359559)')
            wait_until(lambda: generation_states(cluster).get(pending) == 'active',
                       timeout=60, message='credential activation did not finish')
        if phase == 'retired':
            slow_reader.execute('SELECT pg_advisory_unlock(2895359559)')
            wait_rotation(cluster, pending)
        expected = ({old: 'active', pending: 'preparing'} if phase == 'preparing' else
                    {old: 'retiring', pending: 'active'} if phase == 'retiring' else
                    {pending: 'active'})
        assert generation_states(cluster) == expected
        crash_and_restart(cluster, victim)
        assert generation_states(cluster) == expected
        assert_rows(cluster)
        if phase == 'preparing':
            delayed.execute('SELECT pg_advisory_unlock(2895359559)')
        if phase != 'retired':
            slow_reader.execute('SELECT pg_advisory_unlock(2895359559)')
    wait_rotation(cluster, pending)
    assert_rows(cluster)
    # A successful recovery must also release the single-rotation guard.
    wait_rotation(cluster, rotate(cluster))
    assert_rows(cluster)


@contextmanager
def controller_gate(port):
    """A real TCP reset on one link, without firewall privileges or host changes."""
    enabled, rejected = Event(), Event()
    enabled.set()
    sockets = set()
    guard = Lock()

    class Forward(socketserver.BaseRequestHandler):
        def handle(self):
            upstream = None
            try:
                with guard:
                    if not enabled.is_set():
                        rejected.set()
                        return
                    upstream = socket.create_connection(('127.0.0.1', port), timeout=2)
                    self.request.settimeout(2)
                    sockets.update((self.request, upstream))
                while enabled.is_set():
                    ready, _, _ = select.select([self.request, upstream], [], [], 0.1)
                    for source in ready:
                        data = source.recv(65536)
                        if not data:
                            return
                        (upstream if source is self.request else self.request).sendall(data)
            except (OSError, ValueError):
                pass  # reset/close is the injected transport fault
            finally:
                with guard:
                    sockets.discard(self.request)
                    sockets.discard(upstream)
                if upstream is not None:
                    upstream.close()

    class Server(socketserver.ThreadingTCPServer):
        daemon_threads = True
        allow_reuse_address = True

        def cut(self):
            with guard:
                enabled.clear()
                for connection in sockets:
                    try:
                        connection.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass

        def heal(self):
            enabled.set()

    with Server(('127.0.0.1', 0), Forward) as server:
        worker = Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            yield server, rejected
        finally:
            server.cut()
            server.shutdown()
            worker.join(timeout=5)


def test_controller_link_loss_blocks_readiness_until_fresh_reports(handoff_cluster):
    cluster = handoff_cluster
    reader = cluster.replicas[-1]
    with controller_gate(cluster.master.port) as (gate, rejected):
        port = gate.server_address[1]
        reader.execute(f"ALTER SERVER replica_controller OPTIONS (SET port '{port}')")
        conninfo = f'host=127.0.0.1 port={port} dbname=postgres user={reader.username} password={reader.password}'
        reader.execute(f'ALTER SUBSCRIPTION pgwrh_replica_subscription CONNECTION {quote_literal(conninfo)}')
        # Prove that this replica actually uses the proxy before disconnecting it.
        assert reader.query_scalar('SELECT count(*) FROM pgwrh.fdw_credential_state') == 1
        with reader.node.connect() as delayed:
            delayed.execute('SELECT pg_advisory_lock(2895359559)')
            gate.cut()
            pending = rotate(cluster)
            move_to_destination(cluster)
            delayed.execute('SELECT pg_advisory_unlock(2895359559)')
        wait_until(rejected.is_set, timeout=30, message='replica did not retry its severed controller link')
        # Let the reachable replicas finish preparing so the failed readiness
        # check specifically exercises the disconnected reader's stale routes.
        wait_prepared(cluster)
        assert generation_states(cluster)[pending] == 'preparing'
        with pytest.raises(Exception, match='required remote shards'):
            cluster.master.commit_rollout()
        assert_rows(cluster)
        gate.heal()
        wait_rotation(cluster, pending)
        cluster.master.wait_for_rollout_ready(expected_replicas=3, timeout=60)
        cluster.master.commit_rollout()
        assert_rows(cluster)
