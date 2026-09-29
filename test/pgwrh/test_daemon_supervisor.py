"""Restart without a ping, including several pgwrh databases in one server."""
import time

import pytest

from .pgwrh_testkit import ReplicaHandle, ReplicaSpec, wait_until
from .test_local_first_handoff import handoff_cluster  # noqa: F401


def daemon_pids(node):
    return node.execute("""SELECT pid FROM pg_stat_activity
        WHERE datname = current_database() AND application_name = 'pgwrh_sync_daemon'
          AND pid IN (SELECT pid FROM pg_locks WHERE locktype = 'advisory'
              AND classid = 0 AND objid = 517384732 AND objsubid = 1 AND granted)
        ORDER BY pid""")


# Exact oid comparisons: the operator test below defines public.=(oid, regclass).
PGWRH_MARKERS = """SELECT count(*) FROM pg_catalog.pg_depend
    WHERE refclassid = 'pg_catalog.pg_extension'::regclass::oid AND deptype = 'n'
      AND refobjid = (SELECT oid FROM pg_catalog.pg_extension WHERE extname = 'pgwrh')"""
ERASE_PGWRH_MARKERS = """DELETE FROM pg_catalog.pg_depend
    WHERE refclassid = 'pg_catalog.pg_extension'::regclass::oid AND deptype = 'n'
      AND refobjid = (SELECT oid FROM pg_catalog.pg_extension WHERE extname = 'pgwrh')"""


@pytest.mark.parametrize('handoff_cluster', [dict(
    master='controller db', source='source db', destination='destination db', reader='reader db', shared=True,
)], indirect=True)
def test_supervisor_recovers_workers_and_server_without_controller(handoff_cluster):
    cluster = handoff_cluster
    source, destination, reader = cluster.replicas
    expected = cluster.master.execute('SELECT * FROM data.root ORDER BY id')
    # Prevent any ping or metadata response from masking supervisor recovery.
    cluster.master.node.stop()
    reader.execute('SELECT pgwrh.stop_sync_daemon()')
    wait_until(lambda: not daemon_pids(reader.node), timeout=10, message='explicit stop did not stop daemon')
    source.node.stop(['-m', 'immediate'])
    source.node.start()  # all three replica databases share this postmaster
    wait_until(lambda: len(daemon_pids(source.node)) == len(daemon_pids(destination.node)) == 1,
               timeout=20, message='enabled database daemons did not return after server restart')
    assert not daemon_pids(reader.node)
    # Supervised daemons must keep running, beyond the check's 5 s time limit.
    started = daemon_pids(source.node), daemon_pids(destination.node)
    time.sleep(7)
    assert (daemon_pids(source.node), daemon_pids(destination.node)) == started
    for replica in cluster.replicas:
        assert replica.execute('SELECT * FROM data.root ORDER BY id') == expected

    # The launcher itself is supervised by PostgreSQL, then recovers a killed
    # SQL daemon. Its lock must prevent duplicate healthy daemons.
    launcher = source.execute("SELECT pid FROM pg_stat_activity WHERE backend_type = 'pgwrh supervisor'")
    assert len(launcher) == 1
    source.execute(f'SELECT pg_terminate_backend({launcher[0][0]})')
    wait_until(lambda: source.execute("SELECT pid FROM pg_stat_activity WHERE backend_type = 'pgwrh supervisor'")
               not in ([], launcher), timeout=10, message='launcher did not restart')
    previous = daemon_pids(source.node)
    source.execute(f'SELECT pg_terminate_backend({previous[0][0]})')
    wait_until(lambda: len(daemon_pids(source.node)) == 1 and daemon_pids(source.node) != previous,
               timeout=20, message='daemon did not restart after termination')
    for _ in range(3):
        source.execute('SELECT pgwrh.start_sync_daemon(0.1)')
    time.sleep(2)
    assert len(daemon_pids(source.node)) == 1
    assert not daemon_pids(reader.node)
    cluster.master.node.start()
    reader.execute('SELECT pgwrh.start_sync_daemon(0.1)')
    wait_until(lambda: len(daemon_pids(reader.node)) == 1, timeout=20, message='explicit start failed')


def test_disabled_daemon_stays_disabled_on_ping_and_restart(master, postgres_node_factory):
    spec = ReplicaSpec('disabled')
    username, password = master.create_replica_login(spec)
    replica = ReplicaHandle(spec, postgres_node_factory('disabled'), username, password)
    replica.configure_controller(master_port=master.port, start_daemon=False)
    replica.node.stop()
    replica.node.start()
    # Invoke the same trigger used by replication, with durable intent disabled.
    replica.execute("SET session_replication_role = replica; INSERT INTO pgwrh.ping DEFAULT VALUES")
    time.sleep(2)
    assert replica.execute('SELECT enabled FROM pgwrh.sync_daemon_config') == [(False,)]
    assert not daemon_pids(replica.node)


def test_daemon_ignores_caller_statement_timeout(postgres_node_factory):
    # pg_background copies the launching session's settings into the daemon.
    node = postgres_node_factory('daemon_timeout')
    with node.connect() as conn:
        conn.execute("SET statement_timeout = '1s'")
        conn.execute('SELECT pgwrh.start_sync_daemon(0.5)')
        conn.commit()
    wait_until(lambda: len(daemon_pids(node)) == 1, timeout=20, message='daemon did not start')
    started = daemon_pids(node)
    time.sleep(3)
    assert daemon_pids(node) == started


def test_daemon_start_follows_the_calling_transaction(postgres_node_factory):
    # Without the supervisor, nothing restarts a daemon that gave up because
    # it could not yet see its launcher's uncommitted start.
    node = postgres_node_factory('start_in_transaction', preload=False)
    with node.connect() as conn:
        conn.execute('SELECT pgwrh.start_sync_daemon(0.5)')
        time.sleep(1)
        conn.rollback()
        time.sleep(1)
        assert not daemon_pids(node)
        conn.execute('SELECT pgwrh.start_sync_daemon(0.5)')
        time.sleep(1)
        conn.commit()
    wait_until(lambda: len(daemon_pids(node)) == 1, timeout=20,
               message='daemon started inside a transaction did not run after commit')


@pytest.mark.parametrize('commit', [False, True], ids=['rollback', 'commit'])
def test_daemon_start_uses_committed_settings(postgres_node_factory, commit):
    # Enabled settings can outlive a crashed daemon. Disable supervision so it
    # cannot mask whether this launch uses the committed settings after waiting.
    node = postgres_node_factory('committed_daemon_settings', preload=False)
    previous_seconds, launch_seconds = (30, 0.25) if commit else (0.25, 30)
    node.execute(f"""INSERT INTO pgwrh.sync_daemon_config
        VALUES (true, true, {previous_seconds}, 'previous')""")
    # Observe actual loop iterations without requiring a controller or relying
    # on how long reconciliation itself takes.
    node.execute('''CREATE TABLE public.daemon_ticks(pid integer, application_name text);
        CREATE OR REPLACE PROCEDURE pgwrh.sync_replica_worker() LANGUAGE plpgsql AS $$
        BEGIN
            INSERT INTO public.daemon_ticks VALUES (pg_backend_pid(), current_setting('application_name'));
        END $$;
    ''')
    with node.connect() as conn:
        conn.execute(f"SELECT pgwrh.start_sync_daemon({launch_seconds}, 'requested')")
        wait_until(lambda: node.execute("""SELECT count(*) FROM pg_locks
            WHERE relation = 'pgwrh.sync_daemon_config'::regclass
              AND mode = 'ShareLock' AND NOT granted""") == [(1,)],
            timeout=10, message='daemon did not wait for the configuration transaction')
        assert node.execute('SELECT * FROM public.daemon_ticks') == []
        if commit:
            conn.commit()
        else:
            conn.rollback()

    expected_name = 'requested' if commit else 'previous'
    assert node.execute('SELECT refresh_seconds, application_name FROM pgwrh.sync_daemon_config') == [
        (0.25, expected_name),
    ]
    wait_until(lambda: bool(node.execute('SELECT * FROM public.daemon_ticks')),
               timeout=5, message='daemon did not start after the configuration transaction ended')
    started = node.execute('SELECT DISTINCT pid, application_name FROM public.daemon_ticks')
    assert len(started) == 1 and started[0][1] == expected_name
    # The losing settings use a 30 s interval; two ticks within 5 s prove that
    # the daemon also loaded the committed refresh interval.
    wait_until(lambda: node.execute('SELECT count(*) FROM public.daemon_ticks')[0][0] >= 2,
               timeout=5, message='daemon did not use the committed refresh interval')
    assert node.execute('SELECT DISTINCT pid, application_name FROM public.daemon_ticks') == started


def test_supervisor_ignores_operators_created_by_database_owner(postgres_node_factory):
    """Checks run as a superuser in every database, including owner-controlled ones."""
    node = postgres_node_factory('untrusted_owner', install_extension=False)
    node.execute('CREATE ROLE app_owner LOGIN')
    node.execute('CREATE DATABASE app OWNER app_owner')
    node.execute('CREATE EXTENSION pgwrh CASCADE', dbname='app')
    # The owner controls public, which the default search_path includes. Record
    # every call of an operator matching the check query's oid-regclass
    # comparisons, answering as the built-in operator would.
    node.execute('''CREATE TABLE public.calls(caller name);
        CREATE FUNCTION public.recording_eq(oid, regclass) RETURNS boolean LANGUAGE plpgsql AS $$
        BEGIN
            INSERT INTO public.calls VALUES (current_user);
            RETURN $1 OPERATOR(pg_catalog.=) $2::oid;
        END $$;
        CREATE OPERATOR public.= (LEFTARG = oid, RIGHTARG = regclass, FUNCTION = public.recording_eq);
    ''', dbname='app', username='app_owner')
    # Restored drop protection proves that a check ran after the operator existed.
    node.execute(ERASE_PGWRH_MARKERS, dbname='app')
    wait_until(lambda: node.execute(PGWRH_MARKERS, dbname='app') == [(1,)], timeout=20,
               message='supervisor did not check the database')
    assert node.execute('SELECT caller FROM public.calls', dbname='app') == []


def test_extension_works_without_supervisor_preload(postgres_node_factory):
    # Only supervision needs the preload; the library also loads on demand.
    node = postgres_node_factory('no_preload', preload=False)
    assert node.execute("SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'pgwrh supervisor'") == [(0,)]
    node.execute('''CREATE TABLE public.managed(id integer);
        SELECT pgwrh.add_ext_dependency('pg_class', 'public.managed'::regclass)''')
    with node.connect() as conn:
        conn.execute(ERASE_PGWRH_MARKERS)
        conn.execute('SELECT pgwrh.repair_managed_objects()')
        assert conn.execute(PGWRH_MARKERS) == [(2,)]
        conn.commit()
