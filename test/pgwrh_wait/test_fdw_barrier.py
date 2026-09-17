"""Exercise the real receiving wait hook through direct and virtual FDW routes."""
from concurrent.futures import ThreadPoolExecutor

import pytest
from testgres.connection import DatabaseError

from conftest import eventually
from helpers import apply_blocked, barrier, block_apply, token, waiting


@pytest.fixture(params=['direct', 'virtual'])
def routed_pair(pair, nodes, request):
    publisher, subscriber = pair
    # This coordinator has no local subscription. Its custom settings are
    # placeholders; only the receiving subscriber may certify visibility.
    router = nodes('router', preload=False)
    router.execute(f"""
        CREATE EXTENSION pgwrh_fdw;
        CREATE SERVER actual FOREIGN DATA WRAPPER pgwrh_fdw OPTIONS
            (host '127.0.0.1', port '{subscriber.port}', dbname 'postgres',
             application_name 'fdw-barrier-test',
             transaction_parameters 'pgwrh.read_after_subscription,pgwrh.wait_timeout_ms,pgwrh.read_after_lsn');
        CREATE USER MAPPING FOR CURRENT_USER SERVER actual;
        CREATE SERVER routed FOREIGN DATA WRAPPER pgwrh_fdw OPTIONS (members 'actual');
        CREATE USER MAPPING FOR CURRENT_USER SERVER routed;
        CREATE FOREIGN TABLE remote_data (id int, value text)
            SERVER {'actual' if request.param == 'direct' else 'routed'}
            OPTIONS (schema_name 'public', table_name 'data');
    """)
    return publisher, subscriber, router


def read(conn, target, statement, timeout=5000):
    conn.execute('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY')
    try:
        barrier(conn, target, timeout=timeout)
        rows = conn.execute(statement)
        conn.execute('COMMIT')
        return rows
    except BaseException:
        conn.execute('ROLLBACK')
        raise


def test_remote_snapshot_waits_for_actual_apply(routed_pair):
    publisher, subscriber, router = routed_pair
    with router.connect(autocommit=True) as conn, ThreadPoolExecutor(max_workers=1) as pool:
        with block_apply(subscriber) as lock:
            target = token(publisher)
            apply_blocked(subscriber)
            future = pool.submit(read, conn, target,
                                 'SELECT id, value FROM remote_data WHERE id > 0 ORDER BY id')
            waiting(subscriber, 'fdw-barrier-test')
            assert not future.done()
            lock.rollback()
            assert future.result(timeout=10) == [(1, 'write-1')]


def test_prepared_reads_reapply_barrier_on_reuse_and_reconnect(routed_pair):
    publisher, subscriber, router = routed_pair
    with router.connect(autocommit=True) as conn, ThreadPoolExecutor(max_workers=1) as pool:
        conn.execute('SET plan_cache_mode = force_generic_plan')
        conn.execute('PREPARE read_row(int) AS SELECT id, value FROM remote_data WHERE id = $1')
        previous_pid = None
        for ident in (1, 2, 3):
            if ident == 3:
                # Kill the idle cached connection; the same prepared statement
                # must establish a new remote transaction with a fresh barrier.
                subscriber.execute(f'SELECT pg_terminate_backend({previous_pid}, 5000)')
            with block_apply(subscriber) as lock:
                target = token(publisher, ident)
                apply_blocked(subscriber)
                future = pool.submit(read, conn, target, f'EXECUTE read_row({ident})')
                waiting(subscriber, 'fdw-barrier-test')
                pid = subscriber.execute("""SELECT pid FROM pg_stat_activity
                    WHERE application_name = 'fdw-barrier-test'""")[0][0]
                if ident == 2:
                    assert pid == previous_pid
                elif ident == 3:
                    assert pid != previous_pid
                assert not future.done()
                lock.rollback()
                assert future.result(timeout=10) == [(ident, f'write-{ident}')]
                previous_pid = pid


def test_remote_timeout_errors_instead_of_returning_stale_rows(routed_pair):
    publisher, subscriber, router = routed_pair
    with router.connect(autocommit=True) as conn:
        with block_apply(subscriber):
            target = token(publisher)
            apply_blocked(subscriber)
            with pytest.raises(DatabaseError, match='timed out waiting'):
                read(conn, target, 'SELECT id FROM remote_data ORDER BY id', timeout=100)
        # The failed transaction must not poison a later successful transaction
        # or keep its old timeout/watermark on a reused physical connection.
        assert read(conn, target, 'SELECT id FROM remote_data ORDER BY id') == [(0,), (1,)]


def test_each_remote_participant_must_reach_the_watermark(routed_pair, nodes):
    publisher, first, router = routed_pair
    second = nodes('second_subscriber')
    second.execute('CREATE TABLE data(id int PRIMARY KEY, value text)')
    second.execute(f"""CREATE SUBSCRIPTION sub CONNECTION
        'host=127.0.0.1 port={publisher.port} dbname=postgres' PUBLICATION pub
        WITH (slot_name='second_sub')""")
    eventually(lambda: second.execute('SELECT count(*) FROM data') == [(1,)])
    eventually(lambda: second.execute("SELECT bool_and(srsubstate = 'r') FROM pg_subscription_rel") == [(True,)])
    router.execute(f"""CREATE SERVER second FOREIGN DATA WRAPPER pgwrh_fdw OPTIONS
        (host '127.0.0.1', port '{second.port}', dbname 'postgres',
         application_name 'second-barrier-test',
         transaction_parameters 'pgwrh.read_after_subscription,pgwrh.wait_timeout_ms,pgwrh.read_after_lsn');
        CREATE USER MAPPING FOR CURRENT_USER SERVER second;
        CREATE FOREIGN TABLE remote_second(id int, value text) SERVER second
            OPTIONS (schema_name 'public', table_name 'data');""")
    with router.connect(autocommit=True) as conn, ThreadPoolExecutor(max_workers=1) as pool:
        with block_apply(second) as lock:
            target = token(publisher)
            apply_blocked(second)
            eventually(lambda: first.execute(f"SELECT pgwrh.applied_lsn('sub') >= '{target}'") == [(True,)])
            future = pool.submit(read, conn, target, '''SELECT a.id, a.value, b.value
                FROM remote_data a JOIN remote_second b USING (id) WHERE a.id = 1''')
            waiting(second, 'second-barrier-test')
            assert not future.done()  # the first participant's progress is insufficient
            lock.rollback()
            assert future.result(timeout=10) == [(1, 'write-1', 'write-1')]
