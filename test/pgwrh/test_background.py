from __future__ import annotations

from .pgwrh_testkit import wait_until


def assert_background_execution(node):
    """Exercise the same worker API after fresh installation and upgrade."""
    role = node.execute('SELECT quote_ident(pgwrh.pgwrh_replica_role_name())')[0][0]
    node.execute(f'CREATE SCHEMA worker_test AUTHORIZATION {role}')
    with node.connect() as conn:
        conn.execute(f'SET ROLE {role}')
        conn.execute("SET statement_timeout = '10s'")
        conn.execute("SELECT pgwrh.bg_exec_wait('CREATE TABLE worker_test.done (value int)')")
        conn.execute("SELECT pgwrh.bg_exec_wait('INSERT INTO worker_test.done VALUES (1)')")
        assert conn.execute('SELECT * FROM worker_test.done') == [(1,)]
        assert conn.execute("SELECT pgwrh.bg_query_bool('SELECT true')") == [(True,)]
        assert conn.execute("SELECT pgwrh.bg_query_bool('SELECT false')") == [(False,)]
        # More than the default response queue: consuming results must make
        # progress while the worker is producing them.
        conn.execute("""SELECT pgwrh.bg_exec_wait(
            'SELECT repeat(''x'', 1024) FROM generate_series(1, 1024)')""")
        conn.execute("""
            DO $$
            BEGIN
                PERFORM pgwrh.bg_exec_wait('SELECT 1 / 0');
                RAISE EXCEPTION 'worker failure was lost';
            -- pg_background 1.x can report an early SQL error as a lost worker
            -- connection. Both must reach the caller instead of succeeding.
            EXCEPTION WHEN division_by_zero OR connection_failure THEN
                NULL;
            END
            $$
        """)
        assert conn.execute("SELECT pgwrh.exec_script('SELECT 1 / 0')") == [(False,)]
        assert conn.execute("""SELECT pgwrh.exec_non_tx_scripts(ARRAY[
            'INSERT INTO worker_test.done VALUES (2)',
            'SELECT 1 / 0',
            'INSERT INTO worker_test.done VALUES (3)'
        ])""") == [(False,)]
        assert conn.execute('SELECT * FROM worker_test.done ORDER BY value') == [(1,), (2,)]
        # A caught error must not leave the session unable to launch another job.
        assert conn.execute("SELECT pgwrh.bg_query_bool('SELECT true')") == [(True,)]


def test_background_commands_preserve_errors_and_replica_role_permissions(postgres_node_factory):
    assert_background_execution(postgres_node_factory('background'))


def test_detached_worker_finishes_after_launcher_rollback(postgres_node_factory):
    node = postgres_node_factory('background_detached')
    node.execute('CREATE TABLE public.detached_done (value int)')
    with node.connect() as conn:
        conn.execute('BEGIN')
        conn.execute("""SELECT pgwrh.launch_in_background(
            'SELECT pg_sleep(0.2); INSERT INTO public.detached_done VALUES (1)')""")
        conn.rollback()
    wait_until(lambda: node.execute('SELECT * FROM public.detached_done') == [(1,)],
               timeout=10, message='detached worker did not finish after launcher rollback')
