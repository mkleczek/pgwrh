import time

import pytest
from testgres.connection import DatabaseError

from conftest import eventually
from helpers import apply_blocked, barrier, block_apply, reader, token, waiting


def test_release_has_only_fresh_installation_version(nodes):
    node = nodes("release")
    node.execute("CREATE EXTENSION pgwrh CASCADE")
    expected = [("pgwrh", "1.0.0"), ("pgwrh_fdw", "1.0.0"), ("pgwrh_wait", "1.0.0")]
    assert node.execute("""SELECT extname, extversion FROM pg_extension
        WHERE extname IN ('pgwrh', 'pgwrh_fdw', 'pgwrh_wait')
        ORDER BY extname""") == expected
    assert node.execute("""SELECT name, version FROM pg_available_extension_versions
        WHERE name IN ('pgwrh', 'pgwrh_fdw', 'pgwrh_wait')
        ORDER BY name, version""") == expected
    assert node.execute("""SELECT e.extname, p.source, p.target
        FROM pg_extension e CROSS JOIN LATERAL pg_extension_update_paths(e.extname) p
        WHERE e.extname IN ('pgwrh', 'pgwrh_fdw', 'pgwrh_wait')""") == []


@pytest.mark.parametrize("isolation", ["READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"])
def test_wait_before_snapshot(pair, isolation):
    publisher, subscriber = pair
    with block_apply(subscriber) as lock:
        target = token(publisher)
        apply_blocked(subscriber)
        with reader(subscriber, target, isolation) as result:
            waiting(subscriber)
            assert not result.done()
            lock.rollback()
            assert result.result(timeout=10) == [(0,), (1,)]


def test_commit_is_the_visibility_boundary(pair):
    publisher, subscriber = pair
    subscriber.execute("""CREATE FUNCTION delay_commit() RETURNS trigger
        LANGUAGE plpgsql AS $$BEGIN
        PERFORM pg_advisory_xact_lock(71531); RETURN NULL; END$$;
        CREATE CONSTRAINT TRIGGER delayed_commit AFTER INSERT ON data
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION delay_commit();
        ALTER TABLE data ENABLE REPLICA TRIGGER delayed_commit""")
    before = subscriber.execute("SELECT pgwrh.applied_lsn('sub')")[0][0]
    with subscriber.connect() as lock:
        lock.execute("SELECT pg_advisory_xact_lock(71531)")
        target = token(publisher)
        apply_blocked(subscriber)
        assert subscriber.execute("SELECT pgwrh.applied_lsn('sub')")[0][0] == before
        assert subscriber.execute("SELECT id FROM data ORDER BY id") == [(0,)]
        with reader(subscriber, target) as result:
            waiting(subscriber)
            assert not result.done()
            lock.rollback()
            assert result.result(timeout=10) == [(0,), (1,)]


def test_sql_wait_refreshes_only_next_statement(pair):
    publisher, subscriber = pair
    with subscriber.connect() as lock:
        lock.execute("LOCK data IN SHARE MODE")
        target = token(publisher)
        from concurrent.futures import ThreadPoolExecutor
        def run():
            with subscriber.connect() as conn:
                conn.execute("SET application_name='wait-test'")
                # The subquery uses the original statement snapshot.
                old = conn.execute(f"""SELECT pgwrh.wait_for_lsn('sub', '{target}'),
                    (SELECT count(*) FROM data)""")[0][1]
                new = conn.execute("SELECT count(*) FROM data")[0][0]
                return old, new
        with ThreadPoolExecutor(max_workers=1) as pool:
            result = pool.submit(run)
            waiting(subscriber)
            lock.rollback()
            assert result.result(timeout=10) == (1, 2)


def test_immediate_success_and_guc_cleanup(pair):
    _, subscriber = pair
    target = subscriber.execute("SELECT pgwrh.applied_lsn('sub')")[0][0]
    with subscriber.connect(autocommit=True) as conn:
        for ending in ["COMMIT", "ROLLBACK"]:
            conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
            barrier(conn, target, timeout=0)
            barrier(conn, target, timeout=0)
            assert conn.execute("SELECT count(*) FROM data") == [(1,)]
            conn.execute(ending)
            assert conn.execute("SHOW pgwrh.read_after_lsn") == [("",)]
            assert conn.execute("SHOW pgwrh.read_after_subscription") == [("pgwrh_replica_subscription",)]


@pytest.mark.parametrize("isolation", ["READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"])
def test_reject_barrier_after_snapshot(pair, isolation):
    _, subscriber = pair
    with subscriber.connect(autocommit=True) as conn:
        conn.execute(f"BEGIN ISOLATION LEVEL {isolation}")
        conn.execute("SELECT 1")
        with pytest.raises(DatabaseError, match="before the first transaction snapshot"):
            barrier(conn, "0/1")


@pytest.mark.parametrize("isolation", ["REPEATABLE READ", "SERIALIZABLE"])
def test_sql_wait_rejects_fixed_snapshot(pair, isolation):
    _, subscriber = pair
    with subscriber.connect(autocommit=True) as conn:
        conn.execute(f"BEGIN ISOLATION LEVEL {isolation}")
        with pytest.raises(DatabaseError, match="cannot refresh"):
            conn.execute("SELECT pgwrh.wait_for_lsn('sub', '0/1')")


@pytest.mark.parametrize("statement,pattern", [
    ("SET pgwrh.read_after_lsn='0/1'", "top-level SET LOCAL"),
    ("SELECT set_config('pgwrh.read_after_lsn','0/1',true)", "invalid value"),
    ("SET LOCAL pgwrh.read_after_lsn='nonsense'", "invalid value"),
    ("SET LOCAL pgwrh.read_after_lsn='0/0'", "invalid value"),
    ("SAVEPOINT s; SET LOCAL pgwrh.read_after_lsn='0/1'", "top-level SET LOCAL"),
    ("DO $$BEGIN SET LOCAL pgwrh.read_after_lsn='0/1'; END$$", "top-level SET LOCAL"),
])
def test_reject_invalid_barrier_forms(pair, statement, pattern):
    _, subscriber = pair
    with subscriber.connect(autocommit=True) as conn:
        conn.execute("BEGIN")
        with pytest.raises(DatabaseError, match=pattern):
            conn.execute(statement)


def test_barrier_requires_explicit_transaction(pair):
    _, subscriber = pair
    with pytest.raises(DatabaseError, match="explicit transaction"):
        subscriber.execute("SET LOCAL pgwrh.read_after_lsn='0/1'")


def test_timeout_and_recovery(pair):
    _, subscriber = pair
    with subscriber.connect(autocommit=True) as conn:
        conn.execute("BEGIN")
        started = time.monotonic()
        with pytest.raises(DatabaseError, match="timed out waiting"):
            barrier(conn, "FFFF/FFFFFFFF", timeout=100)
        assert 0.08 <= time.monotonic() - started < 3
        conn.execute("ROLLBACK")
        conn.execute("BEGIN")
        barrier(conn, "0/1", timeout=0)
        conn.execute("COMMIT")
        assert conn.execute("SHOW pgwrh.read_after_lsn") == [("",)]


def test_statement_timeout(pair):
    _, subscriber = pair
    with subscriber.connect() as conn:
        conn.execute("SET statement_timeout='100ms'")
        with pytest.raises(DatabaseError, match="statement timeout"):
            barrier(conn, "FFFF/FFFFFFFF", timeout=5000)


def test_cancel_and_subsequent_wait(pair):
    _, subscriber = pair
    with reader(subscriber, "FFFF/FFFFFFFF") as result:
        waiting(subscriber)
        assert subscriber.execute("""SELECT pg_cancel_backend(pid)
            FROM pg_stat_activity WHERE application_name='wait-test'""") == [(True,)]
        with pytest.raises(DatabaseError, match="user request"):
            result.result(timeout=5)
    with reader(subscriber, "0/1") as result:
        assert result.result(timeout=5) == [(0,)]


def test_many_waiters(pair):
    publisher, subscriber = pair
    from contextlib import ExitStack
    with block_apply(subscriber) as lock, ExitStack() as stack:
        target = token(publisher)
        results = [stack.enter_context(reader(subscriber, target, name=f"wait-{i}"))
                   for i in range(8)]
        for i in range(8):
            waiting(subscriber, f"wait-{i}")
        lock.rollback()
        assert all(result.result(timeout=10) == [(0,), (1,)] for result in results)


def test_ordinary_sessions_cannot_publish_watermarks(pair):
    publisher, subscriber = pair
    before = subscriber.execute("SELECT pgwrh.applied_lsn('sub')")[0][0]
    subscriber.execute("""SELECT pg_replication_origin_create('fake');
        SELECT pg_replication_origin_session_setup('fake');
        SELECT pg_replication_origin_xact_setup('FFFF/FFFFFFFF', now());
        INSERT INTO data VALUES (99, 'local')""")
    assert subscriber.execute("SELECT pgwrh.applied_lsn('sub')")[0][0] == before


@pytest.mark.parametrize("query,pattern", [
    ("SELECT pgwrh.wait_for_lsn('missing','0/1',0)", "does not exist"),
    ("SELECT pgwrh.wait_for_lsn('sub','0/0',0)", "nonzero LSN"),
    ("SELECT pgwrh.wait_for_lsn('sub','0/1',-1)", "nonnegative timeout"),
])
def test_invalid_sql_arguments(pair, query, pattern):
    _, subscriber = pair
    with pytest.raises(DatabaseError, match=pattern):
        subscriber.execute(query)


def test_read_only_non_superuser(pair):
    _, subscriber = pair
    subscriber.execute("CREATE ROLE reader LOGIN; GRANT USAGE ON SCHEMA pgwrh TO reader")
    with subscriber.connect(username="reader", autocommit=True) as conn:
        conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        barrier(conn, "0/1", timeout=0)
        conn.execute("COMMIT")


def test_imported_snapshot_cannot_be_repaired(pair):
    _, subscriber = pair
    with subscriber.connect() as exporter, subscriber.connect(autocommit=True) as conn:
        snapshot = exporter.execute("SELECT pg_export_snapshot()")[0][0]
        conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        conn.execute(f"SET TRANSACTION SNAPSHOT '{snapshot}'")
        with pytest.raises(DatabaseError, match="before the first transaction snapshot"):
            barrier(conn, "0/1")


def test_default_subscription_and_reset_all(pair):
    _, subscriber = pair
    subscriber.execute("ALTER SUBSCRIPTION sub RENAME TO pgwrh_replica_subscription")
    with subscriber.connect(autocommit=True) as conn:
        conn.execute("BEGIN")
        conn.execute("SET LOCAL pgwrh.read_after_lsn='0/1'")
        conn.execute("RESET ALL")
        assert conn.execute("SHOW pgwrh.read_after_lsn") == [("0/1",)]
        conn.execute("ROLLBACK")
        assert conn.execute("SHOW pgwrh.read_after_lsn") == [("",)]


def test_reused_reader_connection_sees_each_committed_write(pair):
    publisher, subscriber = pair
    with subscriber.connect(autocommit=True) as conn:
        for ident in range(1, 31):
            target = token(publisher, ident)
            conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            barrier(conn, target)
            assert conn.execute(f"SELECT value FROM data WHERE id={ident}") == [(f"write-{ident}",)]
            conn.execute("COMMIT")
