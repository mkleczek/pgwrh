import pytest
from testgres.connection import DatabaseError

from conftest import eventually
from helpers import reader


def last_commit(conn):
    return conn.execute("SELECT pgwrh.last_commit_lsn()::text")[0][0]


def lsn_value(lsn):
    high, low = lsn.split("/")
    return (int(high, 16) << 32) + int(low, 16)


def test_initial_state_and_uncommitted_writes(nodes):
    node = nodes("initial-token")
    node.execute("CREATE TABLE writes(id int)")
    with node.connect(autocommit=True) as conn:
        assert last_commit(conn) is None
        conn.execute("BEGIN READ ONLY")
        conn.execute("SELECT * FROM writes")
        conn.execute("COMMIT")
        assert last_commit(conn) is None
        conn.execute("BEGIN")
        conn.execute("INSERT INTO writes VALUES(1)")
        assert last_commit(conn) is None
        conn.execute("ROLLBACK")
        assert last_commit(conn) is None
        conn.execute("BEGIN")
        conn.execute("INSERT INTO writes VALUES(2)")
        conn.execute("COMMIT")
        assert lsn_value(last_commit(conn)) > 0
    # A new physical connection has no token, even in the same database.
    with node.connect(autocommit=True) as conn:
        assert last_commit(conn) is None


@pytest.mark.parametrize("synchronous_commit", ["on", "off"])
def test_token_survives_reads_rollbacks_and_failed_commits(nodes, synchronous_commit):
    node = nodes("token-lifetime")
    node.execute("CREATE TABLE writes(id int UNIQUE DEFERRABLE INITIALLY DEFERRED)")
    with node.connect(autocommit=True) as conn:
        conn.execute(f"SET synchronous_commit = {synchronous_commit}")
        conn.execute("INSERT INTO writes VALUES(1)")
        token = last_commit(conn)
        assert lsn_value(token) > 0
        conn.execute("PREPARE get_token AS SELECT pgwrh.last_commit_lsn()::text")
        for begin in ("BEGIN", "BEGIN READ ONLY", "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY"):
            conn.execute(begin)
            assert conn.execute("SELECT * FROM writes") == [(1,)]
            assert last_commit(conn) == token
            conn.execute("COMMIT")
            assert last_commit(conn) == token
        conn.execute("BEGIN")
        conn.execute("INSERT INTO writes VALUES(2)")
        assert last_commit(conn) == token
        conn.execute("ROLLBACK")
        assert last_commit(conn) == token
        conn.execute("BEGIN")
        conn.execute("INSERT INTO writes VALUES(1)")
        with pytest.raises(DatabaseError, match="duplicate key"):
            conn.execute("COMMIT")
        assert last_commit(conn) == token
        conn.execute("BEGIN")
        conn.execute("SAVEPOINT child")
        conn.execute("INSERT INTO writes VALUES(3)")
        conn.execute("RELEASE SAVEPOINT child")
        assert last_commit(conn) == token
        conn.execute("SAVEPOINT aborted_child")
        conn.execute("INSERT INTO writes VALUES(4)")
        conn.execute("ROLLBACK TO SAVEPOINT aborted_child")
        assert last_commit(conn) == token
        conn.execute("COMMIT")
        next_token = last_commit(conn)
        assert lsn_value(next_token) > lsn_value(token)
        assert conn.execute("EXECUTE get_token") == [(next_token,)]
        assert conn.execute("SELECT id FROM writes ORDER BY id") == [(1,), (3,)]


@pytest.mark.parametrize("committed_write", [False, True])
def test_wal_without_a_commit_record_preserves_token(nodes, committed_write):
    node = nodes("nontransactional-wal")
    node.execute("CREATE TABLE writes(id int)")
    with node.connect(autocommit=True) as conn:
        if committed_write:
            conn.execute("INSERT INTO writes VALUES(1)")
        token = last_commit(conn)
        conn.execute("BEGIN READ ONLY")
        # Like pruning WAL from a read, this produces WAL without an XID or
        # commit record. PostgreSQL still updates XactLastCommitEnd at COMMIT.
        conn.execute("SELECT pg_logical_emit_message(false, 'token-test', 'wal')")
        assert conn.execute("SELECT pg_current_xact_id_if_assigned()") == [(None,)]
        conn.execute("COMMIT")
        assert last_commit(conn) == token


def test_concurrent_sessions_keep_their_own_commit_end(nodes):
    node = nodes("concurrent-tokens")
    node.execute("CREATE TABLE writes(id int)")
    with node.connect(autocommit=True) as first, node.connect(autocommit=True) as second:
        first.execute("BEGIN")
        second.execute("BEGIN")
        first.execute("INSERT INTO writes VALUES(1)")
        second.execute("INSERT INTO writes VALUES(2)")
        first.execute("COMMIT")
        first_token = last_commit(first)
        assert last_commit(second) is None
        second.execute("COMMIT")
        second_token = last_commit(second)
        assert lsn_value(second_token) > lsn_value(first_token)
        assert last_commit(first) == first_token
        first.execute("INSERT INTO writes VALUES(3)")
        assert lsn_value(last_commit(first)) > lsn_value(second_token)
        assert last_commit(second) == second_token


def test_ordinary_writer_can_obtain_token(nodes):
    node = nodes("ordinary-writer")
    node.execute("""CREATE TABLE writes(id int);
        CREATE ROLE writer LOGIN;
        GRANT USAGE ON SCHEMA pgwrh TO writer;
        GRANT INSERT ON writes TO writer""")
    with node.connect(username="writer", autocommit=True) as conn:
        assert last_commit(conn) is None
        conn.execute("INSERT INTO writes VALUES(1)")
        assert lsn_value(last_commit(conn)) > 0


def test_commit_token_requires_preload(nodes):
    node = nodes("no-commit-hook", preload=False)
    with pytest.raises(DatabaseError, match="shared_preload_libraries"):
        node.execute("SELECT pgwrh.last_commit_lsn()")


def test_idle_subscription_reaches_exact_commit_token(pair):
    publisher, subscriber = pair
    with publisher.connect(autocommit=True) as writer:
        writer.execute("BEGIN")
        writer.execute("INSERT INTO data VALUES(1, 'application write')")
        writer.execute("COMMIT")
        # Another backend creates unrelated WAL before token retrieval.
        publisher.execute("CREATE TABLE unrelated(id int); INSERT INTO unrelated VALUES(1)")
        target = last_commit(writer)
        overshoot = writer.execute("SELECT pg_current_wal_insert_lsn()::text")[0][0]
        assert lsn_value(overshoot) > lsn_value(target)
    # No further published transaction: the application can disconnect and
    # carry its token to a replica without contacting the controller again.
    subscriber.execute(f"SELECT pgwrh.wait_for_lsn('sub', '{target}', 5000)")
    assert subscriber.execute("SELECT pgwrh.applied_lsn('sub')::text") == [(target,)]
    assert subscriber.execute("SELECT value FROM data WHERE id=1") == [("application write",)]
    with reader(subscriber, target) as result:
        assert result.result(timeout=10) == [(0,), (1,)]
    with pytest.raises(DatabaseError, match="timed out"):
        subscriber.execute(f"SELECT pgwrh.wait_for_lsn('sub', '{overshoot}', 100)")
    # Receiving unrelated WAL cannot move the commit visibility watermark.
    eventually(lambda: subscriber.execute(f"""SELECT EXISTS(
        SELECT FROM pg_stat_subscription WHERE received_lsn >= '{overshoot}')""")[0][0])
    assert subscriber.execute("SELECT pgwrh.applied_lsn('sub')::text") == [(target,)]
