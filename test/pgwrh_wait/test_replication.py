import pytest
from testgres.connection import DatabaseError

from conftest import eventually
from helpers import apply_blocked, barrier, block_apply, reader, token, waiting


@pytest.mark.parametrize("streaming", ["on", "parallel"])
def test_streaming_commit_and_abort(pair, streaming):
    publisher, subscriber = pair
    subscriber.execute(f"ALTER SUBSCRIPTION sub SET (streaming='{streaming}')")
    with block_apply(subscriber) as lock:
        publisher.execute("""INSERT INTO data SELECT i, repeat(md5(i::text),100)
            FROM generate_series(1,2000) i""")
        target = publisher.execute("SELECT pg_current_wal_insert_lsn()::text")[0][0]
        publisher.execute("UPDATE data SET value='heartbeat' WHERE id=0")
        apply_blocked(subscriber)
        if streaming == "parallel":
            eventually(lambda: subscriber.execute("""SELECT EXISTS(
                SELECT FROM pg_stat_subscription WHERE worker_type='parallel apply')""")[0][0])
        with reader(subscriber, target) as result:
            waiting(subscriber)
            lock.rollback()
            assert len(result.result(timeout=15)) == 2001
    with publisher.connect() as writer:
        writer.execute("""INSERT INTO data SELECT i, repeat(md5(i::text),100)
            FROM generate_series(3000,5000) i""")
        writer.rollback()
    target = token(publisher, 6000)
    with reader(subscriber, target) as result:
        rows = result.result(timeout=15)
        assert len(rows) == 2002
        assert rows[-1] == (6000,)


def test_worker_restart_during_wait(pair):
    publisher, subscriber = pair
    with block_apply(subscriber) as lock:
        target = token(publisher)
        apply_blocked(subscriber)
        pid = subscriber.execute("SELECT pid FROM pg_stat_subscription WHERE worker_type='apply'")[0][0]
        with reader(subscriber, target) as result:
            waiting(subscriber)
            subscriber.execute(f"SELECT pg_terminate_backend({pid})")
            eventually(lambda: subscriber.execute(f"""SELECT EXISTS(
                SELECT FROM pg_stat_subscription WHERE worker_type='apply'
                AND pid IS NOT NULL AND pid <> {pid})""")[0][0])
            apply_blocked(subscriber)
            assert not result.done()
            lock.rollback()
            assert result.result(timeout=10) == [(0,), (1,)]


def test_crash_restart_bootstraps_durable_progress(pair):
    _, subscriber = pair
    before = subscriber.execute("SELECT pgwrh.applied_lsn('sub')")[0][0]
    subscriber.execute("CHECKPOINT")
    subscriber.stop(['-m', 'immediate'])
    subscriber.start()
    with reader(subscriber, before) as result:
        assert result.result(timeout=10) == [(0,)]


def test_two_subscribers_wait_independently(pair, nodes):
    publisher, first = pair
    second = nodes("second")
    second.execute("CREATE TABLE data(id int PRIMARY KEY, value text)")
    second.execute(f"""CREATE SUBSCRIPTION sub CONNECTION
        'host=127.0.0.1 port={publisher.port} dbname=postgres'
        PUBLICATION pub WITH (slot_name='second',copy_data=true)""")
    eventually(lambda: second.execute("SELECT count(*) FROM data")[0][0] == 1)
    eventually(lambda: second.execute("SELECT bool_and(srsubstate='r') FROM pg_subscription_rel")[0][0])
    with block_apply(second) as lock:
        target = token(publisher)
        with reader(first, target) as fast, reader(second, target) as slow:
            assert fast.result(timeout=10) == [(0,), (1,)]
            waiting(second)
            assert not slow.done()
            lock.rollback()
            assert slow.result(timeout=10) == [(0,), (1,)]


def test_subscription_identity_does_not_follow_reused_name(pair):
    publisher, subscriber = pair
    subscriber.execute("DROP SUBSCRIPTION sub")
    subscriber.execute(f"""CREATE SUBSCRIPTION sub CONNECTION
        'host=127.0.0.1 port={publisher.port} dbname=postgres'
        PUBLICATION pub WITH (copy_data=false, enabled=false)""")
    assert subscriber.execute("SELECT pgwrh.applied_lsn('sub')") == [(None,)]
    with pytest.raises(DatabaseError, match="timed out"):
        subscriber.execute("SELECT pgwrh.wait_for_lsn('sub','0/1',50)")


def test_unpublished_wal_does_not_advance_monitor(pair):
    publisher, subscriber = pair
    before = subscriber.execute("SELECT pgwrh.applied_lsn('sub')")[0][0]
    publisher.execute("CREATE TABLE unrelated(id int); INSERT INTO unrelated VALUES(1)")
    target = publisher.execute("SELECT pg_current_wal_insert_lsn()::text")[0][0]
    with pytest.raises(DatabaseError, match="timed out"):
        subscriber.execute(f"SELECT pgwrh.wait_for_lsn('sub','{target}',200)")
    assert subscriber.execute("SELECT pgwrh.applied_lsn('sub')")[0][0] == before
    # A subscribed heartbeat makes the otherwise idle watermark reachable.
    publisher.execute("UPDATE data SET value='heartbeat' WHERE id=0")
    with reader(subscriber, target) as result:
        assert result.result(timeout=10) == [(0,)]


def test_initial_sync_is_not_certified(pair, nodes):
    publisher, _ = pair
    subscriber = nodes("initial-sync")
    subscriber.execute("CREATE TABLE data(id int PRIMARY KEY, value text)")
    subscriber.execute(f"""CREATE SUBSCRIPTION sub CONNECTION
        'host=127.0.0.1 port={publisher.port} dbname=postgres'
        PUBLICATION pub WITH (slot_name='initial_sync',copy_data=true,enabled=false)""")
    with pytest.raises(DatabaseError, match="not ready"):
        subscriber.execute("SELECT pgwrh.wait_for_lsn('sub','0/1',0)")


def test_two_phase_is_rejected(pair):
    _, subscriber = pair
    subscriber.execute("ALTER SUBSCRIPTION sub DISABLE")
    subscriber.execute("ALTER SUBSCRIPTION sub SET (two_phase=true)")
    with pytest.raises(DatabaseError, match="two_phase disabled"):
        subscriber.execute("SELECT pgwrh.wait_for_lsn('sub','0/1',0)")


def test_missing_preload_is_explicit(nodes):
    node = nodes("no-preload", preload=False)
    with pytest.raises(DatabaseError, match="shared_preload_libraries"):
        node.execute("SELECT pgwrh.applied_lsn('sub')")
    with node.connect(autocommit=True) as conn:
        conn.execute("LOAD 'pgwrh_wait'")
        conn.execute("BEGIN")
        with pytest.raises(DatabaseError, match="shared_preload_libraries"):
            barrier(conn, "0/1")


def test_pending_skip_is_rejected(pair):
    _, subscriber = pair
    subscriber.execute("ALTER SUBSCRIPTION sub DISABLE")
    subscriber.execute("ALTER SUBSCRIPTION sub SKIP (lsn='FFFF/FFFFFFFF')")
    with pytest.raises(DatabaseError, match="pending skipped transaction"):
        subscriber.execute("SELECT pgwrh.wait_for_lsn('sub','0/1',0)")


@pytest.mark.parametrize("query", [
    "SELECT pgwrh.wait_for_lsn(NULL,'0/1',0)",
    "SELECT pgwrh.wait_for_lsn('sub',NULL,0)",
    "SELECT pgwrh.wait_for_lsn('sub','0/1',NULL)",
])
def test_null_is_not_success(pair, query):
    _, subscriber = pair
    with pytest.raises(DatabaseError, match="must not be null"):
        subscriber.execute(query)


def test_capacity_exhaustion_does_not_break_apply(pair, nodes):
    publisher, _ = pair
    subscriber = nodes("limited", settings="pgwrh.max_tracked_subscriptions=1")
    subscriber.execute("CREATE TABLE data(id int PRIMARY KEY, value text)")
    publisher.execute("CREATE TABLE other(id int); CREATE PUBLICATION other_pub FOR TABLE other")
    subscriber.execute("CREATE TABLE other(id int)")
    subscriber.execute(f"""CREATE SUBSCRIPTION first CONNECTION
        'host=127.0.0.1 port={publisher.port} dbname=postgres'
        PUBLICATION pub WITH (slot_name='limited_first',copy_data=true)""")
    eventually(lambda: subscriber.execute("SELECT bool_and(srsubstate='r') FROM pg_subscription_rel")[0][0])
    target = token(publisher)
    subscriber.execute(f"SELECT pgwrh.wait_for_lsn('first','{target}')")
    subscriber.execute(f"""CREATE SUBSCRIPTION second CONNECTION
        'host=127.0.0.1 port={publisher.port} dbname=postgres'
        PUBLICATION other_pub WITH (slot_name='limited_second',copy_data=false)""")
    publisher.execute("INSERT INTO other VALUES(1)")
    eventually(lambda: subscriber.execute("SELECT count(*) FROM other")[0][0] == 1)
    with pytest.raises(DatabaseError, match="capacity exhausted"):
        subscriber.execute("SELECT pgwrh.applied_lsn('second')")
    assert subscriber.execute("SELECT pgwrh.applied_lsn('first') IS NOT NULL") == [(True,)]


def test_other_subscription_cannot_release_wait(pair):
    publisher, subscriber = pair
    publisher.execute("CREATE TABLE other(id int); CREATE PUBLICATION other_pub FOR TABLE other")
    subscriber.execute("CREATE TABLE other(id int)")
    subscriber.execute(f"""CREATE SUBSCRIPTION other_sub CONNECTION
        'host=127.0.0.1 port={publisher.port} dbname=postgres'
        PUBLICATION other_pub WITH (copy_data=false)""")
    with block_apply(subscriber) as lock:
        target = token(publisher)
        publisher.execute("INSERT INTO other VALUES(1)")
        eventually(lambda: subscriber.execute("SELECT count(*) FROM other")[0][0] == 1)
        assert subscriber.execute(f"SELECT pgwrh.applied_lsn('other_sub') >= '{target}'") == [(True,)]
        with reader(subscriber, target) as result:
            waiting(subscriber)
            assert not result.done()
            lock.rollback()
            assert result.result(timeout=10) == [(0,), (1,)]


def test_worker_restart_does_not_promote_origin_progress(pair):
    publisher, subscriber = pair
    before = subscriber.execute("SELECT pgwrh.applied_lsn('sub')::text")[0][0]
    subscriber.execute("ALTER SUBSCRIPTION sub DISABLE")
    eventually(lambda: subscriber.execute("SELECT pid IS NULL FROM pg_stat_subscription")[0][0])
    publisher.execute("CREATE TABLE unrelated(id int); INSERT INTO unrelated VALUES(1)")
    ahead = publisher.execute("SELECT pg_current_wal_insert_lsn()::text")[0][0]
    # Fault injection: model origin state advancing before our COMMIT callback.
    # A live origin must never replace callback-confirmed progress on restart.
    origin = subscriber.execute("SELECT roname FROM pg_replication_origin")[0][0]
    subscriber.execute(f"SELECT pg_replication_origin_advance('{origin}', '{ahead}')")
    subscriber.execute("ALTER SUBSCRIPTION sub ENABLE")
    publisher.execute("INSERT INTO unrelated VALUES(2)")
    eventually(lambda: subscriber.execute(f"""SELECT EXISTS(
        SELECT FROM pg_stat_subscription WHERE received_lsn >= '{ahead}')""")[0][0])
    assert subscriber.execute("SELECT pgwrh.applied_lsn('sub')::text") == [(before,)]
    with pytest.raises(DatabaseError, match="timed out"):
        subscriber.execute(f"SELECT pgwrh.wait_for_lsn('sub','{ahead}',50)")
