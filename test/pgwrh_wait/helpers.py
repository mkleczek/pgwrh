from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

from conftest import eventually


def token(publisher, ident=1):
    """Sample after COMMIT; a following published heartbeat covers any WAL gap."""
    publisher.execute(f"INSERT INTO data VALUES ({ident}, 'write-{ident}')")
    target = publisher.execute("SELECT pg_current_wal_insert_lsn()::text")[0][0]
    publisher.execute("UPDATE data SET value = value || '.' WHERE id=0")
    return target


def barrier(conn, target, subscription="sub", timeout=5000):
    conn.execute(f"SET LOCAL pgwrh.read_after_subscription = '{subscription}'")
    conn.execute(f"SET LOCAL pgwrh.wait_timeout_ms = {timeout}")
    conn.execute(f"SET LOCAL pgwrh.read_after_lsn = '{target}'")


@contextmanager
def reader(node, target, isolation="REPEATABLE READ", subscription="sub", name="wait-test"):
    def run():
        with node.connect(autocommit=True) as conn:
            conn.execute(f"SET application_name = '{name}'")
            conn.execute(f"BEGIN ISOLATION LEVEL {isolation} READ ONLY")
            barrier(conn, target, subscription)
            rows = conn.execute("SELECT id FROM data ORDER BY id")
            conn.execute("COMMIT")
            return rows
    with ThreadPoolExecutor(max_workers=1) as pool:
        yield pool.submit(run)


def waiting(node, name="wait-test"):
    eventually(lambda: node.execute(f"""SELECT EXISTS(
        SELECT FROM pg_stat_activity WHERE application_name='{name}'
        AND wait_event_type='Extension')""")[0][0])


@contextmanager
def block_apply(subscriber):
    with subscriber.connect() as lock:
        lock.execute("LOCK data IN ACCESS EXCLUSIVE MODE")
        yield lock


def apply_blocked(subscriber):
    eventually(lambda: subscriber.execute("""SELECT EXISTS(
        SELECT FROM pg_stat_activity
        WHERE pid IN (SELECT pid FROM pg_stat_subscription)
        AND wait_event_type = 'Lock')""")[0][0])
