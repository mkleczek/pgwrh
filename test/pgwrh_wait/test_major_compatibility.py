"""Keep the table-read watermark independent of PostgreSQL 19 sequence sync."""
from conftest import eventually


def test_table_wait_ignores_sequence_synchronization(pair):
    publisher, subscriber = pair
    target = subscriber.execute("SELECT pgwrh.applied_lsn('sub')")[0][0]
    major = int(subscriber.execute("SHOW server_version_num")[0][0]) // 10000
    if major >= 19:
        publisher.execute("CREATE SEQUENCE sync_sequence; SELECT setval('sync_sequence', 100)")
        subscriber.execute("CREATE SEQUENCE sync_sequence")
        publisher.execute("CREATE PUBLICATION sequences FOR ALL SEQUENCES")
        subscriber.execute("ALTER SUBSCRIPTION sub SET PUBLICATION pub, sequences WITH (refresh=false)")
        with subscriber.connect() as lock:
            # Sequence synchronization needs a conflicting RowExclusiveLock.
            lock.execute("ALTER SEQUENCE sync_sequence CACHE 1")
            subscriber.execute("ALTER SUBSCRIPTION sub REFRESH PUBLICATION")
            eventually(lambda: subscriber.execute("""SELECT count(*) FROM pg_subscription_rel
                WHERE srrelid = 'sync_sequence'::regclass AND srsubstate <> 'r'""")[0][0] == 1)
            subscriber.execute(f"SELECT pgwrh.wait_for_lsn('sub', '{target}', 0)")
            assert subscriber.execute("SELECT id FROM data") == [(0,)]
            lock.rollback()
        eventually(lambda: subscriber.execute("SELECT last_value FROM sync_sequence")[0][0] == 100)
        eventually(lambda: subscriber.execute("""SELECT count(*) FROM pg_subscription_rel
            WHERE srrelid = 'sync_sequence'::regclass AND srsubstate <> 'r'""")[0][0] == 0)
        assert subscriber.execute("SELECT pgwrh.applied_lsn('sub')")[0][0] == target
    # PostgreSQL 18 and 19 retain the same table-read contract.
    subscriber.execute(f"SELECT pgwrh.wait_for_lsn('sub', '{target}', 0)")
    assert subscriber.execute("SELECT id FROM data") == [(0,)]
