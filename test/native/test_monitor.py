from conftest import eventually


def test_committed_progress(pair):
    publisher, subscriber = pair
    before = subscriber.execute("SELECT pgwrh.applied_lsn('sub')")[0][0]
    publisher.execute("INSERT INTO data VALUES (1, 'visible')")
    eventually(lambda: subscriber.execute(
        f"SELECT pgwrh.applied_lsn('sub') > '{before}'::pg_lsn")[0][0])
    assert subscriber.execute("SELECT value FROM data WHERE id=1") == [("visible",)]


def test_bootstrap_after_restart(pair):
    _, subscriber = pair
    before = subscriber.execute("SELECT pgwrh.applied_lsn('sub')")[0][0]
    subscriber.restart()
    eventually(lambda: subscriber.execute(
        f"SELECT pgwrh.applied_lsn('sub') >= '{before}'::pg_lsn")[0][0])
    assert subscriber.execute("SELECT value FROM data WHERE id=0") == [("baseline",)]


def test_optional_extension_can_be_reinstalled(pair):
    _, subscriber = pair
    subscriber.execute("CREATE EXTENSION pgwrh CASCADE")
    before = subscriber.execute("SELECT pgwrh.applied_lsn('sub')")[0][0]
    version = subscriber.execute("SELECT extversion FROM pg_extension WHERE extname='pgwrh'")
    subscriber.execute("DROP EXTENSION pgwrh_wait")
    assert subscriber.execute("SELECT extversion FROM pg_extension WHERE extname='pgwrh'") == version
    assert subscriber.execute("SELECT to_regprocedure('pgwrh.applied_lsn(text)')") == [(None,)]
    subscriber.execute("CREATE EXTENSION pgwrh_wait")
    assert subscriber.execute("SELECT pgwrh.applied_lsn('sub')")[0][0] == before
