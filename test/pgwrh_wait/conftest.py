"""Real publisher/subscriber clusters; no mocked replication progress."""
import os
from contextlib import ExitStack
from pathlib import Path
import time

import pytest
from testgres import get_new_node, scoped_config

ROOT = Path(__file__).resolve().parents[2]
STAGE = ROOT / ".build/test-stage"


def eventually(check, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(0.02)
    raise AssertionError("condition did not become true before timeout")


@pytest.fixture
def nodes():
    with ExitStack() as stack, scoped_config(use_python_logging=True):
        def create(name, preload=True, settings=""):
            node = stack.enter_context(get_new_node(
                name, bin_dir=os.environ.get("PGWRH_TEST_BIN_DIR")))
            node.init(allow_logical=True)
            node.append_conf("unix_socket_directories = '" + node.base_dir.replace("'", "''") + "'")
            controls = os.environ.get("PGWRH_TEST_CONTROL_PATH", "")
            libraries = os.environ.get("PGWRH_TEST_LIBRARY_PATH", "")
            node.append_conf("postgresql.conf", f"""
max_worker_processes = 24
max_logical_replication_workers = 12
max_sync_workers_per_subscription = 4
max_parallel_apply_workers_per_subscription = 4
max_replication_slots = 16
max_wal_senders = 16
wal_sender_timeout = '60s'
wal_receiver_timeout = '60s'
wal_retrieve_retry_interval = '100ms'
wal_receiver_status_interval = '100ms'
logical_decoding_work_mem = '64kB'
max_prepared_transactions = 10
log_min_messages = debug1
statement_timeout = '15s'
dynamic_library_path = '{STAGE}:$libdir{':' + libraries if libraries else ''}'
extension_control_path = '{STAGE}:$system{':' + controls if controls else ''}'
shared_preload_libraries = '{'pgwrh_wait' if preload else ''}'
{settings}
""")
            node.start()
            node.execute("CREATE EXTENSION pgwrh_wait")
            assert node.execute("""SELECT extname FROM pg_extension
                WHERE extname <> 'plpgsql' ORDER BY extname""") == [("pgwrh_wait",)]
            return node
        yield create


@pytest.fixture
def pair(nodes):
    publisher = nodes("publisher")
    subscriber = nodes("subscriber")
    for node in (publisher, subscriber):
        node.execute("CREATE TABLE data(id int PRIMARY KEY, value text)")
    publisher.execute("CREATE PUBLICATION pub FOR TABLE data")
    subscriber.execute(f"""CREATE SUBSCRIPTION sub CONNECTION
        'host=127.0.0.1 port={publisher.port} dbname=postgres'
        PUBLICATION pub WITH (copy_data=false, streaming=off)""")
    publisher.execute("INSERT INTO data VALUES (0, 'baseline')")
    eventually(lambda: subscriber.execute("SELECT count(*) FROM data")[0][0] == 1)
    eventually(lambda: subscriber.execute("SELECT pgwrh.applied_lsn('sub')")[0][0] is not None)
    return publisher, subscriber
