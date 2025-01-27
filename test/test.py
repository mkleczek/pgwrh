import pytest
import logging
import tempfile
import time

from testgres import get_new_node, scoped_config
from contextlib import ExitStack

@pytest.fixture
def new_postgres_node():
    with ExitStack() as stack:
        with scoped_config(use_python_logging=True):
            def do(name, master=False):
                node = get_new_node(name)
                stack.enter_context(node)
                node.init(allow_logical=master)
                (node
                    .append_conf('max_worker_processes = 100')
                    .append_conf('max_replication_slots = 100')
                    .append_conf('max_wal_senders = 100'))
                node.start()
                node.execute('CREATE EXTENSION pgwrh CASCADE')
                return node
            yield do

def setup_replica(master, user):
    def init(replica):
        with master.connect() as mc:
            mc.begin()
            mc.execute(f'CREATE USER {user} PASSWORD \'{user}\' REPLICATION IN ROLE test_replica;')
            mc.execute(f'SELECT pgwrh.add_shard_host(\'g1\', \'{user}\', \'localhost\', {replica.port})')
            mc.commit()
    return init


@pytest.fixture
def master(new_postgres_node):
    node = new_postgres_node('master', True)
    node.psql(filename='master.sql')
    return node

@pytest.fixture
def register_replicas(master):
    def do(replicas):
        for replica in replicas:
            user = replica.name
            password = replica.name
            with master.connect() as mc:
                mc.begin()
                mc.execute(f'CREATE USER {user} PASSWORD \'{password}\' REPLICATION IN ROLE test_replica;')
                mc.execute(f'SELECT pgwrh.add_shard_host(\'g1\', \'{user}\', \'localhost\', {replica.port})')
                mc.commit()
            replica.execute(f'SELECT pgwrh.configure_controller(\'localhost\', \'{master.port}\', \'{user}\', \'{password}\', refresh_seconds := 0)')
            #replica.execute('select pgwrh.sync_daemon(0)')
    return do

@pytest.fixture
def publish_config_version(master):
    def do():
        master.execute('select pgwrh.mark_pending_version_ready(\'g1\')')
    return do


@pytest.fixture
def replica1(new_postgres_node):
    return new_postgres_node('replica1')
@pytest.fixture
def replica2(new_postgres_node):
    return new_postgres_node('replica2')

def poll_ready(replica):
    replica.poll_query_until('SELECT pgwrh.replica_ready()')

def test_dummy(master, register_replicas, replica1, replica2, publish_config_version):
    register_replicas([replica1, replica2])

    poll_ready(replica1)
    poll_ready(replica2)

    try:
        print(f'Count: {replica1.execute('select count(*) from test.my_data')[0]}')
        pytest.fail('Should have failed with fdw connection error')
    except:
        pass

    publish_config_version()

    poll_ready(replica1)
    poll_ready(replica2)

    query = lambda r: r.execute('select count(*) from test.my_data')[0]
    assert all(query(master) == count for count in map(query, [replica1, replica2]))
