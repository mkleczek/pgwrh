# pgwrh
# Copyright (C) 2024  Michal Kleczek

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.

# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import pytest
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

@pytest.fixture
def master(new_postgres_node):
    pg_node = new_postgres_node('master', True)
    pg_node.psql(filename='masteride.sql')
    class Master:
        port = pg_node.port
        def with_node(self, action):
            action(pg_node)
        def publish_config_version(self):
            pg_node.execute('select pgwrh.mark_pending_version_ready(\'g1\')')
        def register_replica(self, replica):
            user = replica.name
            password = replica.name
            with pg_node.connect() as mc:
                mc.begin()
                mc.execute(f'CREATE USER {user} PASSWORD \'{password}\' REPLICATION IN ROLE test_replica;')
                mc.execute(f'SELECT pgwrh.add_shard_host(\'g1\', \'{user}\', \'localhost\', {replica.port})')
                mc.commit()
            return (replica.name, replica.name)
        def delete_pending_version(self):
            pg_node.execute('select pgwrh.delete_pending_version(\'g1\')')
        def assert_same_result(self, query, replicas):
            assert all(query(pg_node) == result for result in map(query, replicas))
    return Master()

@pytest.fixture
def register_replicas(master):
    def do(replicas):
        for replica in replicas:
            (user, password) = master.register_replica(replica)
            replica.execute(f'SELECT pgwrh.configure_controller(\'localhost\', \'{master.port}\', \'{user}\', \'{password}\', refresh_seconds := 0)')
    return do

def poll_ready(replicas):
    for replica in replicas:
        replica.poll_query_until('SELECT pgwrh.replica_ready()')

def test_dummy(master, register_replicas, new_postgres_node):
    replica1 = new_postgres_node('replica1')
    replica2 = new_postgres_node('replica2')

    replicas = [replica1, replica2]

    register_replicas(replicas)

    poll_ready(replicas)

    try:
        print(f'Count: {replica1.execute('select count(*) from test.my_data')[0]}')
        pytest.fail('Should have failed with fdw connection error')
    except:
        pass

    master.publish_config_version()

    poll_ready(replicas)

    query = lambda r: r.execute('select count(*) from test.my_data')[0]
    master.assert_same_result(query, replicas)

    replica3 = new_postgres_node('replica3')
    try:
        register_replicas([replica3])
        pytest.fail('Should faile with locked version')
    except:
        pass

    master.delete_pending_version()
    register_replicas([replica3])
    replicas.append(replica3)

    poll_ready(replicas)

    master.assert_same_result(query, replicas)

    master.publish_config_version()

    poll_ready(replicas)

    master.assert_same_result(query, replicas)
