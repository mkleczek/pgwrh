import pytest
import logging
import tempfile
import time

from testgres import get_new_node, scoped_config
from unittest import TestCase

class TestRemoteOperations(TestCase):

    def test_temp(self):
        logfile = tempfile.NamedTemporaryFile('w', delete=True)
        with scoped_config(use_python_logging=True):
            with get_new_node().init(allow_logical=True) as master:
                # Configure and start master
                (master
                    .append_conf('max_worker_processes = 100')
                    .append_conf('max_replication_slots = 100')
                    .append_conf('max_wal_senders = 100'))
                master.start()
                master.execute('CREATE EXTENSION pgwrh CASCADE')

                # Initialize master with some tables to replicate, data and sharding configuration
                master.psql(filename='master.sql')
                print(f'my_data master count: {master.execute('select count(*) from test.my_data')}')
                
                print(f'Starting replicas')
                with \
                    get_new_node().init() as replica1, \
                    get_new_node().init() as replica2, \
                    get_new_node().init() as replica3, \
                    get_new_node().init() as replica4:
                    
                    replicas = [(replica1, 'h1'),
                                (replica2, 'h2'),
                                (replica3, 'h3'),
                                (replica4, 'h4')]

                    for (replica, user) in replicas:
                        print(f'Starting replica {user}')
                        (replica
                            .append_conf('max_worker_processes = 100')
                            .append_conf('max_logical_replication_workers = 12')
                            .append_conf('max_replication_slots = 32'))
                        replica.start()

                    # Register replicas in master pgwrh configuration
                    with master.connect() as mc:
                        mc.begin()
                        for replica in replicas:
                            mc.execute(f'SELECT pgwrh.add_shard_host(\'g1\', \'{replica[1]}\', \'localhost\', {replica[0].port})')
                        mc.commit()

                    # Initialize replicas
                    # Run sync_replica_worker so that replica configures itself for replication
                    for (replica, user) in replicas:
                        replica.execute('CREATE EXTENSION pgwrh CASCADE')
                        replica.execute(f'SELECT pgwrh.configure_controller(\'localhost\', \'{master.port}\', \'{user}\', \'{user}\')')
                        replica.execute('select pgwrh.sync_daemon(0)')

                    # All shards are replicated. We can mark pending version as ready
                    master.execute(f'SELECT pgwrh.mark_pending_version_ready(\'g1\')')

                    # Re-run sync_replica_worker on replicas so that foreign tables are reconfigured
                    # for (replica, user) in replicas:
                    #     print(f'Starting replica worker {user}')
                    #     replica.execute('select pgwrh.launch_sync()')

                    time.sleep(5)
                    # Perform some queries on replicas
                    for (replica, _) in replicas:
                        print(f'my_data count: {replica.execute('select count(*) from test.my_data')}')
