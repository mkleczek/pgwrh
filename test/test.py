import pytest
import logging
import tempfile

from testgres import get_new_node, scoped_config
from unittest import TestCase

class TestRemoteOperations(TestCase):

    def test_temp(self):
        """
        Test exec_command for successful command execution.
        """
        logfile = tempfile.NamedTemporaryFile('w', delete=True)
        with scoped_config(use_python_logging=False):
            with get_new_node().init(allow_logical=True) as master:
                master.append_conf('max_worker_processes = 100').append_conf('max_replication_slots = 100')
                master.start()
                master.execute('CREATE EXTENSION pgwrh CASCADE')
                master.psql(filename='master.sql')
                with get_new_node().init() as replica1, get_new_node().init() as replica2, get_new_node().init() as replica3, get_new_node().init() as replica4:
                    replicas = [(replica1, 'h1'), (replica2, 'h2'), (replica3, 'h3')]#, (replica4, 'h4')]
                    for replica in replicas:
                        replica[0].append_conf('max_worker_processes = 100').append_conf('max_logical_replication_workers = 12').append_conf('max_replication_slots = 32')
                        replica[0].start()
                        replica[0].execute('CREATE EXTENSION pgwrh CASCADE')
                        replica[0].execute(f'SELECT pgwrh.configure_controller(\'localhost\', \'{master.port}\', \'{replica[1]}\', \'{replica[1]}\')')
                        replica[0].execute('call pgwrh.sync_replica_worker()')
                        print('----------------------------------------')
                        print(replica[0].execute('select description from pgwrh.sync'))

                    # replica1.execute('select pgwrh.sync_step()')
                    # replica1.execute('select pgwrh.sync_step()')
                    # replica1.execute('select pgwrh.sync_step()')
                    # print('----------------------------------------')
                    # print(replica1.execute('select description from pgwrh.sync'))
                    # replica1.execute('select pgwrh.sync_step()')
                    # print('----------------------------------------')
                    # print(replica1.execute('select description from pgwrh.sync'))
                    # replica1.execute('select pgwrh.sync_step()')
                    # print('----------------------------------------')
                    # print(replica1.execute('select description from pgwrh.sync'))
                    # replica1.execute('select pgwrh.sync_step()')
                    # print('----------------------------------------')
                    # print(replica1.execute('select description from pgwrh.sync'))
                    # replica1.execute('select pgwrh.sync_step()')
                    # check that master's port is found
                    # with open(logfile.name, 'r') as log:
                    #     lines = log.readlines()
                    #     print(lines)

