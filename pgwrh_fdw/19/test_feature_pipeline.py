# SPDX-License-Identifier: AGPL-3.0-only
"""Wire-level concurrency and lifecycle tests for shared-connection pipelining."""
import json
from pathlib import Path
import select
import socket
import struct
import tempfile
import threading
import time
import unittest

from support import Cluster, PgError, literal


class WireProxy:
    """Transparent Unix socket relay recording frontend SQL, not server timings."""
    def __init__(self, cluster, delay=0):
        self.path = Path(tempfile.mkdtemp(prefix='fdw-pipeline-proxy-'))
        self.target = str(cluster.path / ('.s.PGSQL.' + str(cluster.port)))
        self.listener = socket.socket(socket.AF_UNIX)
        self.listener.bind(str(self.path / ('.s.PGSQL.' + str(cluster.port))))
        self.listener.listen()
        self.listener.settimeout(.1)
        self.delay = delay
        self.stop = threading.Event()
        self.condition = threading.Condition()
        self.commands = []
        self.connections = 0
        self.backend_rows = 0
        self.backend_bytes = 0
        self.workers = []
        self.thread = threading.Thread(target=self.accept, daemon=True)
        self.thread.start()

    def accept(self):
        while not self.stop.is_set():
            try:
                client, _ = self.listener.accept()
            except socket.timeout:
                continue
            with self.condition:
                self.connections += 1
                number = self.connections
            worker = threading.Thread(target=self.relay, args=(client, number), daemon=True)
            self.workers.append(worker)
            worker.start()

    def relay(self, client, number):
        server = socket.socket(socket.AF_UNIX)
        server.connect(self.target)
        frontend = bytearray()
        backend = bytearray()
        startup = True
        client.setblocking(False)
        server.setblocking(False)
        buffers = {client: bytearray(), server: bytearray()}
        release = {client: 0, server: 0}
        peer = {client: server, server: client}
        try:
            while not self.stop.is_set():
                now = time.monotonic()
                readers = [source for source in peer if len(buffers[peer[source]]) < 65536]
                writers = [dest for dest in peer if buffers[dest] and now >= release[dest]]
                deadlines = [release[dest] - now for dest in peer if buffers[dest] and release[dest] > now]
                ready, writable, _ = select.select(readers, writers, [], min([.1] + deadlines))
                for source in ready:
                    try:
                        data = source.recv(65536)
                    except BlockingIOError:
                        continue
                    if not data:
                        return
                    dest = peer[source]
                    if not buffers[dest]:
                        release[dest] = time.monotonic() + self.delay / 2
                    buffers[dest].extend(data)
                    if source is server:
                        self.backend_bytes += len(data)
                        backend.extend(data)
                        while len(backend) >= 5:
                            size = 1 + struct.unpack('!I', backend[1:5])[0]
                            if len(backend) < size:
                                break
                            if backend[:1] == b'D':
                                self.backend_rows += 1
                            del backend[:size]
                    if source is client:
                        frontend.extend(data)
                        while True:
                            offset = 0 if startup else 1
                            if len(frontend) < offset + 4:
                                break
                            size = struct.unpack('!I', frontend[offset:offset+4])[0] + offset
                            if len(frontend) < size:
                                break
                            message = bytes(frontend[:size])
                            del frontend[:size]
                            if startup:
                                startup = False
                            elif message[:1] in (b'P', b'Q'):
                                sql = message[5:].split(b'\0')[1 if message[:1] == b'P' else 0].decode()
                                with self.condition:
                                    self.commands.append((number, message[:1], sql))
                                    self.condition.notify_all()
                for dest in writable:
                    try:
                        sent = dest.send(buffers[dest])
                    except BlockingIOError:
                        continue
                    del buffers[dest][:sent]
        except (OSError, ValueError):
            pass
        finally:
            client.close()
            server.close()

    def wait_fetches(self, count, timeout=3):
        def found():
            return len([q for q in self.commands if q[2].startswith('FETCH ')]) >= count
        with self.condition:
            return self.condition.wait_for(found, timeout)

    def close(self):
        self.stop.set()
        self.thread.join(2)
        for worker in self.workers:
            worker.join(2)
        self.listener.close()


class PipelineTests(unittest.TestCase):
    counter = 0

    @classmethod
    def setUpClass(cls):
        cls.cluster = Cluster()
        try:
            cls.cluster.setup()
            cls.admin = cls.cluster.connect()
        except BaseException:
            cls.cluster.close()
            raise

    @classmethod
    def tearDownClass(cls):
        cls.admin.close()
        cls.cluster.close()

    def setUp(self):
        type(self).counter += 1
        self.database = 'pipeline_' + str(self.counter)
        self.admin.sql('CREATE DATABASE ' + self.database)
        self.proxy = WireProxy(self.cluster)
        self.addCleanup(self.proxy.close)
        self.c = self.cluster.connect(self.database)
        self.addCleanup(self.c.close)
        self.c.sql(f'''
            CREATE EXTENSION pgwrh_fdw;
            SET statement_timeout = '10s';
            SET max_parallel_workers_per_gather = 0;
            CREATE TABLE baseline(k int, v text);
            INSERT INTO baseline SELECT i, repeat('v', 100) FROM generate_series(0, 299) i;
            CREATE TABLE items(LIKE baseline) PARTITION BY RANGE(k);
            CREATE SERVER shared FOREIGN DATA WRAPPER pgwrh_fdw OPTIONS
                (host {literal(self.proxy.path)}, port '{self.cluster.port}',
                 dbname '{self.database}', async_capable 'true', fetch_size '7', pipeline_depth '8');
            CREATE USER MAPPING FOR CURRENT_USER SERVER shared;
        ''')
        for i in range(3):
            self.c.sql(f'''
                CREATE TABLE stored{i} AS SELECT * FROM baseline WHERE k/100={i};
                CREATE FOREIGN TABLE f{i} PARTITION OF items
                    FOR VALUES FROM ({i*100}) TO ({(i+1)*100}) SERVER shared
                    OPTIONS(schema_name 'public', table_name 'stored{i}');
            ''')

    def assert_async(self, query='SELECT * FROM items'):
        plan = self.c.scalar('EXPLAIN (FORMAT JSON) ' + query)
        self.assertIn('"Async Capable": true', plan)
        return json.loads(plan)

    def compare(self, query='SELECT * FROM items'):
        self.assertEqual(sorted(self.c.sql(query)), sorted(self.c.sql(query.replace('items', 'baseline'))))

    def test_results_depths_and_legacy(self):
        self.assert_async()
        for depth in [0, 1, 2, 8]:
            with self.subTest(depth=depth):
                self.c.sql(f"ALTER SERVER shared OPTIONS(SET pipeline_depth '{depth}')")
                self.compare()
                self.compare('SELECT count(*),sum(k),sum(length(v)) FROM items')
                self.assertEqual(self.c.scalar('SELECT count(*) FROM items WHERE k<0'), '0')
        self.assertTrue(any(kind == b'P' and sql.startswith('FETCH ') for _, kind, sql in self.proxy.commands))

    def test_multiple_fetches_sent_before_first_finishes(self):
        self.assert_async()
        with self.cluster.connect(self.database) as locker:
            locker.sql('SELECT pg_advisory_lock(123456)')
            self.c.sql('''CREATE FUNCTION hold_first(int) RETURNS int LANGUAGE plpgsql VOLATILE AS
                $$ BEGIN PERFORM pg_advisory_xact_lock(123456); RETURN $1; END $$;
                CREATE VIEW held AS SELECT hold_first(k) k, v FROM stored0;
                ALTER FOREIGN TABLE f0 OPTIONS(SET table_name 'held')''')
            result, errors = [], []
            def run():
                try:
                    result.extend(self.c.sql('SELECT * FROM items'))
                except BaseException as error:
                    errors.append(error)
            worker = threading.Thread(target=run)
            worker.start()
            try:
                self.assertTrue(self.proxy.wait_fetches(3), self.proxy.commands)
                self.assertTrue(worker.is_alive(), 'first FETCH should still be blocked')
                fetches = [(n, kind) for n, kind, sql in self.proxy.commands if sql.startswith('FETCH ')]
                self.assertEqual(len({n for n, _ in fetches}), 1)
                self.assertTrue(all(kind == b'P' for _, kind in fetches))
            finally:
                locker.sql('SELECT pg_advisory_unlock(123456)')
                worker.join(12)
            self.assertFalse(worker.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual(len(result), 300)
            self.assertEqual(self.proxy.connections, 1)

    def test_limits_rescans_and_mixed_scans(self):
        for _ in range(3):
            self.assertEqual(len(self.c.sql('SELECT * FROM items LIMIT 1')), 1)
            self.compare()
        self.c.sql('SET enable_material=off; SET enable_hashjoin=off; SET enable_mergejoin=off')
        self.compare('SELECT n,k FROM generate_series(1,3) n CROSS JOIN items WHERE k%3=n-1 ORDER BY n,k')
        self.c.sql('ALTER TABLE items DETACH PARTITION f0; ALTER TABLE items ATTACH PARTITION stored0 FOR VALUES FROM (0) TO (100)')
        self.compare()

    def test_savepoints_errors_and_cancel(self):
        for parallel in ['false', 'true']:
            self.c.sql(f"ALTER SERVER shared OPTIONS(ADD parallel_abort '{parallel}')" if parallel == 'false' else
                       "ALTER SERVER shared OPTIONS(SET parallel_abort 'true')")
            self.c.sql('BEGIN; SAVEPOINT before_scan')
            self.compare()
            self.c.sql('ROLLBACK TO before_scan')
            self.compare()
            self.c.sql('SAVEPOINT remote_error; ALTER FOREIGN TABLE f1 OPTIONS(SET table_name \'missing_table\')')
            with self.assertRaises(PgError):
                self.c.sql('SELECT * FROM items')
            self.c.sql('ROLLBACK TO remote_error')
            self.compare()
            self.c.sql('COMMIT')
            with self.cluster.connect(self.database) as locker:
                locker.sql('BEGIN; LOCK stored0 IN ACCESS EXCLUSIVE MODE')
                self.c.sql("BEGIN; SAVEPOINT cancel_scan; SET LOCAL statement_timeout='150ms'")
                with self.assertRaises(PgError) as error:
                    self.c.sql('SELECT * FROM items')
                self.assertEqual(error.exception.sqlstate, '57014')
                self.c.sql('ROLLBACK TO cancel_scan')
                locker.sql('ROLLBACK')
                self.compare()
                self.c.sql('COMMIT')

    def test_synchronous_writes_and_parallel_commit(self):
        self.c.sql("ALTER SERVER shared OPTIONS(ADD parallel_commit 'true')")
        self.c.sql('BEGIN')
        self.compare()
        self.c.sql("UPDATE f0 SET v='changed' WHERE k=0")
        self.assertEqual(self.c.scalar('SELECT v FROM f0 WHERE k=0'), 'changed')
        self.c.sql("INSERT INTO f0 VALUES (99,'added')")
        self.c.sql("DELETE FROM f0 WHERE v='added'")
        self.assertEqual(self.c.scalar('SELECT count(*) FROM items'), '300')
        self.c.sql('COMMIT')

    def test_virtual_members_share_pipeline_and_context(self):
        for i in range(3):
            self.c.sql(f"""ALTER TABLE items DETACH PARTITION f{i}; DROP FOREIGN TABLE f{i};
                CREATE SERVER virtual{i} FOREIGN DATA WRAPPER pgwrh_fdw
                    OPTIONS(members 'shared', async_capable 'true', fetch_size '7');
                CREATE USER MAPPING FOR CURRENT_USER SERVER virtual{i};
                CREATE FOREIGN TABLE f{i} PARTITION OF items FOR VALUES FROM ({i*100}) TO ({(i+1)*100})
                    SERVER virtual{i} OPTIONS(schema_name 'public', table_name 'stored{i}')""")
        self.assert_async()
        self.compare()
        fetches = [(n, kind) for n, kind, sql in self.proxy.commands if sql.startswith('FETCH ')]
        self.assertEqual(len({n for n, _ in fetches}), 1)
        self.assertTrue(all(kind == b'P' for _, kind in fetches))
        with self.assertRaises(PgError):
            self.c.sql("ALTER SERVER virtual0 OPTIONS(ADD pipeline_depth '2')")

    def test_large_results_and_conversion_failure(self):
        self.c.sql("UPDATE baseline SET v=repeat('z', 100000)")
        for i in range(3):
            self.c.sql(f"UPDATE stored{i} SET v=repeat('z', 100000)")
        self.compare()
        self.c.sql("CREATE VIEW invalid AS SELECT 'not-an-int'::text k, v FROM stored1")
        self.c.sql("BEGIN; SAVEPOINT bad_value; "
                   "ALTER FOREIGN TABLE f1 OPTIONS(SET table_name 'invalid')")
        with self.assertRaises(PgError) as error:
            self.c.sql('SELECT * FROM items')
        self.assertEqual(error.exception.sqlstate, '22P02')
        self.c.sql('ROLLBACK TO bad_value')
        self.compare('SELECT count(*),sum(k),sum(length(v)) FROM items')
        self.c.sql('COMMIT')

    def test_nested_append_parameter_rescan_and_row_locks(self):
        self.c.sql('SET enable_material=off; SET enable_hashjoin=off; SET enable_mergejoin=off; SET enable_memoize=off')
        query = "SELECT n,k FROM generate_series(0,2) n CROSS JOIN LATERAL (SELECT k FROM items WHERE k%3=n OFFSET 0) f"
        self.assert_async(query)
        self.compare(query)
        self.c.sql('BEGIN; DECLARE outer_scan CURSOR FOR SELECT * FROM items')
        rows = self.c.sql('FETCH 1 FROM outer_scan')
        self.c.sql('SAVEPOINT inner_scan')
        self.compare('SELECT count(*) FROM items')
        self.c.sql('ROLLBACK TO inner_scan')
        rows += self.c.sql('FETCH ALL FROM outer_scan')
        self.assertEqual(sorted(rows), sorted(self.c.sql('SELECT * FROM baseline')))
        self.c.sql('CLOSE outer_scan; COMMIT')
        self.proxy.commands.clear()
        self.c.sql('BEGIN; SELECT * FROM items FOR UPDATE; COMMIT')
        self.assertFalse(any(kind == b'P' and sql.startswith('FETCH ') for _, kind, sql in self.proxy.commands))

    def test_large_parameters_and_connection_loss(self):
        self.c.sql("SET plan_cache_mode=force_generic_plan; PREPARE parameter_scan(text) AS SELECT * FROM items WHERE v=$1")
        self.assert_async("EXECUTE parameter_scan('unused')")
        # Each cursor carries the parameter, enough to exceed socket buffers.
        self.assertEqual(self.c.sql('EXECUTE parameter_scan(' + literal('x' * 2_000_000) + ')'), [])
        self.compare()
        self.c.sql('BEGIN; DECLARE interrupted CURSOR FOR SELECT * FROM items')
        self.c.sql('FETCH 1 FROM interrupted')
        with self.cluster.connect(self.database) as killer:
            remote_pid = killer.scalar("SELECT pid FROM pg_stat_activity WHERE datname=current_database() "
                "AND application_name='pgwrh_fdw' LIMIT 1")
            self.assertIsNotNone(remote_pid)
            killer.sql('SELECT pg_terminate_backend(' + remote_pid + ')')
        with self.assertRaises(PgError):
            self.c.sql('FETCH ALL FROM interrupted')
        self.c.sql('ROLLBACK')
        self.compare()

    def test_released_savepoint_preserves_outer_cursor(self):
        for parallel in ['false', 'true']:
            self.c.sql(f"ALTER SERVER shared OPTIONS(ADD parallel_commit '{parallel}')" if parallel == 'false' else
                       "ALTER SERVER shared OPTIONS(SET parallel_commit 'true')")
            self.c.sql('BEGIN; SAVEPOINT creator; DECLARE surviving CURSOR FOR SELECT * FROM items')
            rows = self.c.sql('FETCH 1 FROM surviving')
            self.c.sql('RELEASE creator; SAVEPOINT later; SELECT count(*) FROM items; ROLLBACK TO later')
            rows += self.c.sql('FETCH ALL FROM surviving')
            self.assertEqual(sorted(rows), sorted(self.c.sql('SELECT * FROM baseline')))
            self.c.sql('CLOSE surviving; COMMIT')

    def test_option_validation(self):
        self.c.sql("ALTER SERVER shared OPTIONS(DROP pipeline_depth)")
        self.compare()
        self.assertFalse(any(kind == b'P' and sql.startswith('FETCH ') for _, kind, sql in self.proxy.commands))
        self.c.sql("ALTER SERVER shared OPTIONS(ADD pipeline_depth '8')")
        for value in ['-1', '1025', '4294967296', '1x', '']:
            with self.subTest(value=value), self.assertRaises(PgError):
                self.c.sql('ALTER SERVER shared OPTIONS(SET pipeline_depth ' + literal(value) + ')')
        with self.assertRaises(PgError):
            self.c.sql("ALTER FOREIGN TABLE f0 OPTIONS(ADD pipeline_depth '2')")


if __name__ == '__main__':
    unittest.main()
