# SPDX-License-Identifier: AGPL-3.0-only
"""Cursor-free streams: wire ordering, spills, lifecycle and remote workers."""
import json
import re
import threading
import time
import unittest

import test_feature_pipeline as pipeline
from support import PgError, literal


class StreamingTests(pipeline.PipelineTests):
    def setUp(self):
        super().setUp()
        self.c.sql("ALTER SERVER shared OPTIONS(ADD streaming_fetch 'true')")

    def selects(self):
        return [(n, kind, sql) for n, kind, sql in self.proxy.commands
                if kind == b'P' and sql.startswith('SELECT ')]

    def test_results_depths_and_legacy(self):
        self.assert_async()
        self.c.sql('UPDATE baseline SET v=NULL WHERE k%5=0')
        for i in range(3):
            self.c.sql(f'UPDATE stored{i} SET v=NULL WHERE k%5=0')
        for depth in [0, 1, 2, 8]:
            for size in [1, 7, 100, 101]:
                with self.subTest(depth=depth, size=size):
                    self.c.sql(f"ALTER SERVER shared OPTIONS(SET pipeline_depth '{depth}', SET fetch_size '{size}')")
                    self.compare()
                    self.assertEqual(self.c.sql('SELECT * FROM f0 WHERE k<0'), [])
        self.assertTrue(self.selects())
        self.assertFalse(any(sql.startswith(('DECLARE ', 'FETCH ')) for _, _, sql in self.proxy.commands))

    def test_multiple_fetches_sent_before_first_finishes(self):
        # The inherited test name now proves multiple plain SELECTs, not FETCHes.
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
                with self.proxy.condition:
                    self.assertTrue(self.proxy.condition.wait_for(lambda: len(self.selects()) >= 3, 4), self.proxy.commands)
                self.assertTrue(worker.is_alive())
                self.assertEqual(len({n for n, _, _ in self.selects()}), 1)
            finally:
                locker.sql('SELECT pg_advisory_unlock(123456)')
                worker.join(12)
            self.assertFalse(worker.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual(len(result), 300)
            self.assertEqual(self.proxy.connections, 1)

    def test_virtual_members_share_pipeline_and_context(self):
        for i in range(3):
            self.c.sql(f"""ALTER TABLE items DETACH PARTITION f{i}; DROP FOREIGN TABLE f{i};
                CREATE SERVER virtual{i} FOREIGN DATA WRAPPER pgwrh_fdw
                    OPTIONS(members 'shared', async_capable 'true', streaming_fetch 'true', fetch_size '7');
                CREATE USER MAPPING FOR CURRENT_USER SERVER virtual{i};
                CREATE FOREIGN TABLE f{i} PARTITION OF items FOR VALUES FROM ({i*100}) TO ({(i+1)*100})
                    SERVER virtual{i} OPTIONS(schema_name 'public', table_name 'stored{i}')""")
        self.assert_async()
        self.compare()
        self.assertEqual(len({n for n, _, _ in self.selects()}), 1)
        self.assertFalse(any(sql.startswith('DECLARE ') for _, _, sql in self.proxy.commands))
        self.c.sql("ALTER FOREIGN TABLE f1 OPTIONS(ADD streaming_fetch 'false')")
        self.compare()
        self.assertTrue(any(sql.startswith('DECLARE ') for _, _, sql in self.proxy.commands))

    def test_large_parameters_and_connection_loss(self):
        self.c.sql("SET plan_cache_mode=force_generic_plan; PREPARE parameter_scan(text) AS SELECT * FROM items WHERE v=$1")
        self.assertEqual(self.c.sql('EXECUTE parameter_scan(' + literal('x' * 2_000_000) + ')'), [])
        self.compare()
        self.c.sql("""CREATE FUNCTION hold_for_kill(int) RETURNS int LANGUAGE plpgsql VOLATILE AS
            $$ BEGIN IF $1>=20 THEN PERFORM pg_advisory_xact_lock(45678); END IF; RETURN $1; END $$;
            CREATE VIEW killable AS SELECT hold_for_kill(k) k, repeat('x',10000) v FROM stored0;
            ALTER FOREIGN TABLE f0 OPTIONS(SET table_name 'killable')""")
        with self.cluster.connect(self.database) as killer:
            killer.sql('SELECT pg_advisory_lock(45678)')
            try:
                self.c.sql('BEGIN; DECLARE interrupted CURSOR FOR SELECT * FROM f0')
                self.assertEqual(len(self.c.sql('FETCH 1 FROM interrupted')), 1)
                remote_pid = killer.scalar("SELECT pid FROM pg_stat_activity WHERE datname=current_database() "
                    "AND application_name='pgwrh_fdw' LIMIT 1")
                killer.sql('SELECT pg_terminate_backend(' + remote_pid + ')')
                with self.assertRaises(PgError):
                    self.c.sql('FETCH ALL FROM interrupted')
                self.c.sql('ROLLBACK')
            finally:
                killer.sql('SELECT pg_advisory_unlock(45678)')
        self.c.sql("ALTER FOREIGN TABLE f0 OPTIONS(SET table_name 'stored0')")
        self.compare()

    def test_streaming_option_precedence_and_fallback(self):
        self.c.sql("ALTER FOREIGN TABLE f0 OPTIONS(ADD streaming_fetch 'false')")
        self.c.sql('SELECT * FROM f0')
        self.assertTrue(any(sql.startswith('DECLARE ') for _, _, sql in self.proxy.commands))
        self.proxy.commands.clear()
        self.c.sql("ALTER SERVER shared OPTIONS(SET streaming_fetch 'false'); ALTER FOREIGN TABLE f0 OPTIONS(SET streaming_fetch 'true')")
        self.assertEqual(len(self.c.sql('SELECT * FROM f0')), 100)
        self.assertTrue(self.selects())
        self.assertFalse(any(sql.startswith('DECLARE ') for _, _, sql in self.proxy.commands))
        for value in ['bad', '', '2']:
            with self.subTest(value=value), self.assertRaises(PgError):
                self.c.sql('ALTER SERVER shared OPTIONS(SET streaming_fetch '+literal(value)+')')
        # Whole-statement writes and row locks keep the existing cursor path.
        for query in ['SELECT * FROM f0 FOR UPDATE',
                      'WITH inserted AS (INSERT INTO stored1 VALUES(999,\'x\') RETURNING k) SELECT f0.* FROM f0, inserted']:
            self.proxy.commands.clear()
            self.c.sql(query)
            self.assertTrue(any(sql.startswith('DECLARE ') for _, _, sql in self.proxy.commands))

    def test_standalone_first_chunk_before_remote_completion(self):
        self.c.sql('''CREATE FUNCTION hold_later(int) RETURNS int LANGUAGE plpgsql VOLATILE AS
            $$ BEGIN IF $1>=20 THEN PERFORM pg_advisory_xact_lock(34567); END IF; RETURN $1; END $$;
            CREATE VIEW held_later AS SELECT hold_later(k) k, repeat('x',10000) v FROM stored0;
            ALTER FOREIGN TABLE f0 OPTIONS(SET table_name 'held_later')''')
        with self.cluster.connect(self.database) as locker:
            locker.sql('SELECT pg_advisory_lock(34567)')
            self.c.sql('BEGIN; DECLARE streamed CURSOR FOR SELECT * FROM f0')
            result, errors = [], []
            def run():
                try:
                    result.extend(self.c.sql('FETCH 1 FROM streamed'))
                except BaseException as error:
                    errors.append(error)
            worker = threading.Thread(target=run)
            worker.start()
            try:
                worker.join(4)
                self.assertFalse(worker.is_alive(), 'first chunk waited for the entire remote query')
                self.assertEqual(errors, [])
                self.assertEqual(result, [('0', 'x'*10000)])
            finally:
                locker.sql('SELECT pg_advisory_unlock(34567)')
                worker.join(12)
            self.c.sql('CLOSE streamed; COMMIT')
            self.assertFalse(any(sql.startswith('DECLARE ') for _, _, sql in self.proxy.commands))

    def test_spill_survives_nested_rollback_and_statement_barrier(self):
        self.c.sql("SET work_mem='64kB'; SET log_temp_files=0; UPDATE stored0 SET v=repeat('wide',10000)")
        expected = self.c.sql('SELECT * FROM stored0')
        start = self.cluster.log.stat().st_size
        self.c.sql('BEGIN; DECLARE outer_rows NO SCROLL CURSOR FOR SELECT * FROM f0')
        rows = self.c.sql('FETCH 1 FROM outer_rows')
        self.c.sql('SAVEPOINT nested')
        # A write must drain the unread outer stream, in the nested resource owner.
        self.c.sql("INSERT INTO f1 VALUES(199,'inside savepoint')")
        self.c.sql('ROLLBACK TO nested')
        rows += self.c.sql('FETCH ALL FROM outer_rows')
        self.assertEqual(sorted(rows), sorted(expected))
        self.c.sql('CLOSE outer_rows; COMMIT')
        self.assertIn('temporary file:', self.cluster.log.read_text()[start:])

    def test_overlapping_local_cursors_and_merge_append(self):
        self.c.sql("SET work_mem='64kB'; UPDATE stored0 SET v=repeat('a',10000); UPDATE stored1 SET v=repeat('b',10000)")
        self.c.sql('BEGIN; DECLARE a NO SCROLL CURSOR FOR SELECT * FROM f0; DECLARE b NO SCROLL CURSOR FOR SELECT * FROM f1')
        a = self.c.sql('FETCH 1 FROM a')
        b = self.c.sql('FETCH 1 FROM b')
        a += self.c.sql('FETCH ALL FROM a')
        b += self.c.sql('FETCH ALL FROM b')
        self.assertEqual(sorted(a), sorted(self.c.sql('SELECT * FROM stored0')))
        self.assertEqual(sorted(b), sorted(self.c.sql('SELECT * FROM stored1')))
        self.c.sql('CLOSE a; CLOSE b; COMMIT')
        query='SELECT * FROM items ORDER BY v,k'
        self.c.sql('SET enable_sort=off')
        self.assertIn('Merge Append', self.c.scalar('EXPLAIN (FORMAT JSON) '+query))
        self.assertEqual(self.c.sql(query), self.c.sql('SELECT * FROM (SELECT * FROM stored0 UNION ALL SELECT * FROM stored1 UNION ALL SELECT * FROM stored2) t ORDER BY v,k'))

    def test_error_after_partial_rows(self):
        self.c.sql('''CREATE FUNCTION fail_later(int) RETURNS int LANGUAGE plpgsql VOLATILE AS
            $$ BEGIN IF $1>=40 THEN RAISE EXCEPTION 'late remote failure'; END IF; RETURN $1; END $$;
            CREATE VIEW broken AS SELECT fail_later(k) k, repeat('z',10000) v FROM stored0;
            ALTER FOREIGN TABLE f0 OPTIONS(SET table_name 'broken')''')
        self.c.sql('BEGIN; SAVEPOINT before_read; DECLARE failing CURSOR FOR SELECT * FROM f0')
        self.assertEqual(len(self.c.sql('FETCH 1 FROM failing')), 1)
        with self.assertRaises(PgError) as error:
            self.c.sql('FETCH ALL FROM failing')
        self.assertEqual(error.exception.sqlstate, 'P0001')
        self.assertIn('late remote failure', str(error.exception))
        self.c.sql('ROLLBACK TO before_read; COMMIT')
        self.c.sql("ALTER FOREIGN TABLE f0 OPTIONS(SET table_name 'stored0')")
        self.compare()

    def test_interrupted_spill_cannot_silently_truncate_outer_scan(self):
        self.c.sql("SET work_mem='64kB'; SET temp_file_limit=0; UPDATE stored0 SET v=repeat('wide',10000)")
        self.c.sql('BEGIN; DECLARE outer_rows NO SCROLL CURSOR FOR SELECT * FROM f0')
        self.assertEqual(len(self.c.sql('FETCH 1 FROM outer_rows')), 1)
        self.c.sql('SAVEPOINT nested')
        with self.assertRaises(PgError) as error:
            self.c.sql("INSERT INTO f1 VALUES(199,'force drain')")
        self.assertIn('temp_file_limit', str(error.exception))
        self.c.sql('ROLLBACK TO nested')
        # Small utility results avoid a second, unrelated spill in the local
        # FETCH portal before it reaches the failed foreign stream.
        with self.assertRaises(PgError) as error:
            for _ in range(100):
                self.c.sql('FETCH 1 FROM outer_rows')
        self.assertIn('streaming operation was aborted', str(error.exception))
        self.c.sql('ROLLBACK; RESET temp_file_limit')
        self.assertEqual(len(self.c.sql('SELECT * FROM f0')), 100)

    def test_remote_parallel_workers(self):
        # auto_explain runs only in the remote backend, and records actual workers.
        self.c.sql("INSERT INTO stored0 SELECT i, repeat('v',100) FROM generate_series(300,20300) i; ANALYZE stored0; ALTER TABLE stored0 SET(parallel_workers=2)")
        options = ('-c session_preload_libraries=auto_explain -c auto_explain.log_min_duration=0 '
                   '-c auto_explain.log_analyze=on -c auto_explain.log_format=json '
                   '-c max_parallel_workers_per_gather=2 -c min_parallel_table_scan_size=0 '
                   '-c parallel_setup_cost=0 -c parallel_tuple_cost=0')
        self.c.sql('ALTER SERVER shared OPTIONS(ADD options '+literal(options)+')')
        for streaming in ['false','true']:
            self.c.sql("ALTER SERVER shared OPTIONS(SET streaming_fetch '"+streaming+"')")
            start = self.cluster.log.stat().st_size
            self.assertEqual(len(self.c.sql('SELECT * FROM f0')), 20101)
            log = self.cluster.log.read_text()[start:]
            if streaming == 'true':
                self.assertRegex(log, r'"Workers Launched":\s*[1-9]')
            else:
                self.assertNotRegex(log, r'"Workers Launched":\s*[1-9]')


import test_context as context_tests
import test_lookup_join as lookup_tests


class StreamingContextTests(context_tests.ContextTests):
    def server(self, name, db, parameters, wrapper="pgwrh_fdw", remote_user=None):
        super().server(name, db, parameters, wrapper, remote_user)
        if wrapper == "pgwrh_fdw":
            self.c.sql(f"ALTER SERVER {name} OPTIONS(ADD streaming_fetch 'true', ADD pipeline_depth '8')")


class StreamingLookupTests(lookup_tests.LookupJoinTests):
    def setUp(self):
        super().setUp()
        for name in ['s0','s1']:
            self.c.sql(f"ALTER SERVER {name} OPTIONS(ADD streaming_fetch 'true', ADD pipeline_depth '8')")

    def test_explain_only_and_remote_estimates(self):
        query = 'SELECT s.id,l.label FROM items s JOIN lookup l ON s.k=l.k WHERE l.enabled'
        self.lookup_plan(query)
        self.assertEqual(self.c.sql('SELECT * FROM pgwrh_fdw_get_connections()'), [])
        self.c.sql("INSERT INTO lookup SELECT n,'key'||n,0,true FROM generate_series(10,100) n; ANALYZE lookup; "
                   'UPDATE lookup SET enabled=false WHERE k>10000')
        self.c.sql("ALTER SERVER s0 OPTIONS(ADD use_remote_estimate 'true'); ALTER SERVER s1 OPTIONS(ADD use_remote_estimate 'true')")
        mark = self.cluster.log.stat().st_size
        self.lookup_plan(query, True)
        logs = self.cluster.log.read_text()[mark:]
        self.assertIn('EXPLAIN SELECT', logs)
        self.assertIn('execute <unnamed>: SELECT', logs)
        self.assertNotIn('DECLARE', logs)
        self.equivalent(query)


if __name__ == '__main__':
    unittest.main()
