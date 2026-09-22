# SPDX-License-Identifier: AGPL-3.0-only
"""Occurrence-preserving lookup joins against disposable PostgreSQL servers."""
import json
import unittest

from support import Cluster, PgError, literal


class LookupJoinTests(unittest.TestCase):
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
        db = 'lookup_test_' + str(self.counter)
        self.admin.sql('CREATE DATABASE ' + db)
        self.c = self.cluster.connect(db)
        self.addCleanup(self.c.close)
        self.c.sql('''
            CREATE EXTENSION pgwrh_fdw;
            SET max_parallel_workers_per_gather = 0;
            CREATE TABLE items(k int, id int, value text) PARTITION BY RANGE(k);
            CREATE TABLE lookup(k int, label text, threshold int, enabled bool);
            INSERT INTO lookup VALUES
              (1, repeat('wide-',2000), 0, true), (1, NULL, 1, true),
              (15001, 'other-shard', 0, true), (NULL, 'null-key', 0, true),
              (19999, 'disabled', 0, false), (40000, 'unmatched', NULL, true);
        ''')
        for i in range(2):
            self.c.sql(f'''
                CREATE TABLE stored{i}(k int, id int, value text);
                INSERT INTO stored{i} SELECT n, n, 'v'||n
                  FROM generate_series({i*10000},{(i+1)*10000-1}) n;
                INSERT INTO stored{i} SELECT * FROM stored{i} WHERE k%10=1;
                CREATE INDEX ON stored{i}(k);
                CREATE VIEW reversed{i} AS SELECT * FROM stored{i} ORDER BY id DESC;
                CREATE SERVER s{i} FOREIGN DATA WRAPPER pgwrh_fdw
                  OPTIONS(host {literal(self.cluster.path)}, port '{self.cluster.port}',
                          dbname '{db}');
                CREATE USER MAPPING FOR CURRENT_USER SERVER s{i};
                CREATE FOREIGN TABLE f{i} PARTITION OF items
                  FOR VALUES FROM({i*10000}) TO({(i+1)*10000}) SERVER s{i}
                  OPTIONS(schema_name 'public', table_name 'reversed{i}');
            ''')
        self.c.sql('ANALYZE f0; ANALYZE f1; ANALYZE lookup')
        self.c.sql('SELECT pgwrh_fdw_disconnect_all()')

    def plan(self, query, analyze=False):
        return json.loads(self.c.scalar('EXPLAIN (VERBOSE, FORMAT JSON' +
            (', ANALYZE' if analyze else '') + ') ' + query))[0]['Plan']

    def nodes(self, node):
        yield node
        for child in node.get('Plans', []):
            yield from self.nodes(child)

    def lookup_plan(self, query, analyze=False):
        plan = self.plan(query, analyze)
        nodes = [n for n in self.nodes(plan)
                 if n.get('Custom Plan Provider') == 'Pgwrh Remote Lookup Join']
        self.assertEqual(len(nodes), 1, plan)
        return nodes[0]

    def equivalent(self, query):
        self.c.sql('SET pgwrh_fdw.enable_lookup_join = off; DROP TABLE IF EXISTS expected; '
                   'CREATE TEMP TABLE expected AS ' + query)
        self.c.sql('SET pgwrh_fdw.enable_lookup_join = on; DROP TABLE IF EXISTS actual; '
                   'CREATE TEMP TABLE actual AS ' + query)
        self.assertEqual(self.c.sql('''
            (TABLE expected EXCEPT ALL TABLE actual)
            UNION ALL (TABLE actual EXCEPT ALL TABLE expected)
        '''), [])

    def test_inner_occurrences_and_retained_output(self):
        query = 'SELECT s.id, s.k, l.label FROM items s JOIN lookup l ON l.k=s.k WHERE l.enabled'
        plan = self.lookup_plan(query, True)
        self.assertEqual(plan['Lookup Join'], 'Remote INNER')
        self.assertEqual(plan['Remote Rows'], 6)
        self.assertEqual(plan['Remote Executions'], 2)
        sql = [n['Remote SQL'] for n in self.nodes(plan)
               if 'pg_catalog.unnest' in n.get('Remote SQL', '')]
        self.assertEqual(len(sql), 2)
        self.assertTrue(all(' JOIN ROWS FROM (pg_catalog.unnest($1::integer[]), pg_catalog.unnest($2::bigint[]))' in q for q in sql), sql)
        self.assertTrue(all('lookup_rowno' in q and 'label' not in q for q in sql), sql)
        self.equivalent(query)

    def test_semi_preserves_identical_shard_rows(self):
        query = 'SELECT s.* FROM items s WHERE EXISTS(SELECT FROM lookup l WHERE l.k=s.k AND l.enabled)'
        plan = self.lookup_plan(query, True)
        self.assertEqual(plan['Lookup Join'], 'Remote EXISTS')
        self.assertEqual(plan['Remote Rows'], 4)
        sql = [n['Remote SQL'] for n in self.nodes(plan)
               if 'pg_catalog.unnest' in n.get('Remote SQL', '')]
        self.assertTrue(all('EXISTS (SELECT 1 FROM ROWS FROM (pg_catalog.unnest' in q and
                            'lookup_rowno' not in q for q in sql), sql)
        self.equivalent(query)

    def test_residual_parallel_arrays(self):
        for tail in ['l.threshold < s.id', 'l.label = s.value']:
            for semi in [False, True]:
                query = (f'SELECT s.* FROM items s WHERE EXISTS(SELECT FROM lookup l WHERE l.k=s.k AND {tail} AND l.enabled)'
                         if semi else
                         f'SELECT s.*, l.label FROM items s JOIN lookup l ON l.k=s.k AND {tail} WHERE l.enabled')
                plan = self.lookup_plan(query, True)
                self.assertEqual(plan['Lookup Condition Columns'], 2)
                self.equivalent(query)

    def test_empty_and_pruned_connections(self):
        query = 'SELECT s.*, l.label FROM items s JOIN lookup l ON l.k=s.k WHERE l.enabled'
        self.c.sql('UPDATE lookup SET enabled=false WHERE k>=10000')
        plan = self.lookup_plan(query, True)
        self.assertEqual(plan['Remote Executions'], 1)
        self.assertEqual(plan['Skipped Shards'], 1)
        self.assertEqual(self.c.sql('SELECT server_name FROM pgwrh_fdw_get_connections()'), [('s0',)])
        self.c.sql('SELECT pgwrh_fdw_disconnect_all(); UPDATE lookup SET enabled=false')
        plan = self.lookup_plan(query, True)
        self.assertEqual(plan['Lookup Rows'], 0)
        self.assertEqual(plan['Remote Executions'], 0)
        self.assertEqual(self.c.sql('SELECT * FROM pgwrh_fdw_get_connections()'), [])
        self.equivalent(query)


if __name__ == '__main__':
    unittest.main()
