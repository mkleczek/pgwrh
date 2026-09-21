# SPDX-License-Identifier: AGPL-3.0-only
"""Check remote SQL and results using real loopback foreign partitions."""
import json
import unittest

from support import Cluster, PgError, literal


class LimitPushdownTests(unittest.TestCase):
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
        database = 'limit_test_' + str(self.counter)
        self.admin.sql('CREATE DATABASE ' + database)
        self.c = self.cluster.connect(database)
        self.addCleanup(self.c.close)
        self.c.sql("""
            CREATE EXTENSION pgwrh_fdw;
            SET max_parallel_workers_per_gather = 0;
            CREATE TABLE all_rows(k int, id int, label text);
            INSERT INTO all_rows
              SELECT i, (i * 37) % 251, 'value-' || (i % 7)
              FROM generate_series(0, 199) i;
            CREATE TABLE items(LIKE all_rows) PARTITION BY RANGE(k);
            CREATE FUNCTION local_keep(int) RETURNS boolean
              LANGUAGE plpgsql IMMUTABLE AS $$ BEGIN RETURN $1 >= 190; END $$;
        """)
        for part in range(2):
            self.c.sql(f"""
                CREATE TABLE stored{part} AS SELECT * FROM all_rows
                  WHERE k >= {part * 100} AND k < {(part + 1) * 100};
                CREATE SERVER s{part} FOREIGN DATA WRAPPER pgwrh_fdw
                  OPTIONS (host {literal(self.cluster.path)},
                           port '{self.cluster.port}', dbname '{database}');
                CREATE USER MAPPING FOR CURRENT_USER SERVER s{part};
                CREATE FOREIGN TABLE f{part} PARTITION OF items
                  FOR VALUES FROM ({part * 100}) TO ({(part + 1) * 100})
                  SERVER s{part} OPTIONS (schema_name 'public', table_name 'stored{part}');
            """)

    def plan(self, query, analyze=False):
        options = 'VERBOSE, FORMAT JSON' + (', ANALYZE' if analyze else '')
        return json.loads(self.c.scalar(f'EXPLAIN ({options}) ' + query))[0]['Plan']

    def nodes(self, plan):
        yield plan
        for child in plan.get('Plans', []):
            yield from self.nodes(child)

    def remote_sql(self, query):
        return [node['Remote SQL'] for node in self.nodes(self.plan(query))
                if 'Remote SQL' in node]

    def assert_limits(self, query, count=2, limited=True):
        statements = self.remote_sql(query)
        self.assertEqual(len(statements), count, statements)
        self.assertTrue(all((' LIMIT ' in sql) == limited for sql in statements), statements)

    def test_append_and_switch(self):
        query = 'SELECT * FROM items LIMIT 7'
        self.c.sql('SET pgwrh_fdw.enable_limit_pushdown = off')
        self.assert_limits(query, limited=False)
        self.c.sql('SET pgwrh_fdw.enable_limit_pushdown = on')
        self.assert_limits(query)
        self.assertEqual(len(self.c.sql(query)), 7)
        self.assertEqual(self.plan(query)['Node Type'], 'Limit')

    def test_remote_filters_and_projection(self):
        query = "SELECT k + 1, label FROM items WHERE id = ANY(ARRAY[37,74,111,148]) LIMIT 3"
        self.assert_limits(query)
        rows = self.c.sql(query)
        self.assertEqual(len(rows), 3)
        expected = self.c.sql(query.replace('items', 'all_rows').replace(' LIMIT 3', ''))
        self.assertTrue(all(row in expected for row in rows))

    def test_union_all(self):
        query = 'SELECT * FROM f0 UNION ALL SELECT * FROM f1 LIMIT 9'
        self.assert_limits(query)
        self.assertEqual(len(self.c.sql(query)), 9)

    def test_mixed_local_and_foreign(self):
        self.c.sql('ALTER TABLE items DETACH PARTITION f0; '
                   'ALTER TABLE items ATTACH PARTITION stored0 FOR VALUES FROM (0) TO (100)')
        query = 'SELECT * FROM items LIMIT 120'
        self.assert_limits(query, count=1)
        self.assertEqual(len(self.c.sql(query)), 120)

    def test_local_filter_is_not_truncated(self):
        query = 'SELECT * FROM items WHERE local_keep(k) LIMIT 8'
        self.assert_limits(query, limited=False)
        self.assertEqual(len(self.c.sql(query)), 8)

    def test_async_append(self):
        self.c.sql("ALTER SERVER s0 OPTIONS (ADD async_capable 'true'); "
                   "ALTER SERVER s1 OPTIONS (ADD async_capable 'true')")
        query = 'SELECT * FROM items LIMIT 120'
        self.assert_limits(query)
        self.assertEqual(len(self.c.sql(query)), 120)
        scans = [node for node in self.nodes(self.plan(query)) if 'Remote SQL' in node]
        self.assertTrue(all(node['Async Capable'] for node in scans), scans)

    def test_generic_limit_parameters(self):
        self.c.sql('SET plan_cache_mode = force_generic_plan; '
                   'PREPARE page(bigint) AS SELECT * FROM items LIMIT $1')
        for bound, expected in [('3', 3), ('130', 130), ('NULL', 200), ('0', 0), ('1', 1)]:
            query = f'EXECUTE page({bound})'
            self.assert_limits(query)
            self.assertEqual(len(self.c.sql(query)), expected)
        with self.assertRaises(PgError) as error:
            self.c.sql('EXECUTE page(-1)')
        self.assertEqual(error.exception.sqlstate, '2201W')

    def test_filter_join_aggregate_and_sort_barriers(self):
        for query in [
            'SELECT * FROM items LIMIT 7 OFFSET 3',
            'SELECT DISTINCT label FROM items ORDER BY label LIMIT 7',
            'SELECT label, count(*) FROM items GROUP BY label ORDER BY label LIMIT 3',
            'SELECT *, count(*) OVER () FROM items LIMIT 3',
            'SELECT k, generate_series(1,3) FROM items LIMIT 7',
            'SELECT i.* FROM items i JOIN all_rows r ON i.k=r.k WHERE r.k>=190 LIMIT 7',
            'SELECT * FROM items ORDER BY random() LIMIT 7',
            'SELECT * FROM items LIMIT (SELECT 7)',
        ]:
            with self.subTest(query=query):
                self.assert_limits(query, limited=False)
                if 'random()' in query:
                    self.assertEqual(len(self.c.sql(query)), 7)
                else:
                    self.assertEqual(self.c.sql(query), self.c.sql(query.replace('items', 'all_rows')))


if __name__ == '__main__':
    unittest.main(verbosity=2)
