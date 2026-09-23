# SPDX-License-Identifier: AGPL-3.0-only
"""Results, split plans and transferred rows for partial partitionwise aggregates."""
import json
import unittest

from support import Cluster, literal


class PartialAggregateTests(unittest.TestCase):
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
        database = 'partial_test_' + str(self.counter)
        self.admin.sql('CREATE DATABASE ' + database)
        self.c = self.cluster.connect(database)
        self.addCleanup(self.c.close)
        self.c.sql('''
            CREATE EXTENSION pgwrh_fdw;
            SET enable_partitionwise_aggregate = on;
            SET max_parallel_workers_per_gather = 0;
            CREATE TABLE baseline(k int, g int, v int, label text COLLATE "C");
            INSERT INTO baseline SELECT i, CASE WHEN i%7=0 THEN NULL ELSE i%5 END,
                CASE WHEN i%11=0 OR i%5=0 THEN NULL ELSE i%97-48 END,
                CASE WHEN i%11=0 THEN NULL ELSE 'value-'||(i%13) END
                FROM generate_series(0,5999) i;
            CREATE TABLE items(LIKE baseline) PARTITION BY RANGE(k);
            CREATE FUNCTION local_keep(int) RETURNS boolean LANGUAGE plpgsql
                IMMUTABLE AS $$ BEGIN RETURN $1%3=0; END $$;
        ''')
        for part in range(3):
            self.c.sql(f'''
                CREATE TABLE stored{part} AS SELECT * FROM baseline
                    WHERE k >= {part * 3000} AND k < {(part+1) * 3000};
                CREATE SERVER s{part} FOREIGN DATA WRAPPER pgwrh_fdw OPTIONS
                    (host {literal(self.cluster.path)}, port '{self.cluster.port}',
                     dbname '{database}', fdw_tuple_cost '1');
                CREATE USER MAPPING FOR CURRENT_USER SERVER s{part};
                CREATE FOREIGN TABLE f{part} PARTITION OF items
                    FOR VALUES FROM ({part*3000}) TO ({(part+1)*3000})
                    SERVER s{part} OPTIONS(schema_name 'public', table_name 'stored{part}');
            ''')
            # Leave the empty partition's estimate unknown to exercise remote
            # empty states as well as empty groups.
            if part < 2:
                self.c.sql(f'ANALYZE f{part}')

    def nodes(self, plan):
        yield plan
        for child in plan.get('Plans', []):
            yield from self.nodes(child)

    def plan(self, query, analyze=False):
        return json.loads(self.c.scalar('EXPLAIN (VERBOSE, FORMAT JSON' +
            (', ANALYZE' if analyze else '') + ') ' + query))[0]['Plan']

    def assert_query(self, query, pushed=True, scans=3):
        plan = self.plan(query)
        foreign = [n for n in self.nodes(plan) if 'Remote SQL' in n]
        self.assertEqual(len(foreign), scans, plan)
        for node in foreign:
            self.assertEqual('Aggregate on' in node.get('Relations', ''), pushed, node)
            self.assertNotIn(' HAVING ', node['Remote SQL'])
        if pushed:
            self.assertTrue(any(n.get('Partial Mode') == 'Finalize'
                                for n in self.nodes(plan)), plan)
        self.assertEqual(self.c.sql(query), self.c.sql(query.replace('items', 'baseline')))
        return plan

    def test_grouped_and_ungrouped(self):
        for group in ['', ' GROUP BY g ORDER BY g NULLS FIRST']:
            prefix = 'g, ' if group else ''
            self.assert_query('SELECT ' + prefix +
                'count(*),count(v),sum(v),min(v),max(v),min(label),max(label) FROM items' + group)

    def test_mixed_local_foreign(self):
        self.c.sql('ALTER TABLE items DETACH PARTITION f0; '
                   'ALTER TABLE items ATTACH PARTITION stored0 FOR VALUES FROM (0) TO (3000)')
        for group in ['', ' GROUP BY g ORDER BY g NULLS FIRST']:
            plan = self.assert_query('SELECT ' + ('g,' if group else '') +
                'count(*),count(v),sum(v),min(v),max(v) FROM items' + group, scans=2)
            self.assertTrue(any(n.get('Partial Mode') == 'Partial' for n in self.nodes(plan)))

    def test_supported_signatures(self):
        # Every admitted min/max signature and both count/sum variants.
        values = {
            'smallint': 'v::smallint', 'integer': 'v', 'bigint': 'v::bigint',
            'real': 'v::real', 'double precision': 'v::double precision',
            'numeric': 'v::numeric', 'text': 'label', 'char(12)': 'label::char(12)',
            'date': "date '2020-01-01'+v",
            'time': "time '12:00'+v*interval '1 second'",
            'timetz': "timetz '12:00+00'+v*interval '1 second'",
            'timestamp': "timestamp '2020-01-01'+v*interval '1 second'",
            'timestamptz': "timestamptz '2020-01-01 UTC'+v*interval '1 second'",
            'interval': "v*interval '1 second'", 'money': 'v::money',
        }
        # Store expressions as columns so normal FDW shippability (e.g. stable
        # timestamptz arithmetic) doesn't obscure aggregate signature coverage.
        for i, (typename, expr) in enumerate(values.items()):
            with self.subTest(type=typename):
                for table in ['baseline', 'stored0', 'stored1', 'stored2']:
                    self.c.sql(f'ALTER TABLE {table} ADD COLUMN x{i} {typename}; '
                               f'UPDATE {table} SET x{i} = {expr}')
                self.c.sql(f'ALTER TABLE items ADD COLUMN x{i} {typename}')
                aggregates = f'min(x{i}),max(x{i}),count(x{i})'
                if typename in ('smallint', 'integer', 'real', 'double precision', 'money'):
                    aggregates += f',sum(x{i})'
                self.assert_query(f'SELECT g,{aggregates} FROM items GROUP BY g ORDER BY g NULLS FIRST')

    def test_filter_having_and_projection(self):
        for query in [
            'SELECT g,count(*) FROM items GROUP BY g HAVING count(*)>900 ORDER BY g',
            'SELECT g FROM items GROUP BY g HAVING sum(v)>0 ORDER BY g',
            'SELECT g,count(*)+1 FROM items GROUP BY g HAVING local_keep(count(*)::int) ORDER BY g',
            'SELECT count(*) FROM items HAVING count(*)>5000',
            'SELECT count(*) FROM items HAVING sum(v)>999999',
            'SELECT g,count(*) FILTER(WHERE v>0),sum(v) FILTER(WHERE k%2=0),'
                'min(v) FILTER(WHERE false) FROM items GROUP BY g ORDER BY g',
            'SELECT g+1,count(*),sum(v) FROM items WHERE v>0 GROUP BY g+1 ORDER BY 1',
            'SELECT g,count(*) FROM items GROUP BY g ORDER BY count(*) DESC,g LIMIT 3',
        ]:
            with self.subTest(query=query):
                self.assert_query(query)

    def test_empty_input_and_all_null_states(self):
        for suffix in [' WHERE v IS NULL', ' WHERE v>10000', ' WHERE k>=6000']:
            scans = 1 if 'k>=' in suffix else 3
            self.assert_query('SELECT count(*),count(v),sum(v),min(v),max(v) FROM items'+suffix, scans=scans)
            self.assert_query('SELECT g,count(v),sum(v) FROM items'+suffix+
                              ' GROUP BY g ORDER BY g NULLS FIRST', scans=scans)
        self.c.sql('TRUNCATE baseline,stored0,stored1,stored2')
        self.assert_query('SELECT count(*),count(v),sum(v),min(v),max(v) FROM items')
        self.assert_query('SELECT g,count(*) FROM items GROUP BY g ORDER BY g')

    def test_unsupported_and_mixed_fallback(self):
        for expression in ['avg(v)', 'sum(v::bigint)', 'sum(v::numeric)',
                           "sum(v*interval '1 second')", 'avg(v::float8)',
                           'array_agg(v ORDER BY k)', 'count(DISTINCT v)', 'sum(v ORDER BY k)',
                           'min(ARRAY[v])']:
            with self.subTest(expression=expression):
                self.assert_query('SELECT g,count(*),'+expression+
                                  ' FROM items GROUP BY g ORDER BY g', pushed=False)
        self.assert_query('SELECT g,count(*) FROM items GROUP BY g HAVING avg(v)>0 ORDER BY g', pushed=False)
        self.assert_query('SELECT g,count(*) FILTER(WHERE local_keep(v)) FROM items GROUP BY g ORDER BY g', pushed=False)
        self.assert_query('SELECT g,count(*) FROM items WHERE local_keep(k) GROUP BY g ORDER BY g', pushed=False)
        self.assert_query('SELECT local_keep(g),count(*) FROM items GROUP BY local_keep(g) ORDER BY 1', pushed=False)
        self.assert_query('SELECT g,count(*) FROM items GROUP BY ROLLUP(g) ORDER BY g NULLS FIRST,count(*)', pushed=False)

    def test_same_name_custom_aggregate(self):
        self.c.sql('''CREATE AGGREGATE public.sum(integer) (sfunc=int4_sum, stype=bigint,
                       combinefunc=int8pl, parallel=safe);
                       ALTER EXTENSION pgwrh_fdw ADD AGGREGATE public.sum(integer)''')
        for part in range(3):
            self.c.sql(f"ALTER SERVER s{part} OPTIONS(ADD extensions 'pgwrh_fdw')")
        self.assert_query('SELECT g,count(*),public.sum(v) FROM items GROUP BY g ORDER BY g', pushed=False)
        # Even a pg_catalog homonym must not be admitted by its name/schema.
        self.c.sql('''CREATE AGGREGATE pg_catalog.sum(boolean) (sfunc=int8inc_any, stype=bigint,
                       combinefunc=int8pl, initcond='0', parallel=safe)''')
        self.assert_query('SELECT g,count(*),sum(v>0) FROM items GROUP BY g ORDER BY g', pushed=False)

    def test_prepared_custom_and_generic(self):
        query = 'SELECT g,count(*),sum(v) FILTER(WHERE v>$1) FROM items WHERE k>=$2 GROUP BY g HAVING count(*)>$3 ORDER BY g'
        self.c.sql('PREPARE remote_q(int,int,int) AS '+query)
        self.c.sql('PREPARE local_q(int,int,int) AS '+query.replace('items','baseline'))
        for mode in ['force_custom_plan','force_generic_plan']:
            self.c.sql('SET plan_cache_mode='+mode)
            for args in ['0,0,0','10,3000,100','0,6000,0','NULL,0,0']:
                plan=self.plan('EXECUTE remote_q('+args+')')
                sqls=[n['Remote SQL'] for n in self.nodes(plan) if 'Remote SQL' in n]
                self.assertTrue(sqls and all('count(*)' in s for s in sqls), plan)
                self.assertEqual(self.c.sql('EXECUTE remote_q('+args+')'),
                                 self.c.sql('EXECUTE local_q('+args+')'))

    def test_control_remote_estimates_and_full_aggregation(self):
        query='SELECT g,count(*),sum(v) FROM items GROUP BY g ORDER BY g'
        self.c.sql('SET enable_partitionwise_aggregate=off')
        self.assert_query(query, pushed=False)
        self.c.sql('SET enable_partitionwise_aggregate=on')
        for part in range(3):
            self.c.sql(f"ALTER SERVER s{part} OPTIONS(ADD use_remote_estimate 'true')")
        self.assert_query(query)
        # Groups including the partition key retain full aggregation, including
        # signatures intentionally excluded from partial pushdown.
        query='SELECT k,count(*),avg(v) FROM items GROUP BY k HAVING count(*)>0 ORDER BY k'
        plan=self.plan(query)
        sqls=[n['Remote SQL'] for n in self.nodes(plan) if 'Remote SQL' in n]
        self.assertTrue(all('avg(' in s and ' HAVING ' in s for s in sqls), sqls)
        self.assertFalse(any(n.get('Partial Mode')=='Finalize' for n in self.nodes(plan)))
        self.assertEqual(self.c.sql(query), self.c.sql(query.replace('items','baseline')))

    def test_redundant_grouping_and_empty_constant_group(self):
        for query in [
            'SELECT g,count(*) FROM items WHERE g=1 GROUP BY g',
            'SELECT g,v,count(*) FROM items WHERE g=v GROUP BY g,v ORDER BY g,v',
            'SELECT 1,count(*) FROM items WHERE v>10000 GROUP BY 1',
        ]:
            self.assertEqual(self.c.sql(query), self.c.sql(query.replace('items','baseline')))

    def test_transfer_reduction(self):
        query='SELECT g,count(*),sum(v) FROM items GROUP BY g ORDER BY g'
        pushed=self.assert_query(query)
        pushed=self.plan(query, analyze=True)
        self.c.sql('SET enable_partitionwise_aggregate=off')
        raw=self.plan(query, analyze=True)
        def rows(plan):
            return sum(n['Actual Rows']*n['Actual Loops'] for n in self.nodes(plan) if 'Remote SQL' in n)
        self.assertEqual(rows(raw),6000)
        self.assertEqual(rows(pushed),12)


if __name__ == '__main__':
    unittest.main(verbosity=2)
