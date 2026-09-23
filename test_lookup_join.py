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

    def test_in_transforms_to_remote_semijoin(self):
        query = 'SELECT s.* FROM items s WHERE s.k IN (SELECT l.k FROM lookup l WHERE l.enabled)'
        plan = self.lookup_plan(query, True)
        self.assertEqual(plan['Lookup Join'], 'Remote EXISTS')
        self.assertEqual(plan['Remote Rows'], 4)
        self.equivalent(query)

    def test_security_barrier_and_nested_partition_fallback(self):
        self.c.sql('CREATE VIEW protected_lookup WITH(security_barrier=true) AS '
                   'SELECT * FROM lookup WHERE enabled')
        query = 'SELECT s.id,l.label FROM items s JOIN protected_lookup l ON s.k=l.k'
        self.assertFalse(any('Custom Plan Provider' in n for n in self.nodes(self.plan(query))))
        self.equivalent(query)
        # The root still has one scalar integer key, but a slot with multiple
        # children needs another routing analysis and is deliberately declined.
        self.c.sql('ALTER TABLE items DETACH PARTITION f0; '
                   'CREATE TABLE slot PARTITION OF items FOR VALUES FROM(0) TO(10000) PARTITION BY RANGE(k); '
                   'CREATE VIEW nested_data0 AS SELECT * FROM stored0 WHERE k<5000; '
                   'CREATE VIEW nested_data1 AS SELECT * FROM stored0 WHERE k>=5000; '
                   "CREATE FOREIGN TABLE nested0 PARTITION OF slot FOR VALUES FROM(0) TO(5000) SERVER s0 OPTIONS(table_name 'nested_data0'); "
                   "CREATE FOREIGN TABLE nested1 PARTITION OF slot FOR VALUES FROM(5000) TO(10000) SERVER s0 OPTIONS(table_name 'nested_data1')")
        query = 'SELECT s.id,l.label FROM items s JOIN lookup l ON s.k=l.k WHERE l.enabled'
        self.assertFalse(any('Custom Plan Provider' in n for n in self.nodes(self.plan(query))))
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


    def test_mixed_local_and_foreign(self):
        self.c.sql('ALTER TABLE items DETACH PARTITION f0; '
                   'ALTER TABLE items ATTACH PARTITION stored0 FOR VALUES FROM(0) TO(10000)')
        for semi in [False, True]:
            query = ('SELECT s.* FROM items s WHERE EXISTS(SELECT FROM lookup l WHERE l.k=s.k AND l.enabled)'
                     if semi else 'SELECT s.*, l.label FROM items s JOIN lookup l ON l.k=s.k WHERE l.enabled')
            plan = self.lookup_plan(query, True)
            self.assertEqual(plan['Remote Executions'], 1)
            self.equivalent(query)

    def test_single_leaf_partition_slots(self):
        for i in range(2):
            bounds = f'FOR VALUES FROM({i*10000}) TO({(i+1)*10000})'
            self.c.sql(f'ALTER TABLE items DETACH PARTITION f{i}; '
                       f'CREATE TABLE slot{i} PARTITION OF items {bounds} PARTITION BY RANGE(k); '
                       f'ALTER TABLE slot{i} ATTACH PARTITION f{i} {bounds}')
        query = 'SELECT s.*, l.label FROM items s JOIN lookup l ON l.k=s.k WHERE l.enabled'
        self.lookup_plan(query, True)
        self.equivalent(query)

    def test_hash_and_list_routing(self):
        for strategy, bounds in [('HASH', ['WITH(MODULUS 2, REMAINDER 0)', 'WITH(MODULUS 2, REMAINDER 1)']),
                                 ('LIST', ['IN ('+','.join(str(k) for k in range(1,2001,2))+')',
                                           'IN ('+','.join(str(k) for k in range(2,2001,2))+')'])]:
            self.c.sql('DROP TABLE items; CREATE TABLE items(k int,id int,value text) PARTITION BY ' + strategy + '(k)')
            for i, bound in enumerate(bounds):
                predicate = (f"satisfies_hash_partition('items'::regclass,2,{i},n)"
                             if strategy == 'HASH' else f'n <= 2000 AND n%2={1-i}')
                self.c.sql(f'CREATE TABLE data_{strategy}{i} AS SELECT n k,n id,NULL::text value '
                           f'FROM generate_series(1,20000) n WHERE {predicate}')
                self.c.sql(f'CREATE FOREIGN TABLE f{i} PARTITION OF items FOR VALUES {bound} SERVER s{i} '
                           f"OPTIONS(table_name 'data_{strategy.lower()}{i}')")
            self.c.sql('ANALYZE f0; ANALYZE f1')
            query = 'SELECT s.*, l.label FROM items s JOIN lookup l ON l.k=s.k WHERE l.enabled'
            self.lookup_plan(query, True)
            self.equivalent(query)
            self.c.sql('SELECT pgwrh_fdw_disconnect_all()')

    def test_standalone_integer_types_and_null_payload_positions(self):
        for typ in ['smallint', 'bigint']:
            name = 'typed_' + typ
            self.c.sql(f'CREATE TABLE {name}(k {typ},id int,value text); '
                       f'INSERT INTO {name} SELECT k,id,value FROM stored0; '
                       f'CREATE TABLE l_{name}(k {typ}, label text, threshold int); '
                       f"INSERT INTO l_{name} VALUES(1,'v1',0),(1,NULL,1),(NULL,'v1',NULL)")
            self.c.sql(f'CREATE FOREIGN TABLE f_{name}(k {typ},id int,value text) SERVER s0 OPTIONS(table_name \'{name}\'); '
                       f'ANALYZE f_{name}; ANALYZE l_{name}')
            query = (f'SELECT s.*,l.label FROM f_{name} s JOIN l_{name} l ON s.k=l.k '
                     'AND (l.label=s.value OR l.label IS NULL) AND l.threshold<s.id+1')
            plan = self.lookup_plan(query, True)
            self.assertEqual(plan['Lookup Rows'], 3)
            self.assertEqual(plan['Lookup Condition Columns'], 3)
            self.equivalent(query)

    def test_generic_plan_current_values_and_runtime_overflow(self):
        for semi in [False, True]:
            self.c.sql('SET plan_cache_mode=force_generic_plan')
            query = ('SELECT s.* FROM items s WHERE EXISTS(SELECT FROM lookup l WHERE l.k=s.k AND l.enabled AND l.k<$1)'
                     if semi else 'SELECT s.*,l.label FROM items s JOIN lookup l ON s.k=l.k WHERE l.enabled AND l.k<$1')
            self.c.sql('PREPARE lookup_query(int) AS ' + query)
            self.lookup_plan('EXECUTE lookup_query(20000)', True)
            self.c.sql("UPDATE lookup SET enabled=false WHERE k=1; UPDATE lookup SET label='changed' WHERE k=15001")
            self.assertEqual(self.lookup_plan('EXECUTE lookup_query(20000)', True)['Remote Rows'], 2)
            self.assertEqual(self.lookup_plan('EXECUTE lookup_query(10)', True)['Remote Executions'], 0)
            # The cached path was costed with generous bounds. The decision is
            # made again from the current materialization before remote output.
            self.c.sql('SET pgwrh_fdw.lookup_join_max_rows=1; UPDATE lookup SET enabled=true WHERE k=1')
            plan = self.lookup_plan('EXECUTE lookup_query(20000)', True)
            self.assertEqual(plan['Lookup Execution'], 'Local overflow fallback')
            self.c.sql('SET pgwrh_fdw.enable_lookup_join=off; PREPARE ordinary(int) AS ' + query)
            expected = self.c.sql('EXECUTE ordinary(20000)')
            self.c.sql('SET pgwrh_fdw.enable_lookup_join=on')
            self.assertCountEqual(self.c.sql('EXECUTE lookup_query(20000)'), expected)
            self.c.sql("INSERT INTO lookup SELECT 1,'extra',0,true FROM generate_series(1,10)")
            self.c.sql('SET pgwrh_fdw.lookup_join_max_rows=10000; SET pgwrh_fdw.lookup_join_max_memory=1')
            self.assertEqual(self.lookup_plan('EXECUTE lookup_query(20000)', True)['Lookup Execution'],
                             'Local overflow fallback')
            self.c.sql('DEALLOCATE ALL; SET pgwrh_fdw.lookup_join_max_memory=8192')

    def test_scalar_payload_transmission_modes(self):
        self.c.sql('''CREATE TABLE scalar_data AS
            SELECT n k, DATE '2024-02-03' d, TIMESTAMP '2024-02-03 12:13:14.123456' t,
              1.2345678901234567::float8 f, INTERVAL '-1 year 2 days 03:04:05.123' v,
              'quote, "brace{ and backslash' || chr(92) tag
            FROM generate_series(1,10000) n;
            CREATE FOREIGN TABLE scalar_foreign(k int,d date,t timestamp,f float8,v interval,tag text)
              SERVER s0 OPTIONS(table_name 'scalar_data');
            CREATE TABLE scalar_lookup AS SELECT *, 'retained'::text label FROM scalar_data WHERE k=1;
            INSERT INTO scalar_lookup SELECT k,d,t,f,v,NULL,NULL FROM scalar_lookup;
        ''')
        self.c.sql("ANALYZE scalar_foreign; ANALYZE scalar_lookup; "
                   "SET DateStyle='SQL, DMY'; SET IntervalStyle=sql_standard; SET extra_float_digits=-3")
        query = ('SELECT s.k,l.label FROM scalar_foreign s JOIN scalar_lookup l ON '
                 's.k=l.k AND s.d=l.d AND s.t=l.t AND s.f=l.f AND s.v=l.v '
                 'AND (s.tag=l.tag OR l.tag IS NULL)')
        plan = self.lookup_plan(query, True)
        self.assertEqual(plan['Lookup Condition Columns'], 6)
        self.assertEqual(plan['Remote Rows'], 2)
        self.equivalent(query)
        self.assertEqual(self.c.scalar('SHOW DateStyle'), 'SQL, DMY')
        self.assertEqual(self.c.scalar('SHOW extra_float_digits'), '-3')

    def test_retained_type_and_attribute_mapping(self):
        self.c.sql("CREATE TYPE local_payload AS ENUM ('a','b'); ALTER TABLE lookup ADD payload local_payload; "
                   "UPDATE lookup SET payload='a' WHERE label IS NOT NULL")
        self.c.sql('ALTER TABLE items DETACH PARTITION f0; DROP FOREIGN TABLE f0; '
                   "CREATE FOREIGN TABLE f0(value text,id int,k int) SERVER s0 OPTIONS(table_name 'reversed0'); "
                   'ALTER TABLE items ATTACH PARTITION f0 FOR VALUES FROM(0) TO(10000); ANALYZE f0')
        query = 'SELECT s.id,l.payload,l.label FROM items s JOIN lookup l ON s.k=l.k WHERE l.enabled'
        plan = self.lookup_plan(query, True)
        self.assertEqual(plan['Lookup Condition Columns'], 1)
        self.assertTrue(all('local_payload' not in n.get('Remote SQL','') and
                            'payload' not in n.get('Remote SQL','') for n in self.nodes(plan)))
        self.equivalent(query)

    def test_virtual_servers_frozen_context_and_savepoints(self):
        for i in range(2):
            self.c.sql(f"ALTER SERVER s{i} OPTIONS(ADD transaction_parameters 'app.request_id'); "
                       f"CREATE SERVER v{i} FOREIGN DATA WRAPPER pgwrh_fdw OPTIONS(members 's{i}'); "
                       f'CREATE USER MAPPING FOR CURRENT_USER SERVER v{i}; DROP FOREIGN TABLE f{i}; '
                       f'CREATE FOREIGN TABLE f{i} PARTITION OF items FOR VALUES FROM({i*10000}) TO({(i+1)*10000}) '
                       f"SERVER v{i} OPTIONS(table_name 'reversed{i}')")
            self.c.sql(f"CREATE OR REPLACE VIEW reversed{i} AS SELECT k,id,current_setting('app.request_id',true) value FROM stored{i}")
        query = 'SELECT s.k,s.value,l.label FROM items s JOIN lookup l ON s.k=l.k WHERE l.enabled'
        self.c.sql("BEGIN; SET LOCAL app.request_id='frozen'; SAVEPOINT first")
        self.lookup_plan(query, True)
        self.assertTrue(all(r[1] == 'frozen' for r in self.c.sql(query)))
        self.c.sql("ROLLBACK TO first; SET LOCAL app.request_id='changed'")
        self.assertTrue(all(r[1] == 'frozen' for r in self.c.sql(query)))
        self.c.sql('COMMIT')
        self.equivalent(query)

    def test_cancel_and_remote_error_cleanup(self):
        self.c.sql('CREATE OR REPLACE VIEW reversed0 AS SELECT k,id,value FROM stored0, '
                   '(SELECT pg_sleep(10)) delay')
        query = 'SELECT s.* FROM items s JOIN lookup l ON s.k=l.k WHERE l.enabled'
        self.lookup_plan(query)
        self.c.sql("SET statement_timeout='100ms'")
        with self.assertRaises(PgError) as error:
            self.c.sql(query)
        self.assertEqual(error.exception.sqlstate, '57014')
        self.c.sql('SET statement_timeout=0')
        self.c.sql('CREATE OR REPLACE VIEW reversed0 AS SELECT k,id,value FROM stored0')
        self.lookup_plan(query, True)
        self.equivalent(query)
        self.c.sql('ALTER VIEW reversed0 RENAME TO unavailable')
        with self.assertRaises(PgError):
            self.c.sql(query)
        self.c.sql('ALTER VIEW unavailable RENAME TO reversed0')
        self.equivalent(query)

    def test_rescan_reuses_materialization(self):
        # Disable Material/Memoize so the unparameterized custom join is rescanned
        # by an outer nested loop. Its own lookup is consumed only once.
        self.c.sql('CREATE TEMP TABLE repetitions(n int); INSERT INTO repetitions SELECT n FROM generate_series(1,10000) n; '
                   'ANALYZE repetitions; SET enable_material=off; SET enable_memoize=off; '
                   'SET join_collapse_limit=1; SET enable_hashjoin=off; SET enable_mergejoin=off')
        query = ('SELECT r.n,j.* FROM repetitions r CROSS JOIN '
                 '(SELECT s.id,l.label FROM items s JOIN lookup l ON s.k=l.k WHERE l.enabled) j WHERE r.n<=3')
        plan = self.lookup_plan(query, True)
        self.assertGreater(plan['Actual Loops'], 1, plan)
        self.assertEqual(plan['Plans'][0]['Actual Loops'], 1, plan)
        self.equivalent(query)

    def test_negative_plans_and_permissions(self):
        self.c.sql("ALTER FOREIGN TABLE f0 OPTIONS(SET table_name 'stored0'); "
                   "ALTER FOREIGN TABLE f1 OPTIONS(SET table_name 'stored1')")
        self.c.sql('CREATE FUNCTION local_condition(int,int) RETURNS bool LANGUAGE plpgsql IMMUTABLE '
                   'AS $$BEGIN RETURN $1=$2; END$$')
        queries = [
            'SELECT s.id,l.label FROM items s LEFT JOIN lookup l ON s.k=l.k',
            'SELECT s.* FROM items s WHERE NOT EXISTS(SELECT FROM lookup l WHERE s.k=l.k)',
            'SELECT s.id,l.label FROM items s JOIN lookup l ON s.k=l.k AND local_condition(s.id,l.threshold)',
            'SELECT s.* FROM items s WHERE EXISTS(SELECT FROM lookup l WHERE s.k=l.k AND local_condition(s.id,l.threshold))',
            'SELECT s.id,l.label FROM items s JOIN lookup l ON s.k=l.k WHERE random()<2',
            'SELECT s.id,l.label FROM items s JOIN lookup l ON s.k=l.k FOR UPDATE OF l',
        ]
        for query in queries:
            self.assertFalse(any('Custom Plan Provider' in n for n in self.nodes(self.plan(query))), query)
            self.equivalent(query)
        query = 'SELECT s.id,l.label FROM items s JOIN lookup l ON s.k=l.k WHERE l.enabled'
        self.c.sql("ALTER FOREIGN TABLE f0 OPTIONS(ADD lookup_join 'false')")
        self.assertFalse(any('Custom Plan Provider' in n for n in self.nodes(self.plan(query))))
        self.equivalent(query)
        self.c.sql('CREATE ROLE restricted; GRANT SELECT ON items TO restricted; SET ROLE restricted')
        with self.assertRaises(PgError) as error:
            self.c.sql(query)
        self.assertEqual(error.exception.sqlstate, '42501')
        self.c.sql('RESET ROLE; GRANT SELECT ON lookup TO restricted; '
                   'ALTER TABLE lookup ENABLE ROW LEVEL SECURITY; '
                   'CREATE POLICY allowed ON lookup USING(k=1); '
                   "ALTER USER MAPPING FOR CURRENT_USER SERVER s0 OPTIONS(ADD password_required 'false')")
        self.c.sql('ALTER FOREIGN TABLE f0 OPTIONS(DROP lookup_join)')
        current_user = self.c.scalar('SELECT current_user')
        for i in range(2):
            self.c.sql(f"CREATE USER MAPPING FOR restricted SERVER s{i} OPTIONS(user {literal(current_user)}, password_required 'false')")
        self.c.sql('DROP TABLE expected,actual; SET ROLE restricted')
        self.assertFalse(any('Custom Plan Provider' in n for n in self.nodes(self.plan(query))))
        self.equivalent(query)
        self.c.sql('RESET ROLE')

    def test_explain_only_and_remote_estimates(self):
        query = 'SELECT s.id,l.label FROM items s JOIN lookup l ON s.k=l.k WHERE l.enabled'
        self.lookup_plan(query)
        self.assertEqual(self.c.sql('SELECT * FROM pgwrh_fdw_get_connections()'), [])
        self.c.sql("INSERT INTO lookup SELECT n,'key'||n,0,true FROM generate_series(10,100) n; ANALYZE lookup; "
                   'UPDATE lookup SET enabled=false WHERE k>10000')
        self.c.sql("ALTER SERVER s0 OPTIONS(ADD use_remote_estimate 'true'); "
                   "ALTER SERVER s1 OPTIONS(ADD use_remote_estimate 'true')")
        mark = self.cluster.log.stat().st_size
        plan = self.lookup_plan(query, True)
        self.assertEqual(plan['Remote Executions'], 1)
        self.assertEqual(plan['Skipped Shards'], 1)
        logs = self.cluster.log.read_text()[mark:]
        self.assertIn('EXPLAIN SELECT', logs)
        self.assertIn('DECLARE', logs)
        self.equivalent(query)


    def create_stock_server(self):
        db = self.c.scalar('SELECT current_database()')
        self.c.sql(f'''CREATE EXTENSION postgres_fdw;
            CREATE SERVER stock FOREIGN DATA WRAPPER postgres_fdw
              OPTIONS(host {literal(self.cluster.path)},port '{self.cluster.port}',dbname '{db}');
            CREATE USER MAPPING FOR CURRENT_USER SERVER stock''')

    def stock_where_shipped(self, table, predicate):
        sql = [n['Remote SQL'] for n in self.nodes(self.plan(f'SELECT id FROM {table} WHERE {predicate}'))
               if 'Remote SQL' in n]
        return any(' WHERE ' in q for q in sql)

    def typed_tables(self, name, typ, expression, op='='):
        self.c.sql(f'''CREATE TABLE {name}_data(k {typ},id int);
            INSERT INTO {name}_data SELECT {expression},n FROM generate_series(1,4000) n;
            CREATE TABLE {name}_lookup(k {typ},label text);
            INSERT INTO {name}_lookup SELECT k,'first' FROM {name}_data WHERE id=1;
            INSERT INTO {name}_lookup SELECT k,NULL FROM {name}_lookup;
            INSERT INTO {name}_lookup VALUES(NULL,'null-key');
            INSERT INTO {name}_data SELECT * FROM {name}_data WHERE id=1;
            CREATE FOREIGN TABLE {name}_foreign(k {typ},id int) SERVER s0 OPTIONS(table_name '{name}_data');
            CREATE FOREIGN TABLE {name}_stock(k {typ},id int) SERVER stock OPTIONS(table_name '{name}_data')''')
        self.c.sql(f'ANALYZE {name}_foreign; ANALYZE {name}_lookup')
        value = self.c.scalar(f'SELECT k::text FROM {name}_data WHERE id=1 LIMIT 1')
        return f'k {op} {literal(value)}::{typ}'

    def test_standard_fdw_builtin_types(self):
        self.create_stock_server()
        cases = [
            ('text', "'key-'||n", '='),
            ('varchar(40)', "'key-'||n", '='),
            ('char(40)', "'key-'||n", '='),
            ('uuid', 'md5(n::text)::uuid', '='),
            ('numeric', 'n::numeric/10', '='),
            ('float8', "CASE WHEN n=1 THEN 'NaN'::float8 ELSE n::float8 END", '='),
            ('bool', 'n=1', '='),
            ('bytea', "decode(lpad(to_hex(n),8,'0'),'hex')", '='),
            ('date', "DATE '2024-01-01'+n", '='),
            ('timestamp', "TIMESTAMP '2024-01-01'+n*INTERVAL '1 day'", '='),
            ('timestamptz', "TIMESTAMPTZ '2024-01-01 00:00:00+00'+n*INTERVAL '1 day'", '='),
            ('time', "TIME '00:00:00'+n*INTERVAL '1 second'", '='),
            ('timetz', "TIMETZ '00:00:00+00'+n*INTERVAL '1 second'", '='),
            ('interval', "n*INTERVAL '1 day'", '='),
            ('inet', "'10.0.0.0'::inet+n", '='),
            ('bit(32)', 'n::bit(32)', '='),
            ('jsonb', "jsonb_build_object('key',n)", '='),
            ('int4range', 'int4range(n,n+1)', '='),
            ('int4multirange', 'int4multirange(int4range(n,n+1))', '='),
            ('point', 'point(n,n)', '~='),
            ('int[]', "CASE WHEN n=1 THEN '[0:1]={1,NULL}'::int[] ELSE ARRAY[n] END", '='),
            ('text[]', "ARRAY[n::text,'comma, quote\" and slash'||chr(92),NULL]", '='),
        ]
        for i, (typ, expression, op) in enumerate(cases):
            with self.subTest(type=typ):
                name = 'typed' + str(i)
                predicate = self.typed_tables(name, typ, expression, op)
                self.assertTrue(self.stock_where_shipped(name + '_stock', predicate), typ)
                for semi in [False, True]:
                    query = (f'SELECT s.id FROM {name}_foreign s WHERE EXISTS('
                             f'SELECT FROM {name}_lookup l WHERE s.k {op} l.k)' if semi else
                             f'SELECT s.id,l.label FROM {name}_foreign s JOIN {name}_lookup l ON s.k {op} l.k')
                    plan = self.lookup_plan(query, True)
                    self.assertEqual(plan['Remote Rows'], 2 if semi else 4)
                    self.assertEqual(plan['Lookup Routing'], 'Single destination')
                    sql = [n['Remote SQL'] for n in self.nodes(plan)
                           if 'pg_catalog.unnest' in n.get('Remote SQL', '')]
                    self.assertEqual(len(sql), 1)
                    self.assertNotIn('label', sql[0])
                    self.assertEqual('lookup_rowno' in sql[0], not semi)
                    if typ == 'int[]':
                        self.assertIn('pg_catalog.unnest($1::text[])', sql[0])
                        self.assertIn('(l.c1::integer[])', sql[0])
                    self.equivalent(query)

    def test_shippable_nonequality_operators(self):
        self.create_stock_server()
        self.typed_tables('pattern', 'text', "'key-'||n")
        self.typed_tables('regex', 'text', "'key-'||n")
        self.c.sql("UPDATE regex_lookup SET k=k||'$'; ANALYZE regex_lookup")
        self.typed_tables('member', 'int[]', 'ARRAY[n]')
        for table, condition, predicate in [
            ('pattern', 's.k LIKE l.k', "k LIKE 'key-1'"),
            ('regex', 's.k ~ l.k', "k ~ 'key-1$'"),
            ('member', 's.id=ANY(l.k)', 'id=ANY(ARRAY[1])'),
            ('member', 's.k && l.k', 'k && ARRAY[1]'),
        ]:
            with self.subTest(condition=condition):
                self.assertTrue(self.stock_where_shipped(table + '_stock', predicate))
                for semi in [False, True]:
                    query = (f'SELECT s.id FROM {table}_foreign s WHERE EXISTS('
                             f'SELECT FROM {table}_lookup l WHERE {condition})' if semi else
                             f'SELECT s.id,l.label FROM {table}_foreign s JOIN {table}_lookup l ON {condition}')
                    self.assertEqual(self.lookup_plan(query, True)['Remote Rows'], 2 if semi else 4)
                    self.equivalent(query)
        # Like stock WHERE pushdown, a collatable function using only local
        # values has no foreign-derived collation provenance.
        self.assertFalse(self.stock_where_shipped('pattern_stock',
            "k ~ ((SELECT k FROM pattern_lookup LIMIT 1)||'$')"))
        for semi in [False, True]:
            condition = "s.k ~ (l.k||'$')"
            query = (f'SELECT s.id FROM pattern_foreign s WHERE EXISTS('
                     f'SELECT FROM pattern_lookup l WHERE {condition})' if semi else
                     f'SELECT s.id,l.label FROM pattern_foreign s JOIN pattern_lookup l ON {condition}')
            self.assertFalse(any('Custom Plan Provider' in n for n in self.nodes(self.plan(query))))
            self.equivalent(query)

    def test_extension_types_follow_server_shippability(self):
        self.create_stock_server()
        # Model extension-owned types, including quoted identifiers. There is
        # no named transport composite installed on the remote server.
        self.c.sql('''CREATE SCHEMA "lookup types";
            CREATE TYPE "lookup types"."Status" AS ENUM('yes','no');
            CREATE DOMAIN "lookup types"."Number" AS numeric CHECK(VALUE>=0);
            CREATE TYPE "lookup types"."Pair" AS (n int,txt text);
            CREATE DOMAIN "lookup types"."Array" AS int[];
            ALTER EXTENSION pgwrh_fdw ADD TYPE "lookup types"."Status";
            ALTER EXTENSION pgwrh_fdw ADD DOMAIN "lookup types"."Number";
            ALTER EXTENSION pgwrh_fdw ADD TYPE "lookup types"."Pair";
            ALTER EXTENSION pgwrh_fdw ADD DOMAIN "lookup types"."Array"''')
        cases = [
            ('Status', "CASE WHEN n=1 THEN 'yes' ELSE 'no' END::\"lookup types\".\"Status\""),
            ('Number', 'n::numeric'),
            ('Pair', "ROW(CASE WHEN n=1 THEN NULL ELSE n END,'quote,\" slash'||chr(92))::\"lookup types\".\"Pair\""),
            ('Array', "CASE WHEN n=1 THEN ARRAY[]::int[] ELSE ARRAY[n,NULL] END"),
        ]
        for i, (typ, expression) in enumerate(cases):
            with self.subTest(type=typ):
                name = 'extension' + str(i)
                predicate = self.typed_tables(name, '"lookup types"."' + typ + '"', expression)
                # A domain-literal cast introduces CoerceToDomain, which stock
                # postgres_fdw intentionally keeps local. The comparison to a
                # base-type value still checks shippability of the column type.
                if typ in ('Number', 'Array'):
                    predicate = predicate.replace('::"lookup types"."' + typ + '"',
                                                  '::numeric' if typ == 'Number' else '::int[]')
                query = f'SELECT s.id,l.label FROM {name}_foreign s JOIN {name}_lookup l ON s.k=l.k'
                self.assertFalse(self.stock_where_shipped(name + '_stock', predicate))
                self.assertFalse(any('Custom Plan Provider' in n for n in self.nodes(self.plan(query))))
                for server in ['s0', 'stock']:
                    self.c.sql(f"ALTER SERVER {server} OPTIONS(ADD extensions 'pgwrh_fdw')")
                self.assertTrue(self.stock_where_shipped(name + '_stock', predicate))
                self.assertEqual(self.lookup_plan(query, True)['Remote Rows'], 4)
                self.equivalent(query)
                semi = f'SELECT s.id FROM {name}_foreign s WHERE EXISTS(SELECT FROM {name}_lookup l WHERE s.k=l.k)'
                self.lookup_plan(semi, True)
                self.equivalent(semi)
                for server in ['s0', 'stock']:
                    self.c.sql(f'ALTER SERVER {server} OPTIONS(DROP extensions)')

    def test_noninteger_partition_routing(self):
        cases = [('text', "lpad({n}::text,5,'0')"),
                 ('uuid', "lpad(to_hex({n}),32,'0')::uuid"),
                 ('numeric', '{n}::numeric/10'),
                 ('date', "DATE '2024-01-01'+{n}"),
                 ('int[]', 'ARRAY[{n}]')]
        for typ, expression in cases:
            for strategy in ['RANGE', 'LIST', 'HASH']:
                with self.subTest(type=typ, strategy=strategy):
                    a = literal(self.c.scalar(f'SELECT ({expression.format(n=1)})::text'))
                    middle = literal(self.c.scalar(f'SELECT ({expression.format(n=2001)})::text'))
                    bounds = ([f'FROM(MINVALUE) TO({middle})', f'FROM({middle}) TO(MAXVALUE)'] if strategy == 'RANGE'
                              else [f'IN ({a})', None] if strategy == 'LIST'
                              else ['WITH(MODULUS 2,REMAINDER 0)', 'WITH(MODULUS 2,REMAINDER 1)'])
                    collate = ''
                    self.c.sql(f'''CREATE TABLE routed_data(k {typ}{collate},id int) PARTITION BY {strategy}(k);
                        CREATE TABLE routed_items(k {typ}{collate},id int) PARTITION BY {strategy}(k);
                        CREATE TABLE routed_lookup(k {typ},label text)''')
                    for i, bound in enumerate(bounds):
                        clause = 'DEFAULT' if bound is None else 'FOR VALUES ' + bound
                        self.c.sql(f'CREATE TABLE routed_data{i} PARTITION OF routed_data {clause}; '
                                   f'CREATE FOREIGN TABLE routed_f{i} PARTITION OF routed_items {clause} SERVER s{i} '
                                   f"OPTIONS(table_name 'routed_data{i}')")
                    self.c.sql(f'''INSERT INTO routed_data SELECT {expression.format(n='n')},n FROM generate_series(1,4000) n;
                        INSERT INTO routed_lookup SELECT k,'first' FROM routed_data WHERE id=1;
                        INSERT INTO routed_lookup SELECT k,NULL FROM routed_lookup;
                        INSERT INTO routed_lookup VALUES(NULL,'null-key');
                        INSERT INTO routed_data SELECT * FROM routed_data WHERE id=1''')
                    self.c.sql('ANALYZE routed_f0; ANALYZE routed_f1; ANALYZE routed_lookup')
                    self.c.sql('SELECT pgwrh_fdw_disconnect_all()')
                    for semi in [False, True]:
                        query = ('SELECT s.id FROM routed_items s WHERE EXISTS(SELECT FROM routed_lookup l WHERE s.k=l.k)'
                                 if semi else 'SELECT s.id,l.label FROM routed_items s JOIN routed_lookup l ON s.k=l.k')
                        plan = self.lookup_plan(query, True)
                        self.assertEqual(plan['Lookup Routing'], 'Partition equality')
                        self.assertEqual(plan['Remote Executions'], 1)
                        self.assertEqual(plan['Skipped Shards'], 1)
                        self.assertEqual(plan['Remote Rows'], 2 if semi else 4)
                        self.assertEqual(self.c.scalar('SELECT count(*) FROM pgwrh_fdw_get_connections()'), '1')
                        self.equivalent(query)
                        self.c.sql('SELECT pgwrh_fdw_disconnect_all()')
                    self.c.sql('DROP TABLE routed_items,routed_data,routed_lookup CASCADE')

    def test_shippable_conditions_without_routing_proof(self):
        conditions = ['s.k+1=l.k', 's.k::bigint=l.k', 's.k IS NOT DISTINCT FROM l.k',
                      's.k=l.k OR s.k+1=l.k']
        for condition in conditions:
            for semi in [False, True]:
                query = (f'SELECT s.id FROM items s WHERE EXISTS(SELECT FROM lookup l WHERE {condition} AND l.enabled)'
                         if semi else f'SELECT s.id,l.label FROM items s JOIN lookup l ON {condition} WHERE l.enabled')
                plan = self.lookup_plan(query, True)
                self.assertEqual(plan['Lookup Routing'], 'All destinations')
                self.assertEqual(plan['Remote Executions'], 2)
                self.equivalent(query)
        self.c.sql('SET plan_cache_mode=force_generic_plan; PREPARE broadcast AS ' + query)
        self.lookup_plan('EXECUTE broadcast', True)
        self.c.sql('SET pgwrh_fdw.lookup_join_max_rows=1')
        self.assertEqual(self.lookup_plan('EXECUTE broadcast', True)['Lookup Execution'], 'Local overflow fallback')
        self.assertCountEqual(self.c.sql('EXECUTE broadcast'), self.c.sql(query))

    def test_array_occurrences_dimensions_and_generic_fallback(self):
        self.create_stock_server()
        self.typed_tables('arrays', 'int[]', 'ARRAY[n]')
        self.c.sql('''TRUNCATE arrays_lookup;
            INSERT INTO arrays_lookup VALUES(ARRAY[]::int[],'empty'),(ARRAY[[1,2],[3,4]],'matrix'),
              ('[0:1]={1,NULL}'::int[],'bounds'),(NULL,'null'),(ARRAY[]::int[],NULL);
            INSERT INTO arrays_data SELECT k,0 FROM arrays_lookup;
            ANALYZE arrays_lookup''')
        query = 'SELECT s.id,l.label FROM arrays_foreign s JOIN arrays_lookup l ON s.k IS NOT DISTINCT FROM l.k'
        self.c.sql('SET plan_cache_mode=force_generic_plan; PREPARE arrays_query AS ' + query)
        self.lookup_plan('EXECUTE arrays_query', True)
        self.equivalent(query)
        self.c.sql("UPDATE arrays_lookup SET k=ARRAY[[9,8],[7,6]] WHERE label='matrix'")
        self.assertCountEqual(self.c.sql('EXECUTE arrays_query'), self.c.sql(query))
        self.c.sql('SET pgwrh_fdw.lookup_join_max_memory=1')
        self.assertEqual(self.lookup_plan('EXECUTE arrays_query', True)['Lookup Execution'], 'Local overflow fallback')
        self.assertCountEqual(self.c.sql('EXECUTE arrays_query'), self.c.sql(query))

    def test_equality_must_match_partition_operator_family(self):
        # Case-insensitive equality matches both ranges; routing the lowercase
        # input through text partition bounds would lose the uppercase match.
        self.c.sql('CREATE EXTENSION citext; DROP TABLE items; '
                   'CREATE TABLE items(k text,id int,value text) PARTITION BY RANGE(k); '
                   'CREATE TABLE family_lookup(k text,label text); '
                   "INSERT INTO family_lookup VALUES('apple','first'),('apple',NULL),(NULL,'null')")
        for i, bounds, key in [(0,"FROM(MINVALUE) TO('a')",'APPLE'),(1,"FROM('a') TO(MAXVALUE)",'apple')]:
            self.c.sql(f"ALTER SERVER s{i} OPTIONS(ADD extensions 'citext'); "
                       f'CREATE TABLE family_data{i}(k text,id int,value text); '
                       f"INSERT INTO family_data{i} SELECT {literal('Z' if i == 0 else 'z')}||n,n,NULL FROM generate_series(1,4000) n; "
                       f"INSERT INTO family_data{i} VALUES({literal(key)},0,NULL); "
                       f'CREATE FOREIGN TABLE family_f{i} PARTITION OF items FOR VALUES {bounds} SERVER s{i} '
                       f"OPTIONS(table_name 'family_data{i}')")
        self.c.sql('ANALYZE family_f0; ANALYZE family_f1; ANALYZE family_lookup')
        query = 'SELECT s.k,l.label FROM items s JOIN family_lookup l ON s.k::citext=l.k::citext'
        plan = self.lookup_plan(query, True)
        self.assertEqual(plan['Lookup Routing'], 'All destinations')
        self.assertEqual(plan['Remote Executions'], 2)
        self.assertEqual(plan['Remote Rows'], 4)
        self.equivalent(query)
        query = 'SELECT s.k,l.label FROM items s JOIN family_lookup l ON s.k=(l.k COLLATE "C")'
        self.assertFalse(any('Custom Plan Provider' in n for n in self.nodes(self.plan(query))))
        self.equivalent(query)


if __name__ == '__main__':
    unittest.main()
