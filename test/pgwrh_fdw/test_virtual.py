# SPDX-License-Identifier: AGPL-3.0-only
"""Virtual-server routing through the ordinary FDW connection entry point."""
import json
from concurrent.futures import ThreadPoolExecutor
import time
import unittest

from support import Cluster, PgError, literal


class VirtualServerTests(unittest.TestCase):
    counter = 0

    @classmethod
    def setUpClass(cls):
        cls.cluster = Cluster()
        try:
            cls.cluster.setup()
            cls.admin = cls.cluster.connect()
            cls.admin.sql("CREATE ROLE v_alice; CREATE ROLE v_bob; CREATE ROLE v_server_owner; "
                          "CREATE ROLE v_remote_a LOGIN; CREATE ROLE v_remote_b LOGIN")
            for member in "abcd":
                db = "virtual_remote_" + member
                cls.admin.sql("CREATE DATABASE " + db)
                cls.admin.sql(f"ALTER DATABASE {db} SET session_preload_libraries = 'context_probe'")
                with cls.cluster.connect(db) as c:
                    c.sql("""
                        CREATE VIEW identity AS SELECT current_database()::text AS member,
                          current_user::text AS remote_user, pg_backend_pid() AS pid,
                          current_setting('app.request_id', true) AS request_id;
                        CREATE TABLE data(id int PRIMARY KEY, value text);
                        INSERT INTO data VALUES (1, 'one'), (2, 'two');
                        GRANT SELECT ON identity, data TO v_remote_a, v_remote_b;
                    """)
        except BaseException:
            cls.cluster.close()
            raise

    @classmethod
    def tearDownClass(cls):
        cls.admin.close()
        cls.cluster.close()

    def setUp(self):
        type(self).counter += 1
        self.db = "virtual_test_" + str(self.counter)
        self.admin.sql("CREATE DATABASE " + self.db)
        self.c = self.cluster.connect(self.db)
        self.addCleanup(self.c.close)
        self.c.sql("CREATE EXTENSION pgwrh_fdw")
        for member in "abcd":
            self.c.sql(f"""CREATE SERVER s_{member} FOREIGN DATA WRAPPER pgwrh_fdw
                OPTIONS (host {literal(self.cluster.path)}, port '{self.cluster.port}',
                         dbname 'virtual_remote_{member}');
                CREATE USER MAPPING FOR CURRENT_USER SERVER s_{member};
            """)
            self.identity_table(member, "s_" + member)

    def virtual(self, name="v", members="s_a", mapping=True, options=""):
        self.c.sql(f"CREATE SERVER {name} FOREIGN DATA WRAPPER pgwrh_fdw "
                   f"OPTIONS (members {literal(members)}{options})")
        if mapping:
            self.c.sql(f"CREATE USER MAPPING FOR PUBLIC SERVER {name}")
        self.identity_table(name, name)

    def identity_table(self, name, server):
        self.c.sql(f"""CREATE FOREIGN TABLE {name}
            (member text, remote_user text, pid int, request_id text)
            SERVER {server} OPTIONS (schema_name 'public', table_name 'identity');
            GRANT SELECT ON {name} TO v_alice, v_bob;
        """)

    def error(self, query, state, fragment=None):
        with self.assertRaises(PgError) as cm:
            self.c.sql(query)
        self.assertEqual(cm.exception.sqlstate, state, str(cm.exception))
        if fragment:
            self.assertIn(fragment, str(cm.exception))

    def wait_for_routing_lock(self, pid, mode):
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if self.admin.scalar(f"SELECT EXISTS (SELECT FROM pg_locks WHERE pid = {pid} "
                                 "AND locktype = 'object' AND classid = 'pg_foreign_server'::regclass "
                                 f"AND mode = {literal(mode)} AND NOT granted)") == "t":
                return
            time.sleep(0.01)
        self.fail(f"backend {pid} did not wait for {mode}")

    def test_weight_validation_and_actual_server_scope(self):
        for value in ("0", "-1", "1.5", "", "NaN", "2147483648"):
            with self.subTest(value=value):
                self.error("ALTER SERVER s_a OPTIONS (ADD load_balance_weight " +
                           literal(value) + ")", "22023")
        self.c.sql("ALTER SERVER s_a OPTIONS (ADD load_balance_weight '2147483647')")
        self.virtual()
        self.error("ALTER SERVER v OPTIONS (ADD load_balance_weight '2')",
                   "HV00D", "not allowed on a virtual server")
        self.error("ALTER FOREIGN TABLE a OPTIONS (ADD load_balance_weight '2')", "HV00D")
        self.error("ALTER USER MAPPING FOR CURRENT_USER SERVER s_a "
                   "OPTIONS (ADD load_balance_weight '2')", "HV00D")

    def refuse_new_connections(self, member, pid):
        db = 'virtual_remote_' + member
        self.admin.sql(f'ALTER DATABASE {db} ALLOW_CONNECTIONS false')
        self.addCleanup(self.admin.sql, f'ALTER DATABASE {db} ALLOW_CONNECTIONS true')
        self.admin.sql(f'SELECT pg_terminate_backend({pid}, 5000)')

    def test_initial_connection_failover_preserves_shared_savepoint_binding(self):
        self.virtual('v', 's_a,s_b')
        self.virtual('peer', 's_b,s_a')
        # A dead idle session outranks an unopened target, deterministically
        # exercising the failed reconnect before falling back to b.
        pid = self.c.scalar('SELECT pid FROM a')
        self.refuse_new_connections('a', pid)
        self.c.sql('BEGIN; SAVEPOINT first_use')
        identity = self.c.sql('SELECT member, pid FROM v')
        self.assertEqual(identity[0][0], 'virtual_remote_b')
        self.c.sql('ROLLBACK TO first_use')
        self.assertEqual(self.c.sql('SELECT member, pid FROM peer'), identity)
        self.c.sql('COMMIT')

    def test_initial_failover_checks_virtual_server_owner(self):
        self.virtual('v', 's_a,s_b,s_c')
        self.c.sql('ALTER SERVER v OWNER TO v_server_owner; '
                   'GRANT USAGE ON FOREIGN SERVER s_a, s_b TO v_server_owner')
        pid = self.c.scalar('SELECT pid FROM a')
        self.refuse_new_connections('a', pid)
        # c has the best reuse rank for the caller, but this route's owner
        # permits only a and b, including when the initial a connection fails.
        self.c.sql('BEGIN; SELECT * FROM c')
        self.assertEqual(self.c.scalar('SELECT member FROM v'), 'virtual_remote_b')
        self.c.sql('COMMIT')

    def test_prepared_join_failover_moves_all_provisional_groups(self):
        query = self.join_tables(left='s_a,s_b,s_c', right='s_b,s_a,s_d')
        pid = self.c.scalar('SELECT pid FROM a')
        self.c.sql('SET plan_cache_mode = force_generic_plan; PREPARE joined AS ' + query)
        self.assertEqual(len(self.remote_joins('EXECUTE joined')), 1)
        self.refuse_new_connections('a', pid)
        self.c.sql('BEGIN; SAVEPOINT first_use')
        self.assertEqual(self.c.sql('EXECUTE joined'), [('1',), ('2',)])
        self.c.sql('ROLLBACK TO first_use')
        identity = self.c.sql('SELECT member, pid FROM v1')
        self.assertEqual(identity[0][0], 'virtual_remote_b')
        self.assertEqual(self.c.sql('SELECT member, pid FROM v2'), identity)
        self.c.sql('COMMIT')

    def test_failed_initial_connections_poison_all_shared_aliases(self):
        self.virtual('v', 's_a,s_b')
        self.virtual('peer', 's_b,s_a')
        for member in 'ab':
            self.c.sql(f"ALTER SERVER s_{member} OPTIONS (SET dbname 'missing_{self.db}_{member}')")
        self.c.sql('BEGIN; SAVEPOINT attempt')
        self.error('SELECT * FROM v', '08001', 'could not connect')
        self.c.sql("ROLLBACK TO attempt; ALTER SERVER s_a OPTIONS (SET dbname 'virtual_remote_a')")
        self.error('SELECT * FROM peer', '08000', 'previous connection acquisition')
        self.c.sql('ROLLBACK')
        log = self.cluster.log.read_text()
        for member in 'ab':
            self.assertIn(f'FATAL:  database "missing_{self.db}_{member}" does not exist', log)

    def test_initial_context_error_does_not_fail_over(self):
        self.virtual('v', 's_a,s_b')
        self.virtual('peer', 's_b,s_a')
        for parameter, value, state in (('ctxprobe.level', 'invalid', '22023'),
                                         ('ctxprobe.token', 'raise08001', '08001')):
            with self.subTest(parameter=parameter):
                self.c.sql(f"ALTER SERVER s_a OPTIONS (ADD transaction_parameters '{parameter}')")
                self.c.sql(f"SET {parameter} = '1'; SELECT * FROM a")
                self.c.sql(f"SET {parameter} = '{value}'; BEGIN; SAVEPOINT attempt")
                self.error('SELECT * FROM v', state)
                self.c.sql('ROLLBACK TO attempt')
                self.error('SELECT * FROM peer', '08000', 'previous connection acquisition')
                self.c.sql('ROLLBACK; ALTER SERVER s_a OPTIONS (DROP transaction_parameters)')

    def test_weights_choose_among_idle_connections_and_can_be_changed(self):
        self.virtual(members="s_a,s_b")
        # Each sample is a new transaction, with both connections equally reusable.
        # Wide bounds distinguish 32:1 from uniform selection without requiring
        # any particular random sequence (failure probability below 1e-8).
        for favored in "ab":
            self.c.sql(f"ALTER SERVER s_{favored} OPTIONS (ADD load_balance_weight '32')")
            self.c.sql("SELECT * FROM a; SELECT * FROM b")
            selected = [self.c.scalar("SELECT member FROM v") for _ in range(128)]
            self.assertGreater(selected.count("virtual_remote_" + favored), 96)
            self.c.sql(f"ALTER SERVER s_{favored} OPTIONS (DROP load_balance_weight)")

    def test_weights_apply_to_pushed_join_selection(self):
        self.join_tables(left="s_a,s_b", right="s_b,s_a")
        query = "SELECT x.member FROM v1 x JOIN v2 y ON x.pid = y.pid"
        self.assertEqual(len(self.remote_joins(query)), 1)
        self.c.sql("ALTER SERVER s_b OPTIONS (ADD load_balance_weight '32')")
        selected = []
        for _ in range(128):
            # No cached target: exercise weighted selection for new connections.
            self.c.sql("SELECT pgwrh_fdw_disconnect_all()")
            selected.append(self.c.scalar(query))
        self.assertGreater(selected.count("virtual_remote_b"), 96)

    def test_reuse_and_shared_transaction_pin_override_weights(self):
        self.virtual("v", "s_a,s_b,s_c")
        self.virtual("peer", "s_c,s_b,s_a")
        self.c.sql("ALTER SERVER s_a OPTIONS (ADD load_balance_weight '2147483647'); "
                   "ALTER SERVER s_c OPTIONS (ADD load_balance_weight '2147483647')")
        idle = self.c.scalar("SELECT pid FROM b")
        # A weight of one beats even maximum weights when it can reuse a session.
        self.assertEqual(self.c.scalar("SELECT pid FROM v"), idle)
        self.c.sql("SELECT * FROM a; SELECT * FROM c")
        self.c.sql("BEGIN; SELECT * FROM b; SAVEPOINT pin")
        self.assertEqual(self.c.scalar("SELECT pid FROM v"), idle)
        self.c.sql("ROLLBACK TO pin; SELECT * FROM a; SELECT * FROM c; "
                   "ALTER SERVER s_b OPTIONS (ADD load_balance_weight '2')")
        self.assertEqual(self.c.scalar("SELECT pid FROM peer"), idle)
        self.c.sql("COMMIT")

    def test_weight_sum_exceeds_32_bits(self):
        self.virtual(members="s_a,s_b,s_c")
        for member in "abc":
            self.c.sql(f"ALTER SERVER s_{member} OPTIONS (ADD load_balance_weight '2147483647')")
        self.c.sql("SELECT * FROM a; SELECT * FROM b; SELECT * FROM c")
        selected = {self.c.scalar("SELECT member FROM v") for _ in range(64)}
        self.assertEqual(selected, {"virtual_remote_" + member for member in "abc"})

    def test_managed_members_validation_and_quoted_identifiers(self):
        self.virtual()
        self.virtual("nested")
        self.c.sql("CREATE EXTENSION postgres_fdw; "
                   "CREATE SERVER stock FOREIGN DATA WRAPPER postgres_fdw")
        for members, state, message in (
            ("ARRAY[]::text[]", "22023", "nonempty one-dimensional"),
            ("ARRAY[['s_a']]", "22023", "one-dimensional"),
            ("ARRAY['s_a',NULL]", "22004", "must not be null"),
            ("NULL", "22004", "must not be null"),
            ("ARRAY['']", "22023", "invalid member server name"),
            ("ARRAY[repeat('a',64)]", "22023", "invalid member server name"),
            ("ARRAY['s_a','s_a']", "22023", "duplicate"),
            ("ARRAY['missing']", "42704", "does not exist"),
            ("ARRAY['nested']", "HV00D", "ordinary server"),
            ("ARRAY['v']", "HV00D", "ordinary server"),
            ("ARRAY['stock']", "HV00D", "same foreign-data wrapper"),
        ):
            with self.subTest(members=members):
                self.error(f"SELECT pgwrh_fdw_set_members('v', {members})", state, message)
        self.error("SELECT pgwrh_fdw_set_members(NULL, ARRAY['s_a'])", "22004")
        self.error("SELECT pgwrh_fdw_set_members('s_a', ARRAY['s_b'])", "42809", "not a pgwrh_fdw virtual")
        self.error("SELECT pgwrh_fdw_set_members('stock', ARRAY['s_b'])", "42809")
        self.c.sql('ALTER SERVER s_b RENAME TO "B, \'quoted\'"; '
                   'ALTER SERVER v RENAME TO "Shard \'quoted\'"')
        self.c.sql("SELECT pgwrh_fdw_set_members(" + literal("Shard 'quoted'") + ", ARRAY[" +
                   literal("B, 'quoted'") + "])")
        self.assertEqual(self.c.scalar("SELECT member FROM v"), "virtual_remote_b")

    def test_managed_members_owner_and_read_only_checks(self):
        self.virtual()
        self.c.sql("GRANT USAGE ON FOREIGN SERVER v TO v_alice; SET ROLE v_alice")
        self.error("SELECT pgwrh_fdw_set_members('v', ARRAY['s_b'])", "42501", "must be owner")
        self.c.sql("RESET ROLE; ALTER SERVER v OWNER TO v_alice; "
                   "GRANT USAGE ON FOREIGN SERVER s_b TO v_alice; SET ROLE v_alice")
        self.c.sql("SELECT pgwrh_fdw_set_members('v', ARRAY['s_b'])")
        self.c.sql("RESET ROLE")
        self.c.sql("BEGIN READ ONLY")
        self.error("SELECT pgwrh_fdw_set_members('v', ARRAY['s_c'])", "25006", "read-only")
        self.c.sql("ROLLBACK")
        self.assertEqual(self.c.scalar("SELECT member FROM v"), "virtual_remote_b")

    def test_managed_members_waits_for_reader_and_savepoint_pin(self):
        self.virtual()
        self.identity_table("second_table", "v")
        with self.cluster.connect(self.db) as writer, ThreadPoolExecutor() as pool:
            writer.sql("SET statement_timeout = '10s'")
            pid = writer.scalar("SELECT pg_backend_pid()")
            for finish in ("COMMIT", "ROLLBACK"):
                with self.subTest(finish=finish):
                    writer.sql("SELECT pgwrh_fdw_set_members('v', ARRAY['s_a'])")
                    self.c.sql("BEGIN; SAVEPOINT first_use")
                    old = self.c.sql("SELECT member, pid FROM second_table")
                    self.c.sql("ROLLBACK TO first_use")
                    update = pool.submit(writer.sql, "SELECT pgwrh_fdw_set_members('v', ARRAY['s_b'])")
                    try:
                        self.wait_for_routing_lock(pid, "AccessExclusiveLock")
                        self.assertFalse(update.done())
                        # A waiting updater must allow the old transaction to finish.
                        self.assertEqual(self.c.sql("SELECT member, pid FROM v"), old)
                    finally:
                        self.c.sql(finish)
                    update.result(timeout=10)
                    self.assertEqual(self.c.scalar("SELECT member FROM v"), "virtual_remote_b")

    def test_managed_members_blocks_cached_reader_until_commit_or_rollback(self):
        self.virtual()
        self.c.sql("SET statement_timeout = '10s'; SET plan_cache_mode = force_generic_plan; "
                   "PREPARE routed AS SELECT member FROM v")
        self.assertEqual(self.c.scalar("EXECUTE routed"), "virtual_remote_a")
        pid = self.c.scalar("SELECT pg_backend_pid()")
        with self.cluster.connect(self.db) as writer, ThreadPoolExecutor() as pool:
            for finish, target in (("COMMIT", "s_b"), ("ROLLBACK", "s_c")):
                with self.subTest(finish=finish):
                    writer.sql(f"BEGIN; SELECT pgwrh_fdw_set_members('v', ARRAY['{target}'])")
                    # The function has returned, but its lock still protects publication.
                    read = pool.submit(self.c.scalar, "EXECUTE routed")
                    try:
                        self.wait_for_routing_lock(pid, "AccessShareLock")
                        self.assertFalse(read.done())
                    finally:
                        writer.sql(finish)
                    self.assertEqual(read.result(timeout=10), "virtual_remote_b")

    def test_managed_members_locks_every_pushed_join_input(self):
        query = self.join_tables(left="s_a,s_b", right="s_b,s_a")
        self.assertEqual(len(self.remote_joins(query)), 1)
        self.c.sql("BEGIN; SAVEPOINT first_use")
        self.assertEqual(self.c.sql(query), [("1",), ("2",)])
        self.c.sql("ROLLBACK TO first_use")
        with self.cluster.connect(self.db) as writer, ThreadPoolExecutor() as pool:
            writer.sql("SET statement_timeout = '10s'")
            pid = writer.scalar("SELECT pg_backend_pid()")
            update = pool.submit(writer.sql, "SELECT pgwrh_fdw_set_members('v2', ARRAY['s_c'])")
            try:
                self.wait_for_routing_lock(pid, "AccessExclusiveLock")
                self.assertEqual(self.c.sql(query), [("1",), ("2",)])
            finally:
                self.c.sql("COMMIT")
            update.result(timeout=10)
        self.assertEqual(self.c.scalar("SELECT member FROM v2"), "virtual_remote_c")

    def test_managed_members_scope_is_alias_not_shared_group(self):
        self.virtual()
        self.virtual("peer")
        self.c.sql("BEGIN; SELECT * FROM v")
        with self.cluster.connect(self.db) as writer:
            writer.sql("SET lock_timeout = '1s'; SELECT pgwrh_fdw_set_members('peer', ARRAY['s_b'])")
        self.assertEqual(self.c.scalar("SELECT member FROM peer"), "virtual_remote_b")
        self.assertEqual(self.c.scalar("SELECT member FROM v"), "virtual_remote_a")
        self.c.sql("COMMIT")

    def test_managed_members_covers_estimates_analyze_import_and_failed_acquisition(self):
        self.virtual(options=", use_remote_estimate 'true'")
        self.c.sql("CREATE SCHEMA imported")
        for query in ("EXPLAIN SELECT * FROM v", "ANALYZE v",
                      "IMPORT FOREIGN SCHEMA public LIMIT TO (data) FROM SERVER v INTO imported"):
            with self.subTest(query=query):
                self.c.sql("BEGIN; SAVEPOINT first_use")
                self.c.sql(query)
                self.c.sql("ROLLBACK TO first_use; SAVEPOINT update_attempt")
                self.error("SELECT pgwrh_fdw_set_members('v', ARRAY['s_b'])", "55000", "after using")
                self.c.sql("ROLLBACK")
        self.c.sql("ALTER SERVER s_a OPTIONS (ADD transaction_parameters 'missing.required'); "
                   "BEGIN; SAVEPOINT first_use")
        self.error("SELECT * FROM v", "42704")
        self.c.sql("ROLLBACK TO first_use")
        self.error("SELECT pgwrh_fdw_set_members('v', ARRAY['s_b'])", "55000", "after using")
        self.c.sql("ROLLBACK")
        self.c.sql("SELECT pgwrh_fdw_set_members('v', ARRAY['s_b'])")
        self.assertEqual(self.c.scalar("SELECT member FROM v"), "virtual_remote_b")

    def test_managed_members_rejects_reads_after_rolled_back_update(self):
        self.virtual()
        self.c.sql("BEGIN; SAVEPOINT update_attempt; "
                   "SELECT pgwrh_fdw_set_members('v', ARRAY['s_b'])")
        self.error("SELECT * FROM v", "55000", "after updating")
        self.c.sql("ROLLBACK TO update_attempt")
        self.error("SELECT * FROM v", "55000", "after updating")
        self.c.sql("ROLLBACK")
        self.assertEqual(self.c.scalar("SELECT member FROM v"), "virtual_remote_a")

    def test_managed_members_updater_timeout_and_retry(self):
        self.virtual()
        self.c.sql("BEGIN; SELECT * FROM v")
        with self.cluster.connect(self.db) as writer:
            writer.sql("SET lock_timeout = '100ms'; BEGIN; SAVEPOINT attempt")
            with self.assertRaises(PgError) as cm:
                writer.sql("SELECT pgwrh_fdw_set_members('v', ARRAY['s_b'])")
            self.assertEqual(cm.exception.sqlstate, "55P03")
            writer.sql("ROLLBACK TO attempt")
            self.assertEqual(self.c.scalar("SELECT member FROM v"), "virtual_remote_a")
            self.c.sql("COMMIT")
            writer.sql("SELECT pgwrh_fdw_set_members('v', ARRAY['s_b']); COMMIT")
        self.assertEqual(self.c.scalar("SELECT member FROM v"), "virtual_remote_b")

    def test_managed_members_reader_timeout_restores_resource_owner(self):
        self.virtual()
        with self.cluster.connect(self.db) as writer:
            writer.sql("BEGIN; SELECT pgwrh_fdw_set_members('v', ARRAY['s_b'])")
            self.c.sql("SET lock_timeout = '100ms'; BEGIN; SAVEPOINT attempt")
            self.error("SELECT * FROM v", "55P03")
            self.c.sql("ROLLBACK TO attempt")
            writer.sql("COMMIT")
        self.assertEqual(self.c.scalar("SELECT member FROM v"), "virtual_remote_b")
        self.c.sql("COMMIT")

    def test_managed_members_runs_ddl_event_triggers_and_rolls_back_errors(self):
        self.virtual()
        self.c.sql("""
            CREATE TABLE ddl_seen(tag text);
            CREATE FUNCTION record_ddl() RETURNS event_trigger LANGUAGE plpgsql AS $$
              BEGIN INSERT INTO ddl_seen VALUES (tg_tag); END $$;
            CREATE EVENT TRIGGER record_alter ON ddl_command_end WHEN TAG IN ('ALTER SERVER')
              EXECUTE FUNCTION record_ddl();
            SELECT pgwrh_fdw_set_members('v', ARRAY['s_b']);
        """)
        self.assertEqual(self.c.sql("TABLE ddl_seen"), [("ALTER SERVER",)])
        self.c.sql("""CREATE OR REPLACE FUNCTION record_ddl() RETURNS event_trigger LANGUAGE plpgsql AS $$
              BEGIN RAISE EXCEPTION 'reject update'; END $$""")
        self.error("SELECT pgwrh_fdw_set_members('v', ARRAY['s_c'])", "P0001", "reject update")
        self.assertEqual(self.c.scalar("SELECT member FROM v"), "virtual_remote_b")

    def test_managed_members_in_initial_installation(self):
        database = self.db + "_installation"
        self.admin.sql("CREATE DATABASE " + database)
        with self.cluster.connect(database) as c:
            c.sql("""CREATE SCHEMA fdw_api;
                     CREATE EXTENSION pgwrh_fdw WITH SCHEMA fdw_api VERSION '0.3.0';
                     CREATE SERVER target FOREIGN DATA WRAPPER pgwrh_fdw;
                     CREATE SERVER route FOREIGN DATA WRAPPER pgwrh_fdw OPTIONS (members 'target');
                     CREATE USER MAPPING FOR PUBLIC SERVER route;
                     CREATE FOREIGN TABLE shard(id int) SERVER route;""")
            before = c.sql("SELECT 'shard'::regclass::oid, ftserver FROM pg_foreign_table")
            self.assertIsNotNone(c.scalar("SELECT to_regprocedure('fdw_api.pgwrh_fdw_set_members(text,text[])')"))
            c.sql("SELECT fdw_api.pgwrh_fdw_set_members('route', ARRAY['target'])")
            self.assertEqual(c.scalar("SELECT extversion FROM pg_extension WHERE extname = 'pgwrh_fdw'"), "0.3.0")
            self.assertEqual(c.sql("SELECT 'shard'::regclass::oid, ftserver FROM pg_foreign_table"), before)

    def test_actual_mapping_and_context(self):
        self.c.sql("ALTER SERVER s_a OPTIONS (ADD transaction_parameters 'app.request_id')")
        self.virtual()
        self.c.sql("BEGIN; SET LOCAL app.request_id = 'original'")
        identity = self.c.sql("SELECT * FROM v")
        self.assertEqual(identity, self.c.sql("SELECT * FROM a"))
        self.assertEqual(identity[0][0], "virtual_remote_a")
        self.assertEqual(identity[0][3], "original")
        self.assertEqual(self.c.sql("SELECT server_name FROM pgwrh_fdw_get_connections()"), [("s_a",)])
        self.c.sql("SET LOCAL app.request_id = 'changed'")
        self.assertEqual(self.c.sql("SELECT * FROM v"), identity)
        self.c.sql("COMMIT; BEGIN; SET LOCAL app.request_id = 'next'")
        self.assertEqual(self.c.sql("SELECT pid, request_id FROM v"), [(identity[0][2], "next")])
        self.c.sql("COMMIT")

    def test_virtual_mapping_is_required_and_empty(self):
        self.virtual(mapping=False)
        self.error("SELECT * FROM v", "42704", "user mapping not found")
        self.c.sql("CREATE USER MAPPING FOR PUBLIC SERVER v OPTIONS (user 'ignored')")
        self.error("SELECT * FROM v", "HV00D", "must have no options")

    def test_option_validation(self):
        for members in ("", " ", "s_a,", ",s_a", "s_a,,s_b", '"unfinished'):
            self.error("CREATE SERVER bad FOREIGN DATA WRAPPER pgwrh_fdw OPTIONS "
                       f"(members {literal(members)})", "22023", "nonempty list")
        self.error("CREATE SERVER bad FOREIGN DATA WRAPPER pgwrh_fdw "
                   "OPTIONS (members 's_a,S_A')", "22023", "duplicate")
        for option in ("host 'ignored'", "transaction_parameters 'app.request_id'",
                       "keep_connections 'false'", "parallel_commit 'true'"):
            self.error("CREATE SERVER bad FOREIGN DATA WRAPPER pgwrh_fdw OPTIONS "
                       f"(members 's_a', {option})", "HV00D", "not allowed on a virtual server")
        self.error("CREATE FOREIGN TABLE bad(id int) SERVER s_a OPTIONS (members 's_b')",
                   "HV00D", "invalid option")

    def test_quoted_names_and_member_validation(self):
        self.c.sql('ALTER SERVER s_a RENAME TO "Server A, quoted"')
        self.virtual(members='"Server A, quoted"')
        self.assertEqual(self.c.scalar("SELECT member FROM v"), "virtual_remote_a")
        self.virtual("missing", "does_not_exist")
        self.error("SELECT * FROM missing", "42704", "does not exist")
        self.virtual("nested", "v")
        self.error("SELECT * FROM nested", "HV00D", "ordinary server")
        self.virtual("self_reference", "self_reference")
        self.error("SELECT * FROM self_reference", "HV00D", "ordinary server")
        self.c.sql("CREATE EXTENSION postgres_fdw; "
                   "CREATE SERVER stock FOREIGN DATA WRAPPER postgres_fdw")
        self.virtual("other_wrapper", "stock")
        self.error("SELECT * FROM other_wrapper", "HV00D", "same foreign-data wrapper")

    def test_effective_user_and_public_fallback(self):
        self.virtual()
        self.c.sql("""
            CREATE USER MAPPING FOR v_alice SERVER s_a
                OPTIONS (user 'v_remote_a', password_required 'false');
            CREATE USER MAPPING FOR PUBLIC SERVER s_a
                OPTIONS (user 'v_remote_b', password_required 'false');
            BEGIN; SET LOCAL ROLE v_alice;
        """)
        alice = self.c.sql("SELECT member, remote_user, pid FROM v")[0]
        self.assertEqual(alice[:2], ("virtual_remote_a", "v_remote_a"))
        self.c.sql("SET LOCAL ROLE v_bob")
        bob = self.c.sql("SELECT member, remote_user, pid FROM v")[0]
        self.assertEqual(bob[:2], ("virtual_remote_a", "v_remote_b"))
        self.assertNotEqual(alice[2], bob[2])
        self.c.sql("SET LOCAL ROLE v_alice")
        self.assertEqual(self.c.sql("SELECT member, remote_user, pid FROM v")[0], alice)
        self.c.sql("COMMIT")

    def test_view_owner_mapping(self):
        self.virtual()
        self.c.sql("""
            CREATE USER MAPPING FOR v_alice SERVER s_a
                OPTIONS (user 'v_remote_a', password_required 'false');
            CREATE VIEW owned_view AS SELECT remote_user FROM v;
            ALTER VIEW owned_view OWNER TO v_alice;
            GRANT SELECT ON owned_view TO v_bob;
            SET ROLE v_bob;
        """)
        self.assertEqual(self.c.scalar("SELECT * FROM owned_view"), "v_remote_a")
        self.error("SELECT * FROM v", "42501", "no accessible members")

    def test_virtual_server_owner_authorizes_query_without_caller_usage(self):
        self.virtual()
        self.c.sql("""
            CREATE USER MAPPING FOR v_alice SERVER s_a
                OPTIONS (user 'v_remote_a', password_required 'false');
            ALTER SERVER v OWNER TO v_server_owner;
            GRANT USAGE ON FOREIGN SERVER s_a TO v_server_owner;
            SET ROLE v_alice;
        """)
        self.assertEqual(self.c.scalar("SELECT has_server_privilege(current_user, 's_a', 'USAGE')"), "f")
        self.assertEqual(self.c.scalar("SELECT remote_user FROM v"), "v_remote_a")

    def test_caller_usage_does_not_replace_virtual_server_owner_usage(self):
        self.virtual()
        self.c.sql("""
            CREATE USER MAPPING FOR PUBLIC SERVER s_a
                OPTIONS (user 'v_remote_a', password_required 'false');
            ALTER SERVER v OWNER TO v_server_owner;
            GRANT USAGE ON FOREIGN SERVER s_a TO v_alice;
            SET ROLE v_alice;
        """)
        self.error("SELECT * FROM v", "42501", "no accessible members")
        self.c.sql("RESET ROLE; GRANT USAGE ON FOREIGN SERVER s_a TO v_server_owner; SET ROLE v_alice")
        self.assertEqual(self.c.scalar("SELECT remote_user FROM v"), "v_remote_a")
        self.c.sql("RESET ROLE; ALTER SERVER v OWNER TO v_bob; SET ROLE v_alice")
        self.error("SELECT * FROM v", "42501", "no accessible members")

    def test_bound_target_rechecks_virtual_server_owner_usage(self):
        self.virtual()
        self.c.sql("""
            CREATE USER MAPPING FOR PUBLIC SERVER s_a
                OPTIONS (user 'v_remote_a', password_required 'false');
            ALTER SERVER v OWNER TO v_server_owner;
            GRANT USAGE ON FOREIGN SERVER s_a TO v_server_owner;
            BEGIN; SET LOCAL ROLE v_alice;
        """)
        self.assertEqual(self.c.scalar("SELECT remote_user FROM v"), "v_remote_a")
        self.c.sql("RESET ROLE; REVOKE USAGE ON FOREIGN SERVER s_a FROM v_server_owner; "
                   "SET LOCAL ROLE v_alice; SAVEPOINT revoked")
        self.error("SELECT * FROM v", "42501", "no longer accessible")
        self.c.sql("ROLLBACK")

    def test_transaction_pin_survives_savepoint_and_membership_change(self):
        self.virtual()
        self.c.sql("BEGIN; SAVEPOINT first_use")
        first = self.c.sql("SELECT member, pid FROM v")
        self.c.sql("ROLLBACK TO first_use; ALTER SERVER v OPTIONS (SET members 's_b')")
        self.assertEqual(self.c.sql("SELECT member, pid FROM v"), first)
        self.c.sql("COMMIT")
        self.assertEqual(self.c.scalar("SELECT member FROM v"), "virtual_remote_b")

    def test_identical_members_share_binding_across_order_and_savepoints(self):
        self.virtual("first", "s_a,s_b,s_c")
        self.virtual("peer", " s_c, s_a, s_b ")
        self.c.sql("BEGIN; SELECT * FROM a; SAVEPOINT first_use")
        identity = self.c.sql("SELECT member, pid FROM first")
        self.c.sql("ROLLBACK TO first_use; SELECT * FROM b; "
                   "ALTER SERVER first OPTIONS (SET members 's_d'); "
                   "ALTER SERVER s_a OPTIONS (ADD application_name 'retire_after_transaction')")
        # A peer inherits the established group even when new groups cannot use a.
        self.assertEqual(self.c.sql("SELECT member, pid FROM peer"), identity)
        self.assertEqual(self.c.sql("SELECT member, pid FROM first"), identity)
        self.c.sql("COMMIT; BEGIN; SELECT * FROM c")
        self.assertEqual(self.c.scalar("SELECT member FROM peer"), "virtual_remote_c")
        self.assertEqual(self.c.scalar("SELECT member FROM first"), "virtual_remote_d")
        self.c.sql("ROLLBACK")

    def test_failed_group_cannot_be_retried_through_unused_peer(self):
        self.virtual("first", "s_a,s_b")
        self.virtual("peer", "s_b,s_a")
        for member in "ab":
            self.c.sql(f"ALTER SERVER s_{member} OPTIONS (ADD transaction_parameters 'missing.required')")
        self.c.sql("BEGIN; SAVEPOINT attempt")
        self.error("SELECT * FROM first", "42704", "was not available")
        self.c.sql("ROLLBACK TO attempt; "
                   "ALTER SERVER s_a OPTIONS (DROP transaction_parameters); "
                   "ALTER SERVER s_b OPTIONS (DROP transaction_parameters); SAVEPOINT retry")
        self.error("SELECT * FROM peer", "08000", "previous connection acquisition")
        self.c.sql("ROLLBACK TO retry; ALTER SERVER peer OPTIONS (SET members 's_c')")
        self.error("SELECT * FROM peer", "08000", "previous connection acquisition")
        self.c.sql("ROLLBACK; ALTER SERVER s_a OPTIONS (DROP transaction_parameters); "
                   "ALTER SERVER s_b OPTIONS (DROP transaction_parameters)")
        self.assertEqual(self.c.scalar("SELECT count(*) FROM peer"), "1")

    def test_peer_rechecks_virtual_mapping_and_selected_target_mapping(self):
        self.virtual("first", "s_a,s_b")
        self.virtual("peer", "s_b,s_a")
        self.c.sql("BEGIN; SELECT * FROM a; SELECT * FROM first; "
                   "ALTER USER MAPPING FOR PUBLIC SERVER peer OPTIONS (ADD user 'ignored'); "
                   "SAVEPOINT invalid_virtual_mapping")
        self.error("SELECT * FROM peer", "HV00D", "must have no options")
        self.c.sql("ROLLBACK TO invalid_virtual_mapping; "
                   "ALTER USER MAPPING FOR PUBLIC SERVER peer OPTIONS (DROP user); "
                   "DROP USER MAPPING FOR CURRENT_USER SERVER s_a; "
                   "CREATE USER MAPPING FOR CURRENT_USER SERVER s_a; SAVEPOINT replaced")
        self.error("SELECT * FROM peer", "08000", "changed during the transaction")
        self.c.sql("ROLLBACK TO replaced")
        self.error("SELECT * FROM first", "08000", "previous connection acquisition")
        self.c.sql("ROLLBACK")

    def test_group_identity_keeps_effective_users_separate(self):
        self.virtual("first", "s_a,s_b")
        self.virtual("peer", "s_b,s_a")
        self.c.sql("""CREATE USER MAPPING FOR v_alice SERVER s_a
                         OPTIONS (user 'v_remote_a', password_required 'false');
                      CREATE USER MAPPING FOR v_bob SERVER s_b
                         OPTIONS (user 'v_remote_b', password_required 'false');
                      BEGIN; SET LOCAL ROLE v_alice""")
        alice = self.c.sql("SELECT member, remote_user, pid FROM first")
        self.c.sql("SET LOCAL ROLE v_bob")
        bob = self.c.sql("SELECT member, remote_user, pid FROM peer")
        self.assertEqual(alice[0][:2], ("virtual_remote_a", "v_remote_a"))
        self.assertEqual(bob[0][:2], ("virtual_remote_b", "v_remote_b"))
        self.assertNotEqual(alice[0][2], bob[0][2])
        self.assertEqual(self.c.sql("SELECT member, remote_user, pid FROM first"), bob)
        self.c.sql("SET LOCAL ROLE v_alice")
        self.assertEqual(self.c.sql("SELECT member, remote_user, pid FROM peer"), alice)
        self.c.sql("COMMIT")

    def test_shared_group_checks_each_virtual_server_owner(self):
        self.virtual("first", "s_a")
        self.virtual("peer", "s_a")
        self.c.sql("""CREATE USER MAPPING FOR PUBLIC SERVER s_a
                         OPTIONS (user 'v_remote_a', password_required 'false');
                      ALTER SERVER first OWNER TO v_server_owner;
                      ALTER SERVER peer OWNER TO v_bob;
                      GRANT USAGE ON FOREIGN SERVER s_a TO v_server_owner""")
        self.c.sql("BEGIN; SET LOCAL ROLE v_alice")
        self.assertEqual(self.c.scalar("SELECT remote_user FROM first"), "v_remote_a")
        self.c.sql("SAVEPOINT peer_access")
        self.error("SELECT * FROM peer", "42501", "no longer accessible")
        self.c.sql("ROLLBACK; GRANT USAGE ON FOREIGN SERVER s_a TO v_bob; "
                   "BEGIN; SET LOCAL ROLE v_alice")
        first = self.c.sql("SELECT member, remote_user, pid FROM first")
        self.assertEqual(self.c.sql("SELECT member, remote_user, pid FROM peer"), first)
        self.c.sql("COMMIT")

    def test_equal_accessible_subsets_do_not_merge_different_groups(self):
        self.virtual("first", "s_a,s_b")
        self.virtual("other", "s_a,s_c")
        self.c.sql("""CREATE USER MAPPING FOR v_alice SERVER s_a
                         OPTIONS (user 'v_remote_a', password_required 'false');
                      BEGIN; SET LOCAL ROLE v_alice; SELECT * FROM first;
                      RESET ROLE;
                      ALTER SERVER s_a OPTIONS (ADD application_name 'retired');
                      SET LOCAL ROLE v_alice; SAVEPOINT attempt""")
        self.error("SELECT * FROM other", "08000", "no usable member connections")
        self.c.sql("ROLLBACK")

    def test_estimation_does_not_attach_unused_peer_to_old_group(self):
        self.virtual("first", "s_a,s_b")
        self.virtual("peer", "s_b,s_a", options=", use_remote_estimate 'true'")
        self.c.sql("BEGIN; SELECT * FROM a; SELECT * FROM first; EXPLAIN SELECT * FROM peer; "
                   "ALTER SERVER peer OPTIONS (SET members 's_c')")
        self.assertEqual(self.c.scalar("SELECT member FROM peer"), "virtual_remote_c")
        self.assertEqual(self.c.scalar("SELECT member FROM first"), "virtual_remote_a")
        self.c.sql("ROLLBACK")

    def test_initialization_failure_remains_failed(self):
        self.virtual()
        self.c.sql("ALTER SERVER s_a OPTIONS (ADD transaction_parameters 'missing.required')")
        self.c.sql("BEGIN; SAVEPOINT attempt")
        self.error("SELECT * FROM v", "42704", "was not available")
        self.c.sql("ROLLBACK TO attempt; ALTER SERVER v OPTIONS (SET members 's_b'); SAVEPOINT retry")
        self.error("SELECT * FROM v", "08000", "previous connection acquisition")
        self.c.sql("ROLLBACK TO retry; SAVEPOINT retry_again")
        self.error("SELECT * FROM v", "08000", "previous connection acquisition")
        self.c.sql("ROLLBACK; ALTER SERVER s_a OPTIONS (DROP transaction_parameters)")
        self.assertEqual(self.c.scalar("SELECT member FROM v"), "virtual_remote_a")

    def test_lost_active_connection_is_not_replaced(self):
        self.virtual()
        self.c.sql("BEGIN")
        pid = self.c.scalar("SELECT pid FROM v")
        self.admin.sql(f"SELECT pg_terminate_backend({pid}, 5000)")
        self.c.sql("SAVEPOINT attempt")
        with self.assertRaises(PgError):
            self.c.sql("SELECT * FROM v")
        self.c.sql("ROLLBACK TO attempt; ALTER SERVER v OPTIONS (SET members 's_b'); SAVEPOINT retry")
        with self.assertRaises(PgError):
            self.c.sql("SELECT * FROM v")
        self.c.sql("ROLLBACK")
        self.assertNotEqual(self.c.scalar("SELECT pid FROM v"), pid)

    def test_idle_connection_reconnect_and_target_option_invalidation(self):
        self.virtual()
        before = self.c.scalar("SELECT pid FROM v")
        self.admin.sql(f"SELECT pg_terminate_backend({before}, 5000)")
        after = self.c.scalar("SELECT pid FROM v")
        self.assertNotEqual(before, after)
        self.c.sql("ALTER SERVER s_a OPTIONS (ADD application_name 'new-options')")
        self.assertNotEqual(after, self.c.scalar("SELECT pid FROM v"))

    def test_mapping_replacement_cannot_change_a_binding(self):
        self.virtual()
        self.c.sql("BEGIN")
        self.c.sql("SELECT * FROM v")
        self.c.sql("DROP USER MAPPING FOR CURRENT_USER SERVER s_a; "
                   "CREATE USER MAPPING FOR CURRENT_USER SERVER s_a; SAVEPOINT attempt")
        self.error("SELECT * FROM v", "08000", "changed during the transaction")
        self.c.sql("ROLLBACK")

    def test_planning_analyze_import_and_generic_plan(self):
        self.virtual(options=", use_remote_estimate 'true'")
        self.c.sql("ALTER SERVER s_a OPTIONS (ADD transaction_parameters 'app.request_id'); "
                   "BEGIN; SET LOCAL app.request_id = 'planning'; "
                   "EXPLAIN SELECT * FROM v")
        self.assertEqual(self.c.scalar("SELECT request_id FROM a"), "planning")
        self.c.sql("ANALYZE v; CREATE SCHEMA imported; "
                   "IMPORT FOREIGN SCHEMA public LIMIT TO (data) FROM SERVER v INTO imported; "
                   "SET plan_cache_mode = force_generic_plan; "
                   "PREPARE p AS SELECT member FROM v; COMMIT")
        self.assertEqual(self.c.scalar("SELECT count(*) FROM imported.data"), "2")
        self.assertEqual(self.c.scalar("EXECUTE p"), "virtual_remote_a")
        self.c.sql("ALTER SERVER v OPTIONS (SET members 's_b')")
        self.assertEqual(self.c.scalar("EXECUTE p"), "virtual_remote_b")

    def test_same_virtual_join_uses_one_connection(self):
        self.virtual(members="s_a,s_b", options=", use_remote_estimate 'true'")
        self.c.sql("""CREATE FOREIGN TABLE data1(id int, value text) SERVER v
                         OPTIONS (table_name 'data');
                      CREATE FOREIGN TABLE data2(id int, value text) SERVER v
                         OPTIONS (table_name 'data');""")
        query = "SELECT x.id FROM data1 x JOIN data2 y USING (id) ORDER BY x.id"
        self.c.sql("BEGIN")
        plan = self.c.scalar("EXPLAIN (VERBOSE, FORMAT JSON) " + query)
        self.assertIn("Remote SQL", plan)
        self.assertIn("JOIN", plan)
        self.assertEqual(self.c.sql(query), [("1",), ("2",)])
        self.assertEqual(self.c.scalar("SELECT count(*) FROM pgwrh_fdw_get_connections()"), "1")
        self.c.sql("COMMIT")

    def test_remote_estimates_do_not_pin_virtual_server(self):
        self.virtual(options=", use_remote_estimate 'true'")
        self.c.sql("BEGIN")
        self.c.sql("EXPLAIN SELECT * FROM v")
        self.c.sql("ALTER SERVER v OPTIONS (SET members 's_b')")
        self.assertEqual(self.c.scalar("SELECT member FROM v"), "virtual_remote_b")
        self.c.sql("COMMIT")

    def join_tables(self, left="s_a,s_b", right="s_b,s_c", estimates=True):
        options = ", use_remote_estimate 'true'" if estimates else ""
        self.virtual("v1", left, options=options)
        self.virtual("v2", right, options=options)
        for name in ("v1", "v2"):
            self.c.sql(f"CREATE FOREIGN TABLE {name}_data(id int, value text) "
                       f"SERVER {name} OPTIONS (table_name 'data')")
        return "SELECT x.id FROM v1_data x JOIN v2_data y USING (id) ORDER BY x.id"

    def remote_joins(self, query):
        plan = json.loads(self.c.scalar("EXPLAIN (VERBOSE, FORMAT JSON) " + query))[0]["Plan"]
        def walk(node):
            result = [node["Remote SQL"]] if "JOIN" in node.get("Remote SQL", "") else []
            for child in node.get("Plans", []):
                result += walk(child)
            return result
        return walk(plan)

    def test_cross_virtual_join_and_transaction_pins(self):
        query = self.join_tables()
        self.c.sql("BEGIN; SAVEPOINT before_join")
        self.assertEqual(len(self.remote_joins(query)), 1)
        self.assertEqual(self.c.sql(query), [("1",), ("2",)])
        self.c.sql("ROLLBACK TO before_join")
        self.assertEqual(self.c.scalar("SELECT member FROM v1"), "virtual_remote_b")
        self.assertEqual(self.c.sql("SELECT pid FROM v1"), self.c.sql("SELECT pid FROM v2"))
        self.c.sql("ALTER SERVER v1 OPTIONS (SET members 's_a'); "
                   "ALTER SERVER v2 OPTIONS (SET members 's_c')")
        self.assertEqual(self.c.sql(query), [("1",), ("2",)])
        self.c.sql("COMMIT")
        self.assertEqual(self.remote_joins(query), [])

    def test_cross_virtual_join_prefers_cached_common_member(self):
        query = self.join_tables("s_a,s_b,s_c", "s_b,s_c,s_d", estimates=False)
        self.c.sql("ANALYZE v1_data; ANALYZE v2_data")
        self.c.sql("SELECT pgwrh_fdw_disconnect_all()")
        pid = self.c.scalar("SELECT pid FROM c")
        self.c.sql("BEGIN")
        self.assertEqual(len(self.remote_joins(query)), 1)
        self.assertEqual(self.c.sql(query), [("1",), ("2",)])
        self.assertEqual(self.c.sql("SELECT member, pid FROM v1"), [("virtual_remote_c", pid)])
        self.assertEqual(self.c.sql("SELECT pid FROM v2"), [(pid,)])
        self.assertEqual(self.c.scalar("SELECT count(*) FROM pgwrh_fdw_get_connections()"), "1")
        self.c.sql("COMMIT")

    def test_cross_virtual_join_uses_full_three_way_intersection(self):
        self.join_tables("s_a,s_b", "s_b,s_c")
        self.virtual("v3", "s_a,s_c", options=", use_remote_estimate 'true'")
        self.c.sql("CREATE FOREIGN TABLE v3_data(id int, value text) SERVER v3 "
                   "OPTIONS (table_name 'data'); SET join_collapse_limit = 1")
        query = ("SELECT x.id FROM (v1_data x JOIN v2_data y USING (id)) "
                 "JOIN v3_data z USING (id) ORDER BY x.id")
        # Every pair intersects, but the three-way join cannot run remotely.
        self.assertTrue(all(sql.count("JOIN") == 1 for sql in self.remote_joins(query)))
        self.assertEqual(self.c.sql(query), [("1",), ("2",)])
        self.c.sql("ALTER SERVER v3 OPTIONS (SET members 's_b,s_c')")
        # Keep all references to the shared v2/v3 group in the inner join.
        query = ("SELECT x.id FROM (v2_data y JOIN v3_data z USING (id)) "
                 "JOIN v1_data x USING (id) ORDER BY x.id")
        self.assertTrue(any(sql.count("JOIN") == 2 for sql in self.remote_joins(query)))
        self.assertEqual(self.c.sql(query), [("1",), ("2",)])

    def test_cross_virtual_join_respects_existing_binding(self):
        query = self.join_tables()
        self.c.sql("BEGIN; SELECT * FROM a; SELECT * FROM v1")
        self.assertEqual(self.c.scalar("SELECT member FROM v1"), "virtual_remote_a")
        self.assertEqual(self.remote_joins(query), [])
        self.assertEqual(self.c.sql(query), [("1",), ("2",)])
        self.c.sql("COMMIT")

    def test_cross_virtual_join_generic_plan_retargets_and_falls_back(self):
        query = self.join_tables("s_a,s_b", "s_a,s_b")
        self.c.sql("SET plan_cache_mode = force_generic_plan; PREPARE j AS " + query)
        self.assertEqual(self.c.sql("EXECUTE j"), [("1",), ("2",)])
        self.c.sql("ALTER SERVER v1 OPTIONS (SET members 's_c'); "
                   "ALTER SERVER v2 OPTIONS (SET members 's_c')")
        self.c.sql("BEGIN")
        self.assertEqual(len(self.remote_joins("EXECUTE j")), 1)
        self.assertEqual(self.c.sql("EXECUTE j"), [("1",), ("2",)])
        self.assertEqual(self.c.scalar("SELECT member FROM v1"), "virtual_remote_c")
        self.c.sql("COMMIT")
        self.c.sql("ALTER SERVER v2 OPTIONS (SET members 's_d')")
        self.assertEqual(self.remote_joins("EXECUTE j"), [])
        self.assertEqual(self.c.sql("EXECUTE j"), [("1",), ("2",)])

    def test_cross_virtual_outer_joins_and_aggregation(self):
        self.join_tables()
        for kind in ("LEFT", "RIGHT", "FULL"):
            query = (f"SELECT x.id, y.id FROM v1_data x {kind} JOIN v2_data y "
                     "ON x.id = y.id AND y.id = 1 ORDER BY x.id, y.id")
            self.assertEqual(len(self.remote_joins(query)), 1)
            rows = self.c.sql(query)
            expected = [("1", "1"), ("2", None)] if kind == "LEFT" else (
                [("1", "1"), (None, "2")] if kind == "RIGHT" else
                [("1", "1"), ("2", None), (None, "2")])
            self.assertEqual(rows, expected)
        query = "SELECT count(*) FROM v1_data x JOIN v2_data y USING (id)"
        self.assertEqual(len(self.remote_joins(query)), 1)
        self.assertEqual(self.c.scalar(query), "2")

    def test_cross_virtual_join_checks_each_server_owner(self):
        query = self.join_tables()
        self.c.sql("""ALTER SERVER v1 OWNER TO v_server_owner;
                      ALTER SERVER v2 OWNER TO v_bob;
                      GRANT USAGE ON FOREIGN SERVER s_a TO v_server_owner;
                      GRANT USAGE ON FOREIGN SERVER s_b, s_c TO v_bob""")
        # The query user can use every target, but only the owners' permitted
        # destinations may participate in a pushed join.
        self.assertEqual(self.remote_joins(query), [])
        self.assertEqual(self.c.sql(query), [("1",), ("2",)])
        self.c.sql("GRANT USAGE ON FOREIGN SERVER s_b TO v_server_owner")
        self.assertEqual(len(self.remote_joins(query)), 1)
        self.assertEqual(self.c.sql(query), [("1",), ("2",)])

    def test_cross_virtual_join_does_not_bypass_target_privileges(self):
        query = self.join_tables()
        self.c.sql("""CREATE USER MAPPING FOR v_alice SERVER s_a
                        OPTIONS (user 'v_remote_a', password_required 'false');
                      CREATE USER MAPPING FOR v_alice SERVER s_c
                        OPTIONS (user 'v_remote_a', password_required 'false');
                      GRANT SELECT ON v1_data, v2_data TO v_alice;
                      SET ROLE v_alice""")
        self.assertEqual(self.remote_joins(query), [])
        self.assertEqual(self.c.sql(query), [("1",), ("2",)])

    def test_cross_virtual_join_failure_poisons_every_input(self):
        query = self.join_tables(estimates=False)
        self.c.sql("ANALYZE v1_data; ANALYZE v2_data")
        self.c.sql("ALTER SERVER s_b OPTIONS (ADD transaction_parameters 'missing.required')")
        self.c.sql("BEGIN; SAVEPOINT attempt")
        self.assertEqual(len(self.remote_joins(query)), 1)
        self.error(query, "42704", "was not available")
        for name in ("v1", "v2"):
            self.c.sql("ROLLBACK TO attempt")
            self.error("SELECT * FROM " + name, "08000", "previous connection acquisition")
        self.c.sql("ROLLBACK")

    def test_cross_virtual_join_generic_plan_checks_later_binding(self):
        query = self.join_tables()
        self.c.sql("SET plan_cache_mode = force_generic_plan; PREPARE j AS " + query)
        self.assertEqual(len(self.remote_joins("EXECUTE j")), 1)
        self.c.sql("BEGIN; SELECT * FROM a; SELECT * FROM v1; SAVEPOINT attempt")
        self.error("EXECUTE j", "08000", "no common target")
        self.c.sql("ROLLBACK TO attempt")
        self.assertEqual(self.c.scalar("SELECT member FROM v1"), "virtual_remote_a")
        # Replanning sees the existing binding and chooses a local join.
        self.c.sql("DEALLOCATE j; PREPARE j AS " + query)
        self.assertEqual(self.remote_joins("EXECUTE j"), [])
        self.assertEqual(self.c.sql("EXECUTE j"), [("1",), ("2",)])
        self.c.sql("ROLLBACK")

    def test_cross_virtual_join_view_owners_and_role_switch(self):
        query = self.join_tables()
        self.c.sql("""CREATE USER MAPPING FOR v_alice SERVER s_b
                         OPTIONS (user 'v_remote_a', password_required 'false');
                      CREATE USER MAPPING FOR v_bob SERVER s_b
                         OPTIONS (user 'v_remote_b', password_required 'false');
                      GRANT SELECT ON v1_data, v2_data TO v_alice, v_bob;
                      CREATE VIEW owned1 AS SELECT * FROM v1_data;
                      CREATE VIEW owned2 AS SELECT * FROM v2_data;
                      ALTER VIEW owned1 OWNER TO v_alice;
                      ALTER VIEW owned2 OWNER TO v_bob;
                      GRANT SELECT ON owned1, owned2 TO v_alice, v_bob;
                      SET plan_cache_mode = force_generic_plan;
                      PREPARE j AS SELECT x.remote_user, y.remote_user
                         FROM v1 x JOIN v2 y ON x.pid = y.pid;
                      SET ROLE v_alice""")
        self.assertEqual(self.c.sql("EXECUTE j"), [("v_remote_a", "v_remote_a")])
        self.assertEqual(len(self.remote_joins(query)), 1)
        different_owners = "SELECT x.id FROM owned1 x JOIN owned2 y USING (id)"
        self.assertEqual(self.remote_joins(different_owners), [])
        self.assertEqual(self.c.sql(different_owners), [("1",), ("2",)])
        self.c.sql("SET ROLE v_bob")
        self.assertEqual(self.c.sql("EXECUTE j"), [("v_remote_b", "v_remote_b")])

    def test_cross_virtual_join_with_actual_server(self):
        self.join_tables()
        self.c.sql("CREATE FOREIGN TABLE actual_data(id int, value text) SERVER s_b "
                   "OPTIONS (table_name 'data', use_remote_estimate 'true')")
        query = "SELECT x.id FROM actual_data x JOIN v1_data y USING (id) ORDER BY x.id"
        self.assertEqual(len(self.remote_joins(query)), 1)
        self.assertEqual(self.c.sql(query), [("1",), ("2",)])

    def test_cross_virtual_join_async_append(self):
        query = self.join_tables()
        self.virtual("v3", "s_b,s_c,s_d", options=", use_remote_estimate 'true', async_capable 'true'")
        self.virtual("v4", "s_b,s_d", options=", use_remote_estimate 'true', async_capable 'true'")
        for name in ("v1", "v2"):
            self.c.sql(f"ALTER SERVER {name} OPTIONS (ADD async_capable 'true', ADD fetch_size '1')")
        for name in ("v3", "v4"):
            self.c.sql(f"CREATE FOREIGN TABLE {name}_data(id int, value text) "
                       f"SERVER {name} OPTIONS (table_name 'data', fetch_size '1')")
        query = ("SELECT x.id FROM v1_data x JOIN v2_data y USING (id) UNION ALL "
                 "SELECT x.id FROM v3_data x JOIN v4_data y USING (id)")
        self.c.sql("BEGIN")
        self.assertEqual(len(self.remote_joins(query)), 2)
        plan = self.c.scalar("EXPLAIN (VERBOSE, FORMAT JSON) " + query)
        self.assertIn('"Async Capable": true', plan)
        for _ in range(2):
            self.assertEqual(sorted(self.c.sql(query)), [("1",), ("1",), ("2",), ("2",)])
        pids = [self.c.scalar("SELECT pid FROM " + name) for name in ("v1", "v2", "v3", "v4")]
        self.assertEqual(len(set(pids)), 1)
        self.c.sql("COMMIT")

    def test_cross_virtual_partitionwise_join_and_pruning(self):
        self.c.sql("CREATE TABLE px(id int, value text) PARTITION BY RANGE (id); "
                   "CREATE TABLE py(id int, value text) PARTITION BY RANGE (id); "
                   "SET enable_partitionwise_join = on; "
                   "SET plan_cache_mode = force_generic_plan")
        for index, members in ((1, "s_a"), (2, "s_b")):
            for side in ("x", "y"):
                route = f"route_{side}{index}"
                self.virtual(route, members, options=", use_remote_estimate 'true'")
                self.c.sql(f"CREATE FOREIGN TABLE p{side}{index} PARTITION OF p{side} "
                           f"FOR VALUES FROM ({index}) TO ({index + 1}) SERVER {route} "
                           "OPTIONS (table_name 'data')")
        # The remote tables must obey their local partition bounds.
        for member, index in (("a", 1), ("b", 2)):
            with self.cluster.connect("virtual_remote_" + member) as c:
                c.sql(f"CREATE VIEW partition_{index} AS SELECT * FROM data WHERE id = {index}")
            for side in ("x", "y"):
                self.c.sql(f"ALTER FOREIGN TABLE p{side}{index} OPTIONS (SET table_name 'partition_{index}')")
        query = "SELECT x.id FROM px x JOIN py y USING (id) ORDER BY x.id"
        self.assertEqual(len(self.remote_joins(query)), 2)
        self.assertEqual(self.c.sql(query), [("1",), ("2",)])
        self.c.sql("PREPARE pj(int) AS SELECT x.id FROM px x JOIN py y USING (id) WHERE x.id = $1")
        self.assertEqual(self.c.sql("EXECUTE pj(1)"), [("1",)])
        self.assertEqual(self.c.sql("EXECUTE pj(2)"), [("2",)])

    def test_cross_virtual_join_repeated_reference_stays_local(self):
        self.join_tables("s_a,s_b,s_c", "s_b")
        self.virtual("v3", "s_c", options=", use_remote_estimate 'true'")
        self.c.sql("CREATE FOREIGN TABLE v3_data(id int, value text) SERVER v3 "
                   "OPTIONS (table_name 'data')")
        query = ("SELECT x.id FROM v1_data x JOIN v2_data y USING (id) UNION ALL "
                 "SELECT x.id FROM v1_data x JOIN v3_data y USING (id)")
        self.assertEqual(self.remote_joins(query), [])
        self.assertEqual(sorted(self.c.sql(query)), [("1",), ("1",), ("2",), ("2",)])
        # The same protection applies to separate scans in one query level.
        query = ("SELECT x.id FROM v1_data x JOIN v2_data y USING (id) "
                 "JOIN v1_data z ON x.id = z.id ORDER BY x.id")
        self.c.sql("SET join_collapse_limit = 1")
        self.assertEqual(self.remote_joins(query), [])
        self.assertEqual(self.c.sql(query), [("1",), ("2",)])

    def test_cross_virtual_join_repeated_reference_inside_whole_join(self):
        self.join_tables()
        query = ("SELECT x.id FROM (v1_data x JOIN v1_data z USING (id)) "
                 "JOIN v2_data y USING (id) ORDER BY x.id")
        self.c.sql("SET join_collapse_limit = 1")
        self.assertTrue(any(sql.count("JOIN") == 2 for sql in self.remote_joins(query)))
        self.assertEqual(self.c.sql(query), [("1",), ("2",)])

    def test_cross_group_join_checks_references_to_sibling_shards(self):
        self.join_tables("s_a,s_b", "s_a")
        self.virtual("v3", "s_b,s_a", options=", use_remote_estimate 'true'")
        self.virtual("v4", "s_b", options=", use_remote_estimate 'true'")
        for name in ("v3", "v4"):
            self.c.sql(f"CREATE FOREIGN TABLE {name}_data(id int, value text) "
                       f"SERVER {name} OPTIONS (table_name 'data')")
        # No virtual server is repeated, but the shared v1/v3 group is repeated.
        query = ("SELECT x.id FROM v1_data x JOIN v2_data y USING (id) UNION ALL "
                 "SELECT x.id FROM v3_data x JOIN v4_data y USING (id)")
        self.assertEqual(self.remote_joins(query), [])
        self.assertEqual(sorted(self.c.sql(query)), [("1",), ("1",), ("2",), ("2",)])

    def test_cross_group_join_binding_is_inherited_by_unused_peer(self):
        query = self.join_tables()
        self.virtual("peer", "s_b,s_a")
        self.c.sql("BEGIN")
        self.assertEqual(len(self.remote_joins(query)), 1)
        self.assertEqual(self.c.sql(query), [("1",), ("2",)])
        pid = self.c.scalar("SELECT pid FROM v1")
        self.c.sql("ALTER SERVER s_b OPTIONS (ADD application_name 'retired')")
        self.assertEqual(self.c.sql("SELECT member, pid FROM peer"), [("virtual_remote_b", pid)])
        self.c.sql("ROLLBACK")

    def test_same_group_repeated_joins_and_scan_share_one_replica(self):
        self.join_tables("s_a,s_b,s_c", "s_c,s_b,s_a")
        self.virtual("v3", "s_b,s_a,s_c", options=", use_remote_estimate 'true'")
        self.c.sql("CREATE FOREIGN TABLE v3_data(id int, value text) SERVER v3 "
                   "OPTIONS (table_name 'data'); BEGIN; SELECT * FROM a; SELECT * FROM b")
        query = ("SELECT x.id FROM v1_data x JOIN v2_data y USING (id) UNION ALL "
                 "SELECT x.id FROM v1_data x JOIN v3_data y USING (id) UNION ALL "
                 "SELECT id FROM v2_data")
        self.assertEqual(len(self.remote_joins(query)), 2)
        self.assertEqual(sorted(self.c.sql(query)), [("1",)] * 3 + [("2",)] * 3)
        pids = [self.c.scalar("SELECT pid FROM " + name) for name in ("v1", "v2", "v3")]
        self.assertEqual(len(set(pids)), 1)
        self.c.sql("ROLLBACK")

    def test_same_group_repeated_reference_in_larger_join(self):
        self.join_tables("s_a,s_b", "s_b,s_a")
        self.c.sql("SET join_collapse_limit = 1")
        query = ("SELECT x.id FROM (v1_data x JOIN v2_data y USING (id)) "
                 "JOIN v1_data z USING (id) ORDER BY x.id")
        self.assertTrue(any(sql.count("JOIN") == 2 for sql in self.remote_joins(query)))
        self.assertEqual(self.c.sql(query), [("1",), ("2",)])

    def test_same_group_async_append_with_repeated_shards(self):
        self.join_tables("s_a,s_b", "s_b,s_a")
        for name in ("v1", "v2"):
            self.c.sql(f"ALTER SERVER {name} OPTIONS (ADD async_capable 'true', ADD fetch_size '1')")
        query = ("SELECT x.id FROM v1_data x JOIN v2_data y USING (id) UNION ALL "
                 "SELECT x.id FROM v2_data x JOIN v1_data y USING (id)")
        self.c.sql("BEGIN")
        self.assertEqual(len(self.remote_joins(query)), 2)
        self.assertIn('"Async Capable": true',
                      self.c.scalar("EXPLAIN (VERBOSE, FORMAT JSON) " + query))
        for _ in range(2):
            self.assertEqual(sorted(self.c.sql(query)), [("1",), ("1",), ("2",), ("2",)])
        self.assertEqual(self.c.sql("SELECT pid FROM v1"), self.c.sql("SELECT pid FROM v2"))
        self.c.sql("COMMIT")

    def test_same_group_repeated_outer_joins_and_aggregation(self):
        self.join_tables("s_a,s_b", "s_b,s_a")
        branch = "SELECT count(*) FROM v1_data x LEFT JOIN v2_data y ON x.id = y.id AND y.id = 1"
        query = branch + " UNION ALL " + branch
        joins = self.remote_joins(query)
        self.assertEqual(len(joins), 2)
        self.assertTrue(all("count(*)" in sql for sql in joins))
        self.assertEqual(self.c.sql(query), [("2",), ("2",)])

    def test_same_group_cached_plan_tracks_bindings_and_topology(self):
        self.join_tables("s_a,s_b", "s_b,s_a")
        query = ("SELECT x.id FROM v1_data x JOIN v2_data y USING (id) UNION ALL "
                 "SELECT id FROM v1_data")
        self.c.sql("SET plan_cache_mode = force_generic_plan; PREPARE j AS " + query)
        self.assertEqual(len(self.remote_joins("EXECUTE j")), 1)
        self.c.sql("BEGIN; SELECT * FROM b; SELECT * FROM v1")
        self.assertEqual(sorted(self.c.sql("EXECUTE j")), [("1",), ("1",), ("2",), ("2",)])
        self.assertEqual(self.c.scalar("SELECT member FROM v2"), "virtual_remote_b")
        self.c.sql("COMMIT")
        with self.cluster.connect(self.db) as other:
            other.sql("ALTER SERVER v1 OPTIONS (SET members 's_c'); "
                      "ALTER SERVER v2 OPTIONS (SET members 's_c')")
        self.c.sql("BEGIN")
        self.assertEqual(len(self.remote_joins("EXECUTE j")), 1)
        self.assertEqual(sorted(self.c.sql("EXECUTE j")), [("1",), ("1",), ("2",), ("2",)])
        self.assertEqual(self.c.scalar("SELECT member FROM v1"), "virtual_remote_c")
        self.c.sql("COMMIT; ALTER SERVER v2 OPTIONS (SET members 's_d')")
        self.assertEqual(self.remote_joins("EXECUTE j"), [])
        self.assertEqual(sorted(self.c.sql("EXECUTE j")), [("1",), ("1",), ("2",), ("2",)])

    def test_topology_change_does_not_merge_previously_bound_groups(self):
        self.join_tables("s_a,s_b", "s_b,s_c")
        self.c.sql("BEGIN; SELECT * FROM a; SELECT * FROM v1; "
                   "SELECT * FROM c; SELECT * FROM v2; "
                   "ALTER SERVER v2 OPTIONS (SET members 's_b,s_a')")
        query = ("SELECT x.id FROM v1_data x JOIN v2_data y USING (id) UNION ALL "
                 "SELECT id FROM v1_data")
        self.assertEqual(self.remote_joins(query), [])
        self.assertEqual(sorted(self.c.sql(query)), [("1",), ("1",), ("2",), ("2",)])
        self.assertEqual(self.c.scalar("SELECT member FROM v1"), "virtual_remote_a")
        self.assertEqual(self.c.scalar("SELECT member FROM v2"), "virtual_remote_c")
        self.c.sql("COMMIT")
        self.assertEqual(len(self.remote_joins(query)), 1)
        self.assertEqual(sorted(self.c.sql(query)), [("1",), ("1",), ("2",), ("2",)])

    def test_same_group_partitionwise_joins_and_generic_pruning(self):
        self.c.sql("CREATE TABLE px(id int, value text) PARTITION BY RANGE (id); "
                   "CREATE TABLE py(id int, value text) PARTITION BY RANGE (id); "
                   "SET enable_partitionwise_join = on; SET plan_cache_mode = force_generic_plan")
        for member in "ab":
            with self.cluster.connect("virtual_remote_" + member) as remote:
                for index in (1, 2):
                    remote.sql(f"CREATE VIEW group_partition_{index} AS "
                               f"SELECT * FROM data WHERE id = {index}")
        for index in (1, 2):
            for side, members in (("x", "s_a,s_b"), ("y", "s_b,s_a")):
                route = f"route_{side}{index}"
                self.virtual(route, members, options=", use_remote_estimate 'true'")
                self.c.sql(f"CREATE FOREIGN TABLE p{side}{index} PARTITION OF p{side} "
                           f"FOR VALUES FROM ({index}) TO ({index + 1}) SERVER {route} "
                           f"OPTIONS (table_name 'group_partition_{index}')")
        query = "SELECT x.id FROM px x JOIN py y USING (id) ORDER BY x.id"
        self.assertEqual(len(self.remote_joins(query)), 2)
        self.assertEqual(self.c.sql(query), [("1",), ("2",)])
        self.c.sql("PREPARE pj(int) AS SELECT x.id FROM px x JOIN py y USING (id) "
                   "WHERE x.id = $1; BEGIN")
        self.assertEqual(self.c.sql("EXECUTE pj(1)"), [("1",)])
        self.assertEqual(self.c.sql("EXECUTE pj(2)"), [("2",)])
        pids = [self.c.scalar("SELECT pid FROM route_" + suffix) for suffix in ("x1", "y1", "x2", "y2")]
        self.assertEqual(len(set(pids)), 1)
        self.c.sql("COMMIT")

    def test_cross_virtual_semijoin_and_local_safety_checks(self):
        self.join_tables()
        query = ("SELECT x.id FROM v1_data x WHERE EXISTS "
                 "(SELECT 1 FROM v2_data y WHERE y.id = x.id AND y.id = 1)")
        plan = self.c.scalar("EXPLAIN (VERBOSE, FORMAT JSON) " + query)
        self.assertIn("EXISTS", plan)
        self.assertEqual(self.c.sql(query), [("1",)])
        self.c.sql("ALTER SERVER v2 OPTIONS (ADD extensions 'pgwrh_fdw')")
        query = "SELECT x.id FROM v1_data x JOIN v2_data y USING (id)"
        self.assertEqual(self.remote_joins(query), [])
        self.c.sql("ALTER SERVER v2 OPTIONS (DROP extensions)")
        self.assertEqual(self.remote_joins(query + " FOR UPDATE OF x"), [])
        self.assertEqual(self.remote_joins(
            "UPDATE v1_data x SET value = y.value FROM v2_data y WHERE x.id = y.id"), [])

    def test_cross_virtual_join_catalog_change_from_another_session(self):
        query = self.join_tables()
        self.c.sql("SET plan_cache_mode = force_generic_plan; PREPARE j AS " + query)
        self.assertEqual(len(self.remote_joins("EXECUTE j")), 1)
        with self.cluster.connect(self.db) as other:
            other.sql("ALTER SERVER v1 OPTIONS (SET members 's_d'); "
                      "ALTER SERVER v2 OPTIONS (SET members 's_d')")
        self.c.sql("BEGIN")
        self.assertEqual(self.c.sql("EXECUTE j"), [("1",), ("2",)])
        self.assertEqual(self.c.scalar("SELECT member FROM v1"), "virtual_remote_d")
        self.c.sql("COMMIT")

    def test_modification_call_sites_and_savepoints(self):
        self.virtual()
        self.c.sql("""CREATE FOREIGN TABLE writes(id int, value text) SERVER v
                         OPTIONS (table_name 'data');
                      PREPARE put(int) AS INSERT INTO writes VALUES ($1, 'written');
                      BEGIN; EXECUTE put(10); SAVEPOINT s; EXECUTE put(11);
                      ROLLBACK TO s; COMMIT;""")
        self.assertEqual(self.c.sql("SELECT id FROM writes WHERE id >= 10"), [("10",)])
        self.c.sql("UPDATE writes SET value = 'updated' WHERE id = 10")
        self.assertEqual(self.c.scalar("SELECT value FROM writes WHERE id = 10"), "updated")
        self.c.sql("DELETE FROM writes WHERE id = 10")
        self.c.sql("BEGIN; TRUNCATE writes")
        self.assertEqual(self.c.scalar("SELECT count(*) FROM writes"), "0")
        self.c.sql("ROLLBACK")
        self.assertEqual(self.c.scalar("SELECT count(*) FROM writes"), "2")

    def test_overlapping_sets_share_an_idle_then_active_connection(self):
        self.virtual("v_abc", "s_a,s_b,s_c")
        self.virtual("v_bcd", "s_b,s_c,s_d")
        pid = self.c.scalar("SELECT pid FROM b")
        self.c.sql("BEGIN")
        self.assertEqual(self.c.scalar("SELECT pid FROM v_abc"), pid)
        self.assertEqual(self.c.scalar("SELECT pid FROM v_bcd"), pid)
        self.assertEqual(self.c.sql("SELECT server_name FROM pgwrh_fdw_get_connections()"), [("s_b",)])
        # Once bound, opening another eligible connection cannot redirect it.
        self.c.sql("SELECT * FROM a")
        self.assertEqual(self.c.scalar("SELECT pid FROM v_abc"), pid)
        self.c.sql("COMMIT")

    def test_active_connection_precedes_idle_connection(self):
        self.virtual(members="s_a,s_b,s_c")
        idle = self.c.scalar("SELECT pid FROM a")
        self.c.sql("BEGIN")
        active = self.c.scalar("SELECT pid FROM b")
        self.assertNotEqual(active, idle)
        self.assertEqual(self.c.scalar("SELECT pid FROM v"), active)
        self.assertEqual(self.c.scalar("SELECT count(*) FROM pgwrh_fdw_get_connections()"), "2")
        self.c.sql("COMMIT")

    def test_cached_connection_for_another_mapping_is_not_borrowed(self):
        self.virtual(members="s_a,s_b")
        self.c.sql("""
            CREATE USER MAPPING FOR v_alice SERVER s_a
                OPTIONS (user 'v_remote_a', password_required 'false');
            CREATE USER MAPPING FOR v_alice SERVER s_b
                OPTIONS (user 'v_remote_b', password_required 'false');
            BEGIN;
        """)
        owner_pid = self.c.scalar("SELECT pid FROM b")
        self.c.sql("SET LOCAL ROLE v_alice")
        alice_pid = self.c.scalar("SELECT pid FROM a")
        self.assertEqual(self.c.sql("SELECT member, remote_user, pid FROM v"),
                         [("virtual_remote_a", "v_remote_a", alice_pid)])
        self.assertNotEqual(owner_pid, alice_pid)
        self.c.sql("COMMIT")

    def test_disjoint_sets_require_separate_connections(self):
        self.virtual("v_ab", "s_a,s_b")
        self.virtual("v_cd", "s_c,s_d")
        self.c.sql("BEGIN")
        self.assertNotEqual(self.c.scalar("SELECT pid FROM v_ab"),
                            self.c.scalar("SELECT pid FROM v_cd"))
        self.assertEqual(self.c.scalar("SELECT count(*) FROM pgwrh_fdw_get_connections()"), "2")
        self.c.sql("COMMIT")

    def test_async_scans_share_pending_request_state(self):
        self.virtual("v_abc", "s_a,s_b,s_c", options=", async_capable 'true'")
        self.virtual("v_bcd", "s_b,s_c,s_d", options=", async_capable 'true'")
        with self.cluster.connect("virtual_remote_b") as remote:
            remote.sql("CREATE TABLE async_data AS SELECT generate_series(1, 200) AS id")
        for name, server in (("async_x", "v_abc"), ("async_y", "v_bcd")):
            self.c.sql(f"CREATE FOREIGN TABLE {name}(id int) SERVER {server} "
                       "OPTIONS (table_name 'async_data', fetch_size '1')")
        self.c.sql("BEGIN")
        pid = self.c.scalar("SELECT pid FROM b")
        query = "SELECT id FROM async_x UNION ALL SELECT id FROM async_y"
        plan = json.loads(self.c.scalar("EXPLAIN (FORMAT JSON) " + query))[0]["Plan"]
        self.assertEqual(plan["Node Type"], "Append")
        self.assertTrue(all(p["Async Capable"] for p in plan["Plans"]))
        expected = sorted([i for i in range(1, 201)] * 2)
        for _ in range(2):
            self.assertEqual(sorted(int(row[0]) for row in self.c.sql(query)), expected)
        self.assertEqual(self.c.scalar("SELECT pid FROM v_abc"), pid)
        self.assertEqual(self.c.scalar("SELECT pid FROM v_bcd"), pid)
        self.assertEqual(self.c.scalar("SELECT count(*) FROM pgwrh_fdw_get_connections()"), "1")
        self.c.sql("COMMIT")

    def test_runtime_partition_pruning_does_not_connect_unused_route(self):
        self.c.sql("""
            CREATE SERVER offline FOREIGN DATA WRAPPER pgwrh_fdw
                OPTIONS (host '/pgwrh-fdw-nonexistent-socket-directory', port '1');
            CREATE USER MAPPING FOR CURRENT_USER SERVER offline;
        """)
        self.virtual("left_route", "s_a")
        self.virtual("right_route", "offline")
        self.c.sql("""
            CREATE TABLE partitioned(id int, value text) PARTITION BY RANGE (id);
            CREATE FOREIGN TABLE part_left PARTITION OF partitioned
                FOR VALUES FROM (0) TO (100) SERVER left_route OPTIONS (table_name 'data');
            CREATE FOREIGN TABLE part_right PARTITION OF partitioned
                FOR VALUES FROM (100) TO (200) SERVER right_route OPTIONS (table_name 'data');
            SET plan_cache_mode = force_generic_plan;
            PREPARE pruned(int) AS SELECT id FROM partitioned WHERE id = $1;
            BEGIN;
        """)
        self.assertEqual(self.c.sql("EXECUTE pruned(1)"), [("1",)])
        self.assertEqual(self.c.sql("SELECT server_name FROM pgwrh_fdw_get_connections()"), [("s_a",)])
        self.c.sql("COMMIT")

    def test_invalidated_active_connection_accepts_only_existing_groups(self):
        self.virtual("bound", "s_b")
        self.virtual("fresh", "s_b,s_c")
        self.virtual("peer", "s_b")
        self.c.sql("BEGIN")
        pid = self.c.scalar("SELECT pid FROM bound")
        self.c.sql("ALTER SERVER s_b OPTIONS (ADD application_name 'changed')")
        self.assertEqual(self.c.scalar("SELECT member FROM fresh"), "virtual_remote_c")
        self.assertEqual(self.c.scalar("SELECT pid FROM bound"), pid)
        self.assertEqual(self.c.scalar("SELECT pid FROM peer"), pid)
        self.c.sql("COMMIT")
        self.assertNotEqual(self.c.scalar("SELECT pid FROM peer"), pid)


if __name__ == "__main__":
    unittest.main(verbosity=2)
