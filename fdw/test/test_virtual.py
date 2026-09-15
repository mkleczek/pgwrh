# SPDX-License-Identifier: AGPL-3.0-only
"""Virtual-server routing through the ordinary FDW connection entry point."""
import json
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
            cls.admin.sql("CREATE ROLE v_alice; CREATE ROLE v_bob; "
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
                GRANT USAGE ON FOREIGN SERVER s_{member} TO v_alice, v_bob;
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
        self.virtual(members="s_a,s_b")
        self.c.sql("""
            CREATE USER MAPPING FOR v_alice SERVER s_a
                OPTIONS (user 'v_remote_a', password_required 'false');
            CREATE USER MAPPING FOR PUBLIC SERVER s_b
                OPTIONS (user 'v_remote_b', password_required 'false');
            REVOKE USAGE ON FOREIGN SERVER s_b FROM v_alice;
            BEGIN; SET LOCAL ROLE v_alice;
        """)
        alice = self.c.sql("SELECT member, remote_user, pid FROM v")[0]
        self.assertEqual(alice[:2], ("virtual_remote_a", "v_remote_a"))
        self.c.sql("SET LOCAL ROLE v_bob")
        bob = self.c.sql("SELECT member, remote_user, pid FROM v")[0]
        self.assertEqual(bob[:2], ("virtual_remote_b", "v_remote_b"))
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

    def test_target_privileges_are_not_bypassed(self):
        self.virtual()
        self.c.sql("""
            CREATE USER MAPPING FOR PUBLIC SERVER s_a
                OPTIONS (user 'v_remote_a', password_required 'false');
            REVOKE USAGE ON FOREIGN SERVER s_a FROM v_alice;
            SET ROLE v_alice;
        """)
        self.error("SELECT * FROM v", "42501", "no accessible members")

    def test_transaction_pin_survives_savepoint_and_membership_change(self):
        self.virtual()
        self.c.sql("BEGIN; SAVEPOINT first_use")
        first = self.c.sql("SELECT member, pid FROM v")
        self.c.sql("ROLLBACK TO first_use; ALTER SERVER v OPTIONS (SET members 's_b')")
        self.assertEqual(self.c.sql("SELECT member, pid FROM v"), first)
        self.c.sql("COMMIT")
        self.assertEqual(self.c.scalar("SELECT member FROM v"), "virtual_remote_b")

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


if __name__ == "__main__":
    unittest.main(verbosity=2)
