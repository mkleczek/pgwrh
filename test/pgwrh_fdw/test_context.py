# SPDX-License-Identifier: AGPL-3.0-only
import json
import unittest

from support import Cluster, PgError, ROOT, literal


class ContextTests(unittest.TestCase):
    counter = 0

    @classmethod
    def setUpClass(cls):
        cls.cluster = Cluster()
        try:
            cls.cluster.setup()
            cls.admin = cls.cluster.connect()
            cls.admin.sql("CREATE ROLE remote_reader LOGIN; CREATE ROLE local_reader")
            for db in ("remote_a", "remote_b"):
                cls.admin.sql(f"CREATE DATABASE {db}")
                cls.admin.sql(f"ALTER DATABASE {db} SET session_preload_libraries = 'context_probe'")
                with cls.cluster.connect(db) as c:
                    c.sql("""
                        CREATE VIEW ctx AS SELECT
                          current_setting('app.request_id', true) AS request_id,
                          current_setting('pgwrh.read_after_lsn', true) AS watermark,
                          current_setting('app.aux', true) AS aux,
                          pg_backend_pid() AS pid,
                          current_setting('ctxprobe.level')::int AS level,
                          current_setting('ctxprobe.token') AS token;
                        CREATE TABLE data(id int PRIMARY KEY, value text,
                          request_id text DEFAULT current_setting('app.request_id', true));
                        INSERT INTO data(id, value) VALUES (1, 'one'), (2, 'two');
                        GRANT SELECT ON ctx, data TO remote_reader;
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
        self.db = "test_" + str(self.counter)
        self.admin.sql(f"CREATE DATABASE {self.db}")
        self.c = self.cluster.connect(self.db)
        self.addCleanup(self.c.close)
        self.c.sql("CREATE EXTENSION pgwrh_fdw")
        self.server("s_a", "remote_a", "app.request_id,pgwrh.read_after_lsn")
        self.server("s_b", "remote_b", "app.request_id,pgwrh.read_after_lsn")
        self.server("s_plain", "remote_a", None)
        for name, server in (("a", "s_a"), ("b", "s_b"), ("plain", "s_plain")):
            self.ctx_table(name, server)

    def server(self, name, db, parameters, wrapper="pgwrh_fdw", remote_user=None):
        opts = f"host {literal(self.cluster.path)}, port '{self.cluster.port}', dbname '{db}'"
        if parameters is not None:
            opts += ", transaction_parameters " + literal(parameters)
        self.c.sql(f"CREATE SERVER {name} FOREIGN DATA WRAPPER {wrapper} OPTIONS ({opts})")
        mapping = "" if remote_user is None else f" OPTIONS (user '{remote_user}')"
        self.c.sql(f"CREATE USER MAPPING FOR CURRENT_USER SERVER {name}{mapping}")

    def ctx_table(self, name, server):
        self.c.sql(f"""CREATE FOREIGN TABLE {name}
            (request_id text, watermark text, aux text, pid int, level int, token text)
            SERVER {server} OPTIONS (schema_name 'public', table_name 'ctx')""")

    def context(self, request="request-42", watermark="0/12345678"):
        self.c.sql("SET LOCAL app.request_id = " + literal(request))
        self.c.sql("SET LOCAL pgwrh.read_after_lsn = " + literal(watermark))

    def begin(self, request="request-42", watermark="0/12345678"):
        self.c.sql("BEGIN")
        self.context(request, watermark)

    def error(self, query, state, fragment=None):
        with self.assertRaises(PgError) as cm:
            self.c.sql(query)
        self.assertEqual(cm.exception.sqlstate, state, str(cm.exception))
        if fragment:
            self.assertIn(fragment, str(cm.exception))

    def test_initial_release_installation(self):
        self.assertEqual(self.c.scalar(
            "SELECT extversion FROM pg_extension WHERE extname = 'pgwrh_fdw'"), "0.1.0")
        self.assertEqual(self.c.sql(
            "SELECT version FROM pg_available_extension_versions WHERE name = 'pgwrh_fdw'"),
            [("0.1.0",)])
        self.assertEqual(self.c.sql("SELECT * FROM pg_extension_update_paths('pgwrh_fdw')"), [])
        self.assertEqual(self.c.scalar(
            "SELECT version FROM pg_get_loaded_modules() WHERE module_name = 'pgwrh_fdw'"), "0.1.0")
        self.assertEqual(self.c.scalar("SELECT count(*) FROM pgwrh_fdw_get_connections()"), "0")
        self.assertEqual(self.c.scalar("SELECT count(*) FROM pgwrh_fdw_get_connections(true)"), "0")
        self.assertEqual(self.c.scalar("SELECT pgwrh_fdw_disconnect('s_a')"), "f")
        self.assertEqual(self.c.scalar("SELECT pgwrh_fdw_disconnect_all()"), "f")

    def test_multiple_participants_and_opaque_values(self):
        self.begin(watermark="opaque, not necessarily an LSN")
        rows = self.c.sql("SELECT request_id, watermark FROM a UNION ALL SELECT request_id, watermark FROM b")
        self.assertEqual(rows, [("request-42", "opaque, not necessarily an LSN")] * 2)
        self.c.sql("COMMIT")

    def test_reuse_empty_and_no_context_leak(self):
        self.begin("first")
        pid = self.c.scalar("SELECT pid FROM a")
        self.c.sql("COMMIT")
        self.begin("second")
        self.assertEqual(self.c.sql("SELECT request_id, pid FROM a"), [("second", pid)])
        self.c.sql("COMMIT")
        # An unregistered custom GUC remains an empty placeholder after RESET.
        self.c.sql("BEGIN")
        self.assertEqual(self.c.sql("SELECT request_id, watermark, pid FROM a"), [("", "", pid)])
        self.c.sql("ROLLBACK")
        self.c.sql("ALTER SERVER s_a OPTIONS (DROP transaction_parameters)")
        self.assertIsNone(self.c.scalar("SELECT request_id FROM a"))

    def test_reconnect_dead_cached_connection(self):
        self.begin("before")
        old = self.c.scalar("SELECT pid FROM a")
        self.c.sql("COMMIT")
        self.admin.sql(f"SELECT pg_terminate_backend({old}, 5000)")
        self.begin("after")
        result = self.c.sql("SELECT request_id, pid FROM a")[0]
        self.assertEqual(result[0], "after")
        self.assertNotEqual(result[1], old)
        self.c.sql("COMMIT")

    def test_reconnect_catalog_invalidation(self):
        self.begin("before")
        old = self.c.scalar("SELECT pid FROM a")
        self.c.sql("COMMIT; ALTER SERVER s_a OPTIONS (ADD application_name 'changed')")
        self.begin("after")
        self.assertEqual(self.c.scalar("SELECT request_id FROM a"), "after")
        self.assertNotEqual(self.c.scalar("SELECT pid FROM a"), old)

    def test_remote_estimate_planning_before_snapshot(self):
        self.c.sql("ALTER SERVER s_a OPTIONS (ADD use_remote_estimate 'true')")
        self.begin("plan")
        mark = self.cluster.log.stat().st_size
        self.c.sql("EXPLAIN SELECT * FROM a")
        logs = self.cluster.log.read_text()[mark:]
        self.assertIn('SET LOCAL "app.request_id"', logs)
        self.assertIn('EXPLAIN SELECT', logs)
        self.assertLess(logs.index('SET LOCAL "app.request_id"'), logs.index('EXPLAIN SELECT', logs.index('SET LOCAL "app.request_id"')))
        self.context("later")
        self.assertEqual(self.c.scalar("SELECT request_id FROM a"), "plan")

    def test_prepared_generic_plan_reuse(self):
        self.c.sql("ALTER SERVER s_a OPTIONS (ADD use_remote_estimate 'true'); SET plan_cache_mode = force_generic_plan")
        self.begin("planned")
        self.c.sql("PREPARE p(text) AS SELECT request_id FROM a WHERE request_id = $1")
        self.assertEqual(self.c.scalar("EXECUTE p('planned')"), "planned")
        self.c.sql("COMMIT")
        self.begin("reused")
        self.assertEqual(self.c.scalar("EXECUTE p('reused')"), "reused")

    def test_prepared_remote_insert(self):
        self.c.sql("CREATE FOREIGN TABLE writes(id int, value text) SERVER s_a OPTIONS (schema_name 'public', table_name 'data'); PREPARE w(int) AS INSERT INTO writes VALUES ($1, 'prepared')")
        for key, request in ((101, "write-one"), (102, "write-two")):
            self.begin(request)
            self.c.sql(f"EXECUTE w({key}); COMMIT")
        with self.cluster.connect("remote_a") as c:
            self.assertEqual(c.sql("SELECT request_id FROM data WHERE id IN (101,102) ORDER BY id"), [("write-one",), ("write-two",)])

    def test_async_append(self):
        self.c.sql("ALTER SERVER s_a OPTIONS (ADD async_capable 'true'); ALTER SERVER s_b OPTIONS (ADD async_capable 'true')")
        self.begin("async")
        query = "SELECT request_id FROM a UNION ALL SELECT request_id FROM b"
        plan = json.loads(self.c.scalar("EXPLAIN (FORMAT JSON) " + query))
        self.assertTrue(all(p.get("Async Capable") for p in plan[0]["Plan"]["Plans"]))
        self.assertEqual(self.c.sql(query), [("async",)] * 2)

    def test_first_access_nested_savepoint_and_rollback(self):
        self.c.sql("BEGIN; SAVEPOINT outer_s; SAVEPOINT inner_s")
        self.context("inside")
        self.assertEqual(self.c.scalar("SELECT request_id FROM a"), "inside")
        self.c.sql("ROLLBACK TO outer_s")
        self.assertEqual(self.c.scalar("SHOW app.request_id"), "")
        self.assertEqual(self.c.scalar("SELECT request_id FROM a"), "inside")
        self.assertEqual(self.c.scalar("SELECT request_id FROM b"), "inside")
        self.c.sql("RELEASE outer_s; COMMIT")

    def test_existing_remote_transaction_savepoints(self):
        self.begin("outer")
        self.c.sql("SELECT * FROM a; SAVEPOINT s")
        self.context("inner")
        self.assertEqual(self.c.scalar("SELECT request_id FROM a"), "outer")
        self.c.sql("ROLLBACK TO s; RELEASE s")
        self.assertEqual(self.c.scalar("SELECT request_id FROM b"), "outer")

    def test_later_set_set_config_and_function_settings_are_frozen(self):
        self.c.sql("""CREATE FUNCTION function_context() RETURNS text LANGUAGE sql
            SET app.request_id = 'function' AS 'SELECT request_id FROM b'""")
        self.begin("frozen")
        self.c.sql("SET LOCAL app.aux = 'early'; SELECT * FROM a")
        self.c.sql("ALTER SERVER s_b OPTIONS (SET transaction_parameters 'app.request_id,app.aux')")
        self.c.sql("SET LOCAL app.request_id = 'late'; SELECT set_config('app.aux','late',true)")
        self.assertEqual(self.c.scalar("SELECT function_context()"), "frozen")
        self.assertEqual(self.c.sql("SELECT request_id, aux FROM b"), [("frozen", "early")])

    def test_quoting_and_empty_values(self):
        values = ["O'Reilly \\ path\nUnicode: café 雪; SET app.request_id='oops' --", "", "DEFAULT"]
        for value in values:
            self.begin(value)
            self.assertEqual(self.c.scalar("SELECT request_id FROM a"), value)
            self.c.sql("COMMIT")

    def test_missing_and_defined_after_freeze(self):
        self.c.sql("ALTER SERVER s_b OPTIONS (SET transaction_parameters 'app.missing')")
        self.begin()
        self.c.sql("SELECT * FROM a; SET LOCAL app.missing = 'too late'; SAVEPOINT s")
        self.error("SELECT * FROM b", "42704", "was not available")
        self.c.sql("ROLLBACK TO s; COMMIT")
        self.begin()
        self.c.sql("SET LOCAL app.missing = 'now early'")
        self.c.sql("SELECT * FROM b")

    def test_missing_on_first_access(self):
        self.error("SELECT * FROM a", "42704", "app.request_id")

    def test_registered_default(self):
        self.c.sql("LOAD 'context_probe'; ALTER SERVER s_a OPTIONS (SET transaction_parameters 'ctxprobe.token,ctxprobe.level')")
        self.assertEqual(self.c.sql("SELECT token, level FROM a"), [("default-token", "7")])

    def test_remote_invalid_value_poisons_failed_initialization(self):
        self.c.sql("ALTER SERVER s_a OPTIONS (SET transaction_parameters 'app.request_id,ctxprobe.level')")
        self.begin()
        self.c.sql("SET LOCAL ctxprobe.level = 'invalid'; SAVEPOINT s")
        self.error("SELECT * FROM a", "22023", "ctxprobe.level")
        self.c.sql("ROLLBACK TO s")
        self.error("SELECT * FROM a", "08000", "was lost")
        self.c.sql("ROLLBACK")
        self.begin("recovered")
        self.c.sql("SET LOCAL ctxprobe.level = '9'")
        self.assertEqual(self.c.sql("SELECT request_id, level FROM a"), [("recovered", "9")])

    def test_remote_permission_checks(self):
        self.server("restricted", "remote_a", "ctxprobe.secret", remote_user="remote_reader")
        self.ctx_table("denied", "restricted")
        self.c.sql("SET ctxprobe.secret = 'private'")
        self.error("SELECT * FROM denied", "42501", "ctxprobe.secret")

    def test_local_visibility_and_role_change(self):
        self.c.sql("LOAD 'context_probe'; GRANT USAGE ON FOREIGN SERVER s_b TO local_reader; GRANT SELECT ON b TO local_reader; CREATE USER MAPPING FOR local_reader SERVER s_b OPTIONS (user 'remote_reader', password_required 'false'); ALTER SERVER s_b OPTIONS (SET transaction_parameters 'ctxprobe.secret')")
        self.begin()
        self.c.sql("SELECT * FROM a; SET LOCAL ROLE local_reader")
        self.error("SELECT * FROM b", "42501", "permission denied to examine")

    def test_option_validation(self):
        invalid = ["", " ", "app.a,", ",app.a", "app.a,,app.b", "app.a,APP.A", "application_name", "search_path", "TimeZone", "postgres_fdw.application_name", "pgwrh_fdw.application_name", "app.bad-name", '"app.a"', "app." + "x" * 60, "a..b", "a.b;RESET ALL"]
        for value in invalid:
            self.error("ALTER SERVER s_a OPTIONS (SET transaction_parameters " + literal(value) + ")", "22023")
        self.error("ALTER FOREIGN TABLE a OPTIONS (ADD transaction_parameters 'app.a')", "HV00D")
        self.c.sql("ALTER SERVER s_a OPTIONS (SET transaction_parameters ' APP.REQUEST_ID , pgwrh.read_after_lsn ')")
        self.begin()
        self.assertEqual(self.c.scalar("SELECT request_id FROM a"), "request-42")

    def test_disabled_representative_behavior(self):
        self.c.sql("""CREATE FOREIGN TABLE normal(id int, value text, request_id text)
            SERVER s_plain OPTIONS (schema_name 'public', table_name 'data')""")
        self.assertEqual(self.c.sql("SELECT id, value FROM normal WHERE id < 3 ORDER BY id"), [("1", "one"), ("2", "two")])
        self.assertIn('Remote SQL', str(self.c.sql("EXPLAIN (VERBOSE) SELECT count(*) FROM normal")))
        self.c.sql("BEGIN; INSERT INTO normal VALUES (300,'new','ordinary'); SAVEPOINT s; UPDATE normal SET value='changed' WHERE id=300; ROLLBACK TO s")
        self.assertEqual(self.c.scalar("SELECT value FROM normal WHERE id=300"), "new")
        self.c.sql("DELETE FROM normal WHERE id=300; COMMIT; CREATE SCHEMA imported; IMPORT FOREIGN SCHEMA public LIMIT TO (data) FROM SERVER s_plain INTO imported")
        self.assertEqual(self.c.scalar("SELECT value FROM imported.data WHERE id=1"), "one")
        self.assertIsNone(self.c.scalar("SELECT request_id FROM plain"))

    def check_coexistence(self, stock_first):
        # Each order uses a fresh database AND backend, so _PG_init order differs.
        db = self.db + "_order"
        self.admin.sql("CREATE DATABASE " + db)
        self.c.close()
        self.c = self.cluster.connect(db)
        self.addCleanup(self.c.close)
        order = ("postgres_fdw", "pgwrh_fdw") if stock_first else ("pgwrh_fdw", "postgres_fdw")
        for ext in order:
            self.c.sql("CREATE EXTENSION " + ext)
        self.server("s_a", "remote_a", "app.request_id,pgwrh.read_after_lsn")
        self.server("stock", "remote_a", None, "postgres_fdw")
        self.ctx_table("a", "s_a")
        self.ctx_table("stock_a", "stock")
        self.c.sql("SET postgres_fdw.application_name = 'stock-name'; SET pgwrh_fdw.application_name = 'fork-name'")
        self.begin("fork-only")
        ours = self.c.sql("SELECT request_id, pid FROM a")[0]
        stock = self.c.sql("SELECT request_id, pid FROM stock_a")[0]
        self.assertEqual(ours[0], "fork-only")
        self.assertIsNone(stock[0])
        self.assertNotEqual(ours[1], stock[1])
        self.assertEqual(self.admin.sql(f"SELECT application_name FROM pg_stat_activity WHERE pid IN ({ours[1]},{stock[1]}) ORDER BY application_name"), [("fork-name",), ("stock-name",)])
        self.assertEqual(self.c.scalar("SELECT count(*) FROM pgwrh_fdw_get_connections()"), "1")
        self.assertEqual(self.c.scalar("SELECT count(*) FROM postgres_fdw_get_connections()"), "1")
        self.c.sql("COMMIT; SELECT pgwrh_fdw_disconnect_all()")
        self.assertEqual(self.c.scalar("SELECT count(*) FROM postgres_fdw_get_connections()"), "1")
        self.assertEqual(self.c.scalar("SELECT count(*) FROM pgwrh_fdw_get_connections()"), "0")

    def test_coexistence_stock_first(self):
        self.check_coexistence(True)

    def test_coexistence_fork_first(self):
        self.check_coexistence(False)


if __name__ == "__main__":
    # Keep the existing test entry point used by the parent repository and CI.
    from test_virtual import VirtualServerTests

    unittest.main(verbosity=2)
