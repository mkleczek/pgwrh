"""Durable ownership, derived drop protection, and identity lifecycle."""
import pytest

from .test_backup_restore import run


INVENTORY = 'SELECT * FROM pgwrh.managed_object ORDER BY 1, 2, 3'
MARKERS = """SELECT d.classid, d.objid FROM pg_depend d JOIN pg_extension e
    ON d.refclassid = 'pg_extension'::regclass AND d.refobjid = e.oid
    WHERE e.extname = 'pgwrh' AND d.deptype = 'n' AND d.objsubid = 0 AND d.refobjsubid = 0
    ORDER BY 1, 2"""
ERASE_MARKERS = """DELETE FROM pg_depend WHERE refclassid = 'pg_extension'::regclass
    AND refobjid = (SELECT oid FROM pg_extension WHERE extname = 'pgwrh') AND deptype = 'n'"""


def seed(node):
    node.execute('''CREATE SCHEMA "managed schema";
        CREATE TABLE "managed schema"."table one" (id integer);
        CREATE INDEX "index one" ON "managed schema"."table one" (id);
        CREATE SERVER "server one" FOREIGN DATA WRAPPER pgwrh_fdw;
        CREATE PUBLICATION "publication one" FOR TABLE "managed schema"."table one";
        SELECT pgwrh.add_ext_dependency('pg_namespace', '"managed schema"'::regnamespace);
        SELECT pgwrh.add_ext_dependency('pg_class', '"managed schema"."table one"'::regclass);
        SELECT pgwrh.add_ext_dependency('pg_class', '"managed schema"."index one"'::regclass);
        SELECT pgwrh.add_ext_dependency('pg_foreign_server', oid) FROM pg_foreign_server WHERE srvname = 'server one';
        SELECT pgwrh.add_ext_dependency('pg_publication', oid) FROM pg_publication WHERE pubname = 'publication one';
        CREATE TABLE public.unrelated(id integer);
    ''')


def test_registry_drives_ownership_without_markers(postgres_node_factory):
    node = postgres_node_factory('registry')
    seed(node)
    before = node.execute(INVENTORY)
    # Keep the supervisor outside this transaction; prove that reconciliation
    # itself needs no marker repair, then exercise idempotent repair explicitly.
    with node.connect() as conn:
        conn.execute(ERASE_MARKERS)
        assert conn.execute(MARKERS) == []
        assert conn.execute(INVENTORY) == before
        owned = conn.execute('SELECT * FROM pgwrh.owned_obj ORDER BY 1, 2')
        assert len(owned) == len(before)
        conn.execute('SELECT pgwrh.repair_managed_objects()')
        conn.execute('SELECT pgwrh.repair_managed_objects()')
        assert conn.execute(MARKERS) == owned
        conn.commit()
    with pytest.raises(Exception, match='other objects depend on it'):
        node.execute('DROP EXTENSION pgwrh')
    node.execute('DROP EXTENSION pgwrh CASCADE')
    assert node.execute('''SELECT to_regclass('public.unrelated'), to_regnamespace('"managed schema"')''') == [('unrelated', None)]
    assert node.execute("SELECT srvname FROM pg_foreign_server WHERE srvname = 'server one'") == []
    assert node.execute("SELECT pubname FROM pg_publication WHERE pubname = 'publication one'") == []


def test_managed_names_follow_rename_and_drop(postgres_node_factory):
    node = postgres_node_factory('registry_ddl')
    seed(node)
    before = node.execute(INVENTORY)
    with node.connect() as conn:
        conn.execute('ALTER SCHEMA "managed schema" RENAME TO rolled_back')
        conn.execute('DROP TABLE rolled_back."table one"')
        conn.rollback()
    assert node.execute(INVENTORY) == before
    node.execute('''ALTER TABLE "managed schema"."table one" RENAME TO renamed;
        ALTER INDEX "managed schema"."index one" RENAME TO renamed_index;
        ALTER SCHEMA "managed schema" RENAME TO renamed_schema;
        ALTER SERVER "server one" RENAME TO renamed_server;
        ALTER PUBLICATION "publication one" RENAME TO renamed_publication;
        CREATE SCHEMA moved;
        ALTER TABLE renamed_schema.renamed SET SCHEMA moved;
    ''')
    assert node.execute('SELECT count(*) FROM pgwrh.owned_obj') == [(len(before),)]
    assert ('table', 'moved', 'renamed') in node.execute(INVENTORY)
    assert ('index', 'moved', 'renamed_index') in node.execute(INVENTORY)
    node.execute('''DROP TABLE moved.renamed;
        CREATE TABLE moved.renamed(id integer);
        DROP SERVER renamed_server;
        CREATE SERVER renamed_server FOREIGN DATA WRAPPER pgwrh_fdw;
        DROP PUBLICATION renamed_publication;
        CREATE PUBLICATION renamed_publication;
        DROP SCHEMA renamed_schema;
        CREATE SCHEMA renamed_schema;
    ''')
    assert node.execute(INVENTORY) == [('publication', '', 'pgwrh_controller_ping')]
    node.execute('SELECT pgwrh.repair_managed_objects()')
    assert node.execute(MARKERS) == node.execute('SELECT * FROM pgwrh.owned_obj ORDER BY 1, 2')


def test_logical_restore_resolves_new_oids(postgres_node_factory, tmp_path):
    source = postgres_node_factory('registry_source')
    seed(source)
    before = source.execute(INVENTORY)
    source.execute("INSERT INTO pgwrh.sync_daemon_config VALUES (true, false, 0.25, 'custom daemon')")
    archive = tmp_path / 'managed.dump'
    run(source, 'pg_dump', '-d', 'postgres', '-Fc', '-f', str(archive))
    target = postgres_node_factory('registry_target', install_extension=False)
    # Force a different OID allocation even on identical initdb installations.
    target.execute('CREATE TABLE oid_padding(id integer)')
    # The bootstrap publication already exists after CREATE EXTENSION; the
    # supported recovery procedure likewise omits its dump entries.
    toc = tmp_path / 'restore.list'
    toc.write_text('\n'.join(line for line in run(source, 'pg_restore', '--list', str(archive)).splitlines()
                             if not ('PUBLICATION' in line and 'pgwrh_controller_ping' in line)))
    run(target, 'pg_restore', '-d', 'postgres', '--exit-on-error', '--single-transaction',
        '--use-list', str(toc), str(archive))
    assert target.execute('SELECT * FROM pgwrh.sync_daemon_config') == source.execute('SELECT * FROM pgwrh.sync_daemon_config')
    assert target.execute(INVENTORY) == before
    assert target.execute('SELECT * FROM pgwrh.owned_obj ORDER BY 1, 2') != source.execute(MARKERS)
    target.execute('SELECT pgwrh.repair_managed_objects()')
    assert target.execute(MARKERS) == target.execute('SELECT * FROM pgwrh.owned_obj ORDER BY 1, 2')


def test_registry_writes_require_administrator(postgres_node_factory):
    node = postgres_node_factory('registry_privileges')
    node.execute('CREATE ROLE app LOGIN')
    for command in ('DELETE FROM pgwrh.managed_object',
                    'SELECT pgwrh.repair_managed_objects()',
                    "SELECT pgwrh.add_ext_dependency('pg_class', 'pgwrh.managed_object'::regclass)"):
        with pytest.raises(Exception, match='permission denied'):
            node.execute(command, username='app')


def test_repair_does_not_race_an_object_drop(postgres_node_factory):
    node = postgres_node_factory('registry_drop_race')
    seed(node)
    with node.connect() as dropping, node.connect() as repairing:
        # Hold the object lock that DROP obtains before its registry trigger.
        dropping.execute('LOCK TABLE "managed schema"."table one" IN ACCESS EXCLUSIVE MODE')
        repairing.execute(ERASE_MARKERS)
        with pytest.raises(Exception, match='being changed concurrently'):
            repairing.execute('SELECT pgwrh.repair_managed_objects()')
        repairing.rollback()
        dropping.execute('DROP TABLE "managed schema"."table one"')
        dropping.commit()
    node.execute('SELECT pgwrh.repair_managed_objects()')
    assert node.execute(MARKERS) == node.execute('SELECT * FROM pgwrh.owned_obj ORDER BY 1, 2')


def test_repair_locks_only_objects_missing_markers(postgres_node_factory):
    node = postgres_node_factory('registry_lock_scope')
    seed(node)
    with node.connect() as ddl, node.connect() as repairing:
        # DDL on an unrelated table must not block restoring drop protection.
        ddl.execute('LOCK TABLE public.unrelated IN ACCESS EXCLUSIVE MODE')
        repairing.execute(ERASE_MARKERS)
        repairing.execute('SELECT pgwrh.repair_managed_objects()')
        assert repairing.execute(MARKERS) == repairing.execute('SELECT * FROM pgwrh.owned_obj ORDER BY 1, 2')
        assert repairing.execute("""SELECT count(*) FROM pg_locks WHERE pid = pg_backend_pid()
            AND locktype = 'relation' AND relation = 'public.unrelated'::regclass""") == [(0,)]
        repairing.commit()
        # With every marker intact, repair locks nothing, not even a managed table.
        ddl.execute('LOCK TABLE "managed schema"."table one" IN ACCESS EXCLUSIVE MODE')
        repairing.execute('SELECT pgwrh.repair_managed_objects()')
        repairing.commit()
        ddl.rollback()
