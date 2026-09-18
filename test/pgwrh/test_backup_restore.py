"""Exercise the logical controller restore procedure documented in recovery.md."""
from pathlib import Path
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[2]


def run(node, tool, *args):
    result = subprocess.run(
        [str(Path(node.bin_dir) / tool), "-h", "127.0.0.1", "-p", str(node.port),
         *args], text=True, capture_output=True,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout


def snapshot(node):
    # Include every extension configuration table, not just the policy API.
    tables = node.execute("""SELECT c::regclass::text FROM pg_extension,
        unnest(extconfig) c WHERE extname = 'pgwrh' ORDER BY 1""")
    return {table: node.execute(f"SELECT to_jsonb(t)::text FROM {table} t ORDER BY 1")
            for (table,) in tables}


@pytest.mark.parametrize("phase", ["committed", "pending", "in_flight"])
def test_controller_dump_restore(postgres_node_factory, tmp_path, phase):
    source = postgres_node_factory("backup_source")
    source.execute("""
        CREATE EXTENSION pgwrh_ui;
        CREATE ROLE backup_reader NOLOGIN;
        CREATE ROLE backup_replica LOGIN REPLICATION IN ROLE backup_reader;
        CREATE SCHEMA data;
        CREATE TABLE data.events (id integer PRIMARY KEY, payload text) PARTITION BY RANGE (id);
        GRANT USAGE ON SCHEMA data TO backup_reader;
        GRANT SELECT ON ALL TABLES IN SCHEMA data TO backup_reader;
        SELECT pgwrh.create_replica_cluster('backup');
        INSERT INTO pgwrh.sharded_table
            (replication_group_id, sharded_table_schema, sharded_table_name, replication_factor,
             min_replica_count_after_az_failure)
        VALUES ('backup', 'data', 'events', 100, 1);
        UPDATE pgwrh.replication_group_config SET min_replica_count = 2,
            min_replica_count_after_az_failure = 1 WHERE version = 'FLOP';
        INSERT INTO pgwrh.sharded_table_az_affinity
            (replication_group_id, sharded_table_schema, sharded_table_name, availability_zone, weight)
        VALUES ('backup', 'data', 'events', 'a', 4);
        SELECT pgwrh.start_rollout('backup');
        SELECT pgwrh.commit_rollout('backup');
    """)
    source.execute((ROOT / "pgwrh_ui/readonly.sql").read_text())
    source.execute((ROOT / "pgwrh_ui/operator.sql").read_text())
    if phase != "committed":
        source.execute("""INSERT INTO pgwrh.sharded_table_az_affinity
            (replication_group_id, sharded_table_schema, sharded_table_name, availability_zone, weight)
            VALUES ('backup', 'data', 'events', 'a', 8)""")
    # A real leaf and member create a nonempty placement snapshot for an interrupted rollout.
    source.execute("""
        CREATE TABLE data.events_1 PARTITION OF data.events FOR VALUES FROM (0) TO (100);
        INSERT INTO data.events VALUES (1, 'restored controller data');
    """)
    if phase == "in_flight":
        source.execute("""SELECT pgwrh.add_replica('backup', 'replica', 'replica.invalid',
            5432, 'backup_replica', 'a', _dbname := 'replica data');
            UPDATE pgwrh.replication_group_config SET min_replica_count = 1,
                min_replica_count_per_availability_zone = 0,
                min_replica_count_after_az_failure = 0 WHERE version = 'FLIP';
            UPDATE pgwrh.sharded_table SET min_replica_count_after_az_failure = 0
                WHERE version = 'FLIP';""")
        source.execute("SELECT pgwrh.start_rollout('backup')")

    before = snapshot(source)
    if phase == "in_flight":
        assert before["pgwrh.shard_assigned_host"]
    locks = source.execute("SELECT * FROM pgwrh.replication_group_lock ORDER BY 1")
    archive = tmp_path / "controller.dump"
    run(source, "pg_dump", "-d", "postgres", "-Fc", "-f", str(archive))
    roles = run(source, "pg_dumpall", "--roles-only", "--no-role-passwords")
    # initdb already created this role on the destination. Preserve its ALTERs.
    bootstrap = source.execute("SELECT quote_ident(current_user)")[0][0]
    roles = roles.replace(f"CREATE ROLE {bootstrap};", "")
    roles_file = tmp_path / "roles.sql"
    roles_file.write_text(roles)

    target = postgres_node_factory("backup_target", install_extension=False)
    run(target, "psql", "-X", "-v", "ON_ERROR_STOP=1", "-d", "postgres", "-f", str(roles_file))
    run(target, "pg_restore", "-d", "postgres", "--exit-on-error", "--single-transaction",
        "--section=pre-data", "--no-publications", "--no-subscriptions", str(archive))
    run(target, "pg_restore", "-d", "postgres", "--exit-on-error", "--single-transaction",
        "--data-only", "--disable-triggers", str(archive))
    run(target, "pg_restore", "-d", "postgres", "--exit-on-error", "--single-transaction",
        "--section=post-data", "--no-publications", "--no-subscriptions", str(archive))
    target.execute("SELECT pgwrh.sync_publications()")
    assert snapshot(target) == before
    assert target.execute("SELECT * FROM pgwrh.replication_group_lock ORDER BY 1") == locks
    assert target.execute("SELECT * FROM data.events") == [(1, "restored controller data")]
    assert target.execute("""SELECT pubname, schemaname, tablename FROM pg_publication_tables ORDER BY 1, 2, 3""") == source.execute(
        "SELECT pubname, schemaname, tablename FROM pg_publication_tables ORDER BY 1, 2, 3")
    assert target.execute("""SELECT rolreplication, pg_has_role('backup_replica', 'backup_reader', 'member')
        FROM pg_roles WHERE rolname = 'backup_replica'""") == [(True, True)]
    assert target.execute("""SELECT has_table_privilege('backup_reader', 'data.events', 'SELECT'),
        has_function_privilege('pgwrh_ui_viewer', 'pgwrh_ui.index(text,text,text,int)', 'EXECUTE'),
        has_function_privilege('pgwrh_ui_operator',
            'pgwrh_ui.mutate(text,text,text,text,text,text,int,text,int,boolean,boolean,text)', 'EXECUTE'),
        has_table_privilege('pgwrh_ui_viewer', 'pgwrh.replication_group', 'SELECT')""") == [(True, True, True, False)]
    with pytest.raises(Exception, match="locked"):
        target.execute("""UPDATE pgwrh.sharded_table_az_affinity SET weight = 99
            WHERE version = (SELECT current_version FROM pgwrh.replication_group WHERE replication_group_id = 'backup')""")
    with pytest.raises(Exception, match="foreign key"):
        target.execute("DELETE FROM pgwrh.replication_group WHERE replication_group_id = 'backup'")
    if phase == "in_flight":
        # A logical backup is not a substitute for fresh replica readiness.
        with pytest.raises(Exception):
            target.execute("SELECT pgwrh.commit_rollout('backup')")
    target.execute((ROOT / "docs/recovery-quarantine.sql").read_text())
    assert target.execute("SELECT count(*) FROM pgwrh.shard_host WHERE online") == [(0,)]
