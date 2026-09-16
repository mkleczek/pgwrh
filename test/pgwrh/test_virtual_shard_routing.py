from pathlib import Path

import pytest

from .pgwrh_testkit import wait_until
from .test_local_first_handoff import handoff_cluster, move_to_destination, wait_prepared


def test_remote_reroute_keeps_objects_and_waits_before_readiness(handoff_cluster):
    cluster = handoff_cluster
    _, _, reader = cluster.replicas
    wait_until(lambda: reader.query_scalar("""SELECT EXISTS (
        SELECT FROM pgwrh.remote_node n JOIN pgwrh.reachable_shard r USING (reg_class)
        WHERE n.node_rel_id = ('data','root')::pgwrh.rel_id)"""), timeout=30,
               message='reader did not aggregate its initial route')
    cluster.master.execute("UPDATE pgwrh.replication_group_member SET same_zone_multiplier = 3 WHERE host_id = 'reader'")
    wait_until(lambda: reader.query_scalar("""SELECT bool_and(w.value = '3')
        FROM pgwrh.owned_server s, LATERAL pgwrh.opts(s.srvoptions) w
        WHERE w.key = 'load_balance_weight'"""), timeout=30,
               message='same-zone weighting did not reach the actual target servers')
    # Object identity is independent of attachment: rollout can temporarily
    # expand the aggregate into leaf routes while keeping its table and server.
    root_oid = reader.query_scalar("SELECT 'data_remote.root'::regclass::oid")
    route_sql = f"""SELECT ft.ftrelid, s.oid, s.srvname, s.srvoptions
        FROM pg_foreign_table ft JOIN pg_foreign_server s ON s.oid = ft.ftserver
        WHERE ft.ftrelid = {root_oid}"""
    before, = reader.execute(route_sql)
    coverage_sql = """SELECT (rel_id).schema_name, (rel_id).table_name
        FROM pgwrh.connected_remote_shard WHERE (rel_id).schema_name = 'data'
        ORDER BY rel_id"""
    expected_shards = [('data', 'p0'), ('data', 'p1')]
    reader.execute('CREATE ROLE application_reader; '
                   'GRANT USAGE ON SCHEMA data, data_remote TO application_reader; '
                   'GRANT SELECT ON data.root, data_remote.root TO application_reader')
    # These temporary PostgreSQL nodes use trust authentication. Allow that
    # explicitly for this test's non-superuser rather than weakening production mappings.
    for server, in reader.execute("""SELECT srvname FROM pgwrh.owned_server s
        WHERE EXISTS (SELECT 1 FROM pgwrh.opts(s.srvoptions) WHERE key = 'load_balance_weight')"""):
        reader.execute(f'ALTER USER MAPPING FOR PUBLIC SERVER "{server}" OPTIONS (ADD password_required \'false\')')
    with reader.node.connect() as old_reader:
        old_reader.execute('SET ROLE application_reader')
        old_reader.execute('SAVEPOINT first_read')
        assert old_reader.execute('SELECT count(*) FROM data.root') == [(32,)]
        old_reader.execute('ROLLBACK TO first_read')
        move_to_destination(cluster)
        wait_prepared(cluster)
        wait_until(lambda: reader.query_scalar(f"""SELECT EXISTS (SELECT 1 FROM pg_locks
            WHERE classid = 'pg_foreign_server'::regclass AND objid = {before[1]}
                AND mode = 'AccessExclusiveLock' AND NOT granted)"""), timeout=60,
                   message='membership update did not wait for the old routing transaction')
        assert reader.execute(route_sql) == [before]
        assert reader.execute(coverage_sql) == expected_shards
        with pytest.raises(Exception, match='required remote shards'):
            cluster.master.commit_rollout()
        # Read through the route pinned before the savepoint rollback, even if
        # data.root now uses leaf servers with different transaction bindings.
        assert old_reader.execute('SELECT count(*) FROM data_remote.root') == [(32,)]
        old_reader.commit()
    cluster.master.wait_for_rollout_ready(expected_replicas=3, timeout=60)
    wait_until(lambda: reader.execute(route_sql) != [before], timeout=30,
               message='aggregate server membership did not change after reader commit')
    after, = reader.execute(route_sql)
    assert before[:3] == after[:3]
    assert before[3] != after[3]
    assert reader.execute(coverage_sql) == expected_shards
    assert reader.query_scalar("""SELECT count(*) FROM pgwrh.owned_server s
        JOIN pg_foreign_data_wrapper f ON f.oid = s.srvfdw
        WHERE f.fdwname <> 'pgwrh_fdw'""") == 0
    cluster.master.commit_rollout()
    cluster.assert_query_results_match('SELECT * FROM data.root ORDER BY id')


def test_assignment_repetitions_become_target_weights(postgres_node_factory):
    node = postgres_node_factory('target_weights')
    helpers = (Path(__file__).resolve().parents[2] / 'pgwrh/src/replica/helpers.sql').read_text()
    definition = helpers.split('CREATE VIEW assignment_target AS', 1)[1].split('CREATE VIEW subscribed_local_shard AS', 1)[0]
    with node.connect() as conn:
        conn.execute("""
            SET search_path = pg_temp, pgwrh, public;
            CREATE TEMP TABLE fdw_shard_assignment (LIKE pgwrh.fdw_shard_assignment);
            INSERT INTO fdw_shard_assignment
                (schema_name, table_name, shard_server_members, host, port, dbname, shard_server_user)
            VALUES ('data', 'a', ARRAY['near','far','near','near'], 'hn,hf,hn,hn', '1,2,1,1', 'db', 'u'),
                   ('data', 'b', ARRAY['far','near','near','near'], 'hf,hn,hn,hn', '2,1,1,1', 'db', 'u');
        """)
        conn.execute('CREATE TEMP VIEW assignment_target AS' + definition)
        assert conn.execute('SELECT host, weight FROM assignment_target ORDER BY host, node_rel_id') == [
            ('hf', 1), ('hf', 1), ('hn', 3), ('hn', 3)]
        assert conn.execute('SELECT count(DISTINCT server_name) FROM assignment_target') == [(2,)]
        conn.execute("UPDATE fdw_shard_assignment SET host = 'hn' WHERE table_name = 'a'")
        assert conn.execute('SELECT count(*) FROM assignment_target') == [(2,)]


def test_route_reports_each_actual_member(postgres_node_factory):
    node = postgres_node_factory('route_members')
    status = (Path(__file__).resolve().parents[2] / 'pgwrh/src/replica/status.sql').read_text()
    definition = status.split('CREATE VIEW remote_server_route AS', 1)[1].split('-- Report leaf coverage', 1)[0]
    with node.connect() as conn:
        conn.execute("""
            SET search_path = pg_temp, pgwrh, public;
            CREATE SERVER actual_a FOREIGN DATA WRAPPER pgwrh_fdw;
            CREATE SERVER actual_b FOREIGN DATA WRAPPER pgwrh_fdw;
            CREATE USER MAPPING FOR PUBLIC SERVER actual_a OPTIONS (user 'reader');
            CREATE USER MAPPING FOR PUBLIC SERVER actual_b OPTIONS (user 'reader');
            CREATE SERVER virtual FOREIGN DATA WRAPPER pgwrh_fdw OPTIONS (members 'actual_b,actual_a');
            CREATE USER MAPPING FOR PUBLIC SERVER virtual;
            CREATE TEMP VIEW owned_server AS SELECT * FROM pg_foreign_server;
        """)
        conn.execute('CREATE TEMP VIEW remote_server_route AS' + definition)
        assert conn.execute("SELECT shard_server_user, shard_server_targets FROM remote_server_route WHERE srvname = 'virtual'") == [
            ('reader', ['actual_a', 'actual_b'])]
        conn.execute("ALTER USER MAPPING FOR PUBLIC SERVER actual_b OPTIONS (SET user 'different')")
        assert conn.execute("SELECT count(*) FROM remote_server_route WHERE srvname = 'virtual'") == [(0,)]
        conn.execute('DROP SERVER actual_b CASCADE')
        assert conn.execute("SELECT count(*) FROM remote_server_route WHERE srvname = 'virtual'") == [(0,)]


def test_retained_remote_table_is_configured_before_reuse(handoff_cluster):
    _, _, reader = handoff_cluster.replicas
    wait_until(lambda: reader.query_scalar("""SELECT EXISTS (
        SELECT 1 FROM pgwrh.remote_node n JOIN pgwrh.reachable_shard r USING (reg_class)
        WHERE n.node_rel_id = ('data','root')::pgwrh.rel_id)"""),
        timeout=30, message='reader did not aggregate its initial route')
    with reader.node.connect() as paused:
        paused.execute('SELECT pg_advisory_lock(2895359559)')
        server, targets, bound = reader.execute("""SELECT a.shard_server_name, a.target_servers, a.remote_bound
            FROM pgwrh.remote_node_assignment a WHERE a.schema_name = 'data' AND a.table_name = 'root'""")[0]
        reader.execute("""ALTER TABLE data.root DETACH PARTITION data_remote.root;
            CREATE SERVER retired FOREIGN DATA WRAPPER pgwrh_fdw OPTIONS (port '1');
            CREATE USER MAPPING FOR PUBLIC SERVER retired OPTIONS (user 'retired')""")
        # Simulate a detached table retained from a previous configuration.
        reader.execute(f"ALTER SERVER {server} OPTIONS (SET members 'retired')")
        commands = lambda: [cmd for row in reader.execute('SELECT commands FROM pgwrh.sync') for cmd in row[0]]
        assert not any('ATTACH PARTITION data_remote.root' in cmd for cmd in commands())
        reader.execute("""DELETE FROM pg_statistic WHERE starelid = 'data_remote.root'::regclass;
            DELETE FROM pgwrh.analyzed_remote_pg_class WHERE oid = 'data_remote.root'::regclass""")
        assert not any('ANALYZE data_remote.root' in cmd for cmd in commands())
        reader.execute(f"SELECT pgwrh_fdw_set_members('{server}', ARRAY[{','.join(repr(t) for t in targets)}])")
        assert any('ANALYZE data_remote.root' in cmd for cmd in commands())
        reader.execute('ANALYZE data_remote.root')
        assert any('ATTACH PARTITION data_remote.root' in cmd for cmd in commands())
        reader.execute(f'ALTER TABLE data.root ATTACH PARTITION data_remote.root {bound}')
        assert reader.query_scalar('SELECT count(*) FROM data.root') == 32
