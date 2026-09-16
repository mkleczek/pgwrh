from pathlib import Path


def test_target_identity_and_canonical_members(postgres_node_factory):
    node = postgres_node_factory('target_identity')
    assert node.execute("""
        SELECT pgwrh.pgwrh_target_servers(ARRAY['b','a','a'], 'hb,ha,ha', '2,1,1', 'db', 'reader')
             = pgwrh.pgwrh_target_servers(ARRAY['a','b'], 'ha,hb', '1,2', 'db', 'reader'),
               pgwrh.pgwrh_target_server('a','ha','1','db','reader')
            <> pgwrh.pgwrh_target_server('a','ha','2','db','reader'),
               pgwrh.pgwrh_target_server('a','ha','1','db','reader')
            <> pgwrh.pgwrh_target_server('a','ha','1','db','rotated')
    """) == [(True, True, True)]
    for roles, hosts, ports in (("ARRAY['a','b']", 'ha', '1,2'),
                                 ("ARRAY['a']", 'ha,hb', '1'),
                                 ("ARRAY['a',NULL]", 'ha,hb', '1,2'),
                                 ("ARRAY[['a']]", 'ha', '1'),
                                 ("ARRAY[]::text[]", '', ''),
                                 ("ARRAY['a']", '', '1')):
        assert node.execute(f"SELECT pgwrh.pgwrh_target_servers({roles}, '{hosts}', '{ports}', 'db', 'reader') IS NULL") == [(True,)]


def test_readiness_requires_nonempty_allowed_actual_targets(postgres_node_factory):
    node = postgres_node_factory('target_readiness')
    definitions = (Path(__file__).resolve().parents[2] / 'pgwrh/src/master/implementation-views.sql').read_text()
    definitions = definitions[definitions.index('CREATE VIEW missing_connected_remote_shard AS'):]
    with node.connect() as conn:
        conn.execute("""
            SET search_path = pg_temp, pgwrh, public;
            CREATE TEMP TABLE shard (replication_group_id text, version text, schema_name text, table_name text);
            INSERT INTO shard VALUES ('g', 'FLIP', 'data', 'leaf');
            CREATE TEMP TABLE shard_assigned_host (replication_group_id text, version text,
                availability_zone text, host_id text, schema_name text, table_name text);
            CREATE TEMP TABLE replication_group_member (replication_group_id text,
                availability_zone text, host_id text, connected_remote_shards json,
                prepared_remote_shards json, connected_local_shards json);
            INSERT INTO replication_group_member VALUES ('g', 'az', 'reader', '[]', '[]', '[]');
            CREATE TEMP TABLE shard_destinations (replication_group_id text, version text,
                schema_name text, table_name text, username text, legacy_server_name text, target_servers text[]);
            INSERT INTO shard_destinations VALUES ('g', 'FLIP', 'data', 'leaf', 'u', 'legacy', ARRAY['a','b']);
        """)
        conn.execute(definitions)
        for targets, server, user, ready in (
            ("NULL::text[]", 'legacy', 'u', True),
            ("NULL::text[]", 'virtual', 'u', False),
            ("ARRAY['b','a']", 'virtual', 'u', True),
            ("ARRAY['b']", 'virtual', 'u', True),
            ("ARRAY['a','retired']", 'virtual', 'u', False),
            ("ARRAY[]::text[]", 'legacy', 'u', False),
            ("ARRAY[NULL]::text[]", 'legacy', 'u', False),
            ("ARRAY['a']", 'virtual', 'old_user', False),
        ):
            for prepared in (False, True):
                field = 'prepared_remote_shards' if prepared else 'connected_remote_shards'
                conn.execute(f"""UPDATE replication_group_member SET
                    connected_remote_shards = '[]', prepared_remote_shards = '[]',
                    connected_local_shards = '[{{"schema_name":"data","table_name":"leaf"}}]'""")
                conn.execute(f"""UPDATE replication_group_member SET {field} = json_build_array(json_build_object(
                    'schema_name', 'data', 'table_name', 'leaf', 'shard_server_name', '{server}',
                    'shard_server_user', '{user}', 'shard_server_targets', {targets}))""")
                assert conn.execute('SELECT count(*) FROM missing_ready_remote_shard') == [(0 if ready else 1,)]
                if prepared:
                    conn.execute("UPDATE replication_group_member SET connected_local_shards = '[]'")
                    assert conn.execute('SELECT count(*) FROM missing_ready_remote_shard') == [(1,)]
        conn.execute('DELETE FROM shard_destinations')
        assert conn.execute('SELECT count(*) FROM missing_ready_remote_shard') == [(1,)]
