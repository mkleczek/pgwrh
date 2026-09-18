from __future__ import annotations

from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest


@pytest.fixture
def selection(postgres_node_factory):
    """Run the production views against a small logical tree, without a controller."""
    node = postgres_node_factory("selection")
    source = Path(__file__).resolve().parents[2] / "pgwrh" / "src" / "replica"
    helpers = (source / "helpers.sql").read_text()
    structure_view = helpers.split("CREATE OR REPLACE VIEW shard_structure_r AS", 1)[1]
    structure_view = structure_view.split("CREATE OR REPLACE VIEW shard_assignment_r AS", 1)[0]
    with node.connect() as conn:
        conn.execute("""
            SET search_path = pg_temp, pgwrh, public;
            CREATE TEMP TABLE fdw_shard_structure (LIKE pgwrh.fdw_shard_structure);
            CREATE TEMP TABLE fdw_shard_assignment (LIKE pgwrh.fdw_shard_assignment);
            CREATE TEMP TABLE fdw_serving_subtree (LIKE pgwrh.fdw_serving_subtree);
            INSERT INTO fdw_shard_structure
                (schema_name, table_name, level, parent_schema_name, parent_table_name,
                 is_leaf, root_schema_name, root_table_name, node_partkeydef)
            VALUES
                ('data', 'root', 0, NULL, NULL, false, 'data', 'root', 'RANGE (id)'),
                ('data', 'left', 1, 'data', 'root', false, 'data', 'root', 'LIST (id)'),
                ('data', 'right', 1, 'data', 'root', false, 'data', 'root', 'LIST (id)'),
                ('leaves', 'a', 2, 'data', 'left', true, 'data', 'root', NULL),
                ('leaves', 'b', 2, 'data', 'left', true, 'data', 'root', NULL),
                ('leaves', 'c', 2, 'data', 'right', true, 'data', 'root', NULL),
                ('leaves', 'd', 2, 'data', 'right', true, 'data', 'root', NULL);
            INSERT INTO fdw_shard_assignment
                (schema_name, table_name, local, connect_remote, shard_server_name,
                 host, port, dbnames, shard_server_user)
            SELECT schema_name, table_name, false, true, 'server1',
                   'host1,host2', '5432,5432', ARRAY['db1','db2'], 'reader'
            FROM fdw_shard_structure WHERE is_leaf;
            UPDATE fdw_shard_assignment SET shard_server_members = ARRAY['host1', 'host2'];
            INSERT INTO fdw_serving_subtree
            SELECT host, schema_name, table_name FROM fdw_shard_structure,
                unnest(ARRAY['host1', 'host2', 'host3']) host WHERE NOT is_leaf;
        """)
        conn.execute("CREATE TEMP VIEW shard_structure_r AS" + structure_view)
        conn.execute((source / "aggregation.sql").read_text())
        yield conn


@pytest.mark.parametrize(("change", "expected"), [
    ("", [("data", "root", 4)]),
    ("UPDATE fdw_shard_assignment SET shard_server_name = table_name",
     [("data", "root", 4)]),
    ("UPDATE fdw_shard_assignment SET host = 'host2,host1', port = '5432,5432', "
     "shard_server_members = ARRAY['host2','host1'], dbnames = ARRAY['db2','db1'] WHERE table_name = 'a'",
     [("data", "root", 4)]),
    ("UPDATE fdw_shard_assignment SET host = 'host1,host2,host1', port = '5432,5432,5432', "
     "shard_server_members = ARRAY['host1','host2','host1'], dbnames = ARRAY['db1','db2','db1'] WHERE table_name = 'a'",
     [("data", "root", 4)]),
    ("UPDATE fdw_shard_assignment SET port = '5432,5433' WHERE table_name = 'a'",
     [("data", "right", 2), ("leaves", "a", 1), ("leaves", "b", 1)]),
    ("UPDATE fdw_shard_assignment SET dbnames = ARRAY['different','db2'] WHERE table_name = 'a'",
     [("data", "right", 2), ("leaves", "a", 1), ("leaves", "b", 1)]),
    ("UPDATE fdw_shard_assignment SET local = true, connect_remote = false WHERE table_name = 'a'",
     [("data", "right", 2), ("leaves", "b", 1)]),
    ("UPDATE fdw_shard_assignment SET shard_server_name = 'server2', host = 'host1,host3' WHERE table_name = 'd'",
     [("data", "left", 2), ("leaves", "c", 1), ("leaves", "d", 1)]),
    ("DELETE FROM fdw_shard_assignment WHERE table_name = 'a'",
     [("data", "right", 2), ("leaves", "b", 1)]),
    ("UPDATE fdw_shard_assignment SET connect_remote = NULL, local = true WHERE table_name = 'a'",
     [("data", "right", 2), ("leaves", "b", 1)]),
    ("UPDATE fdw_shard_assignment SET local = true", [("leaves", name, 1) for name in ("a", "b", "c", "d")]),
    ("UPDATE fdw_shard_assignment SET connect_remote = false", [("data", "root", 4)]),
    ("UPDATE fdw_shard_assignment SET host = '' WHERE table_name = 'a'",
     [("data", "right", 2), ("leaves", "b", 1)]),
    ("UPDATE fdw_shard_assignment SET shard_server_user = 'other' WHERE table_name = 'a'",
     [("data", "right", 2), ("leaves", "a", 1), ("leaves", "b", 1)]),
    ("DELETE FROM fdw_shard_structure WHERE table_name IN ('c', 'd')", [("data", "left", 2)]),
    ("DELETE FROM fdw_serving_subtree WHERE member_role = 'host2' AND table_name = 'root'",
     [("data", "left", 2), ("data", "right", 2)]),
    ("DELETE FROM fdw_serving_subtree WHERE member_role = 'host2'",
     [("leaves", name, 1) for name in ('a', 'b', 'c', 'd')]),
    ("UPDATE fdw_shard_assignment SET shard_server_members = NULL WHERE table_name = 'a'",
     [("data", "right", 2), ("leaves", "b", 1)]),
])
def test_selects_maximal_complete_remote_subtrees(selection, change, expected):
    if change:
        selection.execute(change)
    assert selection.execute("""
        SELECT schema_name, table_name, leaf_count
        FROM remote_node_assignment ORDER BY 1, 2
    """) == expected


def test_selection_is_independent_of_physical_attachments_and_metadata_duplicates(selection):
    selection.execute("INSERT INTO fdw_shard_structure SELECT * FROM fdw_shard_structure")
    assert selection.execute("SELECT table_name, leaf_count FROM remote_node_assignment") == [("root", 4)]


def test_root_bounds_and_separate_roots(selection):
    selection.execute("""
        INSERT INTO fdw_shard_structure
            (schema_name, table_name, level, is_leaf, node_partkeydef, root_schema_name, root_table_name)
        VALUES ('second', 'root', 0, false, 'HASH (id)', 'second', 'root');
        INSERT INTO fdw_shard_structure
            (schema_name, table_name, level, parent_schema_name, parent_table_name,
             is_leaf, root_schema_name, root_table_name)
        VALUES ('second', 'leaf', 1, 'second', 'root', true, 'second', 'root');
        INSERT INTO fdw_shard_assignment
            (schema_name, table_name, local, connect_remote, shard_server_name,
             host, port, dbnames, shard_server_user, shard_server_members)
        SELECT 'second', 'leaf', local, connect_remote, shard_server_name,
               host, port, dbnames, shard_server_user, shard_server_members
        FROM fdw_shard_assignment LIMIT 1;
        INSERT INTO fdw_serving_subtree SELECT host, 'second', 'root'
        FROM unnest(ARRAY['host1', 'host2']) host;
    """)
    assert selection.execute("""
        SELECT schema_name, table_name, remote_bound FROM remote_node_assignment ORDER BY 1
    """) == [("data", "root", "DEFAULT"),
             ("second", "root", "FOR VALUES WITH (modulus 1, remainder 0)")]

from .pgwrh_testkit import MasterHandle, PgwrhCluster, ReplicaSpec, quote_literal, wait_until


ROOTS = ('range_root', 'list_root', 'hash_root', 'empty_root')


@pytest.fixture
def aggregated_cluster(postgres_node_factory):
    master = MasterHandle(postgres_node_factory('master'))
    master.execute('CREATE ROLE test_replica; CREATE SCHEMA data; CREATE SCHEMA leaves AUTHORIZATION test_replica')
    for strategy in ('range', 'list', 'hash'):
        root = f'{strategy}_root'
        master.execute(f'CREATE TABLE data.{root} (id int, k int, payload text) PARTITION BY {strategy} (k)')
        for side, label in enumerate(('left', 'right')):
            bound = {
                'range': f'FOR VALUES FROM ({side * 8}) TO ({(side + 1) * 8})',
                'list': 'FOR VALUES IN (' + ','.join(str(i) for i in range(side * 8, (side + 1) * 8)) + ')',
                'hash': f'FOR VALUES WITH (MODULUS 2, REMAINDER {side})',
            }[strategy]
            master.execute(f'CREATE TABLE data.{root}_{label} PARTITION OF data.{root} {bound} PARTITION BY HASH (id)')
            for leaf in range(2):
                name = f'leaves.{root}_{label}_{leaf}'
                master.execute(f'''CREATE TABLE {name} PARTITION OF data.{root}_{label}
                    (PRIMARY KEY (id)) FOR VALUES WITH (MODULUS 2, REMAINDER {leaf});
                    ALTER TABLE {name} OWNER TO test_replica''')
        master.execute(f"INSERT INTO data.{root} SELECT n, n, 'row ' || n FROM generate_series(0, 15) n")
    master.execute('''
        CREATE TABLE data.empty_root (id int, k int, payload text) PARTITION BY RANGE (k);
        CREATE TABLE leaves.empty_leaf PARTITION OF data.empty_root DEFAULT;
        ALTER TABLE leaves.empty_leaf OWNER TO test_replica;
        SELECT pgwrh.create_replica_cluster('g1');
    ''')
    for root in ROOTS:
        master.execute(f"""INSERT INTO pgwrh.sharded_table
            (replication_group_id, sharded_table_schema, sharded_table_name, replication_factor,
             sharding_key_expression)
            VALUES ('g1', 'data', '{root}', 0, 'SELECT ''shared''')""")
    master.execute("""INSERT INTO pgwrh.shard_index_template
        (replication_group_id, index_template_schema, index_template_table_name, index_template_name, index_template)
        VALUES ('g1', 'data', 'range_root', 'payload_idx', '(payload)')""")
    cluster = PgwrhCluster(master, postgres_node_factory)
    cluster.add_replicas([ReplicaSpec('replica1'), ReplicaSpec('replica2'), ReplicaSpec('reader')])
    master.execute("""DELETE FROM pgwrh.shard_host_weight WHERE host_id = 'reader'
        AND version <> (SELECT current_version FROM pgwrh.replication_group WHERE replication_group_id = 'g1')""")
    cluster.deploy(timeout=60)
    wait_until(lambda: remote_nodes(cluster.replicas[-1]) == [('data', root) for root in sorted(ROOTS)],
               timeout=60, message='initial roots did not aggregate')
    return cluster


def assert_all_rows(cluster):
    for root in ROOTS:
        cluster.assert_query_results_match(f'SELECT * FROM data.{root} ORDER BY id')


def remote_nodes(replica):
    return replica.execute('''SELECT (n.node_rel_id).schema_name, (n.node_rel_id).table_name
        FROM pgwrh.remote_node n JOIN pgwrh.reachable_shard r USING (reg_class)
        ORDER BY 1, 2''')


def test_root_aggregation_preserves_identity_results_and_leaf_coverage(aggregated_cluster):
    cluster = aggregated_cluster
    reader = cluster.replicas[-1]
    expected = [('data', name) for name in sorted(ROOTS)]
    wait_until(lambda: remote_nodes(reader) == expected, timeout=30, message='roots did not aggregate')
    wait_until(lambda: reader.query_scalar("""SELECT count(*) FROM pgwrh.sync
        WHERE description LIKE 'Switching query routes%'""") == 0,
        timeout=30, message='unchanged aggregate bounds caused repeated reattachment')
    assert_all_rows(cluster)
    assert reader.query_scalar('SELECT count(*) FROM pgwrh.connected_remote_shard') == 13
    with reader.node.connect() as conn:
        # Different logical nodes have different virtual servers, but share the
        # actual targets needed for a remote join and one physical connection.
        assert conn.execute("""SELECT count(DISTINCT ftserver) FROM pg_foreign_table
            WHERE ftrelid IN ('data_remote.range_root'::regclass, 'data_remote.list_root'::regclass)""") == [(2,)]
        joined = 'SELECT x.id FROM data_remote.range_root x JOIN data_remote.list_root y USING (id) ORDER BY x.id'
        remote_plan = conn.execute('EXPLAIN (VERBOSE, FORMAT JSON) ' + joined)[0][0][0]['Plan']
        assert remote_plan['Node Type'] == 'Foreign Scan'
        assert 'JOIN' in remote_plan['Remote SQL']
        assert conn.execute(joined) == [(n,) for n in range(16)]
        assert conn.execute('SELECT count(*) FROM pgwrh_fdw_get_connections()') == [(1,)]
    # Detached stable foreign tables are retained for later topology changes.
    assert reader.query_scalar('SELECT count(*) FROM pgwrh.remote_shard r JOIN pgwrh.reachable_shard s USING (reg_class)') == 4
    for root in ROOTS:
        tree = reader.execute(f"SELECT level, isleaf FROM pg_partition_tree('data.{root}') ORDER BY level")
        assert tree == [(0, False), (1, True)]
        assert reader.query_scalar(f"SELECT relkind FROM pg_class WHERE oid = 'data.{root}'::regclass") == 'p'
    plan = reader.execute('EXPLAIN (VERBOSE, FORMAT JSON) SELECT * FROM data.range_root WHERE id = 7')[0][0][0]['Plan']
    assert plan['Node Type'] == 'Foreign Scan'
    assert 'WHERE' in plan['Remote SQL'] and '7' in plan['Remote SQL']
    assert reader.query_scalar('''SELECT count(*) FROM pgwrh.remote_shard r
        JOIN pgwrh.reachable_shard s USING (reg_class)
        JOIN pgwrh.analyzed_remote_pg_class a ON a.oid = r.reg_class''') == 4
    owner = next(replica for replica in cluster.replicas if replica.query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pgwrh.ready_serving_subtree WHERE rel_id = ('data', 'range_root')::pgwrh.rel_id)"))
    with owner.node.connect() as conn:
        conn.execute('SET plan_cache_mode = force_generic_plan')
        conn.execute('PREPARE shield_query (int, int) AS SELECT * FROM data_shield.range_root WHERE k = $1 AND id = $2')
        plan = conn.execute('EXPLAIN (ANALYZE, FORMAT JSON) EXECUTE shield_query(7, 7)')[0][0][0]['Plan']
    def plans(node):
        yield node
        for child in node.get('Plans', []):
            yield from plans(child)
    assert sum(1 for p in plans(plan) if p.get('Relation Name', '').startswith('range_root_')
               and p.get('Actual Loops', 0)) == 1
    assert sum(p.get('Subplans Removed', 0) for p in plans(plan)) > 0
    assert 'UNION' not in owner.query_scalar("SELECT pg_get_viewdef('data_shield.range_root'::regclass)")
    index_name = owner.query_scalar("SELECT format('%I.%I', schema_name, index_name) FROM pgwrh.local_shard_index LIMIT 1")
    with owner.node.connect() as conn:
        conn.execute('SELECT pg_advisory_lock(2895359559)')
        conn.execute(f'DROP INDEX {index_name}')
        # Current indexes are marked optional for maintaining existing routes,
        # but they are required before advertising a new aggregate endpoint.
        assert conn.execute("SELECT EXISTS (SELECT 1 FROM pgwrh.ready_serving_subtree WHERE rel_id = ('data', 'range_root')::pgwrh.rel_id)") == [(False,)]
        conn.execute('ROLLBACK')
        conn.execute('SELECT pg_advisory_unlock(2895359559)')


def set_split_placement(cluster):
    keys = {}
    for owner, other in [('replica1', 'replica2'), ('replica2', 'replica1')]:
        keys[owner] = cluster.master.query_scalar(f"""SELECT n::text FROM generate_series(1, 100) n
            WHERE pgwrh.score(100, n::text, '{owner}') > pgwrh.score(100, n::text, '{other}') LIMIT 1""")
    expression = f"SELECT CASE WHEN $2 LIKE '%_left_%' THEN '{keys['replica1']}' ELSE '{keys['replica2']}' END"
    rows = cluster.master.execute(f"""INSERT INTO pgwrh.sharded_table
        (replication_group_id, sharded_table_schema, sharded_table_name, replication_factor, sharding_key_expression)
        SELECT replication_group_id, sharded_table_schema, sharded_table_name, replication_factor,
               {quote_literal(expression)}
        FROM pgwrh.sharded_table
        WHERE version = (SELECT current_version FROM pgwrh.replication_group WHERE replication_group_id = 'g1')
        ON CONFLICT (replication_group_id, sharded_table_schema, sharded_table_name, version)
        DO UPDATE SET sharding_key_expression = EXCLUDED.sharding_key_expression
        RETURNING sharded_table_name""")
    assert len(rows) == len(ROOTS)


def test_aggregates_fail_over_and_survive_daemon_restart(aggregated_cluster):
    cluster = aggregated_cluster
    cluster.master.execute("""INSERT INTO pgwrh.sharded_table
        (replication_group_id, sharded_table_schema, sharded_table_name, replication_factor, sharding_key_expression)
        SELECT replication_group_id, sharded_table_schema, sharded_table_name, 100, sharding_key_expression
        FROM pgwrh.sharded_table
        WHERE version = (SELECT current_version FROM pgwrh.replication_group WHERE replication_group_id = 'g1')
        ON CONFLICT (replication_group_id, sharded_table_schema, sharded_table_name, version)
        DO UPDATE SET replication_factor = 100, sharding_key_expression = EXCLUDED.sharding_key_expression""")
    cluster.deploy(timeout=60)
    reader = cluster.replicas[-1]
    wait_until(lambda: remote_nodes(reader) == [('data', root) for root in sorted(ROOTS)],
               timeout=60, message='replicated roots did not aggregate after commit')
    # Readiness permits a safe subset while the routing list converges. Wait
    # for both replicas before deliberately removing one of them.
    wait_until(lambda: reader.query_scalar("""SELECT count(*) FROM pgwrh.remote_node_assignment a
        JOIN pgwrh.remote_server_route r ON r.srvname = a.shard_server_name
        WHERE cardinality(r.shard_server_targets) = 2
          AND r.shard_server_targets = a.target_servers""") == len(ROOTS),
        timeout=60, message='reader did not install both failover targets')
    offline = cluster.replicas[0]
    offline.node.stop()
    try:
        for _ in range(3):
            for root in ROOTS:
                assert reader.execute(f'SELECT * FROM data.{root} ORDER BY id') == cluster.master.execute(f'SELECT * FROM data.{root} ORDER BY id')
        reader.execute("""SELECT pg_terminate_backend(pid) FROM pg_stat_activity
            WHERE application_name = 'pgwrh_sync_daemon'""")
        reader.execute('SELECT pgwrh.start_sync_daemon(0.1)')
        wait_until(lambda: reader.query_scalar('SELECT count(*) FROM pgwrh.sync') == 0,
                   timeout=30, message='restarted daemon did not converge')
        assert remote_nodes(reader) == [('data', root) for root in sorted(ROOTS)]
    finally:
        offline.node.start()
    assert_all_rows(cluster)


def test_selection_quotes_relation_identifiers(selection):
    selection.execute("""UPDATE fdw_shard_structure SET
        schema_name = CASE WHEN schema_name = 'data' THEN 'Odd schema' ELSE schema_name END,
        parent_schema_name = CASE WHEN parent_schema_name = 'data' THEN 'Odd schema' ELSE parent_schema_name END,
        root_schema_name = 'Odd schema',
        table_name = CASE WHEN table_name = 'root' THEN 'Root "quoted"' ELSE table_name END,
        parent_table_name = CASE WHEN parent_table_name = 'root' THEN 'Root "quoted"' ELSE parent_table_name END,
        root_table_name = 'Root "quoted"'""")
    selection.execute("""UPDATE fdw_serving_subtree SET schema_name = 'Odd schema',
        table_name = CASE WHEN table_name = 'root' THEN 'Root "quoted"' ELSE table_name END""")
    assert selection.execute('SELECT pgwrh.fqn(remote_rel_id) FROM remote_node_assignment') == [
        ('"Odd schema_remote"."Root ""quoted"""',)
    ]


def test_unchanged_destinations_wait_for_credential_rotation(aggregated_cluster):
    cluster = aggregated_cluster
    reader = cluster.replicas[-1]
    old_nodes = remote_nodes(reader)
    with reader.node.connect() as pause:
        pause.execute('SELECT pg_advisory_lock(2895359559)')
        cluster.master.execute("""INSERT INTO pgwrh.sharded_table
            (replication_group_id, sharded_table_schema, sharded_table_name, replication_factor, sharding_key_expression)
            SELECT replication_group_id, sharded_table_schema, sharded_table_name, replication_factor, sharding_key_expression
            FROM pgwrh.sharded_table
            WHERE version = (SELECT current_version FROM pgwrh.replication_group WHERE replication_group_id = 'g1')
            ON CONFLICT (replication_group_id, sharded_table_schema, sharded_table_name, version)
            DO UPDATE SET sharding_key_expression = EXCLUDED.sharding_key_expression""")
        cluster.master.start_rollout()
        assert cluster.master.rollout_gap_counts()['connected_remote'] > 0
        with pytest.raises(Exception, match='Not all hosts confirmed'):
            cluster.master.commit_rollout()
        assert remote_nodes(reader) == old_nodes
        assert_all_rows(cluster)
        pause.execute('SELECT pg_advisory_unlock(2895359559)')
    cluster.master.wait_for_rollout_ready(expected_replicas=3, timeout=60)
    cluster.master.commit_rollout()
    wait_until(lambda: remote_nodes(reader) == old_nodes, timeout=60,
               message='credential rotation did not restore aggregates after commit')
    assert_all_rows(cluster)


def local_tree(replica, root):
    return replica.execute(f"SELECT relid::text, parentrelid::text, level FROM pg_partition_tree('data.{root}') ORDER BY 1")


def test_retained_partitioned_shields_survive_rollout_until_readers_switch(aggregated_cluster):
    cluster = aggregated_cluster
    reader = cluster.replicas[-1]
    source = next(r for r in cluster.replicas if r.query_scalar('SELECT count(*) FROM pgwrh.connected_local_shard') == 13)
    original = {root: local_tree(source, root) for root in ROOTS}
    with reader.node.connect() as paused:
        paused.execute('SELECT pg_advisory_lock(2895359559)')
        set_split_placement(cluster)
        cluster.master.start_rollout()
        wait_until(lambda: cluster.master.query_scalar("""SELECT count(*) FROM pgwrh.missing_connected_local_shard
            WHERE version = (SELECT target_version FROM pgwrh.replication_group WHERE replication_group_id = 'g1')""") == 0,
                   timeout=60, message='new local copies did not connect')
        wait_until(lambda: source.query_scalar('SELECT count(*) FROM pgwrh.prepared_remote_shard') > 0,
                   timeout=60, message='outgoing local leaves have no prepared replacements')
        assert {root: local_tree(source, root) for root in ROOTS} == original
        with pytest.raises(Exception, match='required remote shards'):
            cluster.master.commit_rollout()
        assert_all_rows(cluster)
        paused.execute('SELECT pg_advisory_unlock(2895359559)')
    cluster.master.wait_for_rollout_ready(expected_replicas=3, timeout=60)
    assert {root: local_tree(source, root) for root in ROOTS} == original
    with source.node.connect() as paused:
        paused.execute('SELECT pg_advisory_lock(2895359559)')
        cluster.master.commit_rollout()
        outgoing = paused.execute("""SELECT (rel_id).schema_name, (rel_id).table_name
            FROM pgwrh.shard_assignment_r WHERE NOT local ORDER BY 1, 2""")
        # Parent candidates remain blocked by the outgoing local attachments.
        assert paused.execute('SELECT schema_name, table_name FROM pgwrh.remote_node_assignment ORDER BY 1, 2') == outgoing
        assert paused.execute('SELECT pgwrh.sync_step()') == [(True,)]
        # The first pass installs the prepared leaves. Aggregation is a later pass.
        assert remote_nodes(source) == outgoing
        paused.execute('SELECT pg_advisory_unlock(2895359559)')
    wait_until(lambda: len(remote_nodes(reader)) == 7, timeout=60, message='split routes did not aggregate')
    wait_until(lambda: len(remote_nodes(source)) < len(outgoing), timeout=60, message='replaced leaves did not aggregate')
    assert_all_rows(cluster)


@pytest.mark.parametrize('unlock', [True, False])
def test_rollback_preserves_partitioned_shields_for_delayed_readers(aggregated_cluster, unlock):
    cluster = aggregated_cluster
    reader = cluster.replicas[-1]
    set_split_placement(cluster)
    cluster.master.start_rollout()
    cluster.master.wait_for_rollout_ready(expected_replicas=3, timeout=60)
    with reader.node.connect() as paused:
        paused.execute('SELECT pg_advisory_lock(2895359559)')
        trees = [(r, node[0], node[1], r.execute(f'SELECT * FROM "{node[0]}_shield"."{node[1]}" ORDER BY id'))
                 for r in cluster.replicas[:-1]
                 for node in r.execute('SELECT (rel_id).schema_name, (rel_id).table_name FROM pgwrh.ready_serving_subtree')]
        cluster.master.rollback_rollout(unlock=unlock)
        assert cluster.master.query_scalar('SELECT count(*) FROM pgwrh.replication_group_config_lock WHERE rollback_unlock IS NOT NULL') == 1
        for replica, schema, table, rows in trees:
            assert replica.execute(f'SELECT * FROM "{schema}_shield"."{table}" ORDER BY id') == rows
        assert_all_rows(cluster)
        paused.execute('SELECT pg_advisory_unlock(2895359559)')
    wait_until(lambda: cluster.master.query_scalar('SELECT count(*) FROM pgwrh.replication_group_config_lock WHERE rollback_unlock IS NOT NULL') == 0,
               timeout=60, message='rollback did not release abandoned configuration')
    wait_until(lambda: remote_nodes(reader) == [('data', root) for root in sorted(ROOTS)],
               timeout=60, message='rollback did not restore root aggregates')
    assert_all_rows(cluster)


def test_active_aggregate_query_delays_handoff_without_changing_its_source(aggregated_cluster):
    cluster = aggregated_cluster
    reader = cluster.replicas[-1]
    source = next(r for r in cluster.replicas if r.query_scalar('SELECT count(*) FROM pgwrh.connected_local_shard') == 13)
    original = local_tree(source, 'range_root')
    with reader.node.connect() as query:
        reader_pid = query.execute('SELECT pg_backend_pid()')[0][0]
        assert query.execute('SELECT count(*) FROM data.range_root') == [(16,)]
        set_split_placement(cluster)
        cluster.master.start_rollout()
        wait_until(lambda: reader.query_scalar(f"""SELECT EXISTS (SELECT 1 FROM pg_stat_activity a
            WHERE {reader_pid} = ANY(pg_blocking_pids(a.pid)))"""),
                   timeout=60, message='reader did not wait for its active query')
        assert local_tree(source, 'range_root') == original
        assert query.execute('SELECT count(*) FROM data.range_root') == [(16,)]
        # Membership/credential publication can block before new local copies
        # finish, unlike the old attachment-only barrier.
        wait_until(lambda: cluster.master.query_scalar("""SELECT count(*) FROM pgwrh.missing_connected_local_shard
            WHERE version = (SELECT target_version FROM pgwrh.replication_group WHERE replication_group_id = 'g1')""") == 0,
                   timeout=60, message='new local copies did not connect')
        with pytest.raises(Exception, match='required remote shards'):
            cluster.master.commit_rollout()
    cluster.master.wait_for_rollout_ready(expected_replicas=3, timeout=60)
    cluster.master.commit_rollout()
    assert_all_rows(cluster)
