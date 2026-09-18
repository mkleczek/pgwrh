from __future__ import annotations

import json
from collections import Counter

import pytest


@pytest.fixture
def az_controller(postgres_node_factory):
    node = postgres_node_factory('az_placement')
    node.execute("""
        DO $$ BEGIN
            FOR i IN 1..4 LOOP
                EXECUTE format('CREATE ROLE %I', 'a' || i);
                EXECUTE format('CREATE ROLE %I', 'b' || i);
                EXECUTE format('CREATE ROLE %I', 'c' || i);
            END LOOP;
        END $$;
        CREATE SCHEMA data;
        CREATE TABLE data.root (id int) PARTITION BY RANGE (id);
        CREATE TABLE data.branch PARTITION OF data.root FOR VALUES FROM (0) TO (100)
            PARTITION BY HASH (id);
        CREATE TABLE data.leaf PARTITION OF data.branch FOR VALUES WITH (MODULUS 1, REMAINDER 0);
        CREATE TABLE data.other PARTITION OF data.root FOR VALUES FROM (100) TO (200);
        SELECT pgwrh.create_replica_cluster('g1');
        INSERT INTO pgwrh.sharded_table
            (replication_group_id, sharded_table_schema, sharded_table_name, replication_factor)
        VALUES ('g1', 'data', 'root', 0);
        SELECT pgwrh.add_replica('g1', z || i, z || i || '.invalid', 5432, (z || i)::regrole, z, 20 * i)
        FROM (VALUES ('a', 4), ('b', 3), ('c', 2)) zones(z, n), generate_series(1, n) i;
        UPDATE pgwrh.replication_group_config SET min_replica_count = 3,
            min_replica_count_per_availability_zone = 0
        WHERE version = 'FLOP';
    """)
    return node


def placements(node, copies=3, minimum=0, survivors=1, affinity=None, keys=200):
    weights = json.dumps(affinity or {})
    return node.execute(f"""SELECT k, p.availability_zone, p.host_id
        FROM generate_series(1, {keys}) k
        CROSS JOIN LATERAL pgwrh.select_shard_hosts(
            'g1', 'FLOP', k::text, {copies}, {minimum}, {survivors}, '{weights}'::jsonb) p
        ORDER BY k, p.availability_zone, p.host_id""")


def test_neutral_weights_preserve_existing_assignments(az_controller):
    node = az_controller
    # Unequal host counts and host weights, all copy counts, several shard keys.
    # Compare against the old placement query, not a reimplementation in Python.
    assert node.execute("""WITH cases AS (
            SELECT k::text AS key, r::bigint AS copies
            FROM generate_series(1, 60) k, generate_series(1, 9) r
        ), compared AS (
            SELECT key, copies,
                ARRAY(SELECT host_id FROM pgwrh.select_shard_hosts(
                    'g1', 'FLOP', key, copies, 0, 0, '{}') ORDER BY host_id) AS actual,
                ARRAY(SELECT h.host_id FROM (
                    SELECT w.host_id, w.availability_zone,
                        row_number() OVER (PARTITION BY w.availability_zone
                            ORDER BY pgwrh.score(w.weight, key, w.host_id) DESC) AS rank
                    FROM pgwrh.shard_host_weight w WHERE w.version = 'FLOP'
                    ORDER BY rank, pgwrh.score(100, key, w.availability_zone) DESC
                    LIMIT copies
                ) h ORDER BY h.host_id) AS expected
            FROM cases
        ) SELECT key, copies FROM compared WHERE actual <> expected""") == []
    assert placements(node, copies=5, minimum=1, survivors=0) == placements(
        node, copies=5, minimum=1, survivors=0, affinity={'a': 8, 'b': 8, 'c': 8})


def test_preferences_are_deterministic_and_obey_survival_limit(az_controller):
    node = az_controller
    rows = placements(node, affinity={'a': 4})
    assert rows == placements(node, affinity={'a': 4})
    for key in range(1, 201):
        hosts = [(z, h) for k, z, h in rows if k == key]
        counts = Counter(z for z, _ in hosts)
        assert len(set(hosts)) == 3
        assert counts['a'] == 2
        assert sorted(counts.values()) == [1, 2]
    assert {'b', 'c'} <= {z for _, z, _ in rows}
    safe = placements(node, survivors=2, affinity={'a': 1000})
    assert Counter((k, z) for k, z, _ in safe) == Counter(
        (k, z) for k in range(1, 201) for z in 'abc')
    assert placements(node, minimum=1, affinity={'a': 1000}) == placements(node, minimum=1)


def test_fractional_slots_are_probabilistic_and_ignore_zone_size(az_controller):
    node = az_controller
    rows = placements(node, copies=1, survivors=0, affinity={'a': 4}, keys=2000)
    counts = Counter(z for _, z, _ in rows)
    assert 0.62 < counts['a'] / len(rows) < 0.71
    assert 0.13 < counts['b'] / len(rows) < 0.20
    assert 0.13 < counts['c'] / len(rows) < 0.20
    # Ratios, rather than absolute scale or number of hosts, define AZ preference.
    assert rows == placements(node, copies=1, survivors=0,
                              affinity={'a': 40, 'b': 10, 'c': 10}, keys=2000)
    before = [(k, z) for k, z, _ in rows]
    node.execute("SELECT pgwrh.add_replica('g1', 'c3', 'c3.invalid', 5432, 'c3', 'c')")
    after = placements(node, copies=1, survivors=0, affinity={'a': 4}, keys=2000)
    assert before == [(k, z) for k, z, _ in after]
    assert all(old == new or new[2] == 'c3' for old, new in zip(rows, after))


def test_capacity_is_redistributed_without_violating_ha(az_controller):
    rows = placements(az_controller, copies=7, survivors=3, affinity={'c': 1000})
    for key in range(1, 201):
        counts = Counter(z for k, z, _ in rows if k == key)
        assert sum(counts.values()) == 7
        assert counts['c'] == 2  # The preferred AZ has only two eligible hosts.
        assert max(counts.values()) <= 4
        assert min(counts.values()) >= 2


@pytest.mark.parametrize('copies,minimum,survivors', [(3, 0, 3), (2, 1, 0), (7, 0, 5), (9, 3, 0)])
def test_infeasible_constraints_are_rejected(az_controller, copies, minimum, survivors):
    with pytest.raises(Exception, match='Cannot place'):
        placements(az_controller, copies=copies, minimum=minimum, survivors=survivors, keys=1)


def test_policy_inheritance_and_explicit_resets(az_controller):
    node = az_controller
    node.execute("""
        UPDATE pgwrh.replication_group_config SET min_replica_count_after_az_failure = 2
            WHERE version = 'FLOP';
        UPDATE pgwrh.sharded_table SET min_replica_count_after_az_failure = 1;
        INSERT INTO pgwrh.sharded_table
            (replication_group_id, sharded_table_schema, sharded_table_name, replication_factor,
             min_replica_count_after_az_failure)
        VALUES ('g1', 'data', 'branch', 0, NULL), ('g1', 'data', 'leaf', 0, 0);
        INSERT INTO pgwrh.sharded_table_az_affinity
            (replication_group_id, sharded_table_schema, sharded_table_name, availability_zone, weight)
        VALUES ('g1', 'data', 'root', 'a', 4), ('g1', 'data', 'root', 'b', 2),
               ('g1', 'data', 'branch', 'a', 1), ('g1', 'data', 'leaf', 'c', 3);
    """)
    assert node.execute("SELECT * FROM pgwrh.shard_placement_policy('g1', 'FLOP', 'data.leaf')") == [
        (0, {'a': 1, 'b': 2, 'c': 3})]
    assert node.execute("SELECT * FROM pgwrh.shard_placement_policy('g1', 'FLOP', 'data.branch')") == [
        (1, {'a': 1, 'b': 2})]
    assert node.execute("SELECT * FROM pgwrh.shard_placement_policy('g1', 'FLOP', 'data.other')") == [
        (1, {'a': 4, 'b': 2})]
    node.execute('UPDATE pgwrh.sharded_table SET min_replica_count_after_az_failure = NULL')
    assert node.execute("SELECT min_replica_count_after_az_failure FROM pgwrh.shard_placement_policy('g1', 'FLOP', 'data.leaf')") == [(2,)]


def test_preview_matches_snapshot_and_reports_unavailable_preferences(az_controller):
    node = az_controller
    node.execute("""INSERT INTO pgwrh.sharded_table_az_affinity
        (replication_group_id, sharded_table_schema, sharded_table_name, availability_zone, weight)
        VALUES ('g1', 'data', 'root', 'a', 4), ('g1', 'data', 'root', 'absent', 100)""")
    preview = node.execute("""SELECT schema_name, table_name, availability_zone, host_id
        FROM pgwrh.preview_shard_placement('g1', 'FLOP') ORDER BY 1, 2, 3, 4""")
    assert len(preview) == 6
    assert node.execute("""SELECT DISTINCT unavailable_preferred_zones
        FROM pgwrh.preview_shard_placement('g1', 'FLOP')""") == [(['absent'],)]
    node.execute("SELECT pgwrh.start_rollout('g1')")
    snapshot_query = """SELECT schema_name, table_name, availability_zone, host_id
        FROM pgwrh.shard_assigned_host WHERE version = 'FLOP' ORDER BY 1, 2, 3, 4"""
    assert node.execute(snapshot_query) == preview
    node.execute('ALTER TABLE data.root DETACH PARTITION data.other')
    assert node.execute(snapshot_query) == preview
    with pytest.raises(Exception, match='locked'):
        node.execute("UPDATE pgwrh.sharded_table_az_affinity SET weight = 2 WHERE availability_zone = 'a'")


def test_infeasible_rollout_is_atomic(az_controller):
    node = az_controller
    node.execute("UPDATE pgwrh.replication_group_config SET min_replica_count_after_az_failure = 3 WHERE version = 'FLOP'")
    with pytest.raises(Exception, match='Cannot place'):
        node.execute("SELECT pgwrh.start_rollout('g1')")
    assert node.execute("SELECT current_version = target_version FROM pgwrh.replication_group") == [(True,)]
    assert node.execute("SELECT count(*) FROM pgwrh.replication_group_config_lock WHERE version = 'FLOP'") == [(0,)]
    assert node.execute('SELECT count(*) FROM pgwrh.shard') == [(0,)]
    assert node.execute('SELECT count(*) FROM pgwrh.shard_assigned_host') == [(0,)]
