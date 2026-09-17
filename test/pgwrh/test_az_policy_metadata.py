from __future__ import annotations

import pytest


@pytest.fixture
def policy_controller(postgres_node_factory):
    node = postgres_node_factory('az_policy')
    # Empty partition trees exercise configuration lifecycle without replica daemons.
    node.execute("""
        CREATE SCHEMA data;
        CREATE TABLE data.root (id int) PARTITION BY RANGE (id);
        CREATE TABLE data.child PARTITION OF data.root FOR VALUES FROM (0) TO (100)
            PARTITION BY HASH (id);
        SELECT pgwrh.create_replica_cluster('g1');
        INSERT INTO pgwrh.sharded_table
            (replication_group_id, sharded_table_schema, sharded_table_name, replication_factor,
             min_replica_count_after_az_failure)
        VALUES ('g1', 'data', 'root', 0, 1),
               ('g1', 'data', 'child', 0, NULL);
        UPDATE pgwrh.replication_group_config SET min_replica_count = 3,
            min_replica_count_per_availability_zone = 0,
            min_replica_count_after_az_failure = 2
        WHERE version = 'FLOP';
        INSERT INTO pgwrh.sharded_table_az_affinity
            (replication_group_id, sharded_table_schema, sharded_table_name, availability_zone, weight)
        VALUES ('g1', 'data', 'root', 'a', 4), ('g1', 'data', 'root', 'b', 2),
               ('g1', 'data', 'child', 'a', 1);
        SELECT pgwrh.start_rollout('g1');
        SELECT pgwrh.commit_rollout('g1');
    """)
    return node


def test_affinity_insert_clones_policies_and_preserves_explicit_override(policy_controller):
    node = policy_controller
    node.execute("""INSERT INTO pgwrh.sharded_table_az_affinity
        (replication_group_id, sharded_table_schema, sharded_table_name, availability_zone, weight)
        VALUES ('g1', 'data', 'root', 'a', 8)""")
    assert node.execute("""SELECT min_replica_count, min_replica_count_per_availability_zone,
        min_replica_count_after_az_failure
        FROM pgwrh.replication_group_config WHERE version = 'FLIP'""") == [(3, 0, 2)]
    assert node.execute("""SELECT sharded_table_name,
        min_replica_count_after_az_failure FROM pgwrh.sharded_table
        WHERE version = 'FLIP' ORDER BY sharded_table_name""") == [
        ('child', None), ('root', 1)]
    assert node.execute("""SELECT sharded_table_name, availability_zone, weight
        FROM pgwrh.sharded_table_az_affinity WHERE version = 'FLIP'
        ORDER BY sharded_table_name, availability_zone""") == [
        ('child', 'a', 1), ('root', 'a', 8), ('root', 'b', 2)]
    assert node.execute("""SELECT weight FROM pgwrh.sharded_table_az_affinity
        WHERE version = 'FLOP' AND sharded_table_name = 'root' AND availability_zone = 'a'""") == [(4,)]


@pytest.mark.parametrize('statement', [
    "UPDATE pgwrh.sharded_table_az_affinity SET weight = 9 WHERE version = 'FLOP'",
    "DELETE FROM pgwrh.sharded_table_az_affinity WHERE version = 'FLOP'",
    "UPDATE pgwrh.sharded_table SET min_replica_count_after_az_failure = 0 WHERE version = 'FLOP'",
    "UPDATE pgwrh.replication_group_config SET min_replica_count_after_az_failure = 0 WHERE version = 'FLOP'",
])
def test_locked_policy_cannot_change(policy_controller, statement):
    with pytest.raises(Exception, match='locked'):
        policy_controller.execute(statement)


@pytest.mark.parametrize('statement', [
    "UPDATE pgwrh.replication_group_config SET min_replica_count_after_az_failure = -1 WHERE version = 'FLIP'",
    "UPDATE pgwrh.sharded_table SET min_replica_count_after_az_failure = -1 WHERE version = 'FLIP'",
    "INSERT INTO pgwrh.sharded_table_az_affinity (replication_group_id, sharded_table_schema, sharded_table_name, availability_zone, weight) VALUES ('g1', 'data', 'root', 'absent', 0)",
])
def test_invalid_policy_values_are_rejected(policy_controller, statement):
    policy_controller.execute("SELECT pgwrh.next_pending_version('g1')")
    policy_controller.execute("""INSERT INTO pgwrh.replication_group_config_clone
        VALUES ('g1', 'FLOP', 'FLIP') ON CONFLICT DO NOTHING""")
    with pytest.raises(Exception, match='check constraint'):
        policy_controller.execute(statement)


def test_affinity_is_extension_configuration(policy_controller):
    assert policy_controller.execute("""SELECT 'pgwrh.sharded_table_az_affinity'::regclass::oid = ANY(extconfig)
        FROM pg_extension WHERE extname = 'pgwrh'""") == [(True,)]
