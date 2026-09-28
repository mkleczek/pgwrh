"""Real heterogeneous controllers/subscribers and replica-to-replica FDW routes."""
from itertools import product

import pytest

from ..pgwrh_testkit import MasterHandle, PgwrhCluster, ReplicaSpec, quote_literal, wait_until


# Include two replicas on the other major as well as replicas on both majors.
# Order matters: replica1 and replica2 own different parts of the tree.
TOPOLOGIES = [versions for versions in product((18, 19), repeat=3) if len(set(versions)) > 1]
ROWS = "SELECT id, bucket, payload, amount, tags, attrs FROM data.items ORDER BY id"


def set_placement(cluster, owners):
    """Give every member real local shards and force reads across peer versions."""
    members = ", ".join(f"({quote_literal(r.name)})" for r in cluster.replicas)
    keys = []
    for owner in owners:
        key = cluster.master.query_scalar(f"""
            SELECT n::text FROM generate_series(1, 1000) n
            WHERE NOT EXISTS (
                SELECT 1 FROM (VALUES {members}) AS hosts(name)
                WHERE name <> {quote_literal(owner)}
                  AND pgwrh.score(100, n::text, name)
                      >= pgwrh.score(100, n::text, {quote_literal(owner)})
            ) ORDER BY n LIMIT 1
        """)
        assert key is not None, f"no placement key for {owner}"
        keys.append(key)
    expression = "SELECT CASE " + " ".join(
        f"WHEN $2 LIKE 'items_p{part}_%' THEN {quote_literal(key)}"
        for part, key in enumerate(keys)
    ) + " END"
    cluster.master.execute(f"""
        INSERT INTO pgwrh.sharded_table
            (replication_group_id, sharded_table_schema, sharded_table_name,
             replication_factor, sharding_key_expression)
        VALUES ('g1', 'data', 'items', 0, {quote_literal(expression)})
        ON CONFLICT (replication_group_id, sharded_table_schema, sharded_table_name, version)
        DO UPDATE SET sharding_key_expression = EXCLUDED.sharding_key_expression
    """)


@pytest.fixture
def make_cluster(postgres_node_factory, postgres_installations):
    clusters = []

    def build(controller_major, replica_majors):
        master = MasterHandle(postgres_node_factory(
            "controller", installation=postgres_installations[controller_major],
        ))
        master.execute("""
            CREATE ROLE test_replica;
            CREATE SCHEMA data AUTHORIZATION test_replica;
            CREATE TABLE data.items (
                id integer, bucket integer, payload text, amount numeric(12, 2),
                tags text[], attrs jsonb
            ) PARTITION BY RANGE (bucket);
        """)
        for part in range(3):
            master.execute(f"""CREATE TABLE data.items_p{part} PARTITION OF data.items
                FOR VALUES FROM ({part}) TO ({part + 1}) PARTITION BY HASH (id)""")
            for leaf in range(2):
                master.execute(f"""
                    CREATE TABLE data.items_p{part}_{leaf} PARTITION OF data.items_p{part}
                        (PRIMARY KEY (id)) FOR VALUES WITH (MODULUS 2, REMAINDER {leaf});
                    ALTER TABLE data.items_p{part}_{leaf} OWNER TO test_replica;
                """)
        master.execute("""
            INSERT INTO data.items
            SELECT n, n % 3, 'row ' || n, n * 1.25,
                   ARRAY['hello', 'zażółć', NULL], jsonb_build_object('id', n, 'ok', true)
            FROM generate_series(1, 60) n;
            SELECT pgwrh.create_replica_cluster('g1');
        """)
        cluster = PgwrhCluster(master, postgres_node_factory)
        clusters.append(cluster)
        cluster.add_replicas([
            ReplicaSpec(f"replica{i}", installation=postgres_installations[major])
            for i, major in enumerate(replica_majors, 1)
        ])
        set_placement(cluster, ["replica1", "replica2", "replica1"])
        cluster.deploy(timeout=60)
        return cluster
    yield build
    # Stop subscribers before publishers, including pg_background sync workers
    # blocked on a peer that has already stopped during failure cleanup.
    for cluster in reversed(clusters):
        for replica in reversed(cluster.replicas):
            replica.node.stop(["-m", "immediate"])


def assert_versions(cluster, expected):
    handles = [cluster.master, *cluster.replicas]
    assert tuple(int(handle.query_scalar("SHOW server_version_num")) // 10000
                 for handle in handles) == expected


def assert_routes_and_schema(cluster):
    for replica in cluster.replicas:
        local = replica.query_scalar("SELECT count(*) FROM pgwrh.connected_local_shard")
        remote = replica.query_scalar("SELECT count(*) FROM pgwrh.connected_remote_shard")
        assert local > 0 and remote > 0, f"{replica.name}: local={local}, remote={remote}"
        assert local + remote == 6
        # The initial copy must have created physical leaves with their replica
        # identity, not merely transported the controller's definitions.
        assert replica.execute("""
            SELECT count(*), bool_and(EXISTS (
                SELECT 1 FROM pg_constraint c WHERE c.conrelid = srrelid AND contype = 'p'
            )), bool_and(srsubstate = 'r')
            FROM pg_subscription_rel
            JOIN pg_class ON oid = srrelid
            WHERE relnamespace = 'data'::regnamespace
        """) == [(local, True, True)]
        plan = replica.execute("EXPLAIN (ANALYZE, FORMAT JSON) " + ROWS)[0][0][0]["Plan"]

        def plans(node):
            yield node
            for child in node.get("Plans", []):
                yield from plans(child)

        assert any(p["Node Type"] == "Foreign Scan" and p["Actual Loops"] > 0
                   for p in plans(plan)), f"{replica.name} did not execute a remote read"
    cluster.assert_query_results_match(ROWS)
    cluster.assert_query_results_match(
        "SELECT bucket, count(*), sum(amount) FROM data.items GROUP BY bucket ORDER BY bucket"
    )


def assert_changes_replicate(cluster):
    cluster.master.execute("""
        INSERT INTO data.items
        SELECT n, n % 3, 'inserted ' || n, -n * 1.25,
               ARRAY['new', NULL], jsonb_build_object('inserted', n)
        FROM generate_series(61, 66) n;
        UPDATE data.items SET payload = 'updated', amount = NULL,
            tags = ARRAY[]::text[], attrs = '{"updated":true}' WHERE id BETWEEN 1 AND 6;
        DELETE FROM data.items WHERE id BETWEEN 7 AND 12;
    """)
    expected = cluster.master.execute(ROWS)
    assert len(expected) == 60
    for replica in cluster.replicas:
        wait_until(lambda: replica.execute(ROWS) == expected, timeout=60,
                   message=f"{replica.name} did not receive inserts, updates and deletes")
        # Check each local subscriber independently: a routed read could mask
        # a stale redundant copy if placement changes in the future.
        for schema, table in replica.execute("""
            SELECT (rel_id).schema_name, (rel_id).table_name FROM pgwrh.connected_local_shard
        """):
            sql = ROWS.replace("data.items", f'"{schema}"."{table}"')
            local_expected = cluster.master.execute(sql)
            wait_until(lambda: replica.execute(sql) == local_expected, timeout=60,
                       message=f"{replica.name}: {schema}.{table} did not catch up")


@pytest.mark.parametrize("versions", TOPOLOGIES, ids=[
    f"controller-{c}_replicas-{a}-{b}" for c, a, b in TOPOLOGIES
])
def test_bootstrap_replication_and_routing(make_cluster, versions):
    cluster = make_cluster(versions[0], versions[1:])
    assert_versions(cluster, versions)
    assert cluster.master.query_scalar("SELECT count(*) FROM data.items") == 60
    assert_routes_and_schema(cluster)
    assert_changes_replicate(cluster)
    assert_routes_and_schema(cluster)


@pytest.mark.parametrize("controller_major,new_major", [(18, 19), (19, 18)],
                         ids=["add-19-to-18", "add-18-to-19"])
def test_add_other_major_and_move_shards(make_cluster, postgres_installations,
                                        controller_major, new_major):
    cluster = make_cluster(controller_major, [controller_major, controller_major])
    previous_version = cluster.master.current_version()
    cluster.add_replica(ReplicaSpec("replica3", installation=postgres_installations[new_major]))
    set_placement(cluster, ["replica1", "replica2", "replica3"])
    cluster.deploy(timeout=60)
    assert cluster.master.current_version() != previous_version
    assert_versions(cluster, (controller_major, controller_major, controller_major, new_major))
    # Committing permits old local copies to remain until their routes switch.
    wait_until(lambda: all(r.query_scalar("SELECT count(*) FROM pgwrh.connected_local_shard") == 2
                           for r in cluster.replicas), timeout=60,
               message="scale-out did not move two leaves to each replica")
    assert_routes_and_schema(cluster)
    assert_changes_replicate(cluster)
    assert_routes_and_schema(cluster)
