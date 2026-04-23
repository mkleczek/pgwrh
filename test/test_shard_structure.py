from __future__ import annotations


def test_shard_structure_deduplicates_versions_during_rollout(
    deployed_two_replica_cluster,
):
    cluster = deployed_two_replica_cluster
    replica = cluster.replicas[0]

    cluster.master.execute(
        """
        CREATE TABLE test.extra_data (
            id bigint,
            category int NOT NULL
        )
        PARTITION BY RANGE (category)
        """
    )
    cluster.master.execute(
        """
        CREATE TABLE test.extra_data_p0
        PARTITION OF test.extra_data
        FOR VALUES FROM (0) TO (100)
        """
    )
    cluster.master.execute(
        """
        INSERT INTO pgwrh.sharded_table (
            replication_group_id,
            sharded_table_schema,
            sharded_table_name,
            replication_factor
        )
        VALUES ('g1', 'test', 'extra_data', 100)
        """
    )

    cluster.master.start_rollout()

    assert cluster.master.current_version() != cluster.master.target_version()
    assert replica.execute(
        """
        SELECT schema_name, table_name, level
        FROM pgwrh.fdw_shard_structure
        WHERE schema_name = 'test' AND table_name LIKE 'extra_data%'
        ORDER BY level, table_name
        """
    ) == [
        ("test", "extra_data", 0),
        ("test", "extra_data_p0", 1),
    ]
    assert replica.execute(
        """
        SELECT schema_name, table_name, level, count(*)
        FROM pgwrh.fdw_shard_structure
        GROUP BY 1, 2, 3
        HAVING count(*) > 1
        """
    ) == []
