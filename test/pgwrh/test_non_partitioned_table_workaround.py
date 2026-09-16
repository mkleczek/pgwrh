from __future__ import annotations

from .pgwrh_testkit import (
    RelationRef,
    assert_shard_hosting_replica_count,
)

QUERY_ALL_ROWS = """
SELECT id, payload, happened_on
FROM test.non_partitioned_data
ORDER BY id
"""
ROOT_TABLE = RelationRef("test", "non_partitioned_data")
ONLY_SHARD = RelationRef("test_shards", "non_partitioned_data_default")


def test_non_partitioned_table_workaround_rolls_out(
    non_partitioned_workaround_cluster_factory,
    two_replica_specs,
):
    cluster = non_partitioned_workaround_cluster_factory(
        two_replica_specs,
        deploy=True,
        commit=True,
    )

    assert cluster.master.current_shards() == [ONLY_SHARD]
    assert_shard_hosting_replica_count(
        cluster.replicas,
        ONLY_SHARD,
        root_table=ROOT_TABLE,
        expected_count=1,
    )
    cluster.assert_query_results_match(QUERY_ALL_ROWS)
