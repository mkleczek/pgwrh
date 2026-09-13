from __future__ import annotations

def test_cluster_factory_registers_multiple_replicas(two_replica_cluster):
    assert [replica.name for replica in two_replica_cluster.replicas] == [
        "replica1",
        "replica2",
    ]
    assert two_replica_cluster.master.replica_count() == 2
    assert two_replica_cluster.master.config_version_count() == 2


def test_initial_rollout_matches_master_results(deployed_two_replica_cluster):
    deployed_two_replica_cluster.assert_query_results_match(
        "SELECT count(*) FROM test.my_data"
    )
