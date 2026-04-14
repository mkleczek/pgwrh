from __future__ import annotations

import pytest

from .pgwrh_testkit import ReplicaSpec


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


@pytest.mark.skip(
    reason="Skeleton only: flesh out rollout assertions once fixture layer settles."
)
def test_scale_out_rollout_to_third_replica(deployed_two_replica_cluster):
    cluster = deployed_two_replica_cluster
    cluster.add_replica(ReplicaSpec("replica3"))
    cluster.deploy(commit=True)
    cluster.assert_query_results_match("SELECT count(*) FROM test.my_data")


@pytest.mark.skip(
    reason="Skeleton only: add shard-placement assertions for weight changes."
)
def test_reweighting_replicas_rolls_out_cleanly(deployed_two_replica_cluster):
    cluster = deployed_two_replica_cluster
    cluster.master.execute(
        """
        SELECT pgwrh.set_replica_weight(
            _replication_group_id := 'g1',
            _availability_zone := 'default',
            _replica_id := 'replica2',
            _weight := 250
        )
        """
    )
    cluster.deploy(commit=True)
    cluster.assert_query_results_match("SELECT count(*) FROM test.my_data")


@pytest.mark.skip(
    reason="Skeleton only: add availability-zone distribution checks."
)
def test_multi_az_rollout_keeps_remote_shards_connected(
    cluster_factory, multi_az_replica_specs
):
    cluster = cluster_factory(multi_az_replica_specs, deploy=True, commit=True)
    cluster.assert_query_results_match("SELECT count(*) FROM test.my_data")


@pytest.mark.skip(
    reason="Skeleton only: add explicit rollback assertions for failed rollouts."
)
def test_failed_rollout_can_be_rolled_back(deployed_two_replica_cluster):
    cluster = deployed_two_replica_cluster
    cluster.add_replica(ReplicaSpec("replica3"))
    cluster.master.start_rollout()
    cluster.master.rollback_rollout()
    assert cluster.master.current_version() == cluster.master.target_version()
