from __future__ import annotations

import random
from typing import Iterable

from .pgwrh_testkit import (
    RelationRef,
    ReplicaHandle,
    ReplicaSpec,
    assert_shard_hosting_replica_count,
    replica_hosts_local_shard,
    wait_until,
)

QUERY_ALL_ROWS = "SELECT count(*) FROM test.my_data"
ROOT_TABLE = RelationRef("test", "my_data")


def _set_pending_replication_factor(master, *, factor: int) -> None:
    updated_tables = master.execute(
        f"""
        UPDATE pgwrh.sharded_table
        SET replication_factor = {factor}
        WHERE
            replication_group_id = {master.group_id!r}
            AND version = pgwrh.next_pending_version({master.group_id!r})
        RETURNING sharded_table_name
        """
    )
    assert updated_tables, "expected pending sharded_table rows to exist"


def _assert_current_shard_replica_count(cluster, *, expected_count: int) -> None:
    current_shards = cluster.master.current_shards()
    assert current_shards, "expected current shard assignments to exist"
    for shard in current_shards:
        assert_shard_hosting_replica_count(
            cluster.replicas,
            shard,
            root_table=ROOT_TABLE,
            expected_count=expected_count,
        )


def _pick_hosting_replica(cluster) -> ReplicaHandle:
    current_shards = cluster.master.current_shards()
    hosting_replicas = [
        replica
        for replica in cluster.replicas
        if any(
            replica_hosts_local_shard(replica, shard, root_table=ROOT_TABLE)
            for shard in current_shards
        )
    ]
    assert hosting_replicas, "expected at least one replica to host a current shard"
    return random.SystemRandom().choice(hosting_replicas)


def _assert_query_results_match(master, replicas: Iterable[ReplicaHandle], *, rounds: int) -> None:
    expected = master.execute(QUERY_ALL_ROWS)
    for _ in range(rounds):
        for replica in replicas:
            assert replica.execute(QUERY_ALL_ROWS) == expected


def _restart_replica(replica: ReplicaHandle) -> None:
    def ready() -> bool:
        try:
            return replica.execute("SELECT 1") == [(1,)]
        except Exception:
            return False

    replica.node.start()
    wait_until(
        ready,
        message=f"{replica.name} did not accept connections after restart",
    )


def test_scale_out_keeps_reads_stable_with_one_replica_offline(cluster_factory):
    cluster = cluster_factory(
        (
            ReplicaSpec("replica1"),
            ReplicaSpec("replica2"),
            ReplicaSpec("replica3"),
        )
    )
    _set_pending_replication_factor(cluster.master, factor=50)
    cluster.deploy(commit=True)

    _assert_current_shard_replica_count(cluster, expected_count=2)
    _assert_query_results_match(cluster.master, cluster.replicas, rounds=1)

    cluster.add_replicas(
        (
            ReplicaSpec("replica4"),
            ReplicaSpec("replica5"),
        )
    )
    cluster.deploy(commit=True)

    _assert_current_shard_replica_count(cluster, expected_count=3)
    _assert_query_results_match(cluster.master, cluster.replicas, rounds=1)

    offline_replica = _pick_hosting_replica(cluster)
    online_replicas = [replica for replica in cluster.replicas if replica is not offline_replica]

    offline_replica.node.stop()
    _assert_query_results_match(cluster.master, online_replicas, rounds=3)

    _restart_replica(offline_replica)
    _assert_query_results_match(cluster.master, cluster.replicas, rounds=2)
