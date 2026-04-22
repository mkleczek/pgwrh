from __future__ import annotations

from dataclasses import dataclass

import pytest

from .pgwrh_testkit import ReplicaSpec, quote_literal

INDEX_TEMPLATE_NAME = "idx_col2"
INDEX_TEMPLATE_DEF = "USING btree (col2)"


@dataclass(frozen=True, slots=True)
class RemoteAssignment:
    member_role: str
    schema_name: str
    table_name: str
    current_server_name: str
    target_server_name: str | None = None


@dataclass(frozen=True, slots=True)
class VisibleAssignment:
    shard_server_name: str
    retained_shard_server_name: str | None
    connect_remote: bool


def _insert_secondary_index_template(master) -> None:
    master.execute(
        """
        INSERT INTO pgwrh.shard_index_template (
            replication_group_id,
            index_template_schema,
            index_template_table_name,
            index_template_name,
            index_template
        ) VALUES (
            {group_id},
            'test',
            'my_data',
            {index_name},
            {index_template}
        )
        """.format(
            group_id=quote_literal(master.group_id),
            index_name=quote_literal(INDEX_TEMPLATE_NAME),
            index_template=quote_literal(INDEX_TEMPLATE_DEF),
        )
    )


def _pick_existing_remote_assignment(master) -> RemoteAssignment:
    rows = master.execute(
        """
        SELECT
            member_role,
            schema_name,
            table_name,
            shard_server_name
        FROM
            pgwrh.shard_assignment_per_member
        WHERE
            replication_group_id = {group_id}
            AND NOT local
        ORDER BY
            member_role,
            schema_name,
            table_name
        LIMIT 1
        """.format(group_id=quote_literal(master.group_id))
    )
    assert rows, "expected at least one remote shard assignment"
    member_role, schema_name, table_name, current_server_name = rows[0]
    assert current_server_name, "expected the remote shard assignment to expose a server"
    return RemoteAssignment(
        member_role=member_role,
        schema_name=schema_name,
        table_name=table_name,
        current_server_name=current_server_name,
    )


def _pick_changing_remote_assignment(master) -> RemoteAssignment:
    rows = master.execute(
        """
        WITH routes AS (
            SELECT
                m.member_role,
                sah.schema_name,
                sah.table_name,
                bool_or(
                    sah.version = g.current_version
                    AND (sah.availability_zone, sah.host_id) = (m.availability_zone, m.host_id)
                ) AS local_current,
                bool_or(
                    sah.version = g.target_version
                    AND (sah.availability_zone, sah.host_id) = (m.availability_zone, m.host_id)
                ) AS local_target,
                md5(
                    string_agg(
                        sah.availability_zone || sah.host_id,
                        ',' ORDER BY sah.availability_zone, sah.host_id
                    ) FILTER (
                        WHERE
                            sah.version = g.current_version
                            AND (sah.availability_zone, sah.host_id) <> (m.availability_zone, m.host_id)
                    )
                ) AS current_server_name,
                md5(
                    string_agg(
                        sah.availability_zone || sah.host_id,
                        ',' ORDER BY sah.availability_zone, sah.host_id
                    ) FILTER (
                        WHERE
                            sah.version = g.target_version
                            AND (sah.availability_zone, sah.host_id) <> (m.availability_zone, m.host_id)
                    )
                ) AS target_server_name
            FROM
                pgwrh.replication_group_member m
                    JOIN pgwrh.replication_group g USING (replication_group_id)
                    JOIN pgwrh.shard_assigned_host sah
                        ON sah.replication_group_id = m.replication_group_id
                       AND sah.version IN (g.current_version, g.target_version)
            WHERE
                m.replication_group_id = {group_id}
            GROUP BY
                m.member_role,
                m.availability_zone,
                m.host_id,
                sah.schema_name,
                sah.table_name
        )
        SELECT
            member_role,
            schema_name,
            table_name,
            current_server_name,
            target_server_name
        FROM
            routes
        WHERE
            NOT local_current
            AND NOT local_target
            AND current_server_name IS DISTINCT FROM target_server_name
        ORDER BY
            member_role,
            schema_name,
            table_name
        LIMIT 1
        """.format(group_id=quote_literal(master.group_id))
    )
    assert rows, "expected a shard whose remote route changes across rollout versions"
    member_role, schema_name, table_name, current_server_name, target_server_name = rows[
        0
    ]
    assert current_server_name, "expected a current remote server for the candidate shard"
    assert target_server_name, "expected a target remote server for the candidate shard"
    return RemoteAssignment(
        member_role=member_role,
        schema_name=schema_name,
        table_name=table_name,
        current_server_name=current_server_name,
        target_server_name=target_server_name,
    )


def _visible_assignment(master, assignment: RemoteAssignment) -> VisibleAssignment:
    rows = master.execute(
        """
        SELECT
            shard_server_name,
            retained_shard_server_name,
            connect_remote
        FROM
            pgwrh.shard_assignment_per_member
        WHERE
            replication_group_id = {group_id}
            AND member_role = {member_role}
            AND schema_name = {schema_name}
            AND table_name = {table_name}
        """.format(
            group_id=quote_literal(master.group_id),
            member_role=quote_literal(assignment.member_role),
            schema_name=quote_literal(assignment.schema_name),
            table_name=quote_literal(assignment.table_name),
        )
    )
    assert rows, f"expected assignment row for {assignment}"
    shard_server_name, retained_shard_server_name, connect_remote = rows[0]
    return VisibleAssignment(
        shard_server_name=shard_server_name,
        retained_shard_server_name=retained_shard_server_name,
        connect_remote=connect_remote,
    )


def _mark_target_hosts_ready_except_indexes(master, assignment: RemoteAssignment) -> None:
    master.execute(
        """
        WITH target_user AS (
            SELECT
                creds.username
            FROM
                pgwrh.replication_group g
                    JOIN pgwrh.replication_group_credentials creds
                        ON creds.replication_group_id = g.replication_group_id
                       AND creds.version = g.target_version
            WHERE
                g.replication_group_id = {group_id}
        )
        UPDATE pgwrh.replication_group_member m
        SET
            subscribed_local_shards =
                (
                    m.subscribed_local_shards::jsonb
                    || jsonb_build_array(
                        jsonb_build_object(
                            'schema_name',
                            {schema_name},
                            'table_name',
                            {table_name}
                        )
                    )
                )::json,
            users =
                (
                    m.users::jsonb
                    || jsonb_build_array((SELECT username FROM target_user))
                )::json
        WHERE
            (m.replication_group_id, m.availability_zone, m.host_id) IN (
                SELECT
                    sah.replication_group_id,
                    sah.availability_zone,
                    sah.host_id
                FROM
                    pgwrh.shard_assigned_host sah
                        JOIN pgwrh.replication_group g USING (replication_group_id)
                WHERE
                    sah.replication_group_id = {group_id}
                    AND sah.version = g.target_version
                    AND sah.schema_name = {schema_name}
                    AND sah.table_name = {table_name}
            )
        """.format(
            group_id=quote_literal(master.group_id),
            schema_name=quote_literal(assignment.schema_name),
            table_name=quote_literal(assignment.table_name),
        )
    )


def _target_hosts_are_missing_indexes(master, assignment: RemoteAssignment) -> bool:
    rows = master.execute(
        """
        SELECT EXISTS (
            SELECT
                1
            FROM
                pgwrh.shard_assigned_host sah
                    JOIN pgwrh.replication_group g USING (replication_group_id)
                    JOIN pgwrh.replication_group_member m
                        USING (replication_group_id, availability_zone, host_id)
                    JOIN pgwrh.shard_index_definition i
                        USING (replication_group_id, version, schema_name, table_name)
            WHERE
                sah.replication_group_id = {group_id}
                AND sah.version = g.target_version
                AND sah.schema_name = {schema_name}
                AND sah.table_name = {table_name}
                AND NOT EXISTS (
                    SELECT
                        1
                    FROM
                        json_to_recordset(m.indexes) AS mi(schema_name text, index_name text)
                    WHERE
                        (mi.schema_name, mi.index_name) = (i.schema_name, i.index_name)
                )
        )
        """.format(
            group_id=quote_literal(master.group_id),
            schema_name=quote_literal(assignment.schema_name),
            table_name=quote_literal(assignment.table_name),
        )
    )
    return bool(rows[0][0])


def _mark_target_indexes_present(master, assignment: RemoteAssignment) -> None:
    master.execute(
        """
        WITH target_indexes AS (
            SELECT
                coalesce(
                    json_agg(
                        json_build_object(
                            'schema_name',
                            schema_name,
                            'index_name',
                            index_name
                        )
                    ),
                    '[]'::json
                ) AS indexes
            FROM
                pgwrh.shard_index_definition i
                    JOIN pgwrh.replication_group g USING (replication_group_id)
            WHERE
                i.replication_group_id = {group_id}
                AND i.version = g.target_version
                AND i.schema_name = {schema_name}
                AND i.table_name = {table_name}
        )
        UPDATE pgwrh.replication_group_member m
        SET
            indexes = (m.indexes::jsonb || ti.indexes::jsonb)::json
        FROM
            target_indexes ti
        WHERE
            (m.replication_group_id, m.availability_zone, m.host_id) IN (
                SELECT
                    sah.replication_group_id,
                    sah.availability_zone,
                    sah.host_id
                FROM
                    pgwrh.shard_assigned_host sah
                        JOIN pgwrh.replication_group g USING (replication_group_id)
                WHERE
                    sah.replication_group_id = {group_id}
                    AND sah.version = g.target_version
                    AND sah.schema_name = {schema_name}
                    AND sah.table_name = {table_name}
            )
        """.format(
            group_id=quote_literal(master.group_id),
            schema_name=quote_literal(assignment.schema_name),
            table_name=quote_literal(assignment.table_name),
        )
    )


def _stop_replicas(cluster) -> None:
    for replica in cluster.replicas:
        replica.node.stop()


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


def test_target_only_index_keeps_existing_remote_route_exposed(
    deployed_two_replica_cluster,
):
    cluster = deployed_two_replica_cluster
    baseline = _pick_existing_remote_assignment(cluster.master)
    _stop_replicas(cluster)

    _insert_secondary_index_template(cluster.master)
    cluster.master.start_rollout()
    _mark_target_hosts_ready_except_indexes(cluster.master, baseline)

    assert _target_hosts_are_missing_indexes(cluster.master, baseline)

    visible = _visible_assignment(cluster.master, baseline)
    assert visible.shard_server_name == baseline.current_server_name
    assert visible.retained_shard_server_name == baseline.current_server_name
    assert visible.connect_remote is True


def test_fresh_target_copy_stays_hidden_until_required_indexes_exist(
    master,
    cluster_factory,
    two_replica_specs,
):
    _insert_secondary_index_template(master)
    cluster = cluster_factory(two_replica_specs, deploy=True, commit=True)

    cluster.add_replica(ReplicaSpec("replica3"))
    _stop_replicas(cluster)
    cluster.master.start_rollout()

    candidate = _pick_changing_remote_assignment(cluster.master)
    _mark_target_hosts_ready_except_indexes(cluster.master, candidate)

    assert _target_hosts_are_missing_indexes(cluster.master, candidate)

    visible = _visible_assignment(cluster.master, candidate)
    assert visible.shard_server_name == candidate.current_server_name
    assert visible.retained_shard_server_name == candidate.current_server_name
    assert visible.connect_remote is False


def test_fresh_target_copy_becomes_visible_after_required_indexes_exist(
    master,
    cluster_factory,
    two_replica_specs,
):
    _insert_secondary_index_template(master)
    cluster = cluster_factory(two_replica_specs, deploy=True, commit=True)

    cluster.add_replica(ReplicaSpec("replica3"))
    _stop_replicas(cluster)
    cluster.master.start_rollout()

    candidate = _pick_changing_remote_assignment(cluster.master)
    _mark_target_hosts_ready_except_indexes(cluster.master, candidate)
    _mark_target_indexes_present(cluster.master, candidate)

    assert not _target_hosts_are_missing_indexes(cluster.master, candidate)

    visible = _visible_assignment(cluster.master, candidate)
    assert visible.shard_server_name == candidate.target_server_name
    assert visible.retained_shard_server_name == candidate.current_server_name
    assert visible.connect_remote is True


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
