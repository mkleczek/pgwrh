from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

MASTER_SEED_SQL = Path(__file__).with_name("master.sql")
DEFAULT_GROUP_ID = "g1"
DEFAULT_TIMEOUT = 30.0
DEFAULT_POLL_INTERVAL = 0.25


@dataclass(frozen=True, slots=True)
class RelationRef:
    schema_name: str
    table_name: str

    def __str__(self) -> str:
        return f"{self.schema_name}.{self.table_name}"


def quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _first_column(row: Any) -> Any:
    if isinstance(row, Mapping):
        return next(iter(row.values()))
    if isinstance(row, Sequence) and not isinstance(row, (str, bytes, bytearray)):
        return row[0]
    return row


def query_scalar(node: Any, sql: str) -> Any:
    rows = node.execute(sql)
    if not rows:
        return None
    return _first_column(rows[0])


def query_scalar_params(node: Any, sql: str, *params: Any) -> Any:
    with node.connect() as conn:
        rows = conn.execute(sql, *params)
    if not rows:
        return None
    return _first_column(rows[0])


def wait_until(
    predicate: Callable[[], bool],
    *,
    timeout: float = DEFAULT_TIMEOUT,
    interval: float = DEFAULT_POLL_INTERVAL,
    message: str,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise AssertionError(message)


def replica_hosts_local_shard(
    replica: "ReplicaHandle",
    shard: RelationRef,
    *,
    root_table: RelationRef,
) -> bool:
    return bool(
        query_scalar_params(
            replica.node,
            """
            WITH refs AS (
                SELECT
                    to_regclass(format('%I.%I', %s::text, %s::text)) AS shard_regclass,
                    to_regclass(format('%I.%I', %s::text, %s::text)) AS root_regclass
            )
            SELECT
                shard_regclass IS NOT NULL
                AND root_regclass IS NOT NULL
                AND EXISTS (
                    SELECT 1
                    FROM pg_partition_tree(root_regclass) AS t
                    WHERE t.relid = shard_regclass
                )
                AND EXISTS (
                    SELECT 1
                    FROM pg_subscription_rel
                    WHERE srrelid = shard_regclass
                )
            FROM refs
            """,
            shard.schema_name,
            shard.table_name,
            root_table.schema_name,
            root_table.table_name,
        )
    )


def assert_shard_hosting_replica_count(
    replicas: Iterable["ReplicaHandle"],
    shard: RelationRef,
    *,
    root_table: RelationRef,
    expected_count: int,
) -> None:
    hosting_replicas = [
        replica.name
        for replica in replicas
        if replica_hosts_local_shard(replica, shard, root_table=root_table)
    ]
    assert len(hosting_replicas) == expected_count, (
        f"expected shard {shard} to be hosted by {expected_count} replicas, "
        f"found {len(hosting_replicas)}: {hosting_replicas}"
    )


@dataclass(frozen=True, slots=True)
class ReplicaSpec:
    name: str
    availability_zone: str = "default"
    weight: int = 100
    refresh_seconds: float = 0.1
    member_role: str | None = None

    @property
    def login_role(self) -> str:
        return self.member_role or self.name

    @property
    def password(self) -> str:
        return f"{self.login_role}_password"


@dataclass(slots=True)
class ReplicaHandle:
    spec: ReplicaSpec
    node: Any
    username: str
    password: str
    group_id: str = DEFAULT_GROUP_ID

    @property
    def name(self) -> str:
        return self.spec.name

    @property
    def port(self) -> int:
        return self.node.port

    def execute(self, sql: str) -> Any:
        return self.node.execute(sql)

    def query_scalar(self, sql: str) -> Any:
        return query_scalar(self.node, sql)

    def configure_controller(
        self,
        *,
        master_port: int,
        host: str = "localhost",
        start_daemon: bool = True,
    ) -> None:
        self.execute(
            """
            SELECT pgwrh.configure_controller(
                host := {host},
                port := {port},
                username := {username},
                password := {password},
                start_daemon := {start_daemon},
                refresh_seconds := {refresh_seconds}
            )
            """.format(
                host=quote_literal(host),
                port=quote_literal(str(master_port)),
                username=quote_literal(self.username),
                password=quote_literal(self.password),
                start_daemon="true" if start_daemon else "false",
                refresh_seconds=self.spec.refresh_seconds,
            )
        )

    def table_row_count(self, table_name: str = "test.my_data") -> int:
        return int(self.query_scalar(f"SELECT count(*) FROM {table_name}"))


@dataclass(slots=True)
class MasterHandle:
    node: Any
    group_id: str = DEFAULT_GROUP_ID

    @property
    def port(self) -> int:
        return self.node.port

    def execute(self, sql: str) -> Any:
        return self.node.execute(sql)

    def query_scalar(self, sql: str) -> Any:
        return query_scalar(self.node, sql)

    def current_shards(self) -> list[RelationRef]:
        rows = self.execute(
            """
            SELECT DISTINCT schema_name, table_name
            FROM
                pgwrh.shard_assigned_host
                    JOIN pgwrh.replication_group USING (replication_group_id)
            WHERE
                replication_group_id = {group_id}
                AND version = current_version
            ORDER BY 1, 2
            """.format(group_id=quote_literal(self.group_id))
        )
        return [
            RelationRef(schema_name=schema_name, table_name=table_name)
            for schema_name, table_name in rows
        ]

    def load_seed(self, seed_sql: Path = MASTER_SEED_SQL) -> None:
        self.node.psql(filename=str(seed_sql))

    def create_replica_login(self, replica: ReplicaSpec) -> tuple[str, str]:
        username = replica.login_role
        password = replica.password
        self.execute(
            """
            CREATE USER {username}
            PASSWORD {password}
            REPLICATION
            IN ROLE test_replica
            """.format(
                username=quote_ident(username),
                password=quote_literal(password),
            )
        )
        return username, password

    def register_replica(self, replica: ReplicaSpec, node: Any) -> ReplicaHandle:
        username, password = self.create_replica_login(replica)
        args = [
            f"_replication_group_id := {quote_literal(self.group_id)}",
            f"_replica_id := {quote_literal(replica.name)}",
            "_host_name := 'localhost'",
            f"_port := {node.port}",
            f"_availability_zone := {quote_literal(replica.availability_zone)}",
            f"_weight := {replica.weight}",
        ]
        if replica.member_role is not None:
            args.append(
                f"_member_role := {quote_literal(replica.member_role)}::regrole"
            )

        self.execute(
            "SELECT pgwrh.add_replica(\n    " + ",\n    ".join(args) + "\n)"
        )

        handle = ReplicaHandle(
            spec=replica,
            node=node,
            username=username,
            password=password,
            group_id=self.group_id,
        )
        handle.configure_controller(master_port=self.port)
        return handle

    def start_rollout(self) -> None:
        self.execute(
            f"SELECT pgwrh.start_rollout({quote_literal(self.group_id)})"
        )

    def commit_rollout(self, *, keep_old_config: bool = False) -> None:
        self.execute(
            """
            SELECT pgwrh.commit_rollout(
                group_id := {group_id},
                keep_old_config := {keep_old_config}
            )
            """.format(
                group_id=quote_literal(self.group_id),
                keep_old_config="true" if keep_old_config else "false",
            )
        )

    def rollback_rollout(self, *, unlock: bool = True) -> None:
        self.execute(
            """
            SELECT pgwrh.rollback_rollout(
                _replication_group_id := {group_id},
                unlock := {unlock}
            )
            """.format(
                group_id=quote_literal(self.group_id),
                unlock="true" if unlock else "false",
            )
        )

    def replica_count(self) -> int:
        return int(
            self.query_scalar(
                """
                SELECT count(*)
                FROM pgwrh.replication_group_member
                WHERE replication_group_id = {group_id}
                """.format(group_id=quote_literal(self.group_id))
            )
        )

    def current_version(self) -> str:
        return str(
            self.query_scalar(
                """
                SELECT current_version
                FROM pgwrh.replication_group
                WHERE replication_group_id = {group_id}
                """.format(group_id=quote_literal(self.group_id))
            )
        )

    def config_version_count(self) -> int:
        return int(
            self.query_scalar(
                """
                SELECT count(*)
                FROM pgwrh.replication_group_config
                WHERE replication_group_id = {group_id}
                """.format(group_id=quote_literal(self.group_id))
            )
        )

    def target_version(self) -> str:
        return str(
            self.query_scalar(
                """
                SELECT target_version
                FROM pgwrh.replication_group
                WHERE replication_group_id = {group_id}
                """.format(group_id=quote_literal(self.group_id))
            )
        )

    def rollout_gap_counts(self, version: str | None = None) -> dict[str, int]:
        version = version or self.target_version()
        filters = (
            "replication_group_id = {group_id} AND version = {version}".format(
                group_id=quote_literal(self.group_id),
                version=quote_literal(version),
            )
        )
        return {
            "subscribed": int(
                self.query_scalar(
                    f"SELECT count(*) FROM pgwrh.missing_subscribed_shard WHERE {filters}"
                )
            ),
            "connected_local": int(
                self.query_scalar(
                    "SELECT count(*) FROM pgwrh.missing_connected_local_shard "
                    f"WHERE {filters}"
                )
            ),
            "connected_remote": int(
                self.query_scalar(
                    "SELECT count(*) FROM pgwrh.missing_connected_remote_shard "
                    f"WHERE {filters}"
                )
            ),
        }

    def wait_for_rollout_ready(
        self,
        *,
        expected_replicas: int,
        timeout: float = DEFAULT_TIMEOUT,
        interval: float = DEFAULT_POLL_INTERVAL,
    ) -> None:
        def ready() -> bool:
            if self.replica_count() != expected_replicas:
                return False
            return all(count == 0 for count in self.rollout_gap_counts().values())

        wait_until(
            ready,
            timeout=timeout,
            interval=interval,
            message=(
                f"Replica rollout for group {self.group_id} did not reach a ready state"
            ),
        )


@dataclass(slots=True)
class PgwrhCluster:
    master: MasterHandle
    node_factory: Callable[[str], Any]
    replicas: list[ReplicaHandle] = field(default_factory=list)

    def add_replica(self, replica: ReplicaSpec) -> ReplicaHandle:
        node = self.node_factory(replica.name)
        handle = self.master.register_replica(replica, node)
        self.replicas.append(handle)
        return handle

    def add_replicas(self, replicas: Iterable[ReplicaSpec]) -> list[ReplicaHandle]:
        return [self.add_replica(replica) for replica in replicas]

    def deploy(
        self,
        *,
        commit: bool = True,
        timeout: float = DEFAULT_TIMEOUT,
        interval: float = DEFAULT_POLL_INTERVAL,
    ) -> "PgwrhCluster":
        self.master.start_rollout()
        self.master.wait_for_rollout_ready(
            expected_replicas=len(self.replicas),
            timeout=timeout,
            interval=interval,
        )
        if commit:
            self.master.commit_rollout()
        return self

    def query_results(self, sql: str) -> list[Any]:
        return [
            self.master.execute(sql),
            *(replica.execute(sql) for replica in self.replicas),
        ]

    def assert_query_results_match(self, sql: str) -> None:
        baseline = self.master.execute(sql)
        for replica in self.replicas:
            assert replica.execute(sql) == baseline
