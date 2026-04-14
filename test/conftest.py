from __future__ import annotations

import os
from contextlib import ExitStack
from pathlib import Path

import pytest
from testgres import get_new_node, scoped_config

from .pgwrh_testkit import (
    DEFAULT_GROUP_ID,
    MASTER_SEED_SQL,
    MasterHandle,
    PgwrhCluster,
    ReplicaSpec,
)

POSTGRES_CONF = (
    "max_worker_processes = 100",
    "max_replication_slots = 100",
    "max_wal_senders = 100",
)
XPG_EXTENSION_PATHS_ENV = "PGWRH_TEST_EXT_PATHS"
POSTGRES_BIN_DIR_ENV = "PGWRH_TEST_BIN_DIR"
DEBUG_ENV = "PGWRH_TEST_DEBUG"
REPO_ROOT = Path(__file__).resolve().parent.parent
LOCAL_EXTENSION_ROOT = REPO_ROOT / ".build" / "testgres-ext"
NON_PARTITIONED_WORKAROUND_MASTER_SEED_SQL = Path(__file__).with_name(
    "master_non_partitioned_workaround.sql"
)
MISSING_EXTENSION_MESSAGE = (
    "pgwrh test extension files are not staged. "
    "Run `make testgres-ext` or set PGWRH_TEST_EXT_PATHS to a directory "
    "containing `extension/pgwrh.control`."
)


def _extension_paths() -> str:
    configured_root = os.environ.get(XPG_EXTENSION_PATHS_ENV)
    extension_root = Path(configured_root) if configured_root else LOCAL_EXTENSION_ROOT
    control_path = extension_root / "extension" / "pgwrh.control"
    if not control_path.exists():
        raise RuntimeError(MISSING_EXTENSION_MESSAGE)
    return str(extension_root)


def _quote_conf_value(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _build_master(
    postgres_node_factory,
    *,
    name: str,
    seed_sql: Path,
) -> MasterHandle:
    node = postgres_node_factory(name)
    master = MasterHandle(node=node, group_id=DEFAULT_GROUP_ID)
    master.load_seed(seed_sql)
    return master


def _build_cluster_factory(master: MasterHandle, postgres_node_factory):
    def build(
        replicas: tuple[ReplicaSpec, ...] | list[ReplicaSpec],
        *,
        deploy: bool = False,
        commit: bool = True,
    ) -> PgwrhCluster:
        cluster = PgwrhCluster(master=master, node_factory=postgres_node_factory)
        cluster.add_replicas(replicas)
        if deploy:
            cluster.deploy(commit=commit)
        return cluster

    return build


@pytest.fixture
def postgres_node_factory():
    with ExitStack() as stack:
        stack.enter_context(scoped_config(use_python_logging=True))

        def build(name: str):
            node = get_new_node(name, bin_dir=os.environ.get(POSTGRES_BIN_DIR_ENV))
            stack.enter_context(node)
            node.init(allow_logical=True)
            for line in POSTGRES_CONF:
                node.append_conf(line)
            extension_paths = _extension_paths()
            if extension_paths:
                node.append_conf(
                    "dynamic_library_path = "
                    + _quote_conf_value(f"$libdir:{extension_paths}")
                )
                node.append_conf(
                    "extension_control_path = "
                    + _quote_conf_value(f"$system:{extension_paths}")
                )
            node.start()
            if os.environ.get(DEBUG_ENV):
                print("bin_dir:", node.bin_dir)
                print("dynamic_library_path:", node.execute("SHOW dynamic_library_path")[0][0])
                print(
                    "extension_control_path:",
                    node.execute("SHOW extension_control_path")[0][0],
                )
            node.execute("CREATE EXTENSION pgwrh CASCADE")
            return node

        yield build


@pytest.fixture
def master(postgres_node_factory) -> MasterHandle:
    return _build_master(
        postgres_node_factory,
        name="master",
        seed_sql=MASTER_SEED_SQL,
    )


@pytest.fixture
def non_partitioned_workaround_master(postgres_node_factory) -> MasterHandle:
    return _build_master(
        postgres_node_factory,
        name="master_non_partitioned_workaround",
        seed_sql=NON_PARTITIONED_WORKAROUND_MASTER_SEED_SQL,
    )


@pytest.fixture
def cluster_factory(master, postgres_node_factory):
    return _build_cluster_factory(master, postgres_node_factory)


@pytest.fixture
def non_partitioned_workaround_cluster_factory(
    non_partitioned_workaround_master,
    postgres_node_factory,
):
    return _build_cluster_factory(
        non_partitioned_workaround_master,
        postgres_node_factory,
    )


@pytest.fixture
def two_replica_specs() -> tuple[ReplicaSpec, ReplicaSpec]:
    return (
        ReplicaSpec("replica1"),
        ReplicaSpec("replica2"),
    )


@pytest.fixture
def three_replica_specs() -> tuple[ReplicaSpec, ReplicaSpec, ReplicaSpec]:
    return (
        ReplicaSpec("replica1"),
        ReplicaSpec("replica2"),
        ReplicaSpec("replica3"),
    )


@pytest.fixture
def multi_az_replica_specs() -> tuple[ReplicaSpec, ReplicaSpec, ReplicaSpec]:
    return (
        ReplicaSpec("replica1", availability_zone="az-a"),
        ReplicaSpec("replica2", availability_zone="az-b"),
        ReplicaSpec("replica3", availability_zone="az-c"),
    )


@pytest.fixture
def two_replica_cluster(cluster_factory, two_replica_specs) -> PgwrhCluster:
    return cluster_factory(two_replica_specs)


@pytest.fixture
def deployed_two_replica_cluster(cluster_factory, two_replica_specs) -> PgwrhCluster:
    return cluster_factory(two_replica_specs, deploy=True, commit=True)


@pytest.fixture
def three_replica_cluster(cluster_factory, three_replica_specs) -> PgwrhCluster:
    return cluster_factory(three_replica_specs)


@pytest.fixture
def deployed_three_replica_cluster(cluster_factory, three_replica_specs) -> PgwrhCluster:
    return cluster_factory(three_replica_specs, deploy=True, commit=True)
