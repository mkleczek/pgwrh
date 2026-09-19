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
    quote_ident,
)

POSTGRES_CONF = (
    "max_worker_processes = 100",
    "max_replication_slots = 100",
    "max_wal_senders = 100",
)
XPG_EXTENSION_PATHS_ENV = "PGWRH_TEST_EXT_PATHS"
POSTGRES_BIN_DIR_ENV = "PGWRH_TEST_BIN_DIR"
DEBUG_ENV = "PGWRH_TEST_DEBUG"
REPO_ROOT = Path(__file__).resolve().parents[2]
LOCAL_EXTENSION_ROOT = REPO_ROOT / ".build" / "testgres-ext"
NON_PARTITIONED_WORKAROUND_MASTER_SEED_SQL = Path(__file__).with_name(
    "master_non_partitioned_workaround.sql"
)
MISSING_EXTENSION_MESSAGE = (
    "pgwrh test extension files are not staged. "
    "Run `make testgres-ext` or set PGWRH_TEST_EXT_PATHS to a directory "
    "containing `extension/pgwrh.control`."
)


class DatabaseNode:
    """Select a database while retaining the testgres node's lifecycle and tools."""

    def __init__(self, node, dbname):
        self.node = node
        self.dbname = dbname

    def __getattr__(self, name):
        return getattr(self.node, name)

    def execute(self, query, **kwargs):
        return self.node.execute(query=query, dbname=self.dbname, **kwargs)

    def connect(self, **kwargs):
        return self.node.connect(dbname=self.dbname, **kwargs)

    def psql(self, **kwargs):
        return self.node.psql(dbname=self.dbname, **kwargs)


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

        def build(name: str, *, install_extension: bool = True, dbname: str | None = None):
            node = get_new_node(name, bin_dir=os.environ.get(POSTGRES_BIN_DIR_ENV))
            stack.enter_context(node)
            node.init(allow_logical=True)
            # Managed source identities always authenticate using SCRAM. Keep
            # test administration and replication transport on the fixture's
            # normal rules, but exercise real authentication on every FDW route.
            hba = Path(node.data_dir) / 'pg_hba.conf'
            hba.write_text('host all /^pgwrh_[0-9a-f]{32}$ all scram-sha-256\n' + hba.read_text())
            # Distribution builds may default to /run/postgresql, which is not
            # writable by the unprivileged test runner.
            node.append_conf("unix_socket_directories = " + _quote_conf_value(node.base_dir))
            for line in POSTGRES_CONF:
                node.append_conf(line)
            extension_paths = _extension_paths()
            if extension_paths:
                node.append_conf(
                    "dynamic_library_path = "
                    + _quote_conf_value(f"{extension_paths}:$libdir")
                )
                node.append_conf(
                    "extension_control_path = "
                    + _quote_conf_value(f"{extension_paths}:$system")
                )
            node.start()
            if dbname is not None:
                node.execute(f'CREATE DATABASE {quote_ident(dbname)}')
                node = DatabaseNode(node, dbname)
            if os.environ.get(DEBUG_ENV):
                print("bin_dir:", node.bin_dir)
                print("dynamic_library_path:", node.execute("SHOW dynamic_library_path")[0][0])
                print(
                    "extension_control_path:",
                    node.execute("SHOW extension_control_path")[0][0],
                )
            if not install_extension:
                return node
            node.execute("CREATE EXTENSION pgwrh CASCADE")
            assert node.execute("SELECT extname FROM pg_extension WHERE extname='postgres_fdw'") == []
            assert node.execute("""SELECT f.fdwname FROM pg_foreign_server s
                JOIN pg_foreign_data_wrapper f ON f.oid = s.srvfdw
                WHERE s.srvname = 'replica_controller'""") == [("pgwrh_fdw",)]
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
