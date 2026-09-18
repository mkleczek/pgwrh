import pytest

from test.pgwrh.conftest import postgres_node_factory  # noqa: F401


@pytest.fixture
def controller(postgres_node_factory):
    node = postgres_node_factory('ui_controller')
    node.execute('CREATE EXTENSION pgwrh_ui')
    return node


@pytest.fixture
def configured(controller):
    controller.execute("""
        CREATE ROLE replica1 LOGIN REPLICATION;
        CREATE ROLE replica2 LOGIN REPLICATION;
        CREATE SCHEMA data;
        CREATE TABLE data.root (id int) PARTITION BY RANGE (id);
        CREATE TABLE data.one PARTITION OF data.root FOR VALUES FROM (0) TO (10);
        CREATE TABLE data.two PARTITION OF data.root FOR VALUES FROM (10) TO (20);
        SELECT pgwrh.create_replica_cluster('g1');
        SELECT pgwrh.add_replica('g1', 'r1', 'r1.invalid', 5432, 'replica1', 'a');
        SELECT pgwrh.add_replica('g1', 'r2', 'r2.invalid', 5432, 'replica2', 'b');
        INSERT INTO pgwrh.sharded_table
            (replication_group_id, sharded_table_schema, sharded_table_name, replication_factor)
        VALUES ('g1', 'data', 'root', 100);
    """)
    return controller
