from __future__ import annotations


def test_shard_structure_exposes_structured_partition_metadata(
    deployed_two_replica_cluster,
):
    replica = deployed_two_replica_cluster.replicas[0]
    rows = replica.execute(
        """
        SELECT
            schema_name,
            table_name,
            level,
            parent_schema_name,
            parent_table_name,
            bound,
            parent_partkeydef,
            node_partkeydef,
            is_leaf,
            root_column_clause,
            local_constraint_clause
        FROM pgwrh.fdw_shard_structure
        WHERE
            (schema_name, table_name) IN (
                ('test', 'my_data'),
                ('test', 'my_data_2022'),
                ('test_shards', 'my_data_2022_0')
            )
        ORDER BY level, schema_name, table_name
        """
    )

    structure = {
        (schema_name, table_name): {
            "level": level,
            "parent_schema_name": parent_schema_name,
            "parent_table_name": parent_table_name,
            "bound": bound,
            "parent_partkeydef": parent_partkeydef,
            "node_partkeydef": node_partkeydef,
            "is_leaf": is_leaf,
            "root_column_clause": root_column_clause,
            "local_constraint_clause": local_constraint_clause,
        }
        for (
            schema_name,
            table_name,
            level,
            parent_schema_name,
            parent_table_name,
            bound,
            parent_partkeydef,
            node_partkeydef,
            is_leaf,
            root_column_clause,
            local_constraint_clause,
        ) in rows
    }

    root = structure[("test", "my_data")]
    assert root["level"] == 0
    assert root["parent_schema_name"] is None
    assert root["parent_table_name"] is None
    assert root["bound"] is None
    assert root["parent_partkeydef"] is None
    assert root["node_partkeydef"] == "RANGE (col3)"
    assert root["is_leaf"] is False
    assert "col1 text" in root["root_column_clause"]
    assert "col2 text" in root["root_column_clause"]
    assert "col3 date" in root["root_column_clause"]
    assert root["local_constraint_clause"] is None

    year_partition = structure[("test", "my_data_2022")]
    assert year_partition["level"] == 1
    assert year_partition["parent_schema_name"] == "test"
    assert year_partition["parent_table_name"] == "my_data"
    assert year_partition["bound"].startswith("FOR VALUES FROM")
    assert year_partition["parent_partkeydef"] == "RANGE (col3)"
    assert year_partition["node_partkeydef"] == "HASH (col2)"
    assert year_partition["is_leaf"] is False
    assert year_partition["root_column_clause"] is None
    assert year_partition["local_constraint_clause"] is None

    leaf = structure[("test_shards", "my_data_2022_0")]
    assert leaf["level"] == 2
    assert leaf["parent_schema_name"] == "test"
    assert leaf["parent_table_name"] == "my_data_2022"
    assert leaf["bound"].startswith("FOR VALUES WITH")
    assert leaf["parent_partkeydef"] == "HASH (col2)"
    assert leaf["node_partkeydef"] is None
    assert leaf["is_leaf"] is True
    assert leaf["root_column_clause"] is None
    assert "PRIMARY KEY (col1)" in leaf["local_constraint_clause"]


def test_replica_bootstrap_uses_structured_slot_first_partition_tree(
    deployed_two_replica_cluster,
):
    replica = deployed_two_replica_cluster.replicas[0]

    columns = [
        row[0]
        for row in replica.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE
                table_schema = 'pgwrh'
                AND table_name = 'fdw_shard_structure'
            ORDER BY ordinal_position
            """
        )
    ]
    assert columns == [
        "schema_name",
        "table_name",
        "level",
        "parent_schema_name",
        "parent_table_name",
        "bound",
        "parent_partkeydef",
        "node_partkeydef",
        "is_leaf",
        "root_column_clause",
        "local_constraint_clause",
    ]

    rows = replica.execute(
        """
        SELECT
            relid::regclass::text,
            parentrelid::regclass::text,
            level
        FROM pg_partition_tree('test.my_data')
        WHERE relid IN (
            'test_slot.my_data_2022'::regclass,
            'test.my_data_2022'::regclass,
            'test_shards_slot.my_data_2022_0'::regclass
        )
        ORDER BY level, relid::regclass::text
        """
    )
    assert rows == [
        ("test_slot.my_data_2022", "test.my_data", 1),
        ("test.my_data_2022", "test_slot.my_data_2022", 2),
        ("test_shards_slot.my_data_2022_0", "test.my_data_2022", 3),
    ]

    relations = {
        row[0]
        for row in replica.execute(
            """
            SELECT to_regclass(name)::text
            FROM unnest(ARRAY[
                'test_template.my_data_2022',
                'test_shards_template.my_data_2022_0',
                'test_shield.my_data',
                'test_shield.my_data_2022',
                'test_shards_shield.my_data_2022_0'
            ]) AS t(name)
            """
        )
    }
    assert relations == {
        "test_template.my_data_2022",
        "test_shards_template.my_data_2022_0",
        "test_shield.my_data",
        "test_shield.my_data_2022",
        "test_shards_shield.my_data_2022_0",
    }
