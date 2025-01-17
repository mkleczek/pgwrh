CREATE ROLE test_replica;
CREATE USER h1 PASSWORD 'h1' REPLICATION IN ROLE test_replica;
CREATE USER h2 PASSWORD 'h2' REPLICATION IN ROLE test_replica;
CREATE USER h3 PASSWORD 'h3' REPLICATION IN ROLE test_replica;
CREATE USER h4 PASSWORD 'h4' REPLICATION IN ROLE test_replica;


CREATE SCHEMA IF NOT EXISTS test;
CREATE SCHEMA IF NOT EXISTS test_shards;
ALTER SCHEMA test_shards OWNER TO test_replica;

--GRANT USAGE ON SCHEMA test_shards TO test_replica;
--ALTER DEFAULT PRIVILEGES IN SCHEMA test_shards GRANT SELECT ON TABLES TO test_replica;

CREATE TABLE test.my_data (col1 text, col2 text, col3 date) PARTITION BY RANGE (col3);

CREATE TABLE test.my_data_2025 PARTITION OF test.my_data FOR VALUES FROM (make_date(2025, 1, 1)) TO (make_date(2026, 1, 1)) PARTITION BY HASH (col2);
CREATE TABLE test.my_data_2024 PARTITION OF test.my_data FOR VALUES FROM (make_date(2024, 1, 1)) TO (make_date(2025, 1, 1)) PARTITION BY HASH (col2);
CREATE TABLE test.my_data_2023 PARTITION OF test.my_data FOR VALUES FROM (make_date(2023, 1, 1)) TO (make_date(2024, 1, 1))  PARTITION BY HASH (col2);
CREATE TABLE test.my_data_2022 PARTITION OF test.my_data FOR VALUES FROM (make_date(2022, 1, 1)) TO (make_date(2023, 1, 1))  PARTITION BY HASH (col2);

DO
$$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT
            format('CREATE TABLE test_shards.my_data_%1$s_%2$s PARTITION OF test.my_data_%1$s (PRIMARY KEY (col1)) FOR VALUES WITH (MODULUS 16, REMAINDER %2$s)', year, rem) stmt,
            format('ALTER TABLE test_shards.my_data_%1$s_%2$s OWNER TO test_replica', year, rem) own
        FROM generate_series(2022, 2025) year, generate_series(0, 15) rem
    LOOP
        EXECUTE r.stmt;
        EXECUTE r.own;
    END LOOP;
END
$$;

INSERT INTO test.my_data
SELECT 'col1: ' || n, 'col2: ' || n, make_date(2022, 1, 1) + n FROM generate_series(1, 1000, 1) as n;

INSERT INTO pgwrh.replication_group
        (replication_group_id, username, password)
    VALUES
        ('g1', 'u', 'p');
INSERT INTO pgwrh.sharded_table
        (replication_group_id, sharded_table_schema, sharded_table_name, replica_count)
    VALUES
        ('g1', 'test', 'my_data', 1),
        ('g1', 'test', 'my_data_2025', 4),
        ('g1', 'test', 'my_data_2024', 2);

-- SELECT pgwrh.add_shard_host('g1', 'h1', 'localhost', 5533);
-- SELECT pgwrh.add_shard_host('g1', 'h2', 'localhost', 5534);
-- SELECT pgwrh.add_shard_host('g1', 'h3', 'localhost', 5535);
-- SELECT pgwrh.add_shard_host('g1', 'h4', 'localhost', 5536);
