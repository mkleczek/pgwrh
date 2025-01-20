CREATE ROLE test_replica;

CREATE SCHEMA IF NOT EXISTS test;
CREATE SCHEMA IF NOT EXISTS test_shards;
ALTER SCHEMA test_shards OWNER TO test_replica;

--GRANT USAGE ON SCHEMA test_shards TO test_replica;
--ALTER DEFAULT PRIVILEGES IN SCHEMA test_shards GRANT SELECT ON TABLES TO test_replica;

CREATE TABLE test.my_data (col1 text, col2 text, col3 date) PARTITION BY RANGE (col3);

CREATE OR REPLACE PROCEDURE test.create_year_shard(year int) LANGUAGE plpgsql AS
$$
DECLARE
    r record;
BEGIN
    EXECUTE format('CREATE TABLE test.my_data_%1$s PARTITION OF test.my_data FOR VALUES FROM (make_date(%1$s, 1, 1)) TO (make_date(%2$s, 1, 1))  PARTITION BY HASH (col2)', year, year + 1);
    FOR r IN
        SELECT
            format('CREATE TABLE test_shards.my_data_%1$s_%2$s PARTITION OF test.my_data_%1$s (PRIMARY KEY (col1)) FOR VALUES WITH (MODULUS 16, REMAINDER %2$s)', year, rem) stmt,
            format('ALTER TABLE test_shards.my_data_%1$s_%2$s OWNER TO test_replica', year, rem) own
        FROM generate_series(0, 15) rem
    LOOP
        EXECUTE r.stmt;
        EXECUTE r.own;
    END LOOP;
END
$$;

DO
$$
DECLARE
    year int;
BEGIN
    FOR year IN 2022..2025 LOOP
        CALL test.create_year_shard(year);
    END LOOP;
END
$$;

CREATE OR REPLACE PROCEDURE test.insert_test_data(years VARIADIC int[]) LANGUAGE sql AS
$$
INSERT INTO test.my_data
SELECT 'col1: ' || n, 'col2: ' || n, make_date(year, 1, 1) + n FROM unnest(years) AS year, generate_series(1, 300, 1) as n;
$$;

CALL test.insert_test_data(2022, 2023, 2024, 2025);

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
