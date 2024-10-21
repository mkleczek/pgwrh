CREATE ROLE test_replica;

CREATE SCHEMA IF NOT EXISTS test;

GRANT USAGE ON SCHEMA test TO test_replica;
ALTER DEFAULT PRIVILEGES IN SCHEMA test GRANT SELECT ON TABLES TO test_replica;

CREATE TABLE test.parent (col1 text, col2 text) PARTITION BY HASH (col2);
CREATE TABLE test.child0 PARTITION OF parent (PRIMARY KEY (col1)) FOR VALUES WITH (MODULUS 16, REMAINDER 0);
CREATE TABLE test.child1 PARTITION OF parent (PRIMARY KEY (col1)) FOR VALUES WITH (MODULUS 16, REMAINDER 1);
CREATE TABLE test.child2 PARTITION OF parent (PRIMARY KEY (col1)) FOR VALUES WITH (MODULUS 16, REMAINDER 2);
CREATE TABLE test.child3 PARTITION OF parent (PRIMARY KEY (col1)) FOR VALUES WITH (MODULUS 16, REMAINDER 3);
CREATE TABLE test.child4 PARTITION OF parent (PRIMARY KEY (col1)) FOR VALUES WITH (MODULUS 16, REMAINDER 4);
CREATE TABLE test.child5 PARTITION OF parent (PRIMARY KEY (col1)) FOR VALUES WITH (MODULUS 16, REMAINDER 5);
CREATE TABLE test.child6 PARTITION OF parent (PRIMARY KEY (col1)) FOR VALUES WITH (MODULUS 16, REMAINDER 6);
CREATE TABLE test.child7 PARTITION OF parent (PRIMARY KEY (col1)) FOR VALUES WITH (MODULUS 16, REMAINDER 7);
CREATE TABLE test.child8 PARTITION OF parent (PRIMARY KEY (col1)) FOR VALUES WITH (MODULUS 16, REMAINDER 8);
CREATE TABLE test.child9 PARTITION OF parent (PRIMARY KEY (col1)) FOR VALUES WITH (MODULUS 16, REMAINDER 9);
CREATE TABLE test.child10 PARTITION OF parent (PRIMARY KEY (col1)) FOR VALUES WITH (MODULUS 16, REMAINDER 10);
CREATE TABLE test.child11 PARTITION OF parent (PRIMARY KEY (col1)) FOR VALUES WITH (MODULUS 16, REMAINDER 11);
CREATE TABLE test.child12 PARTITION OF parent (PRIMARY KEY (col1)) FOR VALUES WITH (MODULUS 16, REMAINDER 12);
CREATE TABLE test.child13 PARTITION OF parent (PRIMARY KEY (col1)) FOR VALUES WITH (MODULUS 16, REMAINDER 13);
CREATE TABLE test.child14 PARTITION OF parent (PRIMARY KEY (col1)) FOR VALUES WITH (MODULUS 16, REMAINDER 14);
CREATE TABLE test.child15 PARTITION OF parent (PRIMARY KEY (col1)) FOR VALUES WITH (MODULUS 16, REMAINDER 15);

CREATE USER h1 PASSWORD 'h1' REPLICATION IN ROLE test_replica;
CREATE USER h2 PASSWORD 'h2' REPLICATION IN ROLE test_replica;

INSERT INTO pgwrh.replication_group (replication_group_id, username, password) VALUES ('g1', 'u', 'p');
INSERT INTO pgwrh.sharded_table (replication_group_id, sharded_table_schema, sharded_table_name, replica_count)
VALUES ('g1', 'test', 'parent', 2);

SELECT add_shard_host('g1', 'h1', 'localhost', 5533);
SELECT add_shard_host('g1', 'h2', 'localhost', 5534);
SELECT add_shard_host('g1', 'h3', 'localhost', 5535);
SELECT add_shard_host('g1', 'h4', 'localhost', 5536);

DO$$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT
            format('CREATE TABLE test.my_data_%1$s_%2$s PARTITION OF test.my_data_%1$s (PRIMARY KEY (col1)) FOR VALUES WITH (MODULUS 16, REMAINDER %2$s)', year, rem) stmt
        FROM generate_series(2022, 2023) year, generate_series(0, 15) rem
    LOOP
        EXECUTE r.stmt;
    END LOOP;
END$$;