CREATE ROLE test_replica;

CREATE SCHEMA IF NOT EXISTS test;

GRANT USAGE ON SCHEMA test TO test_replica;
ALTER DEFAULT PRIVILEGES IN SCHEMA test GRANT SELECT ON TABLES TO test_replica;

CREATE TABLE parent (col1 text, col2 text) PARTITION BY HASH (col2);
CREATE TABLE child0 PARTITION OF parent (PRIMARY KEY (col1)) FOR VALUES WITH (MODULUS 4, REMAINDER 0);
CREATE TABLE child1 PARTITION OF parent (PRIMARY KEY (col1)) FOR VALUES WITH (MODULUS 4, REMAINDER 1);
CREATE TABLE child2 PARTITION OF parent (PRIMARY KEY (col1)) FOR VALUES WITH (MODULUS 4, REMAINDER 2);
CREATE TABLE child3 PARTITION OF parent (PRIMARY KEY (col1)) FOR VALUES WITH (MODULUS 4, REMAINDER 3);

CREATE USER h1 PASSWORD 'h1' REPLICATION IN ROLE test_replica;
CREATE USER h2 PASSWORD 'h2' REPLICATION IN ROLE test_replica;

INSERT INTO replication_group (replication_group_id, username, password) VALUES ('g1', 'u', 'p');
INSERT INTO sharded_table (replication_group_id, sharded_table_schema, sharded_table_name, replica_count) VALUES ('g1', 'test', 'parent', 1);

SELECT add_shard_host('g1', 'h1', 'localhost', 5533);
SELECT add_shard_host('g1', 'h2', 'localhost', 5534);
