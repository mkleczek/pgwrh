\set ON_ERROR_STOP on
BEGIN;
CREATE SCHEMA IF NOT EXISTS demo;
CREATE TABLE IF NOT EXISTS demo.bootstrap (id boolean PRIMARY KEY DEFAULT true CHECK (id));
SELECT NOT EXISTS (SELECT FROM demo.bootstrap) AS initialize \gset
\if :initialize
CREATE ROLE demo_reader;
CREATE USER replica1 PASSWORD 'replica1_demo' REPLICATION IN ROLE demo_reader;
CREATE USER replica2 PASSWORD 'replica2_demo' REPLICATION IN ROLE demo_reader;
CREATE SCHEMA demo_shards AUTHORIZATION demo_reader;
CREATE TABLE demo.events (id integer NOT NULL, message text) PARTITION BY HASH (id);
CREATE TABLE demo_shards.events_0 PARTITION OF demo.events (PRIMARY KEY (id)) FOR VALUES WITH (MODULUS 4, REMAINDER 0);
CREATE TABLE demo_shards.events_1 PARTITION OF demo.events (PRIMARY KEY (id)) FOR VALUES WITH (MODULUS 4, REMAINDER 1);
CREATE TABLE demo_shards.events_2 PARTITION OF demo.events (PRIMARY KEY (id)) FOR VALUES WITH (MODULUS 4, REMAINDER 2);
CREATE TABLE demo_shards.events_3 PARTITION OF demo.events (PRIMARY KEY (id)) FOR VALUES WITH (MODULUS 4, REMAINDER 3);
ALTER TABLE demo_shards.events_0 OWNER TO demo_reader;
ALTER TABLE demo_shards.events_1 OWNER TO demo_reader;
ALTER TABLE demo_shards.events_2 OWNER TO demo_reader;
ALTER TABLE demo_shards.events_3 OWNER TO demo_reader;
INSERT INTO demo.events SELECT n, 'event ' || n FROM generate_series(1, 100) n;
SELECT pgwrh.create_replica_cluster('demo');
INSERT INTO pgwrh.sharded_table
    (replication_group_id, sharded_table_schema, sharded_table_name, replication_factor)
    VALUES ('demo', 'demo', 'events', 50);
SELECT pgwrh.add_replica('demo', 'replica1', 'replica1', 5432);
SELECT pgwrh.add_replica('demo', 'replica2', 'replica2', 5432);
INSERT INTO demo.bootstrap DEFAULT VALUES;
\endif
COMMIT;
