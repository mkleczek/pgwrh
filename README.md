# pgwrh

An extension implementing scale out sharding for PostgreSQL based on logical replication and postgres_fdw.

# Installation

## Prerequisites

| Name | Version |
| :---- | :---: |
| PostgreSQL | 16+ |
| pg_background | 1.2+ |

## Extension installation

Clone the Git repository.
```sh
git clone https://github.com/mkleczek/pgwrh.git
```
Install the extension.
```sh
cd pgwrh
make install
```
Create extension in PostgreSQL database.
```sh
psql -c "CREATE EXTENSION pgwrh CASCADE"
```

# Usage

## On master server

### Create your sharded table partitioning hierarchy

The below example would create a two-level partition hierarchy for `test.my_table`:
* First level by dates in `col3` (split by year)
* Second level by hash on `col2`
```pgsql
CREATE SCHEMA IF NOT EXISTS test;

CREATE TABLE test.my_data (col1 text, col2 text, col3 date) PARTITION BY RANGE (col3);
CREATE TABLE test.my_data_2023 PARTITION OF parent FOR VALUES FROM (make_date(2023, 1, 1)) TO (make_date(2024, 1, 1));
CREATE TABLE test.my_data_2024 PARTITION OF parent FOR VALUES FROM (make_date(2024, 1, 1)) TO (make_date(2025, 1, 1));
CREATE TABLE test.my_data_2025 PARTITION OF parent FOR VALUES FROM (make_date(2025, 1, 1)) TO (make_date(2026, 1, 1));

CREATE SCHEMA IF NOT EXISTS test_shards;
DO$$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT
            format('CREATE TABLE test_shards.my_data_%1$s_%2$s PARTITION OF test.my_data_%1$s (PRIMARY KEY (col1)) FOR VALUES WITH (MODULUS 16, REMAINDER %2$s)', year, rem) stmt
        FROM generate_series(2023, 2025) year, generate_series(0, 15) rem
    LOOP
        EXECUTE r.stmt;
    END LOOP;
END$$;
```

That gives 48 (16 * 3) shards in total.

**Note** that there are no specific requirements for the partitioning hierarchy and any partitioned table can be sharded - the above is only for ilustration purposes.

### Create a replication group

Example:
```pgsql
INSERT INTO pgwrh.replication_group (replication_group_id, username, password)
VALUES ('cluster_01', 'cluster01', 'cluster01password');
```

### Specify what tables to replicate for replication group

Example below would configure 2 copies of every partition of `test.my_data` except partitions of `test.my_data_2024` which will be copied to 4 replicas.
```pgsql
WITH st(schema_name, table_name, replica_count) AS (
    VALUES
        ('test', 'my_data', 2),
        ('test', 'my_data_2024', 4)
)
INSERT INTO pgwrh.sharded_table (replication_group_id, sharded_table_schema, sharded_table_name, replica_count)
SELECT
    'cluster_01', schema_name, table_name, replica_count
FROM
    st;
```

### Configure roles and user accounts for replicas

(Optional) Create a role for you cluster replicas and grant rights to SELECT from shards.
```pgsql
CREATE ROLE cluster_01_replica;

GRANT SELECT ON ALL TABLES IN SCHEMA test_shards TO cluster_01_replica;
```

Configure replicas:
```pgsql
CREATE USER c01r01 PASSWORD 'c01r01Password' REPLICATION IN ROLE cluster_01_replica;
SELECT pgwrh.add_shard_host('cluster_01', 'c01r01', 'replica01.cluster01.myorg', 5432);
```

## On every replica

Make sure `pgwrh` extension is installed.

### Configure connection to master server

Call `configure_controller` function providing username and password of this replica account created on master.
```pgsql
SELECT configure_controller(
    host => 'master.myorg',
    port => '5432',
    username => 'cr01r01', -- same as above
    password => 'c01r01Password' -- same as above
);
```

Replica should create the partition hierarchy and configure logica replication of shards assigned to it by *master*.