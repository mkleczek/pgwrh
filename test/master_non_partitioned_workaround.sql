-- pgwrh
-- Copyright (C) 2024  Michal Kleczek

-- This program is free software: you can redistribute it and/or modify
-- it under the terms of the GNU Affero General Public License as published by
-- the Free Software Foundation, either version 3 of the License, or
-- (at your option) any later version.

-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU Affero General Public License for more details.

-- You should have received a copy of the GNU Affero General Public License
-- along with this program.  If not, see <http://www.gnu.org/licenses/>.

CREATE ROLE test_replica;

CREATE SCHEMA IF NOT EXISTS test;
CREATE SCHEMA IF NOT EXISTS test_shards;
ALTER SCHEMA test_shards OWNER TO test_replica;

-- Workaround for non-partitioned table support:
-- use an existing column as the partition key and keep all rows
-- in one DEFAULT leaf.
CREATE TABLE test.non_partitioned_data (
    id int NOT NULL,
    payload text NOT NULL,
    happened_on date NOT NULL
) PARTITION BY LIST (id);

CREATE TABLE test_shards.non_partitioned_data_default
PARTITION OF test.non_partitioned_data
DEFAULT;

ALTER TABLE test_shards.non_partitioned_data_default OWNER TO test_replica;

INSERT INTO test.non_partitioned_data
SELECT
    n,
    'payload-' || n,
    make_date(2025, 1, 1) + ((n - 1) % 28)
FROM generate_series(1, 32) AS n;

SELECT pgwrh.create_replica_cluster('g1');

INSERT INTO pgwrh.sharded_table
    (replication_group_id, sharded_table_schema, sharded_table_name, replication_factor)
VALUES
    ('g1', 'test', 'non_partitioned_data', 50);
