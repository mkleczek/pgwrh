-- name: replica-fdw

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

CREATE SERVER IF NOT EXISTS replica_controller FOREIGN DATA WRAPPER postgres_fdw OPTIONS (load_balance_hosts 'random');
CREATE USER MAPPING FOR PUBLIC SERVER replica_controller;

CREATE FOREIGN TABLE IF NOT EXISTS fdw_shard_assignment (
    schema_name text,
    table_name text,
    local boolean,
    shard_server_name text,
    host text,
    port text,
    dbname text,
    shard_server_user text,
    pubname text,
    connect_remote boolean,
    retained_shard_server_name text
)
SERVER replica_controller
OPTIONS (table_name 'shard_assignment');

CREATE FOREIGN TABLE IF NOT EXISTS fdw_shard_index (
    schema_name text,
    table_name text,
    index_name text,
    index_template text,
    optional boolean
)
SERVER replica_controller
OPTIONS (table_name 'shard_index');

CREATE FOREIGN TABLE IF NOT EXISTS fdw_shard_structure (
    schema_name text,
    table_name text,
    level int,
    create_table text
)
SERVER replica_controller
OPTIONS (table_name 'shard_structure');

CREATE FOREIGN TABLE fdw_replica_state (
    subscribed_local_shards json,
    indexes json,
    connected_local_shards json,
    connected_remote_shards json,
    users json
) SERVER replica_controller
OPTIONS (table_name 'replica_state');

CREATE FOREIGN TABLE fdw_credentials (
    username text,
    password text
) SERVER replica_controller
OPTIONS (table_name 'credentials');
