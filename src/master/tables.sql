-- name: tables
-- requires: common

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

CREATE TYPE config_version AS ENUM ('FLIP', 'FLOP');
COMMENT ON TYPE config_version IS
'A FLIP/FLAP enum to use as configuration version identifier.';

CREATE TABLE  replication_group (
    replication_group_id text NOT NULL PRIMARY KEY,
    current_version config_version NOT NULL DEFAULT 'FLIP',
    target_version config_version NOT NULL DEFAULT 'FLIP',
    seq_number int NOT NULL DEFAULT 0
);
COMMENT ON TABLE replication_group IS
'Represents a specific cluster (replica group) configuration.
A single sever may be a source of data for multiple groups of replicas.
Each group may have different configuration, in particular:
* what tables should be sharded
* number of desired copies per shard
* member servers and shard hosts topology
';
COMMENT ON COLUMN replication_group.replication_group_id IS
'Unique identifier of a replication group.';
COMMENT ON COLUMN replication_group.current_version IS
'Identifier of currently deployed configuration version.';
COMMENT ON COLUMN replication_group.target_version IS
'Identifier of pending configuration version that is currently being deployed.';

CREATE TABLE replication_group_lock (
    replication_group_id text NOT NULL PRIMARY KEY REFERENCES replication_group(replication_group_id)
);
COMMENT ON TABLE replication_group_lock IS
$$
Having a lock on replication_group ensures accidental DELETE on a group cannot happen.

To delete a replication group it is necessary to delete replication_group_lock first.
$$;

CREATE TABLE  replication_group_config (
    replication_group_id text NOT NULL REFERENCES replication_group(replication_group_id) ON DELETE CASCADE,
    version config_version NOT NULL,

    min_replica_count int NOT NULL CHECK ( min_replica_count >= 0 ) DEFAULT 1,
    min_replica_count_per_availability_zone int NOT NULL CHECK ( min_replica_count_per_availability_zone >= 0 ) DEFAULT 1,

    PRIMARY KEY (replication_group_id, version)
);
COMMENT ON TABLE replication_group_config IS
'Represents a version of configuration of a replication group.

Each cluster (replication group) configuration is versioned to make sure
changes in cluster topology and shards configuration does not cause any downtime.

There may be two versions of configuration present at the same time.
A configuration version might be "pending" or "ready".

Version marked as "ready" (pending = false) is a configuration version that all
replicas installed and configured successfully. The shards assigned to replicas in that version are copied, indexed and available to use.

Version marked as "pending" (pending = true) is a configuration version that is under installaction/configuration by the replicas.

A replica keeps all shards from "ready" configuration even if a shard might be no longer assigned to it in "pending" configuration version.
';

CREATE TABLE replication_group_config_clone (
    replication_group_id text NOT NULL,
    source_version config_version NOT NULL,
    target_version config_version NOT NULL,

    PRIMARY KEY (replication_group_id, target_version),
    CHECK ( source_version <> target_version ),
    FOREIGN KEY (replication_group_id, source_version)
        REFERENCES replication_group_config(replication_group_id, version) ON DELETE CASCADE,
    FOREIGN KEY (replication_group_id, target_version)
        REFERENCES replication_group_config(replication_group_id, version) ON DELETE CASCADE
);

CREATE TABLE  replication_group_config_lock (
    replication_group_id text NOT NULL,
    version config_version NOT NULL,
    -- most probably it should be separate
    -- but for now it is simpler here
    seed uuid NOT NULL DEFAULT gen_random_uuid(),

    PRIMARY KEY (replication_group_id, version),
    FOREIGN KEY (replication_group_id, version)
        REFERENCES replication_group_config(replication_group_id, version)
        ON DELETE CASCADE
);

ALTER TABLE replication_group ADD FOREIGN KEY (replication_group_id, current_version)
REFERENCES replication_group_config_lock(replication_group_id, version) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE replication_group ADD FOREIGN KEY (replication_group_id, target_version)
REFERENCES replication_group_config_lock(replication_group_id, version) DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE  replication_group_member (
    replication_group_id text NOT NULL REFERENCES replication_group(replication_group_id),
    availability_zone text NOT NULL,
    host_id text NOT NULL,
    member_role text NOT NULL UNIQUE,
    same_zone_multiplier smallint NOT NULL CHECK ( same_zone_multiplier BETWEEN 1 AND 5 ) DEFAULT 2,

    subscribed_local_shards json NOT NULL DEFAULT '[]',
    indexes json NOT NULL DEFAULT '[]',
    connected_local_shards json NOT NULL DEFAULT '[]',
    connected_remote_shards json NOT NULL DEFAULT '[]',
    users json NOT NULL DEFAULT '[]',

    PRIMARY KEY (replication_group_id, availability_zone, host_id)
);
COMMENT ON TABLE replication_group_member IS
'Represents a node in a cluster (replication group).

A cluster consists of two types of nodes:

* shard hosts - nodes that replicate and serve data
* non replicating members - nodes that act only as proxies (ie. not hosting any shards)';

CREATE TABLE  shard_host (
    replication_group_id text NOT NULL,
    availability_zone text NOT NULL,
    host_id text NOT NULL,
    host_name text NOT NULL,
    port int NOT NULL CHECK ( port > 0 ),

    online boolean NOT NULL DEFAULT true,

    PRIMARY KEY (replication_group_id, availability_zone, host_id),
    FOREIGN KEY (replication_group_id, availability_zone, host_id)
        REFERENCES replication_group_member(replication_group_id, availability_zone, host_id)
        ON DELETE CASCADE,
    UNIQUE (host_name, port)
);
COMMENT ON TABLE shard_host IS
'Represents a data replicating node in a cluster (replication group).';
COMMENT ON COLUMN shard_host.online IS
'Shard host marked as offline is not going to receive any requests for data from other nodes.
It is still replicating shards assigned to it.

This flag is supposed to be used in situation when a particular node must be
temporarily disconnected from a cluster for maintenance purposes.';

CREATE TABLE  shard_host_weight (
    replication_group_id text NOT NULL,
    availability_zone text NOT NULL,
    host_id text NOT NULL,
    version config_version NOT NULL,
    weight int NOT NULL CHECK ( weight > 0 ),

    PRIMARY KEY (replication_group_id, availability_zone, host_id, version),
    FOREIGN KEY (replication_group_id, availability_zone, host_id)
        REFERENCES shard_host(replication_group_id, availability_zone, host_id)
        ON DELETE CASCADE,
    FOREIGN KEY (replication_group_id, version)
        REFERENCES replication_group_config(replication_group_id, version)
        ON DELETE CASCADE
);
COMMENT ON TABLE shard_host_weight IS
'Weight of a shard host in a specific configuration version';

CREATE TABLE sharded_table (
    replication_group_id text NOT NULL,
    sharded_table_schema text NOT NULL,
    sharded_table_name text NOT NULL,
    version config_version NOT NULL,
    replication_factor decimal(5, 2) NOT NULL CHECK ( replication_factor BETWEEN 0 AND 100 ),
    sharding_key_expression text NOT NULL DEFAULT 'SELECT $1 || $2',

    PRIMARY KEY (replication_group_id, sharded_table_schema, sharded_table_name, version),
    FOREIGN KEY (replication_group_id, version)
        REFERENCES replication_group_config(replication_group_id, version)
        ON DELETE CASCADE
);

CREATE TABLE shard_index_template (
    replication_group_id text NOT NULL,
    version config_version NOT NULL,
    index_template_schema text NOT NULL,
    index_template_table_name text NOT NULL,
    index_template_name name NOT NULL,
    index_template text NOT NULL,

    PRIMARY KEY (replication_group_id, version, index_template_schema, index_template_table_name, index_template_name),
    FOREIGN KEY (replication_group_id, version) REFERENCES replication_group_config(replication_group_id, version) ON DELETE CASCADE
);

-- SNAPSHOT

CREATE TABLE shard (
    replication_group_id text NOT NULL,
    version config_version NOT NULL,
    schema_name text NOT NULL,
    table_name text NOT NULL,
    sharded_table_schema text NOT NULL,
    sharded_table_name text NOT NULL,

    PRIMARY KEY (replication_group_id, version, schema_name, table_name),
    FOREIGN KEY (replication_group_id, version, sharded_table_schema, sharded_table_name)
        REFERENCES sharded_table (replication_group_id, version, sharded_table_schema, sharded_table_name),
    FOREIGN KEY (replication_group_id, version)
        REFERENCES replication_group_config_lock(replication_group_id, version)
        ON DELETE CASCADE
);

CREATE TABLE shard_assigned_host (
    replication_group_id text NOT NULL,
    version config_version NOT NULL,
    schema_name text NOT NULL,
    table_name text NOT NULL,
    availability_zone text NOT NULL,
    host_id text NOT NULL,

    PRIMARY KEY (replication_group_id, version, schema_name, table_name, availability_zone, host_id),
    FOREIGN KEY (replication_group_id, version, schema_name, table_name)
        REFERENCES shard(replication_group_id, version, schema_name, table_name)
        ON DELETE CASCADE,
    FOREIGN KEY (replication_group_id, version, availability_zone, host_id)
        REFERENCES shard_host_weight(replication_group_id, version, availability_zone, host_id)
        DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE shard_assigned_index (
    replication_group_id text NOT NULL,
    version config_version NOT NULL,
    schema_name text NOT NULL,
    table_name text NOT NULL,
    index_template_schema text NOT NULL,
    index_template_table_name text NOT NULL,
    index_template_name name NOT NULL,

    PRIMARY KEY (replication_group_id, version, schema_name, table_name, index_template_schema, index_template_table_name, index_template_name),
    FOREIGN KEY (replication_group_id, version, index_template_schema, index_template_table_name, index_template_name)
        REFERENCES shard_index_template(replication_group_id, version, index_template_schema, index_template_table_name, index_template_name)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (replication_group_id, version, schema_name, table_name)
        REFERENCES shard(replication_group_id, version, schema_name, table_name)
        ON DELETE CASCADE
);
