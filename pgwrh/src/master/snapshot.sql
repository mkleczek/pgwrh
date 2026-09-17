-- name: master-snapshot
-- requires: master-implementation-views master-placement

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

CREATE FUNCTION replication_group_config_snapshot(_replication_group_id text, _version config_version)
    RETURNS void
    SET SEARCH_PATH FROM CURRENT
    LANGUAGE sql
    AS
$$
    WITH shard_snapshot AS MATERIALIZED (
        SELECT _replication_group_id AS replication_group_id, _version AS version, s.*
        FROM "@extschema@".shard_placement_inputs(_replication_group_id, _version) s
    ),
    saved_shard AS (
        INSERT INTO shard
            (replication_group_id, version, schema_name, table_name, sharded_table_schema, sharded_table_name)
        SELECT
            replication_group_id,
            version,
            schema_name,
            table_name,
            sharded_table_schema,
            sharded_table_name
        FROM
            shard_snapshot
    ),
    saved_index AS (
        INSERT INTO shard_assigned_index
            (replication_group_id, version, schema_name, table_name, index_template_schema, index_template_table_name, index_template_name)
        SELECT
            replication_group_id, version, ss.schema_name, ss.table_name, t.index_template_schema, t.index_template_table_name, t.index_template_name
        FROM
            shard_snapshot ss
                JOIN shard_index_template t USING (replication_group_id, version)
                JOIN pg_namespace itn ON itn.nspname = t.index_template_schema
                JOIN pg_class itc ON itc.relnamespace = itn.oid AND itc.relname = t.index_template_table_name
        WHERE
            itc.oid = ANY (SELECT * FROM pg_partition_ancestors(ss.shard_oid))
    )
    INSERT INTO shard_assigned_host (replication_group_id, version, schema_name, table_name, availability_zone, host_id)
    SELECT
        replication_group_id,
        version,
        schema_name,
        table_name,
        availability_zone,
        host_id
    FROM
        shard_snapshot s
            CROSS JOIN LATERAL "@extschema@".select_shard_hosts(
                s.replication_group_id, s.version, s.sharding_key_value, s.replica_count,
                s.min_replica_count_per_availability_zone,
                s.min_replica_count_after_az_failure, s.az_affinity) h
$$;
