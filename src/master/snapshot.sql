-- name: master-snapshot
-- requires: master-implementation-views

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
    WITH sharded_pg_class AS (
        SELECT
            c.oid::regclass,
            st.replication_group_id,
            version,
            sharded_table_schema,
            sharded_table_name,
            replication_factor,
            sharding_key_expression
        FROM
            pg_class c
                JOIN pg_namespace n ON relnamespace = n.oid
                JOIN sharded_table st ON (nspname, relname) = (sharded_table_schema, sharded_table_name)
        WHERE
            (replication_group_id, version) = (_replication_group_id, _version)
    ),
    shard_snapshot AS (
        SELECT
            c.oid,
            st.replication_group_id,
            version,
            nspname AS schema_name,
            relname AS table_name,
            st.sharded_table_schema,
            st.sharded_table_name,
            replication_factor,
            "@extschema@".extract_sharding_key_value(
                    nspname,
                    relname,
                    sharding_key_expression) AS sharding_key_value
        FROM
            pg_class c
                JOIN pg_namespace n ON n.oid = relnamespace
                JOIN sharded_pg_class st ON
                        st.oid = ANY (
                            SELECT * FROM pg_partition_ancestors(c.oid)
                        )
                    AND
                        NOT EXISTS (SELECT 1 FROM
                            sharded_pg_class des
                            WHERE
                                (des.replication_group_id, des.version) = (st.replication_group_id, st.version)
                              AND des.oid = ANY (SELECT * FROM pg_partition_ancestors(c.oid))
                              AND des.oid <> st.oid
                              AND st.oid = ANY (SELECT * FROM pg_partition_ancestors(des.oid))
                        )
        WHERE
            c.relkind = 'r'
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
            itc.oid = ANY (SELECT * FROM pg_partition_ancestors(ss.oid))
    ),
    group_counts AS (
        SELECT
            replication_group_id,
            version,
            count(DISTINCT availability_zone) AS az_count,
            count(*) AS host_count
        FROM
            shard_host_weight
        GROUP BY
            1, 2
    ),
    replicated_shard AS (
        SELECT
            replication_group_id,
            version,
            schema_name,
            table_name,
            greatest(
                    ceil((replication_factor * host_count) / 100),
                    least(min_replica_count, host_count),
                    least(min_replica_count_per_availability_zone * az_count, host_count)) AS replica_count,
            sharding_key_value
        FROM
            shard_snapshot sc
                JOIN replication_group_config USING (replication_group_id, version)
                JOIN group_counts USING (replication_group_id, version)
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
        replicated_shard s
            CROSS JOIN LATERAL (
                SELECT
                    availability_zone,
                    host_id,
                    row_number() OVER (
                        PARTITION BY availability_zone
                        ORDER BY "@extschema@".score(weight, sharding_key_value, host_id) DESC) AS group_rank
                FROM
                    shard_host_weight
                        JOIN shard_host USING (replication_group_id, availability_zone, host_id)
                        JOIN replication_group_member m USING (replication_group_id, availability_zone, host_id)
                WHERE
                    (replication_group_id, version) = (s.replication_group_id, s.version)
                ORDER BY
                    group_rank, "@extschema@".score(100, sharding_key_value, availability_zone) DESC
                LIMIT
                    s.replica_count
            ) h
$$;
