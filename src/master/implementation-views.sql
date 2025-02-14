-- name: master-implementation-views
-- requires: core

-- CREATE OR REPLACE VIEW published_shard AS
-- WITH group_counts AS (
--     SELECT
--         replication_group_id,
--         version,
--         count(DISTINCT availability_zone) AS az_count,
--         count(*) AS host_count
--     FROM
--         shard_host_weight
--     GROUP BY
--         1, 2
-- )
-- SELECT
--     replication_group_id,
--     version,
--     schema_name,
--     table_name,
--     greatest(
--         ceil((replication_factor * host_count) / 100),
--         least(min_replica_count, host_count),
--         least(min_replica_count_per_availability_zone * az_count, host_count)) AS replica_count,
--     "@extschema@".extract_sharding_key_value(schema_name, table_name, sharding_key_expression) AS sharding_key_value,
--     pubname,
--     sc.oid
-- FROM
--     shard_class sc
--         JOIN pg_publication_rel ON sc.oid = prrelid
--         JOIN pg_publication pub ON pub.oid = prpubid AND is_dependent_object('pg_publication', pub.oid)
--         -- JOIN pgwrh_publication pwp USING (pubname, schema_name, table_name) -- is this redundant?
--         JOIN sharded_table USING (replication_group_id, version, sharded_table_schema, sharded_table_name)
--         JOIN replication_group_config USING (replication_group_id, version)
--         JOIN group_counts USING (replication_group_id, version)
-- ;
-- COMMENT ON VIEW published_shard IS
-- 'Provides shards and their number of pending and ready copies based on configuration in sharded_table.
-- A shard is a non-partitioned table which ancestor (can be the table iself) is in sharded_table.
-- The desired number of copies is specified per the whole hierarchy (ie. all partitions of a given table).
--
-- Only shards for which there is a publication are present here.';

-- CREATE VIEW shard_pg_class_index AS
-- SELECT
--     replication_group_id,
--     version,
--     sc.schema_name,
--     sc.table_name,
--     sc.oid,
--     format('%s_%s_%s', sc.table_name, index_name, substring(md5(index_template), 0, 16)) AS index_name,
--     it.index_template
-- FROM
--         shard_class sc
--             JOIN shard_index_template it USING (replication_group_id, version)
-- WHERE EXISTS (SELECT 1 FROM
--     pg_class parent
--         JOIN pg_namespace n ON relnamespace = n.oid
--     WHERE
--             (nspname, relname) = (it.schema_name, it.table_name)
--         AND
--             parent.oid = ANY (SELECT * FROM pg_partition_ancestors(sc.oid))
-- )
-- ;

-- CREATE OR REPLACE VIEW shard_assigned_host AS
-- SELECT
--     replication_group_id,
--     version,
--     schema_name,
--     table_name,
--     pubname,
--     member_role,
--     availability_zone,
--     host_id,
--     host_name,
--     port,
--     online,
--     s.oid,
--     subscribed_local_shards,
--     connected_remote_shards,
--     connected_local_shards,
--     indexes
-- --     bool_or(si IS NOT NULL AND NOT EXISTS (
-- --         SELECT 1 FROM
-- --                 json_to_recordset(indexes) AS i(schema_name text, index_name text)
-- --                 WHERE
-- --                     (schema_name, index_name) = (si.schema_name, si.index_name)
-- --     )) AS missing_index
-- --     EXISTS (SELECT 1 FROM
-- --         shard_pg_class_index si
-- --         WHERE
-- --                 (replication_group_id, version, schema_name, table_name) = (s.replication_group_id, s.version, s.schema_name, s.table_name)
-- --             AND NOT EXISTS (SELECT 1 FROM
-- --                 json_to_recordset((m).indexes) AS i(schema_name text, index_name text)
-- --                 WHERE
-- --                     (schema_name, index_name) = (si.schema_name, si.index_name)
-- --             )
-- --     ) AS missing_index
-- FROM
--     published_shard s
--         CROSS JOIN LATERAL (
--             SELECT
--                 member_role,
--                 availability_zone,
--                 host_id,
--                 host_name,
--                 port,
--                 online,
--                 subscribed_local_shards,
--                 connected_remote_shards,
--                 connected_local_shards,
--                 indexes,
--                 row_number() OVER (
--                     PARTITION BY availability_zone
--                     ORDER BY "@extschema@".score(weight, sharding_key_value, host_id) DESC) AS group_rank
--             FROM
--                 shard_host_weight
--                     JOIN shard_host USING (replication_group_id, availability_zone, host_id)
--                     JOIN replication_group_member m USING (replication_group_id, availability_zone, host_id)
--             WHERE
--                 (replication_group_id, version) = (s.replication_group_id, s.version)
--             ORDER BY
--                 group_rank, "@extschema@".score(100, sharding_key_value, availability_zone) DESC
--             LIMIT
--                 s.replica_count
--         ) h
-- ;
-- COMMENT ON VIEW shard_assigned_host IS
-- 'Provides assignment of shards to hosts for each replication group configuration version.
--
-- Calculation is based on Weighted Randezvous Hash algorithm.';

CREATE VIEW shard_index_definition AS
    SELECT
        replication_group_id,
        version,
        schema_name,
        table_name,
        table_name
            || '_'
            || index_template_name
            || '_'
            || md5(index_template_schema || index_template_table_name || index_template) AS index_name,
        index_template
    FROM
        shard_assigned_index
            JOIN shard_index_template USING (replication_group_id, version, index_template_schema, index_template_table_name, index_template_name)
;

CREATE OR REPLACE VIEW shard_index_per_member AS
WITH shard_class_index AS (
    SELECT
        replication_group_id,
        schema_name,
        table_name,
        index_name,
        index_template,
        bool_or(version = current_version) is_current,
        bool_or(version = target_version AND current_version <> target_version) AS is_target
    FROM
        shard_index_definition
            JOIN replication_group USING (replication_group_id)
    GROUP BY
        1, 2, 3, 4, 5
),
member_shard AS (
    SELECT
        replication_group_id,
        member_role,
        schema_name,
        table_name,
        bool_or(version = current_version) is_current,
        bool_or(version = target_version AND current_version <> target_version) is_target
    FROM
        shard_assigned_host
            JOIN replication_group USING (replication_group_id)
            JOIN replication_group_member USING (replication_group_id, availability_zone, host_id)
    GROUP BY
        1, 2, 3, 4
),
member_shard_index AS (
    SELECT
        replication_group_id,
        member_role,
        schema_name,
        table_name,
        index_name,
        index_template,
        s.is_current AS optional
    FROM
        member_shard s
            JOIN shard_class_index i USING (replication_group_id, schema_name, table_name)
    WHERE
            s.is_current AND i.is_current
        OR
            s.is_target AND i.is_target
)
SELECT
    replication_group_id,
    member_role,
    schema_name,
    table_name,
    index_name,
    index_template,
    optional
FROM
    member_shard_index
;
COMMENT ON VIEW shard_index_per_member IS
'Provides definitions of indexes that should be created for each shard.';

CREATE OR REPLACE FUNCTION has_indexes(_replication_group_id text, _version config_version, indexes json, _schema_name text, _table_name text)
    RETURNS boolean
    STABLE
    LANGUAGE sql
AS
$$
    SELECT
        NOT EXISTS (SELECT 1 FROM
            "@extschema@".shard_index_definition i
            WHERE
                    (  replication_group_id,  version,  schema_name,  table_name) =
                    ( _replication_group_id, _version, _schema_name, _table_name)
                AND
                    NOT EXISTS (SELECT 1 FROM
                        json_to_recordset(indexes) AS mi(schema_name text, index_name text)
                        WHERE
                                (   schema_name,   index_name) =
                                ( i.schema_name, i.index_name)
                    )            
        )
$$;

CREATE FUNCTION subscribes_local_shard(subscribed_local_shards json, schema_name text, table_name text) RETURNS boolean LANGUAGE sql AS
$$
    SELECT EXISTS (SELECT 1 FROM
        json_to_recordset(subscribed_local_shards) AS t(schema_name text, table_name text)
        WHERE (schema_name, table_name) = ($2, $3)
    )
$$;

CREATE OR REPLACE VIEW shard_assignment_per_member AS
SELECT
    replication_group_id,
    availability_zone,
    host_id,
    member_role,
    schema_name,
    table_name,
    local,
    -- foreign server hosting shard
    -- use target configuration server only when transitioning and all remote replicas subscribed to the shard (ie. we can run ANALYZE)
    CASE WHEN current_version <> target_version AND target_subscribed AND target_online
        THEN target_server_name
        ELSE current_server_name
    END AS shard_server_name,
    CASE WHEN current_version <> target_version AND target_subscribed AND target_online
        THEN target_host
        ELSE coalesce(current_host, '')
    END AS host,
    CASE WHEN current_version <> target_version AND target_subscribed AND target_online
        THEN target_port
        ELSE coalesce(current_port, '')
    END AS port,
    current_database() AS dbname,
    username AS shard_server_user,
    password AS shard_server_password,
    pubname(schema_name, table_name) AS pubname,
    current_version = target_version OR (target_online AND target_subscribed AND target_indexed) AS ready, -- can foreign table be connected to slot and made available to clients
    current_server_name AS retained_shard_server_name, -- do not drop foreign tables with this server name (to keep current tables during transition)
    --local AND hosted_shard_subscribed_confirmation IS NULL AS subscription_confirmation_required -- whether confirmation from this member is required
    m AS replication_group_member
FROM
    replication_group_member m
        JOIN replication_group g USING (replication_group_id)
        -- calculate foreign sever names from _all_ assigned hosts
        CROSS JOIN LATERAL (
            SELECT
                schema_name,
                table_name,
                -- is m among assigned hosts regardless of version
                -- every host has to retain shards from both current and target version
                bool_or(member_role = m.member_role) AS local,
                -- server names are independent of shard
                md5(string_agg(sah.availability_zone || sah.host_id, ',' ORDER BY sah.host_id)
                    FILTER (WHERE member_role <> m.member_role AND version = current_version)) AS current_server_name,
                md5(string_agg(sah.availability_zone || sah.host_id, ',' ORDER BY sah.host_id)
                    FILTER (WHERE member_role <> m.member_role AND version = target_version)) AS target_server_name,
                -- is any of target version hosts online?
                bool_or(online) FILTER (WHERE member_role <> m.member_role AND version = target_version) AS target_online,
                -- status of this particular shard
                -- did all target hosts confirmed subscription (so that clients can execute analyze)
                bool_and(subscribes_local_shard(subscribed_local_shards, schema_name, table_name))
                    FILTER (WHERE member_role <> m.member_role AND version = target_version) AS target_subscribed,
                -- did all target version hosts confirm target version indexes (so that clients can expose them as foreign tables)
                -- we want to avoid situation when clients issue queries to hosts that don't have required indexes
                -- as that might disrupt whole cluster due to slow queries causing
                -- a) high resource usage and cache thrashing
                -- b) exhausted connection pools
                bool_and(has_indexes(sah.replication_group_id, version, indexes, schema_name, table_name))
                    FILTER (WHERE member_role <> m.member_role AND version = target_version) AS target_indexed
            FROM
                shard_assigned_host sah
                    JOIN shard_host USING (replication_group_id, availability_zone, host_id)
                    JOIN replication_group_member USING (replication_group_id, availability_zone, host_id)
            WHERE
                    sah.replication_group_id = m.replication_group_id
                AND
                    version IN (current_version, target_version)
            GROUP BY
                1, 2
        ) s
        -- calculate current version foreign server host and port based on _online_ assigned hosts and this member availability zone
        LEFT JOIN LATERAL (
            SELECT
                schema_name,
                table_name,
                string_agg(host_name, ',' ORDER BY sah.host_id) AS current_host,
                string_agg(port::text, ',' ORDER BY sah.host_id) AS current_port
            FROM
                shard_assigned_host sah
                    JOIN shard_host USING (replication_group_id, availability_zone, host_id)
                    JOIN replication_group_member shm USING (replication_group_id, availability_zone, host_id),
                -- multiply hosts in the same availability zone by same_zone_multiplier
                generate_series(1, CASE WHEN m.availability_zone = sah.availability_zone THEN m.same_zone_multiplier ELSE 1 END)
            WHERE
                    sah.replication_group_id = m.replication_group_id
                AND
                    version = current_version
                AND
                    (availability_zone, host_id) <> (m.availability_zone, m.host_id)
                AND
                    online
                AND
                    -- isolate hosts that for some reason are missing current version indexes
                    -- condition is:
                    -- there are no current version indexes that this host did not report
                    -- ideally we could use a function, but it is problematic due to permissions
                    NOT EXISTS (SELECT 1 FROM
                        shard_assigned_host
                            JOIN shard_index_definition i USING (replication_group_id, version, schema_name, table_name)
                        WHERE
                                (availability_zone, host_id) = (sah.availability_zone, sah.host_id)
                            AND version = g.current_version
                            AND NOT EXISTS (SELECT 1 FROM
                                json_to_recordset(shm.indexes) AS mi(schema_name text, index_name text)
                                WHERE
                                    (   schema_name,   index_name) =
                                    ( i.schema_name, i.index_name)
                            )
                    )
            GROUP BY
                1, 2
        ) current_host_port USING (schema_name, table_name)
        LEFT JOIN LATERAL (
            SELECT
                schema_name,
                table_name,
                string_agg(host_name, ',' ORDER BY sah.host_id) AS target_host,
                string_agg(port::text, ',' ORDER BY sah.host_id) AS target_port
            FROM
                shard_assigned_host sah
                    JOIN shard_host USING (replication_group_id, availability_zone, host_id),
                -- multiply hosts in the same availability zone by same_zone_multiplier
                generate_series(1, CASE WHEN m.availability_zone = sah.availability_zone THEN m.same_zone_multiplier ELSE 1 END)
            WHERE
                    sah.replication_group_id = m.replication_group_id
                AND
                    version = target_version
                AND
                    (availability_zone, host_id) <> (m.availability_zone, m.host_id)
                AND
                    online
            GROUP BY
                1, 2
        ) target_host_port USING (schema_name, table_name)
;

CREATE VIEW missing_subscribed_shard AS
SELECT
    replication_group_id, version, availability_zone, host_id, schema_name, table_name
FROM
    shard_assigned_host a
        JOIN replication_group_member USING (replication_group_id, availability_zone, host_id)
WHERE
    NOT EXISTS (SELECT 1 FROM
        json_to_recordset(subscribed_local_shards) AS c(schema_name text, table_name text)
                WHERE (schema_name, table_name) = (a.schema_name, a.table_name)
    )
;

CREATE VIEW missing_connected_local_shard AS
SELECT
    replication_group_id, version, availability_zone, host_id, schema_name, table_name
FROM
    shard_assigned_host a
        JOIN replication_group_member USING (replication_group_id, availability_zone, host_id)
WHERE
    NOT EXISTS (SELECT 1 FROM
        json_to_recordset(connected_local_shards) AS c(schema_name text, table_name text)
                WHERE (schema_name, table_name) = (a.schema_name, a.table_name)
    )
;

CREATE VIEW missing_connected_remote_shard AS
    WITH remote_shard AS (
        SELECT
            m.*,
            version,
            schema_name,
            table_name
        FROM
            replication_group_member m
                JOIN shard ms USING (replication_group_id)
        WHERE
            NOT EXISTS (SELECT 1 FROM shard_assigned_host WHERE
                        (  replication_group_id,   availability_zone,   host_id,    schema_name,    table_name) =
                        (m.replication_group_id, m.availability_zone, m.host_id, ms.schema_name, ms.table_name))
    )
    SELECT
        replication_group_id, version, availability_zone, host_id, schema_name, table_name
    FROM
        remote_shard s
    WHERE
        NOT EXISTS (SELECT 1 FROM
            json_to_recordset(connected_remote_shards) AS c(schema_name text, table_name text)
                    WHERE (schema_name, table_name) = (s.schema_name, s.table_name)
        )
;
