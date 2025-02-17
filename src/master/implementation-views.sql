-- name: master-implementation-views
-- requires: core

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

CREATE VIEW replication_group_credentials AS
SELECT
    replication_group_id,
    version,
    usernamegen(replication_group_id, version, seed) AS username,
    passgen(replication_group_id, version, seed) AS password
FROM
    replication_group_config_lock
;

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
    CASE WHEN current_version <> target_version AND target_subscribed AND target_online AND target_user_created
        THEN target_server_name
        ELSE current_server_name
    END AS shard_server_name,
    CASE WHEN current_version <> target_version AND target_subscribed AND target_online AND target_user_created
        THEN target_host
        ELSE coalesce(current_host, '')
    END AS host,
    CASE WHEN current_version <> target_version AND target_subscribed AND target_online AND target_user_created
        THEN target_port
        ELSE coalesce(current_port, '')
    END AS port,
    current_database() AS dbname,
    CASE WHEN current_version <> target_version AND target_subscribed AND target_online AND target_user_created
        THEN target_credentials.username
        ELSE current_username
    END AS shard_server_user,
    -- If shard is remote in target version, and it is ready, connect it to slot instead of the local one
    -- (but keep the local one if it is still be marked as "local" above)
    CASE WHEN current_version <> target_version
        THEN target_remote AND target_subscribed AND target_online AND target_user_created
        ELSE NOT local
    END AS connect_remote,
    pubname(schema_name, table_name) AS pubname,
    current_server_name AS retained_shard_server_name, -- do not drop foreign tables with this server name (to keep current tables during transition)
    --local AND hosted_shard_subscribed_confirmation IS NULL AS subscription_confirmation_required -- whether confirmation from this member is required
    m AS replication_group_member
FROM
    replication_group_member m
        JOIN replication_group g USING (replication_group_id)
        JOIN replication_group_credentials current_credentials USING (replication_group_id)
        JOIN replication_group_credentials target_credentials USING (replication_group_id)
        CROSS JOIN LATERAL (
            SELECT
                schema_name,
                table_name,
                -- is m among assigned hosts regardless of version
                -- every host has to retain shards from both current and target version
                bool_or(member_role = m.member_role) AS local,
                bool_and(member_role <> m.member_role)
                    FILTER ( WHERE version = target_version) AS target_remote,
                -- server names are independent of shard
                md5(string_agg(sah.availability_zone || sah.host_id, ',' ORDER BY sah.availability_zone, sah.host_id)
                    FILTER (WHERE member_role <> m.member_role AND version = current_version)) AS current_server_name,
                md5(string_agg(sah.availability_zone || sah.host_id, ',' ORDER BY sah.availability_zone, sah.host_id)
                    FILTER (WHERE member_role <> m.member_role AND version = target_version)) AS target_server_name,
                -- is any of target version hosts online?
                bool_or(online) FILTER (WHERE member_role <> m.member_role AND version = target_version) AS target_online,
                -- status of this particular shard
                -- did all target hosts confirmed subscription (so that clients can execute analyze)
                bool_and(subscribes_local_shard)
                    FILTER (WHERE member_role <> m.member_role AND version = target_version) AS target_subscribed,
                -- did all target version hosts confirm target version indexes (so that clients can expose them as foreign tables)
                -- we want to avoid situation when clients issue queries to hosts that don't have required indexes
                -- as that might disrupt whole cluster due to slow queries, that in turn cause
                -- a) high resource usage and cache thrashing
                -- b) exhausted connection pools
                bool_and(has_all_indexes)
                    FILTER (WHERE member_role <> m.member_role AND version = target_version) AS target_indexed,
                -- If all current hosts confirmed creation of target version user
                -- then we rotate credentials
                CASE WHEN bool_and(target_user_created) FILTER ( WHERE member_role <> m.member_role AND version = current_version)
                    THEN target_credentials.username
                    ELSE current_credentials.username
                END AS current_username,
                bool_and(target_user_created)
                    FILTER ( WHERE member_role <> m.member_role AND version = target_version) AS target_user_created
            FROM
                shard_assigned_host sah
                    JOIN shard_host USING (replication_group_id, availability_zone, host_id)
                    JOIN replication_group_member USING (replication_group_id, availability_zone, host_id)
                    -- check if all required indexes are created
                    CROSS JOIN LATERAL (SELECT NOT EXISTS (SELECT 1 FROM
                        shard_index_definition i
                        WHERE
                                (    i.replication_group_id,   i.version,   i.schema_name,   i.table_name) =
                                (  sah.replication_group_id, sah.version, sah.schema_name, sah.table_name)
                            AND
                                NOT EXISTS (SELECT 1 FROM
                                    json_to_recordset(indexes) AS mi(schema_name text, index_name text)
                                            WHERE
                                                (   schema_name,   index_name) =
                                                ( i.schema_name, i.index_name)
                                )
                    )) i(has_all_indexes)
                    -- check if shard is subscribed
                    CROSS JOIN LATERAL (
                        SELECT EXISTS (SELECT 1 FROM
                            json_to_recordset(subscribed_local_shards) AS t(schema_name text, table_name text)
                            WHERE
                                (    schema_name,     table_name) =
                                (sah.schema_name, sah.table_name)
                    )) s(subscribes_local_shard)
                    CROSS JOIN LATERAL (
                        SELECT EXISTS (SELECT 1 FROM
                            json_array_elements_text(users) AS t(username)
                            WHERE username = target_credentials.username)
                    ) u(target_user_created)
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
WHERE
        current_credentials.version = current_version
    AND target_credentials.version = target_version
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
                        (  replication_group_id,    version,   availability_zone,   host_id,    schema_name,    table_name) =
                        (m.replication_group_id, ms.version, m.availability_zone, m.host_id, ms.schema_name, ms.table_name))
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
