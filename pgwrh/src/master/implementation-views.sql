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
            || substr(md5(index_template_schema || index_template_table_name || index_template), 1, 5) AS index_name,
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
        bool_or(version = current_version OR rollback_unlock IS NOT NULL) is_current,
        bool_or(version = target_version AND current_version <> target_version) AS is_target
    FROM
        shard_index_definition
            JOIN replication_group USING (replication_group_id)
            JOIN replication_group_config_lock USING (replication_group_id, version)
    GROUP BY
        1, 2, 3, 4, 5
),
member_shard AS (
    SELECT
        replication_group_id,
        member_role,
        schema_name,
        table_name,
        bool_or(version = current_version OR rollback_unlock IS NOT NULL) is_current,
        bool_or(version = target_version AND current_version <> target_version) is_target
    FROM
        shard_assigned_host
            JOIN replication_group USING (replication_group_id)
            JOIN replication_group_member USING (replication_group_id, availability_zone, host_id)
            JOIN replication_group_config_lock USING (replication_group_id, version)
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

-- Destination identity belongs to the protocol even while generation remains
-- shared by the group. Future generators can specialize this view per member.
CREATE VIEW replica_credentials AS
SELECT m.replication_group_id, m.member_role, c.version, c.username, c.password
FROM replication_group_member m
    JOIN replication_group_credentials c USING (replication_group_id);

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
    -- if target route is new, wait for target indexes before switching traffic to it
    CASE WHEN target_route_ready
        THEN target_server_name
        ELSE current_server_name
    END AS shard_server_name,
    CASE WHEN target_route_ready
        THEN target_host
        ELSE coalesce(current_host, '')
    END AS host,
    CASE WHEN target_route_ready
        THEN target_port
        ELSE coalesce(current_port, '')
    END AS port,
    CASE WHEN target_route_ready THEN target_dbnames ELSE current_dbnames END AS dbnames,
    CASE WHEN target_route_ready THEN target_users ELSE current_users END AS shard_server_users,
    -- Prepare the effective remote route when eligible. Replicas keep retained
    -- local copies attached until they are no longer assigned locally.
    CASE WHEN current_version <> target_version
        THEN target_remote AND target_route_ready
        ELSE current_remote
    END AS connect_remote,
    pubname(schema_name, table_name) AS pubname,
    current_server_name AS retained_shard_server_name, -- do not drop foreign tables with this server name (to keep current tables during transition)
    --local AND hosted_shard_subscribed_confirmation IS NULL AS subscription_confirmation_required -- whether confirmation from this member is required
    m AS replication_group_member,
    CASE WHEN target_route_ready THEN target_members ELSE current_members END AS shard_server_members
FROM
    replication_group_member m
        JOIN replication_group g USING (replication_group_id)
        CROSS JOIN LATERAL (
            SELECT
                schema_name,
                table_name,
                -- is m among assigned hosts regardless of version
                -- every host has to retain shards from both current and target version
                bool_or(member_role = m.member_role) AS local,
                bool_and(member_role <> m.member_role)
                    FILTER (WHERE version = current_version) AS current_remote,
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
                -- did all target version hosts confirm target version indexes
                -- (so that fresh target copies can be exposed safely as foreign tables)
                -- we want to avoid situation when clients issue queries to hosts that don't have required indexes
                -- as that might disrupt whole cluster due to slow queries, that in turn cause
                -- a) high resource usage and cache thrashing
                -- b) exhausted connection pools
                bool_and(has_all_indexes)
                    FILTER (WHERE member_role <> m.member_role AND version = target_version) AS target_indexed,
                -- If all current hosts confirmed creation of target version user
                -- then we rotate credentials
                bool_and(target_user_created)
                    FILTER (WHERE member_role <> m.member_role AND version = current_version) AS current_users_ready,
                bool_and(target_user_created)
                    FILTER ( WHERE member_role <> m.member_role AND version = target_version) AS target_user_created
            FROM
                shard_assigned_host sah
                    JOIN shard_host USING (replication_group_id, availability_zone, host_id)
                    JOIN replication_group_member shm USING (replication_group_id, availability_zone, host_id)
                    CROSS JOIN LATERAL (
                        SELECT c.username AS target_username FROM replica_credentials c
                        WHERE c.replication_group_id = sah.replication_group_id
                            AND c.member_role = shm.member_role AND c.version = g.target_version
                    ) target_credentials
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
                            WHERE username = target_credentials.target_username)
                    ) u(target_user_created)
            WHERE
                    sah.replication_group_id = m.replication_group_id
                AND
                    (version IN (current_version, target_version) OR EXISTS (
                        SELECT 1 FROM replication_group_config_lock l
                        WHERE (l.replication_group_id, l.version) = (sah.replication_group_id, sah.version)
                            AND l.rollback_unlock IS NOT NULL
                    ))
            GROUP BY
                1, 2
        ) s
        CROSS JOIN LATERAL (
            SELECT
                current_version <> target_version
                AND target_subscribed
                AND target_online
                AND target_user_created
                AND (
                    current_server_name IS NOT DISTINCT FROM target_server_name
                    OR target_indexed
                ) AS target_route_ready
        ) route
        -- calculate current version foreign server host and port based on _online_ assigned hosts and this member availability zone
        LEFT JOIN LATERAL (
            SELECT
                schema_name,
                table_name,
                string_agg(host_name, ',' ORDER BY sah.availability_zone, sah.host_id) AS current_host,
                array_agg(shm.member_role ORDER BY sah.availability_zone, sah.host_id) AS current_members,
                array_agg(dbname ORDER BY sah.availability_zone, sah.host_id) AS current_dbnames,
                array_agg(credential.username ORDER BY sah.availability_zone, sah.host_id) AS current_users,
                string_agg(port::text, ',' ORDER BY sah.availability_zone, sah.host_id) AS current_port
            FROM
                shard_assigned_host sah
                    JOIN shard_host USING (replication_group_id, availability_zone, host_id)
                    JOIN replication_group_member shm USING (replication_group_id, availability_zone, host_id)
                    CROSS JOIN LATERAL (
                        SELECT c.username FROM replica_credentials c
                        WHERE c.replication_group_id = sah.replication_group_id
                            AND c.member_role = shm.member_role
                            AND c.version = CASE WHEN current_users_ready THEN g.target_version ELSE g.current_version END
                    ) credential,
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
                string_agg(host_name, ',' ORDER BY sah.availability_zone, sah.host_id) AS target_host,
                array_agg(member_role ORDER BY sah.availability_zone, sah.host_id) AS target_members,
                array_agg(dbname ORDER BY sah.availability_zone, sah.host_id) AS target_dbnames,
                array_agg(credential.username ORDER BY sah.availability_zone, sah.host_id) AS target_users,
                string_agg(port::text, ',' ORDER BY sah.availability_zone, sah.host_id) AS target_port
            FROM
                shard_assigned_host sah
                    JOIN shard_host USING (replication_group_id, availability_zone, host_id)
                    JOIN replication_group_member shm USING (replication_group_id, availability_zone, host_id)
                    CROSS JOIN LATERAL (
                        SELECT c.username FROM replica_credentials c
                        WHERE c.replication_group_id = sah.replication_group_id
                            AND c.member_role = shm.member_role AND c.version = g.target_version
                    ) credential,
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

-- Allowed concrete destinations for each retained controller configuration.
-- A route may exclude offline replicas, but every published target must belong
-- to the configuration being confirmed before old local copies can retire.
CREATE VIEW shard_destinations AS
SELECT a.replication_group_id, a.version, a.schema_name, a.table_name,
       jsonb_object_agg(pgwrh_target_server(m.member_role, h.host_name, h.port::text, h.dbname, c.username),
                        c.username) AS target_mappings
FROM shard_assigned_host a
    JOIN shard_host h USING (replication_group_id, availability_zone, host_id)
    JOIN replication_group_member m USING (replication_group_id, availability_zone, host_id)
    JOIN replica_credentials c USING (replication_group_id, version, member_role)
GROUP BY a.replication_group_id, a.version, a.schema_name, a.table_name;

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
            LEFT JOIN shard_destinations d USING (replication_group_id, version, schema_name, table_name)
    WHERE
        NOT EXISTS (SELECT 1 FROM
            json_to_recordset(connected_remote_shards) AS c(schema_name text, table_name text, shard_server_name text, shard_server_targets jsonb)
            WHERE (c.schema_name, c.table_name) = (s.schema_name, s.table_name)
                AND jsonb_typeof(c.shard_server_targets) = 'object'
                AND c.shard_server_targets <> '{}'::jsonb
                AND d.target_mappings @> c.shard_server_targets
        )
;

-- A prepared replacement is sufficient only while the leaf is served locally.
CREATE VIEW missing_ready_remote_shard AS
SELECT s.*
FROM missing_connected_remote_shard s
    JOIN replication_group_member m USING (replication_group_id, availability_zone, host_id)
    LEFT JOIN shard_destinations d USING (replication_group_id, version, schema_name, table_name)
WHERE NOT EXISTS (
    SELECT 1 FROM json_to_recordset(m.prepared_remote_shards)
        AS p(schema_name text, table_name text, shard_server_name text, shard_server_targets jsonb)
    WHERE (p.schema_name, p.table_name) = (s.schema_name, s.table_name)
        AND jsonb_typeof(p.shard_server_targets) = 'object'
        AND p.shard_server_targets <> '{}'::jsonb
        AND d.target_mappings @> p.shard_server_targets
        AND EXISTS (
            SELECT 1 FROM json_to_recordset(m.connected_local_shards) AS l(schema_name text, table_name text)
            WHERE (l.schema_name, l.table_name) = (s.schema_name, s.table_name)
        )
);
