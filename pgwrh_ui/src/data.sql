-- SPDX-License-Identifier: AGPL-3.0-or-later
-- These private read models use only controller-local state. In particular,
-- reading a page must never create a pending configuration or contact replicas.

CREATE FUNCTION group_state(group_id text DEFAULT NULL)
RETURNS TABLE (
    replication_group_id text, current_version pgwrh.config_version,
    target_version pgwrh.config_version, proposed_version pgwrh.config_version,
    phase text, replica_count bigint, shard_count bigint
)
LANGUAGE sql STABLE SET search_path = pg_catalog, pg_temp AS $$
    SELECT g.replication_group_id, g.current_version, g.target_version,
        CASE WHEN rollback_pending THEN g.current_version
             WHEN g.current_version <> g.target_version THEN g.target_version
             WHEN draft_exists THEN pgwrh.next_version(g.current_version)
             ELSE g.current_version END,
        CASE WHEN rollback_pending THEN 'rolling_back'
             WHEN g.current_version <> g.target_version THEN 'rolling_out'
             WHEN draft_exists THEN 'draft'
             ELSE 'stable' END,
        (SELECT count(*) FROM pgwrh.replication_group_member m
         WHERE m.replication_group_id = g.replication_group_id),
        (SELECT count(*) FROM pgwrh.shard s
         WHERE s.replication_group_id = g.replication_group_id AND s.version = g.current_version)
    FROM pgwrh.replication_group g
    CROSS JOIN LATERAL (
        SELECT EXISTS (SELECT FROM pgwrh.replication_group_config_lock l
                       WHERE l.replication_group_id = g.replication_group_id
                         AND l.rollback_unlock IS NOT NULL) AS rollback_pending,
               EXISTS (SELECT FROM pgwrh.replication_group_config c
                       WHERE c.replication_group_id = g.replication_group_id
                         AND c.version <> g.current_version
                         AND NOT pgwrh.is_locked(c.replication_group_id, c.version)) AS draft_exists
    ) flags
    WHERE group_id IS NULL OR g.replication_group_id = group_id
$$;

CREATE FUNCTION replica_state(group_id text)
RETURNS TABLE (
    availability_zone text, host_id text, member_role text, host_name text, port int, dbname text,
    online boolean, current_weight int, pending_weight int,
    current_copies bigint, target_copies bigint, reported_local_copies int,
    reported_remote_routes int, prepared_remote_routes int,
    slot_count bigint, active_slots bigint, confirmed_lag_bytes numeric,
    sessions bigint
)
LANGUAGE sql STABLE SET search_path = pg_catalog, pg_temp AS $$
    SELECT m.availability_zone, m.host_id, m.member_role, h.host_name, h.port, h.dbname,
        h.online, cw.weight, pw.weight,
        (SELECT count(*) FROM pgwrh.shard_assigned_host a
         WHERE (a.replication_group_id, a.availability_zone, a.host_id, a.version) =
               (m.replication_group_id, m.availability_zone, m.host_id, g.current_version)),
        (SELECT count(*) FROM pgwrh.shard_assigned_host a
         WHERE (a.replication_group_id, a.availability_zone, a.host_id, a.version) =
               (m.replication_group_id, m.availability_zone, m.host_id, g.target_version)),
        json_array_length(m.connected_local_shards),
        json_array_length(m.connected_remote_shards),
        json_array_length(m.prepared_remote_shards),
        slots.total, slots.active, slots.lag,
        (SELECT count(*) FROM pg_stat_activity a WHERE a.usename = m.member_role AND a.datname=current_database())
    FROM pgwrh.replication_group_member m
    JOIN pgwrh.replication_group g USING (replication_group_id)
    LEFT JOIN pgwrh.shard_host h USING (replication_group_id, availability_zone, host_id)
    LEFT JOIN pgwrh.shard_host_weight cw
      ON (cw.replication_group_id, cw.availability_zone, cw.host_id, cw.version) =
         (m.replication_group_id, m.availability_zone, m.host_id, g.current_version)
    LEFT JOIN pgwrh.shard_host_weight pw
      ON (pw.replication_group_id, pw.availability_zone, pw.host_id, pw.version) =
         (m.replication_group_id, m.availability_zone, m.host_id, pgwrh.next_version(g.current_version))
    CROSS JOIN LATERAL (
        -- Use the same slot-to-role convention as pgwrh.replication_status,
        -- aggregating multiple slots so a member always occupies one row.
        SELECT count(*) AS total, count(*) FILTER (WHERE s.active) AS active,
               max(pg_current_wal_lsn() - s.confirmed_flush_lsn) AS lag
        FROM pg_replication_slots s
        WHERE s.database=current_database() AND s.slot_type='logical'
          AND array_to_string(trim_array(regexp_split_to_array(s.slot_name, '_'), 1), '_') = m.member_role
    ) slots
    WHERE m.replication_group_id = group_id
$$;

CREATE FUNCTION rollout_blockers(group_id text)
RETURNS TABLE (
    version pgwrh.config_version, availability_zone text, host_id text,
    schema_name text, table_name text, kind text, detail text
)
LANGUAGE sql STABLE SET search_path = pg_catalog, pg_temp AS $$
    SELECT s.version, s.availability_zone, s.host_id, s.schema_name, s.table_name,
        CASE WHEN EXISTS (
            SELECT FROM json_to_recordset(m.subscribed_local_shards) AS r(schema_name text, table_name text)
            WHERE (r.schema_name, r.table_name) = (s.schema_name, s.table_name)
        ) THEN 'local' ELSE 'subscription' END,
        CASE WHEN EXISTS (
            SELECT FROM json_to_recordset(m.subscribed_local_shards) AS r(schema_name text, table_name text)
            WHERE (r.schema_name, r.table_name) = (s.schema_name, s.table_name)
        ) THEN 'Local copy is not reported connected. Check required indexes and attachment.'
          ELSE 'Subscription is not reported ready. Initial copying or subscription setup may be pending.' END
    FROM pgwrh.missing_connected_local_shard s
    JOIN pgwrh.replication_group g USING (replication_group_id)
    JOIN pgwrh.replication_group_member m USING (replication_group_id, availability_zone, host_id)
    WHERE s.replication_group_id = group_id AND s.version = g.target_version
    UNION ALL
    SELECT s.version, s.availability_zone, s.host_id, s.schema_name, s.table_name,
        'remote', 'Target remote route is not ready. A prepared replacement qualifies only while the leaf is served locally.'
    FROM pgwrh.missing_ready_remote_shard s
    JOIN pgwrh.replication_group g USING (replication_group_id)
    WHERE s.replication_group_id = group_id AND s.version = g.target_version
$$;

CREATE FUNCTION placement_diff(group_id text)
RETURNS TABLE (
    schema_name text, table_name text, availability_zone text, host_id text,
    change text, source text, online boolean, reported_local boolean
)
LANGUAGE plpgsql STABLE SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    state record;
BEGIN
    SELECT * INTO STRICT state FROM pgwrh_ui.group_state(group_id);
    RETURN QUERY
    WITH current_placement AS MATERIALIZED (
        SELECT a.schema_name, a.table_name, a.availability_zone, a.host_id
        FROM pgwrh.shard_assigned_host a
        WHERE a.replication_group_id = group_id AND a.version = state.current_version
    ), proposed AS MATERIALIZED (
        SELECT p.schema_name, p.table_name, p.availability_zone, p.host_id
        FROM pgwrh.preview_shard_placement(group_id, state.proposed_version) p
        WHERE state.phase = 'draft'
        UNION ALL
        SELECT a.schema_name, a.table_name, a.availability_zone, a.host_id
        FROM pgwrh.shard_assigned_host a
        WHERE state.phase <> 'draft'
          AND a.replication_group_id = group_id AND a.version = state.proposed_version
    ), changes AS (
        SELECT coalesce(p.schema_name, c.schema_name) AS schema_name,
            coalesce(p.table_name, c.table_name) AS table_name,
            coalesce(p.availability_zone, c.availability_zone) AS availability_zone,
            coalesce(p.host_id, c.host_id) AS host_id,
            CASE WHEN c.host_id IS NULL THEN 'add'
                 WHEN p.host_id IS NULL THEN 'remove' ELSE 'keep' END AS change
        FROM current_placement c FULL JOIN proposed p
          USING (schema_name, table_name, availability_zone, host_id)
    )
    SELECT c.schema_name, c.table_name, c.availability_zone, c.host_id, c.change,
        CASE state.phase WHEN 'draft' THEN 'Draft preview'
                         WHEN 'rolling_out' THEN 'Target snapshot' ELSE 'Current snapshot' END,
        h.online,
        EXISTS (SELECT FROM json_to_recordset(m.connected_local_shards) AS r(schema_name text, table_name text)
                WHERE (r.schema_name, r.table_name) = (c.schema_name, c.table_name))
    FROM changes c
    LEFT JOIN pgwrh.shard_host h ON h.replication_group_id = group_id
        AND (h.availability_zone, h.host_id) = (c.availability_zone, c.host_id)
    LEFT JOIN pgwrh.replication_group_member m ON m.replication_group_id = group_id
        AND (m.availability_zone, m.host_id) = (c.availability_zone, c.host_id);
END
$$;
