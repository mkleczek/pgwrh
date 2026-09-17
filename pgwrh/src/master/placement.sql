-- name: master-placement
-- requires: master-helpers

CREATE FUNCTION shard_placement_policy(
    _replication_group_id text, _version config_version, _shard regclass)
RETURNS TABLE (
    min_replica_count_after_az_failure int,
    az_affinity jsonb)
LANGUAGE sql STABLE SET SEARCH_PATH FROM CURRENT AS
$$
    WITH ancestors AS MATERIALIZED (
        SELECT st.*,
               (SELECT count(*) FROM pg_partition_ancestors(a.relid)) AS depth
        FROM pg_partition_ancestors(_shard) a(relid)
        JOIN pg_class c ON c.oid = a.relid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN sharded_table st
          ON (st.sharded_table_schema, st.sharded_table_name) = (n.nspname, c.relname)
        WHERE (st.replication_group_id, st.version) = (_replication_group_id, _version)
    )
    SELECT coalesce((SELECT a.min_replica_count_after_az_failure FROM ancestors a
                     WHERE a.min_replica_count_after_az_failure IS NOT NULL
                     ORDER BY a.depth DESC LIMIT 1), cfg.min_replica_count_after_az_failure),
           coalesce((SELECT jsonb_object_agg(w.availability_zone, w.weight)
                     FROM (SELECT DISTINCT ON (f.availability_zone) f.availability_zone, f.weight
                           FROM ancestors a JOIN sharded_table_az_affinity f
                             USING (replication_group_id, version, sharded_table_schema, sharded_table_name)
                           ORDER BY f.availability_zone, a.depth DESC) w), '{}'::jsonb)
    FROM replication_group_config cfg
    WHERE (cfg.replication_group_id, cfg.version) = (_replication_group_id, _version)
$$;

CREATE FUNCTION shard_placement_inputs(_replication_group_id text, _version config_version)
RETURNS TABLE (
    shard_oid oid, schema_name text, table_name text,
    sharded_table_schema text, sharded_table_name text,
    sharding_key_value text, replica_count bigint,
    min_replica_count_per_availability_zone int,
    min_replica_count_after_az_failure int,
    az_affinity jsonb)
LANGUAGE sql STABLE SET SEARCH_PATH FROM CURRENT AS
$$
    WITH configured AS MATERIALIZED (
        SELECT c.oid, st.*
        FROM sharded_table st
        JOIN pg_namespace n ON n.nspname = st.sharded_table_schema
        JOIN pg_class c ON (c.relnamespace, c.relname) = (n.oid, st.sharded_table_name)
        WHERE (st.replication_group_id, st.version) = (_replication_group_id, _version)
    ), host_counts AS (
        SELECT count(*) AS host_count, count(DISTINCT h.availability_zone) AS az_count
        FROM shard_host_weight h
        WHERE (h.replication_group_id, h.version) = (_replication_group_id, _version)
    )
    SELECT c.oid, n.nspname::text, c.relname::text,
           st.sharded_table_schema, st.sharded_table_name,
           "@extschema@".extract_sharding_key_value(n.nspname, c.relname, st.sharding_key_expression),
           greatest(ceil(st.replication_factor * hc.host_count / 100),
                    least(cfg.min_replica_count, hc.host_count),
                    least(cfg.min_replica_count_per_availability_zone::bigint * hc.az_count, hc.host_count))::bigint,
           cfg.min_replica_count_per_availability_zone,
           p.min_replica_count_after_az_failure, p.az_affinity
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    CROSS JOIN LATERAL (
        SELECT t.* FROM configured t
        WHERE t.oid = ANY (SELECT * FROM pg_partition_ancestors(c.oid))
        ORDER BY (SELECT count(*) FROM pg_partition_ancestors(t.oid)) DESC LIMIT 1
    ) st
    JOIN replication_group_config cfg
      ON (cfg.replication_group_id, cfg.version) = (st.replication_group_id, st.version)
    CROSS JOIN host_counts hc
    CROSS JOIN LATERAL "@extschema@".shard_placement_policy(_replication_group_id, _version, c.oid) p
    WHERE c.relkind = 'r'
$$;

CREATE FUNCTION select_shard_hosts(
    _replication_group_id text, _version config_version, _sharding_key_value text,
    _replica_count bigint, _min_per_zone int,
    _min_surviving int, _affinity jsonb)
RETURNS TABLE (availability_zone text, host_id text)
LANGUAGE plpgsql STABLE SET SEARCH_PATH FROM CURRENT AS
$$
DECLARE
    max_per_zone bigint := _replica_count - _min_surviving;
    zone_count bigint;
    capacity bigint;
    undersized_zone boolean;
    remaining numeric;
    shares jsonb := '{}'::jsonb;
    capped jsonb;
    capped_count numeric;
BEGIN
    SELECT count(*), coalesce(sum(least(z.host_count, max_per_zone)), 0),
           coalesce(bool_or(z.host_count < _min_per_zone), false)
    INTO zone_count, capacity, undersized_zone
    FROM (SELECT count(*) AS host_count FROM shard_host_weight h
          WHERE (h.replication_group_id, h.version) = (_replication_group_id, _version)
          GROUP BY h.availability_zone) z;

    IF _min_surviving > _replica_count
       OR zone_count * _min_per_zone > _replica_count
       OR (zone_count > 0 AND max_per_zone < _min_per_zone)
       OR capacity < _replica_count OR undersized_zone THEN
        RAISE EXCEPTION 'Cannot place % copies for shard key % in replication group %',
            _replica_count, _sharding_key_value, _replication_group_id
            USING ERRCODE = 'check_violation',
                  DETAIL = format('Zones: %s; minimum per zone: %s; required survivors after one AZ failure: %s; eligible capacity under that limit: %s.',
                                  zone_count, _min_per_zone, _min_surviving, capacity),
                  HINT = 'Change the configured copy count, add eligible hosts, or explicitly revise the HA requirements.';
    END IF;

    -- Reserve the hard per-AZ floor, then apportion the remaining slots by AZ
    -- weight. Saturated zones give their excess share back to the other zones.
    -- Each pass fixes at least one zone or finishes, so this is bounded by Z.
    remaining := _replica_count - zone_count * _min_per_zone;
    LOOP
        WITH zones AS (
            SELECT h.availability_zone,
                   least(count(*), max_per_zone) - _min_per_zone AS extra_capacity,
                   coalesce((_affinity ->> h.availability_zone)::int, 1) AS weight
            FROM shard_host_weight h
            WHERE (h.replication_group_id, h.version) = (_replication_group_id, _version)
              AND NOT shares ? h.availability_zone
            GROUP BY h.availability_zone
        ), proposed AS (
            SELECT z.*, remaining * z.weight / sum(z.weight) OVER () AS share
            FROM zones z
        )
        SELECT jsonb_object_agg(p.availability_zone, p.extra_capacity)
                   FILTER (WHERE p.extra_capacity <= p.share),
               sum(p.extra_capacity) FILTER (WHERE p.extra_capacity <= p.share)
        INTO capped, capped_count
        FROM proposed p;

        IF capped IS NULL THEN
            WITH zones AS (
                SELECT h.availability_zone,
                       coalesce((_affinity ->> h.availability_zone)::int, 1) AS weight
                FROM shard_host_weight h
                WHERE (h.replication_group_id, h.version) = (_replication_group_id, _version)
                  AND NOT shares ? h.availability_zone
                GROUP BY h.availability_zone
            ), proposed AS (
                SELECT z.availability_zone, remaining * z.weight / sum(z.weight) OVER () AS share
                FROM zones z
            )
            SELECT shares || coalesce(jsonb_object_agg(p.availability_zone, p.share), '{}'::jsonb)
            INTO shares FROM proposed p;
            EXIT;
        END IF;
        shares := shares || capped;
        remaining := remaining - capped_count;
    END LOOP;

    RETURN QUERY
    WITH ranked_hosts AS (
        SELECT h.availability_zone, h.host_id,
               row_number() OVER (PARTITION BY h.availability_zone
                   ORDER BY "@extschema@".score(h.weight, _sharding_key_value, h.host_id) DESC,
                            h.host_id COLLATE "C") AS host_rank,
               (shares ->> h.availability_zone)::numeric AS share
        FROM shard_host_weight h
        WHERE (h.replication_group_id, h.version) = (_replication_group_id, _version)
    )
    SELECT h.availability_zone, h.host_id FROM ranked_hosts h
    WHERE h.host_rank <= _min_per_zone + ceil(h.share)
    ORDER BY (h.host_rank <= _min_per_zone + floor(h.share)) DESC,
             -- Fractional slots compete using WRH. Equal fractions retain the
             -- existing AZ ranking, so neutral affinities preserve placement.
             (h.share - floor(h.share)) * "@extschema@".score(1, _sharding_key_value, h.availability_zone) DESC,
             h.availability_zone COLLATE "C", h.host_rank
    LIMIT _replica_count;
END
$$;

CREATE FUNCTION preview_shard_placement(_replication_group_id text, _version config_version)
RETURNS TABLE (
    schema_name text, table_name text, replica_count bigint,
    min_replica_count_after_az_failure int,
    az_affinity jsonb, unavailable_preferred_zones text[], availability_zone text, host_id text)
LANGUAGE sql STABLE SET SEARCH_PATH FROM CURRENT AS
$$
    SELECT s.schema_name, s.table_name, s.replica_count,
           s.min_replica_count_after_az_failure, s.az_affinity,
           ARRAY(SELECT a.key FROM jsonb_each_text(s.az_affinity) a
                 WHERE a.value::int > 1 AND NOT EXISTS (
                     SELECT 1 FROM shard_host_weight w
                     WHERE (w.replication_group_id, w.version, w.availability_zone) =
                           (_replication_group_id, _version, a.key)) ORDER BY a.key),
           h.availability_zone, h.host_id
    FROM "@extschema@".shard_placement_inputs(_replication_group_id, _version) s
    LEFT JOIN LATERAL "@extschema@".select_shard_hosts(
        _replication_group_id, _version, s.sharding_key_value, s.replica_count,
        s.min_replica_count_per_availability_zone,
        s.min_replica_count_after_az_failure, s.az_affinity) h ON true
$$;
COMMENT ON FUNCTION preview_shard_placement(text, config_version) IS
'Calculate placement from the specified configuration and current source partition tree without starting a rollout.
Reports effective policies and preferred zones without eligible hosts; rejects infeasible HA requirements.
Use shard_assigned_host for the immutable assignments of a configuration already rolled out.';
