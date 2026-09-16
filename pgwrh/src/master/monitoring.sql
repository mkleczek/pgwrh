-- name: master-monitoring
-- requires: tables

CREATE VIEW replication_status AS
WITH sessions AS (
    SELECT
        usename AS member_role, count(*) AS num_sessions
    FROM
        pg_stat_activity
    GROUP BY usename
)
SELECT
    replication_group_id,
    availability_zone,
    host_id,
    pg_size_pretty(pg_current_wal_lsn() - confirmed_flush_lsn) AS lag,
    coalesce(num_sessions, 0) AS num_sessions
FROM
    replication_group_member m
        LEFT JOIN pg_replication_slots ON
            array_to_string(trim_array(regexp_split_to_array(slot_name, '_'), 1), '_') = m.member_role
        LEFT JOIN sessions USING (member_role)
;
COMMENT ON VIEW replication_status IS
$$
Shows replication status of all replicas.
$$;
