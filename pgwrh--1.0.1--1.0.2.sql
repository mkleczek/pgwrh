DROP TRIGGER forbid_not_pending_version_insert ON replication_group_config;
DROP TRIGGER forbid_not_pending_version_delete ON replication_group_config;

ALTER TABLE replication_group ADD COLUMN ready_version config_version NOT NULL DEFAULT 'FLIP';

INSERT INTO replication_group_config
SELECT replication_group_id, coalesce(next_version(version), 'FLIP'), FALSE
FROM replication_group g LEFT JOIN replication_group_config c ON (c.replication_group_id, pending) = (g.replication_group_id, TRUE)
ON CONFLICT DO NOTHING;

UPDATE replication_group g
SET ready_version = (SELECT version FROM replication_group_config WHERE replication_group_id = g.replication_group_id AND NOT pending);

ALTER TABLE replication_group ADD FOREIGN KEY (replication_group_id, ready_version)
REFERENCES replication_group_config(replication_group_id, version)
DEFERRABLE INITIALLY DEFERRED;

CREATE OR REPLACE FUNCTION replication_group_prepare_config() RETURNS trigger LANGUAGE plpgsql AS
$$
BEGIN
    INSERT INTO @extschema@.replication_group_config VALUES (NEW.replication_group_id, NEW.ready_version)
    ON CONFLICT DO NOTHING;
    RETURN NEW;
END
$$;
CREATE OR REPLACE TRIGGER replication_group_before_insert
AFTER INSERT ON replication_group
FOR EACH ROW EXECUTE FUNCTION replication_group_prepare_config();

CREATE OR REPLACE FUNCTION is_ready(group_id text, version config_version) RETURNS boolean
SET SEARCH_PATH FROM CURRENT
LANGUAGE sql STABLE AS
$$
SELECT EXISTS (SELECT 1 FROM replication_group WHERE replication_group_id = group_id AND ready_version = $2)
$$;

CREATE OR REPLACE FUNCTION next_pending_version(group_id text) RETURNS config_version
LANGUAGE sql AS
$$
INSERT INTO @extschema@.replication_group_config
SELECT replication_group_id, pgwrh.next_version(ready_version)
FROM @extschema@.replication_group
WHERE replication_group_id = group_id
ON CONFLICT DO NOTHING;

SELECT pgwrh.next_version(ready_version)
FROM @extschema@.replication_group
WHERE replication_group_id = group_id
$$;

CREATE OR REPLACE FUNCTION mark_pending_version_ready(group_id text) RETURNS void
SET SEARCH_PATH FROM CURRENT
LANGUAGE sql AS
$$
WITH updated AS (
    UPDATE @extschema@.replication_group g SET ready_version = @extschema@.next_version(ready_version)
    WHERE
        replication_group_id = group_id AND
        EXISTS (
            SELECT 1 FROM @extschema@.replication_group_config
            WHERE
                replication_group_id = g.replication_group_id AND
                version = @extschema@.next_version(g.ready_version)
    )
    RETURNING *
)
DELETE FROM @extschema@.replication_group_config c
WHERE
    replication_group_id = group_id AND
    NOT EXISTS (
        SELECT 1 FROM updated
        WHERE
            replication_group_id = c.replication_group_id AND
            ready_version = c.version
    )
$$;

CREATE OR REPLACE FUNCTION score(weight int, VARIADIC text[]) RETURNS double precision IMMUTABLE LANGUAGE sql AS
$$SELECT weight / -ln(pgwrh.stable_hash(VARIADIC $2)::double precision / ((2147483649)::bigint - (-2147483648)::bigint) + 0.5::double precision)$$;

CREATE OR REPLACE VIEW published_shard AS
WITH sharded_pg_class AS (
    SELECT
        st.replication_group_id,
        c.oid::regclass,
        version,
        replica_count
    FROM
        pg_class c
            JOIN pg_namespace n ON relnamespace = n.oid
            JOIN sharded_table st ON (nspname, relname) = (sharded_table_schema, sharded_table_name)
)
SELECT
    replication_group_id,
    version,
    nspname AS schema_name,
    relname AS table_name,
    replica_count,
    publication_name
FROM
    pg_class c
        JOIN pg_wrh_publication pwp ON c.oid = pwp.published_shard
        JOIN pg_publication_rel pr ON c.oid = prrelid
        JOIN pg_publication pub ON pub.oid = prpubid AND pub.pubname = pwp.publication_name
        JOIN pg_namespace n ON n.oid = relnamespace
        JOIN sharded_pg_class st ON st.oid = ANY (
            SELECT * FROM pg_partition_ancestors(c.oid)
        ) AND NOT EXISTS (
            SELECT 1
            FROM sharded_pg_class des
            WHERE
                des.oid = ANY (SELECT * FROM pg_partition_ancestors(c.oid))
                AND des.oid <> st.oid
                AND st.oid = ANY (SELECT * FROM pg_partition_ancestors(des.oid)) 
        )
WHERE
    c.relkind = 'r';
COMMENT ON VIEW published_shard IS
'Provides shards and their number of pending and ready copies based on configuration in sharded_table.
A shard is a non-partitioned table which ancestor (can be the table iself) is in sharded_table.
The desired number of copies is specified per the whole hierarchy (ie. all partitions of a given table).

Only shards for which there is a publication are present here.';

CREATE OR REPLACE VIEW shard_assignment AS
WITH shard_assigned_host AS (
    SELECT
        replication_group_id,
        version,
        schema_name,
        table_name,
        publication_name,
        availability_zone,
        host_id,
        host_name,
        port,
        online
    FROM
        published_shard s
            CROSS JOIN LATERAL (
                SELECT
                    availability_zone,
                    host_id,
                    host_name,
                    port,
                    online,
                    row_number() OVER (
                        PARTITION BY availability_zone
                        ORDER BY pgwrh.score(weight, s.schema_name, s.table_name, host_id) DESC) AS group_rank
                FROM
                    shard_host_weight
                        JOIN shard_host USING (replication_group_id, host_id)
                        JOIN replication_group_member USING (replication_group_id, host_id)
                WHERE
                    (replication_group_id, version) = (s.replication_group_id, s.version)
                ORDER BY
                    group_rank, pgwrh.score(100, s.schema_name, s.table_name, availability_zone) DESC
                LIMIT
                    s.replica_count
            ) h
)
SELECT
    schema_name,
    table_name,
    -- is this member one of the assigned hosts?
    bool_or(m.host_id = sah.host_id) AS local,
    -- foreign server name, host and port
    -- take all assigned hosts that are
    -- -- ready (ie. NOT pending)
    -- -- online
    -- -- not this member
    -- sort hosts by their id to minimize number of foreign servers
    -- (ie. avoid having different foreign servers for different permutations the same hosts)
    md5(coalesce(string_agg(sah.host_id, ',' ORDER BY sah.host_id)
        FILTER (WHERE online AND g.ready_version = sah.version AND m.host_id <> sah.host_id), '')) AS shard_server_name,
    coalesce(string_agg(host_name, ',' ORDER BY sah.host_id)
        FILTER (WHERE online AND g.ready_version = sah.version AND m.host_id <> sah.host_id), '') AS host,
    coalesce(string_agg(port::text, ',' ORDER BY sah.host_id)
        FILTER (WHERE online AND g.ready_version = sah.version AND m.host_id <> sah.host_id), '') AS port,
    current_database() AS dbname,
    username AS shard_server_user,
    password AS shard_server_password,
    publication_name
FROM
    replication_group_member m
        JOIN replication_group g USING (replication_group_id)
        JOIN shard_assigned_host sah USING (replication_group_id),
        -- multiply hosts in the same availability zone as this member
        generate_series(1, CASE WHEN m.availability_zone = sah.availability_zone THEN m.same_zone_multiplier ELSE 1 END)
WHERE member_role = CURRENT_ROLE
GROUP BY
    schema_name, table_name, dbname, shard_server_user, shard_server_password, publication_name;

CREATE OR REPLACE VIEW shard_index AS
WITH parent_class AS (
    SELECT DISTINCT
        replication_group_id,
        c.oid::regclass,
        it.index_name,
        it.index_template,
        ready_version = version AS pending
    FROM
        pg_class c
            JOIN pg_namespace n ON relnamespace = n.oid
            JOIN shard_index_template it ON (nspname, relname) = (schema_name, table_name)
            JOIN replication_group USING (replication_group_id)
)
SELECT
    nspname AS schema_name,
    relname AS table_name,
    format('%s_%s_%s', relname, index_name, substring(md5(index_template), 0, 16)) AS index_name,
    index_template,
    pending
FROM
    pg_class c
        JOIN pg_wrh_publication pwp ON c.oid = pwp.published_shard
        JOIN pg_publication_rel pr ON c.oid = prrelid
        JOIN pg_publication pub ON pub.oid = prpubid AND pub.pubname = pwp.publication_name
        JOIN pg_namespace n ON n.oid = relnamespace
        JOIN parent_class p ON p.oid = ANY (
            SELECT * FROM pg_partition_ancestors(c.oid)
        )
        JOIN replication_group_member m USING (replication_group_id)
WHERE
    c.relkind = 'r'
    AND m.member_role = CURRENT_ROLE;



---- Drop unused controller objects
DROP VIEW shard_counts;

ALTER TABLE replication_group_config DROP COLUMN pending;


--- Replica
CREATE OR REPLACE FUNCTION launch_in_background(commands text) RETURNS void LANGUAGE plpgsql AS
$$
DECLARE
    pid int;
BEGIN
    pid := (select pg_background_launch(commands));
    PERFORM pg_sleep(0.1);
    PERFORM pg_background_detach(pid);
END
$$;

CREATE OR REPLACE FUNCTION launch_sync() RETURNS void LANGUAGE sql AS
$$
SELECT @extschema@.launch_in_background('CAll @extschema@.sync_replica_worker();')
$$;

CREATE OR REPLACE FUNCTION sync_daemon(seconds real) RETURNS void LANGUAGE sql AS
$$
SELECT @extschema@.launch_in_background(format('
        CAll @extschema@.sync_replica_worker();
        SELECT pg_sleep(%1$s);
        SELECT pgwrh.sync_daemon(%1$s);
    ', seconds))
$$;

CREATE OR REPLACE FUNCTION exec_script(script text) RETURNS boolean LANGUAGE plpgsql AS
$$
BEGIN
    PERFORM * FROM pg_background_result(pg_background_launch(script)) AS discarded(result text);
    RETURN TRUE;
EXCEPTION
    WHEN OTHERS THEN
        RETURN FALSE;
END
$$;

CREATE OR REPLACE FUNCTION exec_non_tx_scripts(scripts text[]) RETURNS boolean LANGUAGE plpgsql AS
$$
DECLARE
    cmd text;
    err text;
BEGIN
    FOREACH cmd IN ARRAY scripts LOOP
        PERFORM * FROM pg_background_result(pg_background_launch(cmd)) AS discarded(result text);
    END LOOP;
    RETURN TRUE;
EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS err = MESSAGE_TEXT;
        raise NOTICE '%', err;
        RETURN FALSE;
END
$$;

CREATE OR REPLACE FUNCTION sync_step() RETURNS boolean LANGUAGE plpgsql AS
$$
DECLARE
    r record;
    cmd text;
    err text;
BEGIN
    -- Select commands to execute in a separate transaction so that we don't keep any locks here
    FOR r IN SELECT * FROM pg_background_result(pg_background_launch('select * from @extschema@.sync')) AS (transactional boolean, async boolean, description text, commands text[]) LOOP
        RAISE NOTICE '%', r.description;
        IF r.transactional THEN
            IF r.async THEN
                PERFORM @extschema@.launch_in_background(array_to_string(r.commands, ';'));
            ELSE
                PERFORM @extschema@.exec_script(array_to_string(r.commands, ';'));
            END IF;
        ELSE
            IF r.async THEN
                IF array_length(r.commands, 1) > 1 THEN
                    PERFORM @extschema@.launch_in_background(format('SELECT @extschema@.exec_non_tx_scripts(ARRAY[%s])', (SELECT string_agg(format('%L', c), ',') FROM unnest(r.commands) AS c)));
                ELSE
                    PERFORM @extschema@.launch_in_background(r.commands[1]);
                END IF;
            ELSE
                FOREACH cmd IN ARRAY r.commands LOOP
                    PERFORM @extschema@.exec_script(cmd);
                END LOOP;
            END IF;
        END IF;
    END LOOP;
    RETURN FOUND;
EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS err = MESSAGE_TEXT;
        raise WARNING '%', err;
        PERFORM pg_sleep(1);
        RETURN TRUE;
END
$$;
