-- name: master-helpers
-- requires: tables

CREATE FUNCTION pubname(schema_name text, table_name text) RETURNS text IMMUTABLE LANGUAGE sql AS
$$
    SELECT 'pgwrh_' || md5(schema_name || table_name);
$$;
GRANT EXECUTE ON FUNCTION pubname(schema_name text, table_name text) TO PUBLIC;

CREATE FUNCTION usernamegen(replication_group_id text, version config_version, seed uuid)
    RETURNS text
    IMMUTABLE
    LANGUAGE sql
    AS
$$
    SELECT 'pgwrh_' || current_database() || '_' || replication_group_id || '_' || right(md5(version || seed::text), 5);
$$;
GRANT EXECUTE ON FUNCTION usernamegen(replication_group_id text, version config_version, seed uuid) TO PUBLIC;
CREATE FUNCTION passgen(replication_group_id text, version config_version, seed uuid)
    RETURNS text
    IMMUTABLE
    LANGUAGE sql
AS
$$
    SELECT encode(sha256(convert_to(replication_group_id || version || seed::text, 'UTF8')), 'hex');
$$;
GRANT EXECUTE ON FUNCTION passgen(replication_group_id text, version config_version, seed uuid) TO PUBLIC;

CREATE OR REPLACE FUNCTION next_version(version config_version) RETURNS config_version
    IMMUTABLE
    SET SEARCH_PATH FROM CURRENT
    LANGUAGE sql AS
$$
SELECT CASE version WHEN 'FLIP' THEN 'FLOP' ELSE 'FLIP' END::config_version
$$;


CREATE OR REPLACE FUNCTION prev_version(version config_version) RETURNS config_version
    IMMUTABLE
    LANGUAGE sql AS
$$
SELECT "@extschema@".next_version(version)
$$;

CREATE OR REPLACE FUNCTION is_locked(group_id text, version config_version) RETURNS boolean
    LANGUAGE sql STABLE AS
$$
SELECT EXISTS (SELECT 1 FROM
    "@extschema@".replication_group_config_lock
               WHERE replication_group_id = $1 AND version = $2
)
$$;


CREATE OR REPLACE FUNCTION next_pending_version(group_id text) RETURNS config_version
    LANGUAGE sql AS
$$
INSERT INTO "@extschema@".replication_group_config
SELECT
    replication_group_id, "@extschema@".next_version(current_version)
FROM
    "@extschema@".replication_group
WHERE
    replication_group_id = group_id
ON CONFLICT DO NOTHING;

SELECT
    "@extschema@".next_version(current_version)
FROM
    "@extschema@".replication_group
WHERE
    replication_group_id = group_id
$$;


CREATE OR REPLACE FUNCTION clone_config(group_id text, _target_version config_version) RETURNS void
    SET SEARCH_PATH FROM CURRENT
    LANGUAGE sql AS
$$
    -- Do not do anything if a clone already exists
WITH clone AS (
    INSERT INTO "@extschema@".replication_group_config_clone (replication_group_id, source_version, target_version)
        VALUES (group_id, prev_version(_target_version), _target_version)
        ON CONFLICT DO NOTHING
        RETURNING *
),
     -- clone weights
     _ AS (
         INSERT INTO shard_host_weight (replication_group_id, availability_zone, host_id, version, weight)
             SELECT
                 replication_group_id, availability_zone, host_id, target_version, weight
             FROM
                 shard_host_weight
                     JOIN clone USING (replication_group_id)
             WHERE
                 version = source_version
             ON CONFLICT DO NOTHING
     ),
     -- clone shards if necessary
     __ AS (
         INSERT INTO sharded_table (replication_group_id, sharded_table_schema, sharded_table_name, version, replication_factor)
             SELECT replication_group_id, sharded_table_schema, sharded_table_name, target_version, replication_factor
             FROM
                 sharded_table
                     JOIN clone USING (replication_group_id)
             WHERE
                 version = source_version
             ON CONFLICT DO NOTHING
     )
-- clone index templates if necessary
INSERT INTO shard_index_template (replication_group_id, version, index_template_schema, index_template_table_name, index_template_name, index_template)
SELECT replication_group_id, target_version, index_template_schema, index_template_table_name, index_template_name, index_template
FROM
    shard_index_template
        JOIN clone c USING (replication_group_id)
WHERE
    version = source_version
ON CONFLICT DO NOTHING
$$;

CREATE OR REPLACE FUNCTION delete_pending_version(group_id text) RETURNS void LANGUAGE sql AS
$$
DELETE FROM "@extschema@".replication_group_config c
WHERE NOT EXISTS (SELECT 1 FROM
    "@extschema@".replication_group
                  WHERE replication_group_id = c.replication_group_id AND c.version IN (current_version, target_version)
)
$$;
COMMENT ON FUNCTION delete_pending_version(group_id text) IS
    'Removes pending (ie. the one that is not pointed to by replication_group(current_version)) configuration version.

    Removal of pending version may trigger removal of no longer needed shards on the replicas.
    So it must be performed with caution after verifying no replicas assume presence of these shards on other replicas';
-- TODO fix comment

CREATE OR REPLACE FUNCTION stable_hash(VARIADIC text[]) RETURNS int IMMUTABLE LANGUAGE sql AS
$$
SELECT ('x' || substr(md5(array_to_string($1, '', '')), 1, 8))::bit(32)::int
$$;

CREATE OR REPLACE FUNCTION score(weight int, VARIADIC text[]) RETURNS double precision IMMUTABLE LANGUAGE sql AS
$$
SELECT weight / -ln("@extschema@".stable_hash(VARIADIC $2)::double precision / ((2147483649)::bigint - (-2147483648)::bigint) + 0.5::double precision)
$$;

CREATE OR REPLACE FUNCTION extract_sharding_key_value(schema_name text, table_name text, sharding_key_expression text) RETURNS text IMMUTABLE LANGUAGE plpgsql AS
$$
DECLARE
    result text;
BEGIN
    EXECUTE sharding_key_expression INTO result USING schema_name, table_name;
    RETURN result;
END
$$;

CREATE OR REPLACE FUNCTION to_regclass(st sharded_table) RETURNS regclass STABLE LANGUAGE sql AS
$$
SELECT to_regclass(st.sharded_table_schema || '.' || st.sharded_table_name)
$$;
