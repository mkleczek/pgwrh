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
    LANGUAGE sql AS
$$
SELECT CASE version WHEN 'FLIP' THEN 'FLOP' ELSE 'FLIP' END::"@extschema@".config_version
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
    "@extschema@".replication_group_config_lock l
               WHERE replication_group_id = $1 AND l.version = $2
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
COMMENT ON FUNCTION next_pending_version(group_id text) IS
'Inserts next pending version into replication_group_config and returns it.';


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
