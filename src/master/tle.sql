-- name: master-tle

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

CREATE FUNCTION pgtle_available_extensions()
    RETURNS TABLE(name text, default_version text, description text, requires text[])
    STABLE
    SECURITY DEFINER
    LANGUAGE sql
    AS
$$
    SELECT
        name,
        default_version,
        comment AS description,
        requires
    FROM
        pgtle.available_extensions();
$$;
SELECT exec_dynamic(format(
        'GRANT EXECUTE ON FUNCTION pgtle_available_extensions() TO %I', pgwrh_replica_role_name()));
-------------------
-------------------
CREATE FUNCTION pgtle_available_extension_versions()
    RETURNS TABLE(name text, version text)
    STABLE
    SECURITY DEFINER
    LANGUAGE sql
AS
$$
SELECT
    name,
    version
FROM
    pgtle.available_extension_versions()
WHERE
    EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_tle');
$$;
SELECT exec_dynamic(format(
        'GRANT EXECUTE ON FUNCTION pgtle_available_extension_versions() TO %I', pgwrh_replica_role_name()));
-------------------
-------------------
CREATE FUNCTION
pgtle_extension_update_paths(name text)
    RETURNS TABLE(path text)
    STABLE
    SECURITY DEFINER
    LANGUAGE sql
AS
$$
SELECT
    path
FROM
    pgtle.extension_update_paths(name)
WHERE
    EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_tle');
$$;
SELECT exec_dynamic(format(
        'GRANT EXECUTE ON FUNCTION pgtle_extension_update_paths(name text) TO %I', pgwrh_replica_role_name()));
-------------------
-------------------
CREATE VIEW tle_available_extension AS
    SELECT
        *
    FROM pgtle_available_extensions()
    WHERE
        EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_tle')
;
SELECT exec_dynamic(format(
        'GRANT SELECT ON tle_available_extension TO %I', pgwrh_replica_role_name()));
-------------------
-------------------
CREATE FUNCTION tle_ext(name text, version text)
    RETURNS text
    STABLE
    SECURITY DEFINER
    LANGUAGE plpgsql
    AS
$$
DECLARE
    result text;
BEGIN
    EXECUTE format('SELECT pgtle.%I()', format('%s--%s.sql', name, version)) INTO result;
    RETURN result;
END
$$;
SELECT exec_dynamic(format(
        'GRANT EXECUTE ON FUNCTION tle_ext(name text, version text) TO %I', pgwrh_replica_role_name()));
-------------------
-------------------
CREATE FUNCTION tle_ext(name text, from_version text, to_version text)
    RETURNS text
    STABLE
    SECURITY DEFINER
    LANGUAGE plpgsql
    AS
$$
DECLARE
    result text;
BEGIN
    EXECUTE format('SELECT pgtle.%I()', format('%s--%s--%s.sql', name, from_version, to_version)) INTO result;
    RETURN result;
END
$$;
SELECT exec_dynamic(format(
        'GRANT EXECUTE ON FUNCTION tle_ext(name text, from_version text, to_version text) TO %I', pgwrh_replica_role_name()));
-------------------
-------------------
CREATE VIEW tle_extension_version AS
    SELECT
        *,
        tle_ext(name, version) AS ext
    FROM
        pgtle_available_extension_versions()
    WHERE
        EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_tle')
;
SELECT exec_dynamic(format(
        'GRANT SELECT ON tle_extension_version TO %I', pgwrh_replica_role_name()));
-------------------
-------------------
CREATE VIEW tle_extension_update_path AS
    SELECT
        name,
        parr[1] AS from_version,
        parr[2] AS to_version,
        tle_ext(name, parr[1], parr[2]) AS ext
    FROM
        pgtle_available_extensions(),
        pgtle_extension_update_paths(name),
        string_to_array(path, '--') parr
    WHERE
            EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_tle')
        AND array_length(parr, 1) = 2
;
SELECT exec_dynamic(format(
        'GRANT SELECT ON tle_extension_update_path TO %I', pgwrh_replica_role_name()));
