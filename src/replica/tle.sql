-- name: replica-tle

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

CREATE FOREIGN TABLE fdw_tle_available_extension (
    name text,
    default_version text,
    description text,
    requires text[]
)
SERVER replica_controller
OPTIONS (table_name 'tle_available_extension');

CREATE FOREIGN TABLE fdw_tle_extension_version (
    name text,
    version text,
    ext text
)
SERVER replica_controller
OPTIONS (table_name 'tle_extension_version');

CREATE FOREIGN TABLE fdw_tle_extension_update_path (
    name text,
    from_version text,
    to_version text,
    ext text
)
SERVER replica_controller
OPTIONS (table_name 'tle_extension_update_path');

CREATE FOREIGN TABLE fdw_extension_to_install (
    name text,
    target_version text
)
SERVER replica_controller
OPTIONS (table_name 'extension_to_install');

CREATE FUNCTION tle_sync() RETURNS SETOF text LANGUAGE sql AS
$$
    -- Install all extensions that we don't have
    SELECT
        pgtle.install_extension(name, version, description, ext, requires)
    FROM
        "@extschema@".fdw_tle_available_extension tae
            JOIN "@extschema@".fdw_tle_extension_version USING (name)
    WHERE
            default_version = version
        AND NOT EXISTS (SELECT 1 FROM
                pgtle.available_extensions()
                WHERE name = tae.name
            );

    -- Install all ext versions that we don't have yet
    SELECT
        pgtle.install_extensions_version_sql(name, version, ext)
    FROM
        "@extschema@".fdw_tle_extension_version tev
            JOIN pgtle.available_extensions() USING (name)
    WHERE
        NOT EXISTS (SELECT 1 FROM
            pgtle.available_extension_versions()
            WHERE (name, version) = (tev.name, tev.version)
        );

    -- Install all update paths we don't have yet
    SELECT
        pgtle.install_update_path(name, from_version, to_version, ext)
    FROM
        "@extschema@".fdw_tle_update_path tup
    WHERE
        NOT EXISTS (SELECT 1 FROM
            pgtle.extension_update_paths(tup.name)
                    WHERE
                        (source, target) = (tup.from_version, tup.to_version)
        );

    -- Set default versions of updated extensions
    SELECT
        pgtle.set_default_version(name, remote.default_version)
    FROM
        "@extschema@".fdw_tle_available_extension remote
            JOIN pgtle.available_extensions() local USING (name)
    WHERE
        remote.default_version <> local.default_version;

    -- Return statements creating and updating extensions
    -- (if there are any to install/update)
    SELECT
        format('CREATE EXTENSION %I VERSION %L CASCADE', name, target_version)
    FROM
        "@extschema@".fdw_extension_to_install eti
    WHERE
        NOT EXISTS (SELECT 1 FROM
            pg_extension
            WHERE
                extname = eti.name
    )
    UNION ALL
    SELECT
        format('ALTER EXTENSION %I UPDATE TO %L', name, target_version)
    FROM
        "@extschema@".fdw_extension_to_install eti
            JOIN pg_extension e ON extname = name AND extversion <> target_version
    WHERE
        EXISTS (SELECT 1 FROM pgtle.extension_update_paths(name) WHERE source = extversion AND target = target_version);
$$;
