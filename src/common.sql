-- name: common

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

\echo Use "CREATE EXTENSION pgwrh CASCADE" to load this file. \quit

GRANT USAGE ON SCHEMA "@extschema@" TO PUBLIC;

CREATE FUNCTION pgwrh_replica_role_name() RETURNS text IMMUTABLE LANGUAGE sql AS
$$
    SELECT format('pgwrh_replica_%s', current_database());
$$;
CREATE FUNCTION exec_dynamic(cmd text) RETURNS void LANGUAGE plpgsql AS
$$
BEGIN
    EXECUTE cmd;
END;
$$;

DO
$$
DECLARE
    r record;
BEGIN
    FOR r IN SELECT format('CREATE ROLE %I', pgwrh_replica_role_name()) AS stmt WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = pgwrh_replica_role_name()) LOOP
        EXECUTE r.stmt;
    END LOOP;
END
$$;

CREATE OR REPLACE FUNCTION add_ext_dependency(_classid regclass, _objid oid) RETURNS void LANGUAGE sql AS
$$
    INSERT INTO pg_depend (classid, objid, refclassid, refobjid, deptype, objsubid, refobjsubid)
    SELECT _classid, _objid, 'pg_extension'::regclass, e.oid, 'n', 0 ,0
    FROM pg_extension e WHERE e.extname = 'pgwrh'
$$;

CREATE OR REPLACE FUNCTION select_add_ext_dependency(_classid regclass, oidexpr text) RETURNS text LANGUAGE sql AS
$$SELECT format('SELECT "@extschema@".add_ext_dependency(%L, %s)', _classid, oidexpr)$$;

CREATE OR REPLACE FUNCTION select_add_ext_dependency(_classid regclass, name_attr text, name text) RETURNS text LANGUAGE sql AS
$$SELECT format('SELECT "@extschema@".add_ext_dependency(%1$L, (SELECT oid FROM %1$s WHERE %I = %L))', _classid, name_attr, name)$$;

CREATE OR REPLACE FUNCTION is_dependent_object(_classid regclass, _objid oid) RETURNS boolean STABLE LANGUAGE sql AS
$$
    SELECT EXISTS (SELECT 1 FROM
        pg_depend
            JOIN pg_extension e ON refclassid = 'pg_extension'::regclass AND refobjid = e.oid
        WHERE
            e.extname = 'pgwrh'
        AND
            classid = _classid
        AND
            objid = _objid
    )
$$;

CREATE VIEW owned_obj AS
    SELECT
        classid,
        objid
    FROM
        pg_depend d JOIN pg_extension e ON
                refclassid = 'pg_extension'::regclass
            AND refobjid = e.oid
    WHERE
            d.deptype = 'n'
        AND e.extname = 'pgwrh'
;

CREATE VIEW owned_server AS
    SELECT
        s.*
    FROM
        pg_foreign_server s JOIN owned_obj ON
                classid = 'pg_foreign_server'::regclass
            AND objid = s.oid
;