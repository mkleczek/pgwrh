-- Managed identities survive both logical restore and pg_upgrade. The normal
-- pg_depend rows below are only a derived cache for DROP EXTENSION protection.
CREATE TABLE managed_object (
    object_kind text NOT NULL,
    schema_name text NOT NULL,
    object_name text NOT NULL,
    PRIMARY KEY (object_kind, schema_name, object_name),
    CHECK (object_kind IN ('table', 'foreign table', 'view', 'materialized view',
                          'index', 'sequence', 'schema', 'server', 'publication'))
);

-- CREATE EXTENSION seeds the ping publication itself. Do not COPY its row on
-- top of that seed during a logical restore. Binary upgrade copies all rows.
SELECT pg_catalog.pg_extension_config_dump('managed_object',
    $$WHERE NOT (object_kind = 'publication' AND schema_name = '' AND object_name = 'pgwrh_controller_ping')$$);

CREATE VIEW managed_catalog_object AS
SELECT 'pg_class'::regclass::oid AS classid, c.oid AS objid,
       CASE c.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'table'
            WHEN 'f' THEN 'foreign table' WHEN 'v' THEN 'view'
            WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index'
            WHEN 'I' THEN 'index' WHEN 'S' THEN 'sequence' END AS object_kind,
       n.nspname::text AS schema_name, c.relname::text AS object_name
FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p', 'f', 'v', 'm', 'i', 'I', 'S')
UNION ALL
SELECT 'pg_namespace'::regclass::oid, oid, 'schema', '', nspname::text FROM pg_catalog.pg_namespace
UNION ALL
SELECT 'pg_foreign_server'::regclass::oid, oid, 'server', '', srvname::text FROM pg_catalog.pg_foreign_server
UNION ALL
SELECT 'pg_publication'::regclass::oid, oid, 'publication', '', pubname::text FROM pg_catalog.pg_publication;

CREATE OR REPLACE VIEW owned_obj AS
SELECT c.classid, c.objid FROM "@extschema@".managed_object m
JOIN "@extschema@".managed_catalog_object c USING (object_kind, schema_name, object_name);

CREATE OR REPLACE FUNCTION is_dependent_object(_classid regclass, _objid oid)
RETURNS boolean STABLE LANGUAGE sql SET search_path = pg_catalog AS
$$ SELECT EXISTS (SELECT FROM "@extschema@".owned_obj WHERE classid = _classid AND objid = _objid) $$;

CREATE OR REPLACE VIEW owned_server AS
SELECT s.* FROM pg_catalog.pg_foreign_server s JOIN "@extschema@".owned_obj o
ON o.classid = 'pg_foreign_server'::regclass AND o.objid = s.oid;

-- Hold the same object locks used by PostgreSQL DDL while editing pg_depend.
CREATE FUNCTION lock_managed_object(regclass, oid) RETURNS boolean
AS 'pgwrh', 'pgwrh_lock_managed_object' LANGUAGE C STRICT VOLATILE;
REVOKE ALL ON FUNCTION lock_managed_object(regclass, oid) FROM PUBLIC;

CREATE OR REPLACE FUNCTION add_ext_dependency(_classid regclass, _objid oid)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS
$$
BEGIN
    IF NOT "@extschema@".lock_managed_object(_classid, _objid) OR
       NOT EXISTS (SELECT FROM "@extschema@".managed_catalog_object
                   WHERE classid = _classid AND objid = _objid) THEN
        RAISE EXCEPTION 'Unsupported or missing managed object: catalog %, OID %', _classid, _objid;
    END IF;
    INSERT INTO "@extschema@".managed_object
        SELECT object_kind, schema_name, object_name FROM "@extschema@".managed_catalog_object
        WHERE classid = _classid AND objid = _objid
        ON CONFLICT DO NOTHING;
    -- Serialize repeated registration of the same identity, including when its
    -- registry row survived but its derived marker did not.
    PERFORM FROM "@extschema@".managed_object m
        JOIN "@extschema@".managed_catalog_object c USING (object_kind, schema_name, object_name)
        WHERE c.classid = _classid AND c.objid = _objid FOR UPDATE OF m;
    INSERT INTO pg_catalog.pg_depend (classid, objid, refclassid, refobjid, deptype, objsubid, refobjsubid)
        SELECT _classid, _objid, 'pg_extension'::regclass, e.oid, 'n', 0, 0
        FROM pg_catalog.pg_extension e WHERE e.extname = 'pgwrh' AND NOT EXISTS (
            SELECT FROM pg_catalog.pg_depend d WHERE d.classid = _classid AND d.objid = _objid
            AND d.refclassid = 'pg_extension'::regclass AND d.refobjid = e.oid
            AND d.deptype = 'n' AND d.objsubid = 0 AND d.refobjsubid = 0);
END
$$;

CREATE FUNCTION repair_managed_objects() RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog AS
$$
BEGIN
    -- Registration takes a row-exclusive table lock. Serialize repair against
    -- registration and other repairs so an object gets exactly one marker.
    LOCK TABLE "@extschema@".managed_object IN SHARE ROW EXCLUSIVE MODE;
    -- Lock only objects that need a marker. Unfenced, the planner evaluates
    -- the lock while scanning every catalog row, before joining the registry.
    WITH missing AS MATERIALIZED (
        SELECT o.classid, o.objid, e.oid AS extoid
        FROM "@extschema@".owned_obj o CROSS JOIN pg_catalog.pg_extension e
        WHERE e.extname = 'pgwrh' AND NOT EXISTS (
            SELECT FROM pg_catalog.pg_depend d WHERE d.classid = o.classid AND d.objid = o.objid
            AND d.refclassid = 'pg_extension'::regclass AND d.refobjid = e.oid
            AND d.deptype = 'n' AND d.objsubid = 0 AND d.refobjsubid = 0))
    INSERT INTO pg_catalog.pg_depend (classid, objid, refclassid, refobjid, deptype, objsubid, refobjsubid)
        SELECT classid, objid, 'pg_extension'::regclass, extoid, 'n', 0, 0 FROM missing
        WHERE "@extschema@".lock_managed_object(classid::regclass, objid);
END
$$;
REVOKE ALL ON FUNCTION repair_managed_objects() FROM PUBLIC;
REVOKE ALL ON FUNCTION add_ext_dependency(regclass, oid) FROM PUBLIC;

-- These are object names already exposed by PostgreSQL's catalogs. Writes and
-- repair remain administrator-only; invoker functions can still resolve names.
GRANT SELECT ON managed_object, managed_catalog_object TO PUBLIC;

-- Transient, backend-scoped OIDs bridge an ALTER's old and new names. They are
-- neither ownership nor restore data. No table-wide lock is held across DDL;
-- sync commands can run in separate pg_background transactions.
CREATE UNLOGGED TABLE managed_object_ddl (
    backend_pid integer NOT NULL,
    classid oid NOT NULL,
    objid oid NOT NULL,
    object_kind text NOT NULL,
    schema_name text NOT NULL,
    object_name text NOT NULL,
    PRIMARY KEY (backend_pid, classid, objid)
);

CREATE FUNCTION managed_object_ddl_start() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS
$$
BEGIN
    DELETE FROM "@extschema@".managed_object_ddl WHERE backend_pid = pg_backend_pid();
    INSERT INTO "@extschema@".managed_object_ddl
        SELECT pg_backend_pid(), c.classid, c.objid, m.object_kind, m.schema_name, m.object_name
        FROM "@extschema@".managed_object m JOIN "@extschema@".managed_catalog_object c
        USING (object_kind, schema_name, object_name);
END
$$;

CREATE FUNCTION managed_object_ddl_end() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS
$$
BEGIN
    IF EXISTS (SELECT FROM "@extschema@".managed_object_ddl d
               JOIN "@extschema@".managed_catalog_object c USING (classid, objid)
               WHERE d.backend_pid = pg_backend_pid() AND d.object_kind = 'publication'
                 AND d.object_name = 'pgwrh_controller_ping' AND c.object_name <> d.object_name) THEN
        RAISE EXCEPTION 'The pgwrh controller ping publication cannot be renamed';
    END IF;
    UPDATE "@extschema@".managed_object m
        SET object_kind = c.object_kind, schema_name = c.schema_name, object_name = c.object_name
        FROM "@extschema@".managed_object_ddl d
        JOIN "@extschema@".managed_catalog_object c USING (classid, objid)
        WHERE d.backend_pid = pg_backend_pid()
          AND (m.object_kind, m.schema_name, m.object_name) = (d.object_kind, d.schema_name, d.object_name)
          AND (c.object_kind, c.schema_name, c.object_name) IS DISTINCT FROM
              (d.object_kind, d.schema_name, d.object_name);
    DELETE FROM "@extschema@".managed_object_ddl WHERE backend_pid = pg_backend_pid();
END
$$;

CREATE FUNCTION managed_object_sql_drop() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS
$$
BEGIN
    -- DROP EXTENSION ... CASCADE can remove the registry/trigger themselves.
    IF to_regclass('"@extschema@".managed_object') IS NULL THEN RETURN; END IF;
    DELETE FROM "@extschema@".managed_object m
        USING pg_event_trigger_dropped_objects() d
        WHERE d.objsubid = 0 AND m.object_kind = d.object_type
          AND m.schema_name = coalesce(d.schema_name, '') AND m.object_name = d.object_name;
END
$$;

CREATE EVENT TRIGGER pgwrh_managed_ddl_start ON ddl_command_start
WHEN TAG IN ('ALTER TABLE', 'ALTER FOREIGN TABLE', 'ALTER VIEW', 'ALTER MATERIALIZED VIEW',
             'ALTER INDEX', 'ALTER SEQUENCE', 'ALTER SCHEMA', 'ALTER SERVER', 'ALTER PUBLICATION')
EXECUTE FUNCTION managed_object_ddl_start();
CREATE EVENT TRIGGER pgwrh_managed_ddl_end ON ddl_command_end
WHEN TAG IN ('ALTER TABLE', 'ALTER FOREIGN TABLE', 'ALTER VIEW', 'ALTER MATERIALIZED VIEW',
             'ALTER INDEX', 'ALTER SEQUENCE', 'ALTER SCHEMA', 'ALTER SERVER', 'ALTER PUBLICATION')
EXECUTE FUNCTION managed_object_ddl_end();
CREATE EVENT TRIGGER pgwrh_managed_sql_drop ON sql_drop EXECUTE FUNCTION managed_object_sql_drop();
