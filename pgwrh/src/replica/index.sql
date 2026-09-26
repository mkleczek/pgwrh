-- name: replica-index
-- requires: replica-fdw
-- requires: replica-helpers

-- Intent and retry diagnostics survive restart/upgrade. Runtime reservations
-- live in server-wide DSM, never in this database-local table.
CREATE TABLE index_build_job (
    job_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    schema_name text NOT NULL,
    table_name text NOT NULL,
    index_name name NOT NULL,
    index_template text NOT NULL,
    attempts integer NOT NULL DEFAULT 0,
    retry_after timestamptz NOT NULL DEFAULT '-infinity',
    last_sqlstate text,
    last_error text,
    completed_at timestamptz,
    UNIQUE (schema_name, index_name)
);
SELECT pg_catalog.pg_extension_config_dump('index_build_job', '');
SELECT pg_catalog.pg_extension_config_dump('index_build_job_job_id_seq', '');

CREATE FUNCTION launch_index_build(bigint, oid) RETURNS boolean
AS 'pgwrh', 'pgwrh_launch_index_build' LANGUAGE C STRICT VOLATILE;
CREATE FUNCTION index_build_tasks()
RETURNS TABLE (datid oid, relid oid, job_id bigint, pid integer, starting boolean)
AS 'pgwrh', 'pgwrh_index_build_tasks' LANGUAGE C VOLATILE;
REVOKE ALL ON FUNCTION launch_index_build(bigint, oid), index_build_tasks() FROM PUBLIC;

-- Controller templates on partitioned roots are expanded onto physical shards
-- by shard_assigned_index. Never recursively build indexes on routing parents:
-- they can contain foreign partitions and CIC does not support them.
CREATE VIEW desired_local_index AS
SELECT lr.reg_class, lr.rel_id, si.*
FROM fdw_shard_index si JOIN local_rel lr
    ON (si.schema_name, si.table_name) = ((lr.rel_id).schema_name, (lr.rel_id).table_name)
WHERE (lr.pc).relkind = 'r';

CREATE VIEW missing_local_index AS
SELECT si.* FROM desired_local_index si
WHERE NOT EXISTS (
    SELECT FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
    WHERE i.indrelid = si.reg_class AND c.relname = si.index_name AND i.indisvalid
      AND EXISTS (SELECT FROM owned_obj o WHERE o.classid = 'pg_class'::regclass AND o.objid = c.oid)
);

CREATE FUNCTION fail_index_build(_job bigint, _state text, _error text) RETURNS void
LANGUAGE sql SET search_path = pg_catalog AS $$
    UPDATE "@extschema@".index_build_job SET last_sqlstate = _state, last_error = _error,
        retry_after = clock_timestamp() + make_interval(secs => least(60, power(2, least(attempts, 6)))::int)
    WHERE job_id = _job
$$;

CREATE FUNCTION enqueue_index_build(_schema text, _table text, _index name, _template text)
RETURNS boolean LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    job "@extschema@".index_build_job;
    rel oid;
    state text;
    message text;
BEGIN
    rel := to_regclass(format('%I.%I', _schema, _table));
    IF NOT EXISTS (SELECT FROM pg_class WHERE oid = rel AND relkind = 'r') THEN
        RAISE EXCEPTION 'Index builds require a physical shard table: %.%', _schema, _table;
    END IF;
    INSERT INTO "@extschema@".index_build_job(schema_name, table_name, index_name, index_template)
        VALUES (_schema, _table, _index, _template) ON CONFLICT DO NOTHING;
    SELECT * INTO job FROM "@extschema@".index_build_job
        WHERE schema_name = _schema AND index_name = _index FOR UPDATE;
    IF (job.table_name, job.index_template) IS DISTINCT FROM (_table, _template) THEN
        RAISE EXCEPTION 'Index job identity already has a different definition: %.%', _schema, _index;
    END IF;
    IF job.retry_after > clock_timestamp() THEN RETURN false; END IF;
    BEGIN
        IF NOT "@extschema@".launch_index_build(job.job_id, rel) THEN RETURN false; END IF;
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS state = RETURNED_SQLSTATE, message = MESSAGE_TEXT;
        UPDATE "@extschema@".index_build_job SET attempts = attempts + 1 WHERE job_id = job.job_id;
        PERFORM "@extschema@".fail_index_build(job.job_id, state, message);
        RETURN false;
    END;
    UPDATE "@extschema@".index_build_job SET attempts = attempts + 1, completed_at = NULL,
        -- Also throttle workers killed before they can persist an error.
        retry_after = clock_timestamp() + make_interval(secs => least(60, power(2, least(attempts + 1, 6)))::int)
    WHERE job_id = job.job_id;
    RETURN true;
END
$$;

-- A worker can exit after the valid-index commit but before final bookkeeping.
-- Only the catalog AND the durable ownership registry prove completion.
CREATE FUNCTION recover_index_builds() RETURNS void
LANGUAGE sql SET search_path = pg_catalog AS $$
    UPDATE "@extschema@".index_build_job j SET completed_at = clock_timestamp(),
        retry_after = '-infinity', last_sqlstate = NULL, last_error = NULL
    FROM pg_namespace n, pg_class t, pg_class c, pg_index i, "@extschema@".owned_obj o
    WHERE j.completed_at IS NULL AND n.nspname = j.schema_name
      AND t.relnamespace = n.oid AND t.relname = j.table_name
      AND c.relnamespace = n.oid AND c.relname = j.index_name
      AND i.indexrelid = c.oid AND i.indrelid = t.oid AND i.indisvalid
      AND o.classid = 'pg_class'::regclass AND o.objid = c.oid
      AND NOT EXISTS (SELECT FROM "@extschema@".index_build_tasks() active
          WHERE active.datid = (SELECT oid FROM pg_database WHERE datname = current_database())
            AND active.job_id = j.job_id)
$$;

CREATE FUNCTION schedule_index_builds() RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE candidate record;
BEGIN
    PERFORM "@extschema@".recover_index_builds();
    -- Initial subscription setup truncates the table before copying it. Do
    -- not race that DDL with CIC's session locks and old-snapshot wait.
    FOR candidate IN SELECT i.* FROM "@extschema@".missing_local_index i
        WHERE EXISTS (SELECT FROM "@extschema@".subscribed_local_shard s WHERE s.reg_class = i.reg_class)
        ORDER BY optional, schema_name, table_name, index_name LOOP
        PERFORM "@extschema@".enqueue_index_build(candidate.schema_name, candidate.table_name,
            candidate.index_name, candidate.index_template);
    END LOOP;
END
$$;

CREATE FUNCTION index_build_command(_job bigint, _relation oid, _cleanup boolean) RETURNS text
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    job "@extschema@".index_build_job;
    rel oid;
    idx oid;
    valid boolean;
BEGIN
    SELECT * INTO STRICT job FROM "@extschema@".index_build_job WHERE job_id = _job;
    rel := to_regclass(format('%I.%I', job.schema_name, job.table_name));
    IF rel IS DISTINCT FROM _relation OR
       NOT EXISTS (SELECT FROM pg_class WHERE oid = rel AND relkind = 'r') THEN
        RAISE EXCEPTION 'Index target is no longer a physical table';
    END IF;
    -- Retained through the following top-level command, closing rename/drop
    -- races between resolving the job identity and PostgreSQL's own DDL locks.
    EXECUTE format('LOCK TABLE %I.%I IN ACCESS SHARE MODE', job.schema_name, job.table_name);
    idx := to_regclass(format('%I.%I', job.schema_name, job.index_name));
    IF idx IS NOT NULL THEN
        IF NOT "@extschema@".lock_managed_object('pg_class', idx) OR NOT EXISTS (
            SELECT FROM "@extschema@".owned_obj WHERE classid = 'pg_class'::regclass AND objid = idx
        ) THEN
            RAISE EXCEPTION 'Refusing to adopt or drop unrelated index %.%', job.schema_name, job.index_name;
        END IF;
        SELECT indisvalid INTO valid FROM pg_index WHERE indexrelid = idx AND indrelid = rel;
        IF NOT FOUND THEN RAISE EXCEPTION 'Index identity belongs to a different table'; END IF;
        IF valid THEN RETURN NULL; END IF;
        IF _cleanup THEN
            RETURN format('DROP INDEX CONCURRENTLY %I.%I', job.schema_name, job.index_name);
        END IF;
        RAISE EXCEPTION 'Invalid index still exists after cleanup';
    END IF;
    IF _cleanup THEN RETURN NULL; END IF;
    RETURN format('CREATE INDEX CONCURRENTLY %I ON %I.%I %s',
        job.index_name, job.schema_name, job.table_name, job.index_template);
END
$$;

CREATE FUNCTION register_index_build(_job bigint, _index oid) RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM "@extschema@".index_build_job j
        JOIN pg_namespace n ON n.nspname = j.schema_name
        JOIN pg_class t ON t.relnamespace = n.oid AND t.relname = j.table_name
        JOIN pg_index i ON i.indrelid = t.oid
        JOIN pg_class c ON c.oid = i.indexrelid AND c.relnamespace = n.oid AND c.relname = j.index_name
        WHERE j.job_id = _job AND c.oid = _index
    ) THEN RAISE EXCEPTION 'Created index does not match its intended job'; END IF;
    PERFORM "@extschema@".add_ext_dependency('pg_class', _index);
END
$$;

CREATE FUNCTION finish_index_build(_job bigint) RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE idx oid;
BEGIN
    SELECT c.oid INTO idx FROM "@extschema@".index_build_job j
        JOIN pg_namespace n ON n.nspname = j.schema_name
        JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = j.index_name
        JOIN pg_class t ON t.relnamespace = n.oid AND t.relname = j.table_name
        JOIN pg_index i ON i.indexrelid = c.oid AND i.indisvalid
            AND i.indrelid = t.oid
        JOIN "@extschema@".owned_obj o ON o.classid = 'pg_class'::regclass AND o.objid = c.oid
        WHERE j.job_id = _job;
    IF idx IS NULL THEN RAISE EXCEPTION 'Index did not finish as a valid managed index'; END IF;
    PERFORM "@extschema@".add_ext_dependency('pg_class', idx);
    UPDATE "@extschema@".index_build_job SET completed_at = clock_timestamp(),
        retry_after = '-infinity', last_error = NULL, last_sqlstate = NULL WHERE job_id = _job;
END
$$;

CREATE VIEW index_build_status AS
SELECT j.*, t.pid, t.relid, t.starting,
    p.index_relid, p.phase, p.current_locker_pid,
    CASE WHEN t.pid IS NULL THEN ARRAY[]::integer[] ELSE pg_blocking_pids(t.pid) END AS blocking_pids
FROM index_build_job j LEFT JOIN index_build_tasks() t
    ON t.datid = (SELECT oid FROM pg_database WHERE datname = current_database()) AND t.job_id = j.job_id
LEFT JOIN pg_stat_progress_create_index p ON p.pid = t.pid;

REVOKE ALL ON FUNCTION fail_index_build(bigint, text, text),
    enqueue_index_build(text, text, name, text), recover_index_builds(), schedule_index_builds(),
    index_build_command(bigint, oid, boolean), register_index_build(bigint, oid),
    finish_index_build(bigint) FROM PUBLIC;
