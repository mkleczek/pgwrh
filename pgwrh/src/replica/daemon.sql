-- name: replica-daemon
-- requires: replica-sync
-- requires: replica-status

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

CREATE OR REPLACE FUNCTION bg_detach(_handle "@extschema:pg_background@".pg_background_handle) RETURNS void LANGUAGE plpgsql AS
$$
BEGIN
    IF _handle IS NULL OR (_handle).pid IS NULL THEN
        RETURN;
    END IF;

    PERFORM "@extschema:pg_background@".pg_background_detach_v2((_handle).pid, (_handle).cookie);
END
$$;

CREATE OR REPLACE FUNCTION bg_safe_detach(_handle "@extschema:pg_background@".pg_background_handle) RETURNS void LANGUAGE plpgsql AS
$$
BEGIN
    -- Error paths may race with worker-side cleanup, so detach must be best-effort here.
    BEGIN
        PERFORM "@extschema@".bg_detach(_handle);
    EXCEPTION
        WHEN OTHERS THEN
            NULL;
    END;
END
$$;

CREATE OR REPLACE FUNCTION bg_exec_wait(script text) RETURNS void LANGUAGE plpgsql AS
$$
DECLARE
    _handle "@extschema:pg_background@".pg_background_handle;
BEGIN
    -- Consume the response so worker errors reach the caller and a full response
    -- queue cannot block completion. wait_v2 alone does neither. Commands return
    -- command tags; scripts returning rows must end with a single text column.
    SELECT
        *
    INTO
        _handle
    FROM
        "@extschema:pg_background@".pg_background_launch_v2(script);

    PERFORM * FROM "@extschema:pg_background@".pg_background_result_v2((_handle).pid, (_handle).cookie)
        AS discarded(result text);
    _handle := NULL;
EXCEPTION
    WHEN OTHERS THEN
        PERFORM "@extschema@".bg_safe_detach(_handle);
        RAISE;
END
$$;

CREATE OR REPLACE FUNCTION bg_query_bool(script text) RETURNS boolean LANGUAGE plpgsql AS
$$
DECLARE
    _handle "@extschema:pg_background@".pg_background_handle;
    _result boolean;
BEGIN
    -- sync_replica_worker uses this for the "should I run another pass?" handshake with sync_step.
    SELECT
        *
    INTO
        _handle
    FROM
        "@extschema:pg_background@".pg_background_launch_v2(script);

    SELECT
        r.result
    INTO
        _result
    FROM
        "@extschema:pg_background@".pg_background_result_v2((_handle).pid, (_handle).cookie) AS r(result boolean);

    -- result_v2 consumes and detaches the worker handle.
    _handle := NULL;
    RETURN _result;
EXCEPTION
    WHEN OTHERS THEN
        PERFORM "@extschema@".bg_safe_detach(_handle);
        RAISE;
END
$$;

CREATE OR REPLACE FUNCTION bg_sync_commands()
    RETURNS TABLE (async boolean, transactional boolean, description text, commands text[])
    LANGUAGE plpgsql AS
$$
DECLARE
    _handle "@extschema:pg_background@".pg_background_handle;
BEGIN
    -- Read the sync plan in a separate transaction so sync_step does not keep any locks
    -- from querying sync while it is busy executing the returned commands.
    SELECT
        *
    INTO
        _handle
    FROM
        -- The metadata plan expands many catalog expressions but returns few
        -- rows. JIT compilation can dominate every pass on LLVM-enabled servers.
        -- This worker owns its session, so application query settings are untouched.
        "@extschema:pg_background@".pg_background_launch_v2('SET jit = off; select async, transactional, description, commands from "@extschema@".sync');

    RETURN QUERY
        SELECT
            r.async,
            r.transactional,
            r.description,
            r.commands
        FROM
            "@extschema:pg_background@".pg_background_result_v2((_handle).pid, (_handle).cookie)
                AS r(async boolean, transactional boolean, description text, commands text[]);

    -- result_v2 consumes and detaches the worker handle.
    _handle := NULL;
EXCEPTION
    WHEN OTHERS THEN
        PERFORM "@extschema@".bg_safe_detach(_handle);
        RAISE;
END
$$;

CREATE OR REPLACE FUNCTION bg_submit_detached(commands text) RETURNS void LANGUAGE plpgsql AS
$$
DECLARE
    _handle "@extschema:pg_background@".pg_background_handle;
BEGIN
    -- submit_v2 is the fire-and-forget path: once the worker is launched we immediately
    -- detach and let it continue independently of the caller.
    SELECT
        *
    INTO
        _handle
    FROM
        "@extschema:pg_background@".pg_background_submit_v2(commands);

    PERFORM "@extschema@".bg_detach(_handle);
EXCEPTION
    WHEN OTHERS THEN
        PERFORM "@extschema@".bg_safe_detach(_handle);
        RAISE;
END
$$;

-- Expose the public entry point through the cookie-protected worker API.
CREATE OR REPLACE FUNCTION launch_in_background(commands text) RETURNS void LANGUAGE sql AS
$$
SELECT "@extschema@".bg_submit_detached(commands);
$$;

CREATE OR REPLACE FUNCTION launch_sync() RETURNS void LANGUAGE sql AS
$$
SELECT "@extschema@".bg_submit_detached('CAll "@extschema@".sync_replica_worker();')
$$;

-- Durable intent, also carried through pg_upgrade and logical restore. Empty on
-- controllers: creating the extension alone must not start replica sync.
CREATE TABLE sync_daemon_config (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    enabled boolean NOT NULL,
    refresh_seconds real NOT NULL CHECK (refresh_seconds > 0 AND refresh_seconds < 'Infinity'::real),
    application_name text NOT NULL
);
SELECT pg_catalog.pg_extension_config_dump('sync_daemon_config', '');

CREATE OR REPLACE PROCEDURE sync_daemon(seconds real, _application_name text DEFAULT 'pgwrh_sync_daemon') LANGUAGE plpgsql AS
$$
DECLARE
    err text;
BEGIN
    IF pg_try_advisory_lock(517384732) THEN
        -- The launching transaction may not have committed its saved intent.
        -- Wait for writers of the settings so the loop sees commit or rollback.
        LOCK TABLE "@extschema@".sync_daemon_config IN SHARE MODE;
        PERFORM "@extschema@".repair_managed_objects();
        -- Release repair's locks before waiting on separately committed workers.
        COMMIT;
        -- Read the saved settings in a fresh transaction after waiting. The
        -- launch arguments may belong to a start request that was rolled back.
        SELECT c.refresh_seconds, c.application_name INTO seconds, _application_name
        FROM "@extschema@".sync_daemon_config c WHERE c.enabled;
        IF NOT FOUND THEN
            RETURN;
        END IF;
        PERFORM set_config('application_name', _application_name, FALSE);
        LOOP
            EXIT WHEN NOT EXISTS (SELECT FROM "@extschema@".sync_daemon_config WHERE enabled);
            BEGIN
                CALL "@extschema@".sync_replica_worker();
            EXCEPTION
                WHEN OTHERS THEN
                    GET STACKED DIAGNOSTICS err = MESSAGE_TEXT;
                    RAISE WARNING '%', err;
            END;
            COMMIT;
            PERFORM pg_sleep(seconds);
            EXIT WHEN NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pgwrh');
        END LOOP;
    END IF;
END
$$;

-- pg_background copies the launching session's settings into the daemon and
-- its workers. A statement_timeout from the caller, role or database would
-- cancel the long-running daemon, so both launchers clear it.
CREATE OR REPLACE FUNCTION start_sync_daemon(seconds real, application_name text DEFAULT 'pgwrh_sync_daemon')
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog SET statement_timeout = 0 AS
$$
BEGIN
    INSERT INTO "@extschema@".sync_daemon_config VALUES (true, true, seconds, application_name)
    ON CONFLICT (singleton) DO UPDATE SET enabled = true,
        refresh_seconds = EXCLUDED.refresh_seconds, application_name = EXCLUDED.application_name;
    PERFORM "@extschema@".bg_submit_detached(format('CALL "@extschema@".sync_daemon(%s, %L)', seconds, application_name));
END
$$;
REVOKE ALL ON FUNCTION start_sync_daemon(real, text) FROM PUBLIC;

CREATE FUNCTION stop_sync_daemon() RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS
$$
BEGIN
    UPDATE "@extschema@".sync_daemon_config SET enabled = false;
    -- Only terminate the daemon holding our session lock in this database.
    PERFORM pg_terminate_backend(l.pid) FROM pg_catalog.pg_locks l
    JOIN pg_catalog.pg_stat_activity a ON a.pid = l.pid
    WHERE l.locktype = 'advisory' AND l.database = (SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database())
      AND l.classid = 0 AND l.objid = 517384732 AND l.objsubid = 1 AND l.granted
      AND a.backend_type = 'pg_background' AND l.pid <> pg_backend_pid();
END
$$;
REVOKE ALL ON FUNCTION stop_sync_daemon() FROM PUBLIC;

-- Called by the preloaded supervisor and the replication ping. These entry
-- points respect saved intent; only start_sync_daemon enables a stopped daemon.
CREATE FUNCTION supervise_sync_daemon() RETURNS void LANGUAGE plpgsql
SET search_path = pg_catalog SET statement_timeout = 0 AS
$$
DECLARE
    config "@extschema@".sync_daemon_config;
BEGIN
    IF EXISTS (SELECT FROM "@extschema@".owned_obj o WHERE NOT EXISTS (
        SELECT FROM pg_catalog.pg_depend d JOIN pg_catalog.pg_extension e ON e.oid = d.refobjid
        WHERE e.extname = 'pgwrh' AND d.refclassid = 'pg_extension'::regclass
          AND d.classid = o.classid AND d.objid = o.objid AND d.deptype = 'n'
          AND d.objsubid = 0 AND d.refobjsubid = 0)) THEN
        PERFORM "@extschema@".repair_managed_objects();
    END IF;
    SELECT * INTO config FROM "@extschema@".sync_daemon_config WHERE enabled;
    IF FOUND AND NOT EXISTS (SELECT FROM pg_catalog.pg_locks
        WHERE locktype = 'advisory' AND database = (SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database())
          AND classid = 0 AND objid = 517384732 AND objsubid = 1 AND granted) THEN
        PERFORM "@extschema@".bg_submit_detached(format('CALL "@extschema@".sync_daemon(%s, %L)',
            config.refresh_seconds, config.application_name));
    END IF;
END
$$;
REVOKE ALL ON FUNCTION supervise_sync_daemon() FROM PUBLIC;

CREATE OR REPLACE FUNCTION exec_script(script text) RETURNS boolean LANGUAGE plpgsql AS
$$
DECLARE
    err text;
BEGIN
    PERFORM "@extschema@".bg_exec_wait(script);
    RETURN TRUE;
EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS err = MESSAGE_TEXT;
        raise WARNING '%', err;
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
        PERFORM "@extschema@".bg_exec_wait(cmd);
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
    IF pg_try_advisory_xact_lock(2895359559) THEN
        -- bg_sync_commands snapshots the sync plan in a separate transaction before we
        -- start executing it here.
        FOR r IN
            SELECT
                *
            FROM
                "@extschema@".bg_sync_commands()
        LOOP
            RAISE NOTICE '%', r.description;
            IF r.transactional THEN
                IF r.async THEN
                    PERFORM "@extschema@".bg_submit_detached(array_to_string(r.commands, ';'));
                ELSE
                    PERFORM "@extschema@".exec_script(array_to_string(r.commands || 'SELECT '''''::text, ';'));
                END IF;
            ELSE
                IF r.async THEN
                    IF array_length(r.commands, 1) > 1 THEN
                        PERFORM "@extschema@".bg_submit_detached(format('SELECT "@extschema@".exec_non_tx_scripts(ARRAY[%s])', (SELECT string_agg(format('%L', c), ',') FROM unnest(r.commands) AS c)));
                    ELSE
                        PERFORM "@extschema@".bg_submit_detached(r.commands[1]);
                    END IF;
                ELSE
                    FOREACH cmd IN ARRAY r.commands LOOP
                        -- A failed subscription change must not be followed by
                        -- truncating a copy that is still subscribed.
                        EXIT WHEN NOT "@extschema@".exec_script(cmd);
                    END LOOP;
                END IF;
            END IF;
        END LOOP;
        -- Index admission is separate from the synchronous plan: an active or
        -- capacity-limited build must not keep the pass loop busy or suppress
        -- reports about healthy shards. The launcher commits durable intent
        -- before its workers start catalog work.
        DECLARE had_commands boolean := FOUND;
        BEGIN
            PERFORM "@extschema@".bg_exec_wait('SELECT "@extschema@".schedule_index_builds()::text');
            RETURN had_commands;
        END;
    ELSE
        RETURN FALSE;
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS err = MESSAGE_TEXT;
        raise WARNING '%', err;
        PERFORM pg_sleep(1);
        RETURN TRUE;
END
$$;

CREATE OR REPLACE PROCEDURE sync_replica_worker() LANGUAGE plpgsql AS
$$
BEGIN
    LOOP
        DECLARE again boolean;
        BEGIN
            again := "@extschema@".bg_query_bool('SELECT "@extschema@".sync_step()');
            PERFORM "@extschema@".bg_exec_wait('SELECT ''ignored'' FROM "@extschema@".report_state()');
            EXIT WHEN NOT again;
        END;
    END LOOP;
    PERFORM "@extschema@".bg_exec_wait('SELECT ''ignored'' FROM "@extschema@".cleanup_analyzed_pg_class()');
END
$$;


-- -- CREATE OR REPLACE FUNCTION sync_trigger() RETURNS trigger LANGUAGE plpgsql AS
-- -- $$BEGIN
-- --     PERFORM @extschema@.launch_sync();
-- --     RETURN NULL;
-- -- END$$;
-- -- CREATE OR REPLACE TRIGGER sync_trigger AFTER INSERT ON config_change FOR EACH ROW EXECUTE FUNCTION sync_trigger();
-- -- ALTER TABLE config_change ENABLE REPLICA TRIGGER sync_trigger;
