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
    -- Use wait_v2 for commands where we only care about completion, not returned rows.
    SELECT
        *
    INTO
        _handle
    FROM
        "@extschema:pg_background@".pg_background_launch_v2(script);

    PERFORM "@extschema:pg_background@".pg_background_wait_v2((_handle).pid, (_handle).cookie);
    PERFORM "@extschema@".bg_detach(_handle);
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
        "@extschema:pg_background@".pg_background_launch_v2('select async, transactional, description, commands from "@extschema@".sync');

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

CREATE OR REPLACE FUNCTION launch_sync() RETURNS void LANGUAGE sql AS
$$
SELECT "@extschema@".bg_submit_detached('CAll "@extschema@".sync_replica_worker();')
$$;

CREATE OR REPLACE PROCEDURE sync_daemon(seconds real, _application_name text DEFAULT 'pgwrh_sync_daemon') LANGUAGE plpgsql AS
$$
DECLARE
    err text;
BEGIN
    IF pg_try_advisory_lock(517384732) THEN
        PERFORM set_config('application_name', _application_name, FALSE);
        LOOP
            BEGIN
                CAll "@extschema@".sync_replica_worker();
            EXCEPTION
                WHEN OTHERS THEN
                    GET STACKED DIAGNOSTICS err = MESSAGE_TEXT;
                    raise WARNING '%', err;
            END;
            COMMIT;
            PERFORM pg_sleep(seconds);
            EXIT WHEN NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pgwrh');
        END LOOP;
    END IF;
END
$$;

CREATE OR REPLACE FUNCTION start_sync_daemon(seconds real, application_name text DEFAULT 'pgwrh_sync_daemon') RETURNS void LANGUAGE sql AS
$$
SELECT "@extschema@".bg_submit_detached(format('
        CALL "@extschema@".sync_daemon(%s, %L);
    ', seconds, application_name))
$$;

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
                    PERFORM "@extschema@".exec_script(array_to_string(r.commands, ';'));
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
                        PERFORM "@extschema@".exec_script(cmd);
                    END LOOP;
                END IF;
            END IF;
        END LOOP;
        RETURN FOUND;
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
    WHILE "@extschema@".bg_query_bool('SELECT "@extschema@".sync_step()') LOOP
    END LOOP;
    PERFORM "@extschema@".bg_exec_wait('SELECT "@extschema@".report_state()');
    PERFORM "@extschema@".bg_exec_wait('SELECT "@extschema@".cleanup_analyzed_pg_class()');
END
$$;


-- -- CREATE OR REPLACE FUNCTION sync_trigger() RETURNS trigger LANGUAGE plpgsql AS
-- -- $$BEGIN
-- --     PERFORM @extschema@.launch_sync();
-- --     RETURN NULL;
-- -- END$$;
-- -- CREATE OR REPLACE TRIGGER sync_trigger AFTER INSERT ON config_change FOR EACH ROW EXECUTE FUNCTION sync_trigger();
-- -- ALTER TABLE config_change ENABLE REPLICA TRIGGER sync_trigger;
