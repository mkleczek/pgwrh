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

CREATE OR REPLACE FUNCTION launch_in_background(commands text) RETURNS void LANGUAGE plpgsql AS
$$
DECLARE
    pid int;
BEGIN
    pid := (select "@extschema:pg_background@".pg_background_launch(commands));
    PERFORM pg_sleep(0.1);
    PERFORM "@extschema:pg_background@".pg_background_detach(pid);
END
$$;

CREATE OR REPLACE FUNCTION launch_sync() RETURNS void LANGUAGE sql AS
$$
SELECT "@extschema@".launch_in_background('CAll "@extschema@".sync_replica_worker();')
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
SELECT "@extschema@".launch_in_background(format('
        CALL "@extschema@".sync_daemon(%s, %L);
    ', seconds, application_name))
$$;

CREATE OR REPLACE FUNCTION exec_script(script text) RETURNS boolean LANGUAGE plpgsql AS
$$
DECLARE
    err text;
BEGIN
    PERFORM * FROM "@extschema:pg_background@".pg_background_result("@extschema:pg_background@".pg_background_launch(script)) AS discarded(result text);
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
        PERFORM * FROM "@extschema:pg_background@".pg_background_result("@extschema:pg_background@".pg_background_launch(cmd)) AS discarded(result text);
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
        -- Select commands to execute in a separate transaction so that we don't keep any locks here
        FOR r IN SELECT * FROM "@extschema:pg_background@".pg_background_result("@extschema:pg_background@".pg_background_launch('select async, transactional, description, commands from "@extschema@".sync')) AS (async boolean, transactional boolean, description text, commands text[]) LOOP
            RAISE NOTICE '%', r.description;
            IF r.transactional THEN
                IF r.async THEN
                    PERFORM "@extschema@".launch_in_background(array_to_string(r.commands, ';'));
                ELSE
                    PERFORM "@extschema@".exec_script(array_to_string(r.commands || 'SELECT '''''::text, ';'));
                END IF;
            ELSE
                IF r.async THEN
                    IF array_length(r.commands, 1) > 1 THEN
                        PERFORM "@extschema@".launch_in_background(format('SELECT "@extschema@".exec_non_tx_scripts(ARRAY[%s])', (SELECT string_agg(format('%L', c), ',') FROM unnest(r.commands) AS c)));
                    ELSE
                        PERFORM "@extschema@".launch_in_background(r.commands[1]);
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
    WHILE r FROM "@extschema:pg_background@".pg_background_result("@extschema:pg_background@".pg_background_launch('SELECT "@extschema@".sync_step()')) AS r(r boolean) LOOP
    END LOOP;
    PERFORM * FROM "@extschema:pg_background@".pg_background_result("@extschema:pg_background@".pg_background_launch('SELECT ''ignored'' FROM "@extschema@".report_state()')) AS r(ignored text);
END
$$;


-- -- CREATE OR REPLACE FUNCTION sync_trigger() RETURNS trigger LANGUAGE plpgsql AS
-- -- $$BEGIN
-- --     PERFORM @extschema@.launch_sync();
-- --     RETURN NULL;
-- -- END$$;
-- -- CREATE OR REPLACE TRIGGER sync_trigger AFTER INSERT ON config_change FOR EACH ROW EXECUTE FUNCTION sync_trigger();
-- -- ALTER TABLE config_change ENABLE REPLICA TRIGGER sync_trigger;
