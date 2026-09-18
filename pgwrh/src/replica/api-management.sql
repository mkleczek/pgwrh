-- name: replica-api-management
-- requires: replica-daemon
-- requires: replica-helpers

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

CREATE OR REPLACE FUNCTION configure_controller(host text, port text, username text, password text, start_daemon boolean DEFAULT true, refresh_seconds real DEFAULT 20, dbname text DEFAULT current_database())
    RETURNS void
    SET SEARCH_PATH FROM CURRENT
    SECURITY DEFINER
    LANGUAGE plpgsql AS
$$
DECLARE
    r record;
    controller_conninfo text;
BEGIN
    IF dbname IS NULL OR dbname = '' THEN
        RAISE EXCEPTION 'Controller database name must not be empty';
    END IF;
    FOR r IN SELECT * FROM "@extschema@".update_server_options('replica_controller', host, port, dbname) AS u(cmd) LOOP
            EXECUTE r.cmd;
    END LOOP;
    FOR r IN SELECT * FROM "@extschema@".update_user_mapping('replica_controller', username, password) AS u(cmd) LOOP
            EXECUTE r.cmd;
    END LOOP;
    -- libpq values need backslash escaping, independently of SQL literal quoting.
    SELECT string_agg(key || '=' || chr(39) ||
        replace(replace(value, chr(92), chr(92) || chr(92)), chr(39), chr(92) || chr(39)) || chr(39), ' ')
    INTO controller_conninfo
    FROM unnest(ARRAY['host', 'port', 'user', 'password', 'dbname'],
                ARRAY[host, port, username, password, dbname]) AS o(key, value);
    PERFORM exec_dynamic(format('CREATE TRIGGER make_sure_daemon_started_on_ping AFTER INSERT ON ping
        FOR ROW EXECUTE FUNCTION make_sure_daemon_started_on_ping_trigger(%s)', refresh_seconds));
    ALTER TABLE ping ENABLE REPLICA TRIGGER make_sure_daemon_started_on_ping;
    INSERT INTO shard_subscription (subname) VALUES ('pgwrh_replica_subscription');
    PERFORM "@extschema@".bg_exec_wait(
        format('CREATE SUBSCRIPTION pgwrh_replica_subscription CONNECTION %L PUBLICATION %I WITH (copy_data = false, %s)',
               controller_conninfo || ' target_session_attrs=primary', 'pgwrh_controller_ping',
               (
                   SELECT string_agg(format('%s = %L', key, val), ', ') FROM (
                         SELECT
                             'slot_name' AS key,
                             username || '_' || (random() * 10000000)::bigint::text AS val-- random slot_name
                         UNION ALL
                         -- add failover = 'true' option for PostgreSQL >= 17
                         SELECT
                             'failover' AS key,
                             'true' AS val
                         WHERE
                             substring(current_setting('server_version') FROM '\d{2}')::int >= 17
                  ) opts
               )
        )
    );
    IF start_daemon THEN
        PERFORM "@extschema@".start_sync_daemon(refresh_seconds);
    END IF;
END
$$;
