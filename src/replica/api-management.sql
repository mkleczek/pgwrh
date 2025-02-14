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

CREATE OR REPLACE FUNCTION configure_controller(host text, port text, username text, password text, start_daemon boolean DEFAULT true, refresh_seconds real DEFAULT 30)
    RETURNS void
    SET SEARCH_PATH FROM CURRENT
    LANGUAGE plpgsql AS
$$
DECLARE
    r record;
BEGIN
    FOR r IN SELECT * FROM "@extschema@".update_server_options('replica_controller', host, port) AS u(cmd) LOOP
            EXECUTE r.cmd;
        END LOOP;
    FOR r IN SELECT * FROM "@extschema@".update_user_mapping('replica_controller', username, password) AS u(cmd) LOOP
            EXECUTE r.cmd;
        END LOOP;
    IF start_daemon THEN
        PERFORM "@extschema@".start_sync_daemon(refresh_seconds);
    END IF;
END
$$;
