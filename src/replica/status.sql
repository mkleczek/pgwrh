-- name: replica-status
-- requires: replica-fdw
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

CREATE VIEW connected_local_shard AS
    SELECT
        ls.rel_id
    FROM
        subscribed_local_shard ls
            JOIN rel slot ON ls.slot_rel_id = slot.rel_id AND (ls).parent.reg_class = slot.reg_class
;
COMMENT ON VIEW connected_local_shard IS
$$
Local shards ready and connected to slots.

Local shard is considered ready if
* it is subscribed and its subscription state is 'r'
* all non-optional indexes are created
$$;

-- TODO maybe better would be to use pg_depend to link local and foreign tables for the same shard
CREATE VIEW connected_remote_shard AS
    SELECT
        ls.rel_id
    FROM
        remote_shard rs
            JOIN rel slot ON slot.reg_class = (rs).parent.reg_class
            JOIN local_rel ls ON ls.slot_rel_id = slot.rel_id
;
COMMENT ON VIEW connected_remote_shard IS
$$
Remote shards ready to use and connected to slots.

Remote shard is considered ready if ANALYZE was performed on corresponding foreign table.
$$;

CREATE VIEW local_shard_index AS
    SELECT
        (ic).schema_name,
        (ic).table_name AS index_name
    FROM
        subscribed_local_shard ls
            JOIN pg_index i ON i.indrelid = ls.reg_class
            JOIN rel ic ON ic.reg_class = i.indexrelid
    WHERE
        NOT EXISTS (SELECT 1 FROM
            pg_constraint
            WHERE conindid = i.indexrelid
        )
;
COMMENT ON VIEW local_shard_index IS
$$
Indexes on local shards except constraint indexes.
$$;

CREATE FUNCTION report_state() RETURNS void LANGUAGE sql AS
$$
    UPDATE "@extschema@".fdw_replica_state
        SET
            subscribed_local_shards = (SELECT coalesce((SELECT json_agg(rel_id) FROM "@extschema@".subscribed_local_shard), '[]')),
            connected_local_shards = (SELECT coalesce((SELECT json_agg(rel_id) FROM "@extschema@".connected_local_shard), '[]')),
            connected_remote_shards = (SELECT coalesce((SELECT json_agg(rel_id) FROM "@extschema@".connected_remote_shard), '[]')),
            indexes = (SELECT coalesce((SELECT json_agg(i) FROM "@extschema@".local_shard_index i), '[]'));
$$;
COMMENT ON FUNCTION report_state() IS
$$
Updates controller with information about current state of a replica.
# Details
Function performs UPDATE on controller replica_state view setting
subscribed_local_shards, connected_local_shards, connected_remote_shards, indexes
columns to JSON arrays containing lists of tables and indexes having
corresponding state.
$$;