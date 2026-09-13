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
            JOIN reachable_shard reachable ON reachable.reg_class = ls.reg_class
            JOIN rel slot ON ls.slot_rel_id = slot.rel_id AND (ls).parent.reg_class = slot.reg_class
;
COMMENT ON VIEW connected_local_shard IS
$$
Local shards ready and connected to slots.

Local shard is considered ready if
* it is subscribed and its subscription state is 'r'
* all non-optional indexes are created
$$;

-- Report leaf coverage of the actual reachable foreign tables, not the desired
-- tree or attachments hidden inside a detached subtree.
CREATE VIEW connected_remote_shard AS
    SELECT DISTINCT d.rel_id, n.srvname AS shard_server_name, u.value AS shard_server_user
    FROM remote_node n
        JOIN reachable_shard reachable ON reachable.reg_class = n.reg_class
        JOIN shard_descendant d ON d.ancestor_rel_id = n.node_rel_id
        JOIN shard_structure_r leaf ON leaf.rel_id = d.rel_id AND leaf.is_leaf
        JOIN pg_user_mappings um ON um.srvname = n.srvname AND um.umuser = 0
        CROSS JOIN LATERAL opts(um.umoptions) u
    WHERE u.key = 'user';
COMMENT ON VIEW connected_remote_shard IS
'Leaf coverage and destinations of remote routes reachable from a managed root.';

CREATE VIEW prepared_remote_shard AS
SELECT sa.rel_id, rs.srvname AS shard_server_name, u.value AS shard_server_user
FROM shard_assignment_r sa JOIN remote_shard rs ON rs.rel_id = sa.remote_rel_id
    JOIN pg_user_mappings um ON um.srvname = rs.srvname AND um.umuser = 0
    CROSS JOIN LATERAL opts(um.umoptions) u
WHERE u.key = 'user' AND (
    EXISTS (SELECT 1 FROM pg_statistic WHERE starelid = rs.reg_class)
    OR EXISTS (SELECT 1 FROM analyzed_remote_pg_class WHERE oid = rs.reg_class)
);
COMMENT ON VIEW prepared_remote_shard IS
'Analyzed foreign leaf replacements, including those staged behind retained local copies.';

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

-- Advertise only complete, physically local partition trees. Subscription
-- readiness alone is insufficient: a copied leaf can still be detached.
CREATE VIEW ready_serving_subtree AS
WITH structure AS MATERIALIZED (
    SELECT DISTINCT * FROM shard_structure_r
), required_index AS MATERIALIZED (
    -- "optional" permits keeping an existing local route during index creation;
    -- a newly advertised serving subtree must have every configured index.
    SELECT * FROM fdw_shard_index
)
SELECT node.rel_id
FROM structure node JOIN reachable_shard root USING (rel_id)
WHERE NOT node.is_leaf
    AND EXISTS (SELECT 1 FROM shard_descendant d JOIN structure leaf USING (rel_id)
                WHERE d.ancestor_rel_id = node.rel_id AND leaf.is_leaf)
    AND NOT EXISTS (
        SELECT 1 FROM pg_partition_tree(root.reg_class) actual
        WHERE actual.isleaf AND NOT EXISTS (
            SELECT 1 FROM shard_descendant d JOIN structure leaf USING (rel_id)
                JOIN rel physical USING (rel_id)
            WHERE d.ancestor_rel_id = node.rel_id AND leaf.is_leaf
                AND physical.reg_class = actual.relid
        )
    )
    AND NOT EXISTS (
        SELECT 1 FROM shard_descendant d JOIN structure s USING (rel_id)
            LEFT JOIN local_rel original USING (rel_id)
            LEFT JOIN local_rel slot ON slot.rel_id = s.slot_rel_id
        WHERE d.ancestor_rel_id = node.rel_id AND (
            original.reg_class IS NULL
            OR (s.rel_id <> node.rel_id AND (
                (original.parent).rel_id IS DISTINCT FROM s.slot_rel_id
                OR (slot.parent).rel_id IS DISTINCT FROM s.parent_rel_id
                OR original.bound IS DISTINCT FROM s.bound
                OR slot.bound IS DISTINCT FROM s.bound
            ))
            OR s.is_leaf AND (
                NOT EXISTS (SELECT 1 FROM connected_local_shard ready WHERE ready.rel_id = s.rel_id)
                OR EXISTS (SELECT 1 FROM required_index i
                    WHERE (i.schema_name, i.table_name)::rel_id = s.rel_id
                        AND NOT EXISTS (SELECT 1 FROM local_shard_index ready
                            WHERE (ready.schema_name, ready.index_name) = (i.schema_name, i.index_name)))
            )
        )
    );

CREATE FUNCTION report_state() RETURNS void LANGUAGE plpgsql AS
$$
BEGIN
    -- A report must describe a completed sync pass, never an in-flight handoff.
    IF NOT pg_try_advisory_xact_lock(2895359559) THEN RETURN; END IF;
    UPDATE "@extschema@".fdw_replica_state
        SET
            serving_subtrees = (SELECT coalesce(json_agg(rel_id), '[]') FROM "@extschema@".ready_serving_subtree),
            subscribed_local_shards = (SELECT coalesce((SELECT json_agg(rel_id) FROM "@extschema@".subscribed_local_shard), '[]')),
            connected_local_shards = (SELECT coalesce((SELECT json_agg(rel_id) FROM "@extschema@".connected_local_shard), '[]')),
            connected_remote_shards = (SELECT coalesce(json_agg(json_build_object(
                'schema_name', (rel_id).schema_name, 'table_name', (rel_id).table_name,
                'shard_server_name', shard_server_name, 'shard_server_user', shard_server_user)), '[]')
                FROM "@extschema@".connected_remote_shard),
            prepared_remote_shards = (SELECT coalesce(json_agg(json_build_object(
                'schema_name', (rel_id).schema_name, 'table_name', (rel_id).table_name,
                'shard_server_name', shard_server_name, 'shard_server_user', shard_server_user)), '[]')
                FROM "@extschema@".prepared_remote_shard),
            indexes = (SELECT coalesce((SELECT json_agg(i) FROM "@extschema@".local_shard_index i), '[]')),
            users = (SELECT coalesce((SELECT json_agg(u.rolname)
                                      FROM pg_roles u
                                               JOIN pg_auth_members ON member = u.oid
                                               JOIN pg_roles gr ON
                                                    gr.oid = roleid
                                                AND gr.rolname = format('pgwrh_replica_%s', current_database())),
                                     '[]'));
END
$$;
COMMENT ON FUNCTION report_state() IS
'Reports subscriptions, local and remote attachments, prepared remote replacements,
indexes, users, and complete local serving subtrees to the controller.';
