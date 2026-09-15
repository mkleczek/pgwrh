-- name: replica-aggregation
-- requires: replica-helpers
-- requires: replica-fdw

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

-- Use the controller's logical tree: physical ancestors change when we aggregate.
CREATE VIEW shard_descendant AS
WITH RECURSIVE structure AS MATERIALIZED (
    SELECT DISTINCT * FROM shard_structure_r
), descendants AS (
    SELECT rel_id AS ancestor_rel_id, rel_id FROM structure
    UNION ALL
    SELECT d.ancestor_rel_id, s.rel_id
    FROM descendants d JOIN structure s ON s.parent_rel_id = d.rel_id
)
SELECT * FROM descendants;

-- Identify foreign tables by their remote shield, even after the desired route
-- changes. An old attached aggregate must survive until its replacement is ready.
CREATE VIEW remote_node AS
SELECT s.rel_id AS node_rel_id, s.root_rel_id, rs.*
FROM remote_shard rs
    JOIN pg_foreign_table ft ON ft.ftrelid = rs.reg_class
    JOIN shard_structure_r s ON s.table_name = rs.table_name
WHERE EXISTS (
    SELECT 1 FROM opts(ft.ftoptions)
    WHERE key = 'schema_name' AND value = s.view_schema_name
);

CREATE VIEW current_shard_attachment AS
WITH structure AS MATERIALIZED (
    SELECT DISTINCT * FROM shard_structure_r
), managed AS (
    SELECT root_rel_id, rel_id FROM structure
    UNION
    SELECT root_rel_id, slot_rel_id FROM structure WHERE level > 0
    UNION
    SELECT root_rel_id, rel_id FROM remote_node
)
SELECT m.root_rel_id, p.rel_id AS parent_rel_id, c.rel_id, c.reg_class,
       p.reg_class AS parent_reg_class
FROM managed m JOIN local_rel c USING (rel_id)
    JOIN rel p ON p.reg_class = (c.parent).reg_class;

CREATE VIEW reachable_shard AS
WITH RECURSIVE roots AS (
    SELECT DISTINCT root_rel_id FROM shard_structure_r
), reachable AS (
    SELECT r.rel_id, r.reg_class
    FROM roots JOIN rel r ON r.rel_id = roots.root_rel_id
    UNION ALL
    SELECT r.rel_id, r.reg_class
    FROM reachable p JOIN pg_inherits i ON i.inhparent = p.reg_class
        JOIN rel r ON r.reg_class = i.inhrelid
)
SELECT * FROM reachable;

CREATE VIEW remote_node_assignment AS
WITH structure AS MATERIALIZED (
    SELECT DISTINCT * FROM shard_structure_r
), assignment AS MATERIALIZED (
    SELECT a.*, pgwrh_target_servers(shard_server_members, host, port, dbname, shard_server_user) AS target_servers
    FROM fdw_shard_assignment a
), descendants AS MATERIALIZED (
    SELECT * FROM shard_descendant
), serving AS MATERIALIZED (
    SELECT * FROM fdw_serving_subtree
), eligible AS (
    SELECT
        d.ancestor_rel_id AS rel_id,
        min(a.shard_server_name) AS shard_server_name,
        min(a.host) AS host,
        min(a.port) AS port,
        min(a.dbname) AS dbname,
        min(a.shard_server_user) AS shard_server_user,
        count(*) AS leaf_count,
        min(a.target_servers::text)::text[] AS target_servers
    FROM descendants d
        JOIN structure s ON s.rel_id = d.rel_id
        LEFT JOIN assignment a USING (schema_name, table_name)
    -- Empty partitioned nodes and missing leaf assignments block aggregation.
    WHERE s.is_leaf OR NOT EXISTS (
        SELECT 1 FROM structure child WHERE child.parent_rel_id = s.rel_id
    )
    GROUP BY d.ancestor_rel_id
    HAVING bool_and(coalesce((d.ancestor_rel_id = s.rel_id OR (
        NOT a.local
        -- A departing local leaf is replaced by its prepared foreign leaf first.
        -- Only a later pass may collapse that remote-only subtree.
        AND NOT EXISTS (SELECT 1 FROM current_shard_attachment c WHERE c.rel_id = s.rel_id)
        AND cardinality(a.shard_server_members) > 0
        AND NOT EXISTS (
            SELECT 1 FROM unnest(a.shard_server_members) AS members(member_role)
            WHERE NOT EXISTS (
                SELECT 1 FROM serving ready
                WHERE ready.member_role = members.member_role
                    AND (ready.schema_name, ready.table_name)::rel_id = d.ancestor_rel_id
            )
        )
    )), FALSE))
       AND bool_and(coalesce(s.is_leaf AND (NOT a.local OR a.connect_remote)
                             AND a.shard_server_name IS NOT NULL
                             AND a.host <> '' AND a.port <> '' AND a.target_servers IS NOT NULL, FALSE))
       AND count(DISTINCT (a.shard_server_name, a.host, a.port,
                           a.dbname, a.shard_server_user)) = 1
)
SELECT
    s.*,
    pgwrh_shard_server(s.schema_name, s.table_name) AS shard_server_name,
    e.host, e.port, e.dbname, e.shard_server_user,
    e.leaf_count,
    format('%s_remote', s.schema_name) AS shard_server_schema_name,
    (format('%s_remote', s.schema_name), s.table_name)::rel_id AS remote_rel_id,
    CASE WHEN s.level > 0 THEN s.bound
         WHEN s.node_partkeydef LIKE 'HASH %' THEN 'FOR VALUES WITH (MODULUS 1, REMAINDER 0)'
         ELSE 'DEFAULT'
    END AS remote_bound,
    e.target_servers
FROM eligible e JOIN structure s USING (rel_id)
WHERE NOT EXISTS (
    SELECT 1 FROM descendants d JOIN eligible parent ON parent.rel_id = d.ancestor_rel_id
    WHERE d.rel_id = e.rel_id AND d.ancestor_rel_id <> e.rel_id
);
COMMENT ON VIEW remote_node_assignment IS
'Highest complete remote subtrees with identical effective destinations. Placement,
subscriptions and readiness remain leaf-based; selection runs on each replica.';

-- Physical routes are derived from the logical tree, including detached subtrees.
CREATE VIEW desired_shard_attachment AS
WITH structure AS MATERIALIZED (
    SELECT DISTINCT * FROM shard_structure_r
), remote AS MATERIALIZED (
    SELECT * FROM remote_node_assignment
)
SELECT s.root_rel_id, s.parent_rel_id, s.slot_rel_id AS rel_id, s.bound, s.level * 2 - 1 AS depth
FROM structure s
WHERE s.level > 0 AND NOT EXISTS (
    SELECT 1 FROM remote r WHERE r.rel_id = s.root_rel_id
)
UNION ALL
SELECT s.root_rel_id, s.slot_rel_id,
       CASE WHEN a.local THEN s.rel_id ELSE coalesce(r.remote_rel_id, s.rel_id) END,
       s.bound, s.level * 2
FROM structure s LEFT JOIN remote r USING (rel_id)
    LEFT JOIN fdw_shard_assignment a ON (a.schema_name, a.table_name) = (s.schema_name, s.table_name)
WHERE s.level > 0 AND NOT EXISTS (
    SELECT 1 FROM shard_descendant d JOIN remote r ON r.rel_id = d.ancestor_rel_id
    WHERE d.rel_id = s.rel_id AND d.ancestor_rel_id <> s.rel_id
)
UNION ALL
SELECT root_rel_id, rel_id, remote_rel_id, remote_bound, 1
FROM remote WHERE level = 0;
