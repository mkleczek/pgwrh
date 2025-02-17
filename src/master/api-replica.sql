-- name: api-replica
-- requires: master-implementation-views

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

CREATE OR REPLACE VIEW shard_structure AS
WITH stc AS (
    SELECT
        st.replication_group_id,
        c.oid::regclass 
    FROM
        pg_class c
            JOIN pg_namespace n ON relnamespace = n.oid
            JOIN sharded_table st ON (nspname, relname) = (sharded_table_schema, sharded_table_name)
),
roots AS (
    SELECT *
    FROM stc r
    WHERE NOT EXISTS (SELECT 1 FROM stc WHERE replication_group_id = r.replication_group_id AND oid <> r.oid AND oid = ANY (SELECT * FROM pg_partition_ancestors(r.oid)))
)
SELECT
    n.nspname AS schema_name,
    c.relname AS table_name,
    level,
    format('CREATE TABLE IF NOT EXISTS %I.%I %s%s',
        n.nspname, c.relname,
        CASE WHEN level = 0
            -- root of the partition tree - need to define attributes
            THEN
                '(' ||
                    (
                        SELECT string_agg(format('%I %s', attname, atttypid::regtype), ',')
                        FROM pg_attribute WHERE attrelid = t.relid AND attnum >= 1
                    ) ||
                    coalesce(
                        ', ' || (SELECT string_agg(pg_get_constraintdef(c.oid), ', ') FROM pg_constraint c WHERE conrelid = t.relid AND conislocal),
                        ''
                    ) ||
                ')'
            -- partition - no attributes necessary
            ELSE
                format('PARTITION OF %I.%I%s %s',
                    pn.nspname, p.relname,
                    coalesce(
                        ' (' || (SELECT string_agg(pg_get_constraintdef(c.oid), ', ') FROM pg_constraint c WHERE conrelid = t.relid AND conislocal) || ')',
                        ''
                    ),
                    pg_get_expr(c.relpartbound, c.oid))
        END,
        CASE WHEN t.isleaf
            THEN
                ''
            ELSE
                ' PARTITION BY ' || pg_get_partkeydef(t.relid)
        END
    ) AS create_table
FROM
    roots r
        JOIN replication_group_member m USING (replication_group_id)
        JOIN replication_group USING (replication_group_id),
        pg_partition_tree(oid) t
        JOIN pg_class c ON t.relid = c.oid JOIN pg_namespace n ON c.relnamespace = n.oid
        LEFT JOIN pg_class p ON t.parentrelid = p.oid LEFT JOIN pg_namespace pn ON p.relnamespace = pn.oid
WHERE
    (
            c.relkind = 'p'
        OR
            c.relkind = 'r'
        AND
            EXISTS (SELECT 1 FROM
                shard_assigned_host
                WHERE
                        replication_group_id = m.replication_group_id
                    AND
                        schema_name = n.nspname AND table_name = c.relname
                    AND
                        version IN (current_version, target_version)
            )
    )
    AND
        member_role = CURRENT_ROLE;

GRANT SELECT ON shard_structure TO PUBLIC;


CREATE OR REPLACE VIEW shard_assignment AS
SELECT
    schema_name,
    table_name,
    local,
    shard_server_name,
    host,
    port,
    dbname,
    shard_server_user,
    pubname,
    ready,
    retained_shard_server_name
FROM
    shard_assignment_per_member
WHERE
    member_role = CURRENT_ROLE
;
GRANT SELECT ON shard_assignment TO PUBLIC;

COMMENT ON VIEW shard_assignment IS
'Main view implementing shard assignment logic.

Presents a particular replication_group_member (as identified by member_role) view of the cluster (replicaton_group).
Each member sees all shards with the following information for each shard:
* "local" flag saying if this shard should be replicated to this member
* information on how to connect to remote replicas for this shard: host, port, dbname, user, password';

CREATE OR REPLACE VIEW shard_index AS
SELECT
    schema_name,
    table_name,
    index_name,
    index_template,
    optional
FROM
    shard_index_per_member
WHERE
    member_role = CURRENT_ROLE
;
GRANT SELECT ON shard_index TO PUBLIC;

CREATE VIEW replica_state AS
    SELECT
        subscribed_local_shards,
        indexes,
        connected_local_shards,
        connected_remote_shards,
        users
    FROM replication_group_member
    WHERE
        member_role = CURRENT_ROLE
;

-- CREATE FUNCTION update_replica_state() RETURNS trigger LANGUAGE plpgsql AS
-- $$
-- BEGIN
--     INSERT INTO replica_state_per_member (member_role, subscribed_local_shards, indexes, connected_local_shards, connected_remote_shards)
--     VALUES (CURRENT_ROLE, NEW.subscribed_local_shards, NEW.indexes, NEW.connected_local_shards, NEW.connected_remote_shards)
--     ON CONFLICT (member_role) DO UPDATE SET
--         subscribed_local_shards = REJECTED.subscribed_local_shards,
--         indexes = REJECTED.indexes,
--         connected_local_shards = REJECTED.connected_local_shards,
--         connected_remote_shards = REJECTED.connected_remote_shards;
--     RETURN NEW;
-- END
-- $$;
-- CREATE TRIGGER update_replica_state_trigger INSTEAD OF INSERT OR UPDATE ON replica_state FOR EACH ROW EXECUTE FUNCTION update_replica_state();
GRANT SELECT, INSERT, UPDATE ON replica_state TO PUBLIC;

CREATE VIEW credentials AS
SELECT
    creds.username,
    creds.password
FROM
    replication_group_member
        JOIN replication_group_credentials creds USING (replication_group_id)
WHERE
    member_role = CURRENT_ROLE;
GRANT SELECT ON credentials TO PUBLIC;
