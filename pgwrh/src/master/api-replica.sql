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
WITH nodes AS (
    SELECT DISTINCT ON (s.schema_name, s.table_name)
        CASE WHEN l.schema_name IS NULL THEN s
             ELSE ROW(l.*)::shard_structure_snapshot END AS node
    FROM shard_structure_snapshot s
        JOIN replication_group g USING (replication_group_id)
        JOIN replication_group_config_lock k USING (replication_group_id, version)
        JOIN replication_group_member m USING (replication_group_id)
        LEFT JOIN live_shard_structure l
            ON (l.replication_group_id, l.version, l.schema_name, l.table_name,
                l.root_schema_name, l.root_table_name) =
               (s.replication_group_id, s.version, s.schema_name, s.table_name,
                s.root_schema_name, s.root_table_name)
    WHERE m.member_role = CURRENT_ROLE
        AND (s.version IN (g.current_version, g.target_version) OR k.rollback_unlock IS NOT NULL)
    -- Prefer the target snapshot for a node shared by both configurations.
    -- Existing nodes still follow live bound/reparenting changes; missing nodes
    -- retain their last snapshotted topology until their version is retired.
    ORDER BY s.schema_name, s.table_name, (s.version = g.target_version) DESC,
             (s.version = g.current_version) DESC
)

SELECT
    (node).schema_name,
    (node).table_name,
    (node).level,
    (node).parent_schema_name,
    (node).parent_table_name,
    (node).bound,
    (node).parent_partkeydef,
    (node).node_partkeydef,
    (node).is_leaf,
    (node).root_column_clause,
    (node).local_constraint_clause,
    (node).root_schema_name,
    (node).root_table_name
FROM nodes;

GRANT SELECT ON shard_structure TO PUBLIC;


CREATE OR REPLACE VIEW shard_assignment AS
SELECT
    schema_name,
    table_name,
    local,
    shard_server_name,
    host,
    port,
    dbnames,
    shard_server_users,
    pubname,
    connect_remote,
    retained_shard_server_name,
    shard_server_members
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
* positionally aligned shard_server_members, host, port and dbnames for remote replicas
* shard_server_users identifying the credentials for those destinations';

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
        users,
        prepared_remote_shards,
        serving_subtrees,
        credential_generation,
        subscribed_publications
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

-- Local-first retention keeps advertised trees intact until readers acknowledge
-- target routes at commit (or restored routes at rollback completion).
CREATE VIEW serving_subtree AS
SELECT m.member_role, s.schema_name, s.table_name
FROM replication_group_member reader
    JOIN replication_group_member m USING (replication_group_id)
    CROSS JOIN LATERAL json_to_recordset(m.serving_subtrees) s(schema_name text, table_name text)
WHERE reader.member_role = CURRENT_ROLE;
GRANT SELECT ON serving_subtree TO PUBLIC;

-- Only verifiers belonging to this destination leave the controller here.
CREATE VIEW local_credentials WITH (security_barrier = true) AS
SELECT creds.username, creds.verifier AS password
FROM replica_credentials creds
WHERE creds.member_role = CURRENT_ROLE;
GRANT SELECT ON local_credentials TO PUBLIC;
COMMENT ON VIEW local_credentials IS
'Incoming logins for CURRENT_ROLE. The password column contains a target-specific SCRAM verifier, never a source password.';

-- Only this source's reusable secret leaves the controller here.
CREATE VIEW remote_credentials WITH (security_barrier = true) AS
SELECT creds.member_role, creds.username, creds.password
FROM replica_credentials creds
WHERE creds.source_role = CURRENT_ROLE;
GRANT SELECT ON remote_credentials TO PUBLIC;
COMMENT ON VIEW remote_credentials IS
'Only CURRENT_ROLE source passwords, paired with their destination member for outbound mappings.';

CREATE VIEW credential_state WITH (security_barrier = true) AS
SELECT c.generation, c.username
FROM source_credential c JOIN credential_generation g USING (replication_group_id, generation)
WHERE c.source_role = CURRENT_ROLE AND g.state = 'active';
GRANT SELECT ON credential_state TO PUBLIC;
