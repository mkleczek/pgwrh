-- name: replica-sync
-- requires: replica-tables
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

CREATE OR REPLACE VIEW sync(async, transactional, description, commands) AS
WITH shard_assignment AS MATERIALIZED (
    SELECT * FROM shard_assignment_r
),
remote_assignment AS MATERIALIZED (
    SELECT * FROM remote_node_assignment
),
local_shard AS (
    SELECT * FROM shard_assignment WHERE local
),
shard_structure AS MATERIALIZED (
    SELECT DISTINCT * FROM shard_structure_r
),
root_shard_structure AS (
    SELECT * FROM shard_structure WHERE level = 0
),
nonroot_shard_structure AS (
    SELECT * FROM shard_structure WHERE level > 0
),
missing_root_shard_structure AS (
    SELECT DISTINCT ON (s.rel_id)
        s.*
    FROM
        root_shard_structure s
    WHERE
        NOT EXISTS (SELECT 1 FROM local_rel WHERE rel_id = s.rel_id)
    ORDER BY
        s.rel_id,
        s.level,
        s.schema_name,
        s.table_name
),
missing_nonroot_slot AS (
    SELECT DISTINCT ON (s.slot_rel_id)
        s.level,
        s.schema_name,
        s.table_name,
        s.slot_rel_id,
        s.parent_rel_id,
        s.bound,
        s.parent_partkeydef
    FROM
        nonroot_shard_structure s
    WHERE
        NOT EXISTS (SELECT 1 FROM local_rel WHERE rel_id = s.slot_rel_id)
    ORDER BY
        s.slot_rel_id,
        s.level,
        s.schema_name,
        s.table_name
),
missing_nonroot_rel AS (
    SELECT DISTINCT ON (s.rel_id)
        s.level,
        s.schema_name,
        s.table_name,
        s.rel_id,
        s.slot_rel_id,
        s.bound,
        s.node_partkeydef,
        s.is_leaf
    FROM
        nonroot_shard_structure s
    WHERE
        NOT EXISTS (SELECT 1 FROM local_rel WHERE rel_id = s.rel_id)
    ORDER BY
        s.rel_id,
        s.level,
        s.schema_name,
        s.table_name
),
missing_template_shard_structure AS (
    SELECT DISTINCT ON (s.template_rel_id)
        s.level,
        s.schema_name,
        s.table_name,
        s.template_rel_id,
        s.template_schema_name,
        coalesce(s.parent_partkeydef, s.node_partkeydef) AS parent_partkeydef,
        original_rel.reg_class
    FROM
        shard_structure s
            JOIN local_rel original_rel ON original_rel.rel_id = s.rel_id
    WHERE
        NOT EXISTS (SELECT 1 FROM local_rel WHERE rel_id = s.template_rel_id)
    ORDER BY
        s.template_rel_id,
        s.level,
        s.schema_name,
        s.table_name
),
missing_view_shard_structure AS (
    SELECT DISTINCT ON (s.view_rel_id)
        s.level,
        s.schema_name,
        s.table_name,
        s.rel_id,
        s.view_rel_id,
        s.view_schema_name
    FROM
        shard_structure s
            JOIN local_rel original_rel ON original_rel.rel_id = s.rel_id
    WHERE
        NOT EXISTS (SELECT 1 FROM rel WHERE rel_id = s.view_rel_id)
    ORDER BY
        s.view_rel_id,
        s.level,
        s.schema_name,
        s.table_name
),
missing_nonroot_command AS (
    SELECT
        level,
        1 AS phase,
        schema_name,
        table_name,
        format('CREATE TABLE %s PARTITION OF %s %s PARTITION BY %s',
            fqn(slot_rel_id),
            fqn(parent_rel_id),
            bound,
            parent_partkeydef
        ) AS command
    FROM
        missing_nonroot_slot

    UNION ALL

    SELECT
        level,
        2 AS phase,
        schema_name,
        table_name,
        add_ext_dependency(slot_rel_id) AS command
    FROM
        missing_nonroot_slot

    UNION ALL

    SELECT
        level,
        3 AS phase,
        schema_name,
        table_name,
        format('CREATE TABLE %s PARTITION OF %s %s%s',
            fqn(rel_id),
            fqn(slot_rel_id),
            bound,
            coalesce(' PARTITION BY ' || node_partkeydef, '')
        ) AS command
    FROM
        missing_nonroot_rel

    UNION ALL

    SELECT
        level,
        4 AS phase,
        schema_name,
        table_name,
        add_ext_dependency(rel_id) AS command
    FROM
        missing_nonroot_rel

    UNION ALL

    SELECT
        level,
        5 AS phase,
        schema_name,
        table_name,
        format('ALTER TABLE %s DETACH PARTITION %s',
            fqn(slot_rel_id),
            fqn(rel_id)
        ) AS command
    FROM
        missing_nonroot_rel
    WHERE
        is_leaf
),
slot_schema AS (
    SELECT DISTINCT slot_schema_name
    FROM nonroot_shard_structure
),
template_schema AS (
    SELECT DISTINCT template_schema_name
    FROM shard_structure
),
view_schema AS (
    SELECT DISTINCT view_schema_name
    FROM shard_structure
),
shard_schema AS (
    SELECT DISTINCT schema_name FROM shard_structure
),
shard_server AS (
    SELECT DISTINCT
        shard_server_name,
        shard_server_schema_name,
        host,
        port,
        dbname,
        shard_server_user,
        target_servers
    FROM
        remote_assignment
    WHERE
        shard_server_name IS NOT NULL
),
target_server AS (
    SELECT a.server_name, a.host, a.port, a.dbname, a.shard_server_user, max(a.weight) AS weight
    FROM assignment_target a
    WHERE EXISTS (SELECT 1 FROM shard_server s WHERE a.server_name = ANY(s.target_servers))
    GROUP BY a.server_name, a.host, a.port, a.dbname, a.shard_server_user
),
shard_server_schema AS (
    SELECT DISTINCT shard_server_schema_name
    FROM remote_assignment
    WHERE shard_server_name IS NOT NULL
),
server_host_port AS (
    SELECT
        s.*,
        host,
        port
    FROM
        pg_foreign_server s,
        LATERAL (
            SELECT h.value AS host, p.value AS port
            FROM opts(srvoptions) AS h, opts(srvoptions) AS p
            WHERE h.key = 'host' AND p.key = 'port'
        ) AS opts
),
owned_namespace AS (
    SELECT
        n.*
    FROM
        pg_namespace n JOIN owned_obj ON classid = 'pg_namespace'::regclass AND objid = n.oid
),
owned_subscription AS (
    SELECT * FROM pg_subscription s JOIN shard_subscription USING (subname)
),
shard_index AS (
    SELECT
        reg_class,
        rel_id,
        si.*
    FROM
        fdw_shard_index si
            JOIN local_rel lr ON (si.schema_name, si.table_name) = ((rel_id).schema_name, (rel_id).table_name)
),
missing_index AS (
    SELECT
        *
    FROM
        shard_index si
    WHERE
        NOT EXISTS (
            SELECT 1 FROM pg_index i JOIN pg_class ic ON i.indexrelid = ic.oid
            WHERE
                i.indrelid = si.reg_class AND
                ic.relname = si.index_name
        )
),
missing_required_index AS (
    SELECT
        *
    FROM
        missing_index
    WHERE
        NOT optional
),
-- A retained foreign table can still have a route from an earlier topology.
-- Configure it before analysis or reattachment, even when statistics survive.
configured_remote_shard AS (
    SELECT rs.*
    FROM remote_shard rs
        JOIN remote_assignment a ON a.remote_rel_id = rs.rel_id
        JOIN remote_server_route r ON r.srvname = rs.srvname
    WHERE r.shard_server_targets = a.target_servers
        AND r.shard_server_user = a.shard_server_user
),
ready_remote_shard AS (
    SELECT
        *
    FROM
        configured_remote_shard
    WHERE
        EXISTS (SELECT 1 FROM
            pg_statistic s
            WHERE s.starelid = reg_class
        )
        OR
        EXISTS (SELECT 1 FROM
            analyzed_remote_pg_class
            WHERE oid = reg_class
        )
),
ready_local_shard AS (
    SELECT
        *
    FROM
        subscribed_local_shard s
    WHERE
        NOT EXISTS (
            SELECT 1 FROM missing_required_index
            WHERE
                reg_class = s.reg_class
        )
),
desired_attachment AS MATERIALIZED (
    SELECT * FROM desired_shard_attachment
),
current_attachment AS MATERIALIZED (
    SELECT * FROM current_shard_attachment
),
ready_root AS (
    SELECT DISTINCT s.root_rel_id
    FROM shard_structure s
    WHERE NOT EXISTS (
        SELECT 1 FROM desired_attachment d
            LEFT JOIN local_rel child ON child.rel_id = d.rel_id
            LEFT JOIN local_rel parent ON parent.rel_id = d.parent_rel_id
        WHERE d.root_rel_id = s.root_rel_id AND (
            child.reg_class IS NULL OR parent.reg_class IS NULL
            OR (child.pc).relkind = 'f' AND NOT EXISTS (
                SELECT 1 FROM ready_remote_shard r WHERE r.reg_class = child.reg_class
            )
            OR (child.pc).relkind = 'r' AND NOT EXISTS (
                SELECT 1 FROM ready_local_shard r WHERE r.reg_class = child.reg_class
            )
        )
    )
),
attachment_command AS (
    SELECT c.root_rel_id, 0 AS phase, 0 AS depth, c.rel_id,
           format('ALTER TABLE %s DETACH PARTITION %s', c.parent_reg_class, c.reg_class) AS command
    FROM current_attachment c
    WHERE NOT EXISTS (
        SELECT 1 FROM desired_attachment d
        WHERE (d.parent_rel_id, d.rel_id) = (c.parent_rel_id, c.rel_id)
    )
    UNION ALL
    SELECT d.root_rel_id, 1, d.depth, d.rel_id,
           format('ALTER TABLE %s ATTACH PARTITION %s %s',
                  fqn(d.parent_rel_id), fqn(d.rel_id), d.bound)
    FROM desired_attachment d
    WHERE NOT EXISTS (
        SELECT 1 FROM current_attachment c
        WHERE (d.parent_rel_id, d.rel_id) = (c.parent_rel_id, c.rel_id)
    )
),
roles AS (
    SELECT * FROM fdw_credentials
),
scripts (async, transactional, description, commands) AS (
    SELECT
        FALSE,
        TRUE,
        format('Found schemas [%s] to create.',
               string_agg(format('%I', schema_name), ', ')),
        array_agg(format('CREATE SCHEMA IF NOT EXISTS %I', schema_name))
        ||
        array_agg(select_add_ext_dependency('pg_namespace', format('%L::regnamespace', schema_name)))
    FROM
        (
            SELECT schema_name FROM shard_schema
            UNION ALL
            SELECT slot_schema_name FROM slot_schema
            UNION ALL
            SELECT template_schema_name FROM template_schema
            UNION ALL
            SELECT view_schema_name FROM view_schema
        ) s(schema_name)
    WHERE NOT EXISTS (SELECT 1 FROM
        pg_namespace
        WHERE nspname = schema_name
    )
    GROUP BY 1, 2

    UNION ALL
    SELECT
        FALSE,
        TRUE,
        format('Found root tables [%s] to create.',
            string_agg(format('%I.%I', schema_name, table_name), ', ')),
        array_agg(
            format(
                'CREATE TABLE IF NOT EXISTS %I.%I (%s%s)%s',
                schema_name,
                table_name,
                root_column_clause,
                coalesce(', ' || local_constraint_clause, ''),
                coalesce(' PARTITION BY ' || node_partkeydef, '')
            )
            ORDER BY level, schema_name, table_name
        )
        ||
        array_agg(add_ext_dependency(rel_id) ORDER BY level, schema_name, table_name)
    FROM
        missing_root_shard_structure s
    GROUP BY 1, 2 -- make sure we produce empty set when no results

    UNION ALL
    -- Build the partition tree level by level: create all slots for a level first,
    -- then create all original nodes for that same level.
    (
        SELECT
            FALSE,
            TRUE,
            format('Creating partition structure for level %s', level),
            array_agg(command ORDER BY phase, schema_name, table_name)
        FROM
            missing_nonroot_command
        GROUP BY
            1,
            2,
            level
        ORDER BY
            level
    )

    UNION ALL
    -- Create partition templates for every non-root node. They stay detached and are
    -- only used as DDL parents for remote tables.
    SELECT
        FALSE,
        TRUE,
        format('Creating partition templates [%s]', string_agg(fqn(s.template_rel_id), ', ')),
        coalesce(
            array_agg(
                format('CREATE TABLE %s (LIKE %s) PARTITION BY %s',
                    fqn(s.template_rel_id),
                    s.reg_class,
                    s.parent_partkeydef
                )
                ORDER BY s.level, s.schema_name, s.table_name
            ),
            ARRAY[]::text[]
        )
        ||
        coalesce(
            array_agg(add_ext_dependency(s.template_rel_id) ORDER BY s.level, s.schema_name, s.table_name),
            ARRAY[]::text[]
        )
    FROM
        missing_template_shard_structure s
            JOIN pg_namespace n ON n.nspname = s.template_schema_name
    GROUP BY 1, 2

    UNION ALL
    -- Create shield views for every managed partition node.
    SELECT
        FALSE,
        TRUE,
        format('Creating shield views [%s]', string_agg(fqn(s.view_rel_id), ', ')),
        coalesce(
            array_agg(
                format('CREATE OR REPLACE VIEW %s AS SELECT * FROM %s',
                    fqn(s.view_rel_id),
                    fqn(s.rel_id)
                )
                ORDER BY s.level, s.schema_name, s.table_name
            ),
            ARRAY[]::text[]
        )
        ||
        coalesce(
            array_agg(
                format('GRANT SELECT ON %s TO %I', fqn(s.view_rel_id), pgwrh_replica_role_name())
                ORDER BY s.level, s.schema_name, s.table_name
            ),
            ARRAY[]::text[]
        )
        ||
        coalesce(
            array_agg(add_ext_dependency(s.view_rel_id) ORDER BY s.level, s.schema_name, s.table_name),
            ARRAY[]::text[]
        )
    FROM
        missing_view_shard_structure s
            JOIN pg_namespace n ON n.nspname = s.view_schema_name
    GROUP BY 1, 2

    UNION ALL
    -- CLEANUP: DROP unnecessary slot and remote (per shard server) schemas
    SELECT
        FALSE,
        TRUE,
        format('Removing unused schemas [%s]', string_agg(nspname, ', ')),
        ARRAY[
            format('DROP SCHEMA IF EXISTS %s CASCADE', string_agg(quote_ident(nspname), ','))
        ]
    FROM
        owned_namespace n
    WHERE
        n.nspname <> '@extschema@'
        AND NOT EXISTS (
            SELECT 1 FROM shard_schema WHERE n.nspname = schema_name
        )
        AND NOT EXISTS (
            SELECT 1 FROM slot_schema WHERE n.nspname = slot_schema_name
        )
        AND NOT EXISTS (
            SELECT 1 FROM template_schema WHERE n.nspname = template_schema_name
        )
        AND NOT EXISTS (
            SELECT 1 FROM view_schema WHERE n.nspname = view_schema_name
        )
        AND NOT EXISTS (
            SELECT 1 FROM shard_structure WHERE n.nspname = format('%s_remote', schema_name)
        )
        AND NOT EXISTS (
            SELECT 1 FROM remote_shard r WHERE (r.pc).relnamespace = n.oid AND r.parent IS NOT NULL
        )
        -- Make sure not to drop schemas that contain subscribed tables
        -- This can happen because dropping publications from subscription
        -- is done in separate transaction so there is a race condition.
        -- Adding this condition resolves that by postponing dropping
        -- schemas until after publications drop.
        AND NOT EXISTS (SELECT 1 FROM
            pg_subscription_rel JOIN pg_class c ON srrelid = c.oid
            WHERE
                c.relnamespace = n.oid
        )
    GROUP BY 1, 2

    UNION ALL
    -- Make sure user accounts for local shards are created
    SELECT
        FALSE,
        TRUE,
        format('User accounts [%s] to access local shards need to be created.', string_agg(username, ', ')),
        array_agg(format('CREATE USER %I PASSWORD %L IN ROLE %I', username, password, "@extschema@".pgwrh_replica_role_name()))
    FROM
        roles
    WHERE
                NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = username)
            AND EXISTS (SELECT 1 FROM pg_roles WHERE rolname = "@extschema@".pgwrh_replica_role_name())
    GROUP BY 1, 2 -- make sure we produce empty set when no results

    UNION ALL
    -- Clean up
    SELECT
        FALSE,
        TRUE,
        format('Dropping no longer needed roles [%s]', string_agg(u.rolname, ', ')),
        array_agg(format('DROP ROLE %I', u.rolname))
    FROM
        pg_roles u
            JOIN pg_auth_members ON member = u.oid
            JOIN pg_roles gr ON gr.oid = roleid AND gr.rolname = "@extschema@".pgwrh_replica_role_name()
    WHERE
            NOT EXISTS (SELECT 1 FROM roles WHERE u.rolname = username)
    GROUP BY 1, 2 -- make sure we produce empty set when no results

    UNION ALL
    -- Grant USAGE on local shards view schemas
    SELECT
        FALSE,
        TRUE,
        format('Found view shard schemas [%s] without proper access rights for other replicas', string_agg(view_schema_name, ', ')),
        ARRAY[
            format('GRANT USAGE ON SCHEMA %s TO %I', string_agg(quote_ident(view_schema_name), ', '), pgwrh_replica_role_name())
        ]
    FROM
        view_schema s
            JOIN pg_namespace n ON n.nspname = s.view_schema_name
            JOIN pg_roles ON
                    rolname = pgwrh_replica_role_name()
                AND NOT has_schema_privilege(rolname, n.oid, 'USAGE')

    GROUP BY 1, 2

    UNION ALL
    -- Change all attachments of a root together; never expose a partial expansion.
    SELECT
        FALSE,
        TRUE,
        format('Switching query routes for %s', fqn(c.root_rel_id)),
        ARRAY[format('LOCK TABLE ONLY %s IN ACCESS EXCLUSIVE MODE', fqn(c.root_rel_id))]
        || array_agg(command ORDER BY phase, depth DESC, rel_id)
    FROM attachment_command c JOIN ready_root USING (root_rel_id)
    GROUP BY c.root_rel_id

    UNION ALL
    -- Subscriptions
    SELECT
        FALSE,
        FALSE,
        format('Adding missing shards [%s] to subscription [%s]', string_agg((sc).reg_class::text, ', '), s.subname),
        ARRAY[
            format('TRUNCATE %s',
                string_agg((sc).reg_class::text, ', ')
            ),
            format('ALTER SUBSCRIPTION %I ADD PUBLICATION %s WITH (copy_data = true)',
                s.subname,
                string_agg(quote_ident(sc.pubname), ', ')
            )
        ]
    FROM
        local_shard sc JOIN owned_subscription s USING (subname)
    WHERE
        NOT EXISTS (
            SELECT 1 FROM unnest(s.subpublications) AS pub(name)
            WHERE pub.name = sc.pubname
        )
    GROUP BY
        s.subname

    UNION ALL
    -- create missing indexes
    SELECT * FROM
    (
        SELECT
            TRUE,
            TRUE,
            format('Creating missing index [%s] ON [%s]', index_name, reg_class),
            ARRAY[
                format('CREATE INDEX IF NOT EXISTS %I ON %s %s',
                    index_name,
                    reg_class,
                    index_template
                ),
                add_ext_dependency(((rel_id).schema_name, index_name)::rel_id)
            ]
        FROM
            missing_index
        WHERE
            -- there is no way to find out what index is being created
            -- so we only allow one concurrent indexing for any given table
            NOT EXISTS (
                SELECT 1 FROM pg_stat_progress_create_index WHERE relid = reg_class
            )
        LIMIT
            -- make sure no more than max_worker_processes/2 indexing operations at the same time
            greatest(0, current_setting('max_worker_processes')::int/2 - (SELECT count(*) FROM pg_stat_progress_create_index))
    ) AS sub

    UNION ALL
    -- DROP indexes not defined in index_template
    -- make sure we do not drop constraint indexes
    SELECT
        FALSE,
        TRUE,
        format('Dropping unnecessary indexes [%s] on %s', string_agg(i.indexrelid::regclass::text, ', '), string_agg(reg_class::text, ', ')),
        ARRAY[
            format('DROP INDEX %s', string_agg(i.indexrelid::regclass::text, ', '))
        ]
    FROM
        pg_index i
            JOIN pg_class ic ON ic.oid = i.indexrelid
            JOIN shard_assignment sa ON sa.reg_class = i.indrelid
    WHERE
            NOT EXISTS (SELECT 1 FROM
                shard_index t
                WHERE ic.relname = t.index_name AND i.indrelid = reg_class
            )
        AND (sa.local OR NOT EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = i.indrelid))
        AND NOT EXISTS (SELECT 1 FROM
                pg_constraint
                WHERE conindid = i.indexrelid
            )
    GROUP BY 1, 2

    UNION ALL
    -- DROP subscriptions for no longer hosted shards
    SELECT
        FALSE,
        FALSE,
        format('Dropping subscribed publications for no longer hosted shards [%s]', string_agg(pub.name, ', ')),
        ARRAY[
            format('ALTER SUBSCRIPTION %I DROP PUBLICATION %s',
                s.subname,
                string_agg(quote_ident(pub.name), ', ')
            ),
            -- FIXME There is a race condition here when cascade delete shard schemas
            (
                SELECT format('TRUNCATE %s', string_agg(srrelid::regclass::text, ', '))
                FROM
                    pg_subscription_rel
                WHERE
                    srsubid = s.oid
                    AND NOT EXISTS (
                        SELECT 1 FROM local_shard WHERE reg_class = srrelid
                    )
            )
        ]
    FROM
        owned_subscription s, unnest(s.subpublications) pub(name)
    WHERE
        NOT EXISTS (
            SELECT 1 FROM local_shard WHERE pubname = pub.name
        )
        AND pub.name NOT IN ('pgwrh_controller_ping')
        AND NOT EXISTS (
            SELECT 1 FROM pg_subscription_rel sr JOIN pg_inherits i ON i.inhrelid = sr.srrelid
            WHERE sr.srsubid = s.oid
                AND NOT EXISTS (SELECT 1 FROM local_shard WHERE reg_class = sr.srrelid)
        )
    GROUP BY
        s.oid, s.subname

----- REMOTE SHARDS ------
    UNION ALL
    -- Actual servers are shared by every shard using the same endpoint and user.
    SELECT FALSE, TRUE,
        format('Creating target servers [%s]', string_agg(server_name, ', ')),
        array_agg(format('CREATE SERVER %I FOREIGN DATA WRAPPER pgwrh_fdw OPTIONS
            (host %L, port %L, dbname %L, load_balance_weight %L)',
            server_name, host, port, dbname, weight::text))
        || array_agg(format('CREATE USER MAPPING FOR PUBLIC SERVER %I OPTIONS (user %L, password %L)',
            server_name, username, password))
        -- The virtual server owner authorizes target access. Querying roles use
        -- the PUBLIC mapping with ordinary table permissions, without server USAGE.
        || array_agg(select_add_ext_dependency('pg_foreign_server'::regclass, 'srvname', server_name))
    FROM target_server JOIN roles ON shard_server_user = username
    WHERE NOT EXISTS (SELECT 1 FROM pg_foreign_server WHERE srvname = server_name)
    GROUP BY 1, 2

    UNION ALL
    -- Each logical shard keeps one virtual server and an empty mapping.
    SELECT FALSE, TRUE,
        format('Creating virtual shard servers [%s]', string_agg(shard_server_name, ', ')),
        array_agg(format('CREATE SERVER %I FOREIGN DATA WRAPPER pgwrh_fdw OPTIONS
            (members %L, async_capable ''true'', updatable ''false'', truncatable ''false'',
             extensions %L, fdw_tuple_cost ''99999'', analyze_sampling ''auto'')',
            shard_server_name, array_to_string(target_servers, ','),
            (SELECT string_agg(extname, ', ') FROM pg_extension)))
        || array_agg(format('CREATE USER MAPPING FOR PUBLIC SERVER %I', shard_server_name))
        || array_agg(select_add_ext_dependency('pg_foreign_server'::regclass, 'srvname', shard_server_name))
    FROM shard_server
    WHERE NOT EXISTS (SELECT 1 FROM pg_foreign_server WHERE srvname = shard_server_name)
        AND NOT EXISTS (SELECT 1 FROM unnest(target_servers) t(name)
                        WHERE NOT EXISTS (SELECT 1 FROM pg_foreign_server WHERE srvname = t.name))
    GROUP BY 1, 2

    UNION ALL
    -- create missing remote schemas
    SELECT
        FALSE,
        TRUE,
        format('Found remote schemas [%s] to create.', string_agg(shard_server_schema_name, ', ')),
        array_agg(format('CREATE SCHEMA IF NOT EXISTS %I', shard_server_schema_name))
        ||
        array_agg(select_add_ext_dependency('pg_namespace'::regclass, format('%L::regnamespace', shard_server_schema_name)))
    FROM
        shard_server_schema
    WHERE
        NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = shard_server_schema_name)
    GROUP BY 1, 2

    UNION ALL
    -- Create missing remote shards
    SELECT
        FALSE,
        TRUE,
        format('Creating missing remote shards [%s]', string_agg(fqn(remote_rel_id), ', ')),
        array_agg(
            format('CREATE FOREIGN TABLE %s PARTITION OF %s %s SERVER %I OPTIONS (schema_name %L)',
                fqn(remote_rel_id),
                template.reg_class,
                sa.remote_bound,
                shard_server_name,
                (sa).view_schema_name
            )
        )
        ||
        array_agg(add_ext_dependency(remote_rel_id))
        ||
        array_agg(
            format('ALTER TABLE %s DETACH PARTITION %s',
                template.reg_class,
                fqn(remote_rel_id)
            )
        )
    FROM
        remote_assignment sa
            JOIN local_rel template ON template.rel_id = sa.template_rel_id
            JOIN pg_namespace ns ON ns.nspname = shard_server_schema_name
            JOIN pg_foreign_server fs ON fs.srvname = shard_server_name
    WHERE
        NOT EXISTS (SELECT 1 FROM
            rel
            WHERE rel_id = remote_rel_id
        )
    GROUP BY 1, 2

    UNION ALL
    -- Finish parent shield reads within the sync pass before reporting routes.
    -- Leaf analysis can run in the background while retained copies remain available.
    SELECT
        is_leaf,
        TRUE,
        format('Analyze remote shards [%s]', reg_class),
        ARRAY [
            format('ANALYZE %s', reg_class),
            format('INSERT INTO "@extschema@".analyzed_remote_pg_class (oid) VALUES (%s) ON CONFLICT DO NOTHING', reg_class::oid)
        ]
    FROM (
        SELECT
            rs.reg_class, remote_assignment.is_leaf
        FROM
            configured_remote_shard rs
                JOIN remote_assignment ON rs.rel_id = remote_rel_id
        WHERE
                NOT EXISTS (SELECT 1 FROM
                    pg_statistic s
                    WHERE s.starelid = rs.reg_class
                )
            AND NOT EXISTS (SELECT 1 FROM
                    analyzed_remote_pg_class
                    WHERE oid = rs.reg_class
                )
            AND NOT EXISTS (SELECT 1 FROM
                pg_stat_progress_analyze
                WHERE
                        datname = current_database()
                    AND relid = rs.reg_class
                )
        -- run maximum 5 background analysis concurrently
        LIMIT greatest(
            0,
            least(
                5,
                current_setting('max_worker_processes')::int - 6 - (SELECT count(*) FROM pg_stat_progress_analyze WHERE datname = current_database())))
    ) sub

    UNION ALL
    -- DROP remote shards no longer in use
    SELECT
        FALSE,
        TRUE,
        format('Dropping remote shards [%s] no longer in use', string_agg(reg_class::text, ', ')),
        ARRAY[
            format('DROP FOREIGN TABLE IF EXISTS %s', string_agg(reg_class::text, ', '))
        ]
    FROM
        remote_shard rs
    WHERE
        NOT EXISTS (SELECT 1 FROM
            shard_structure s
            WHERE rs.rel_id = (format('%s_remote', s.schema_name), s.table_name)::rel_id
              AND rs.srvname = pgwrh_shard_server(s.schema_name, s.table_name)
        )
        AND rs.parent IS NULL
    GROUP BY 1, 2

    UNION ALL
    -- This worker transaction waits for old readers, then publishes membership.
    -- The existing sync loop reports readiness only after the worker commits.
    SELECT FALSE, TRUE, format('Updating targets of virtual server %I', srvname),
        ARRAY[format('SELECT "@extschema:pgwrh_fdw@".pgwrh_fdw_set_members(%L, %L::text[])',
                     srvname, target_servers::text)]
    FROM owned_server JOIN shard_server ON srvname = shard_server_name
    WHERE target_servers IS DISTINCT FROM
          (SELECT array_agg(name ORDER BY name) FROM opts(srvoptions), unnest(vals) name
           WHERE key = 'members')
        AND NOT EXISTS (SELECT 1 FROM unnest(target_servers) t(name)
                        WHERE NOT EXISTS (SELECT 1 FROM pg_foreign_server WHERE srvname = t.name))

    UNION ALL
    -- Preference changes do not replace endpoint identity or require a handoff.
    SELECT FALSE, TRUE, format('Updating routing weight of target %I', srvname),
        ARRAY[format('ALTER SERVER %I OPTIONS (%s load_balance_weight %L)', srvname,
                     CASE WHEN current_weight IS NULL THEN 'ADD' ELSE 'SET' END, weight::text)]
    FROM owned_server JOIN target_server ON srvname = server_name
        CROSS JOIN LATERAL (SELECT (SELECT value FROM opts(srvoptions)
                                   WHERE key = 'load_balance_weight') AS current_weight) w
    WHERE current_weight IS DISTINCT FROM weight::text

    UNION ALL
    -- DROP remote servers (and all dependent objects) for non-existent remote shards
    SELECT
        FALSE,
        TRUE,
        format('Found server %s for non-existent shard. Dropping.', string_agg(srvname, ', ')),
        array_agg(format('DROP SERVER IF EXISTS %I CASCADE', srvname))
    FROM
        owned_server fs
    WHERE
        NOT EXISTS (SELECT 1 FROM remote_shard r WHERE r.srvname = fs.srvname)
        AND
            fs.srvname <> 'replica_controller'
        AND NOT EXISTS (SELECT 1 FROM shard_server WHERE fs.srvname = shard_server_name)
        AND NOT EXISTS (SELECT 1 FROM target_server WHERE fs.srvname = server_name)
        AND NOT EXISTS (SELECT 1 FROM owned_server v, LATERAL opts(v.srvoptions) o
                        WHERE o.key = 'members' AND fs.srvname = ANY(o.vals))
    GROUP BY 1, 2 -- make sure we produce empty set when no results


)
SELECT
    *
FROM
    scripts
;
-- FIXME should it be PUBLIC?
GRANT SELECT ON sync TO PUBLIC;

CREATE FUNCTION cleanup_analyzed_pg_class() RETURNS void LANGUAGE sql AS
$$
    DELETE
    FROM "@extschema@".analyzed_remote_pg_class ac
    WHERE
        NOT EXISTS (SELECT 1 FROM pg_class WHERE oid = ac.oid)
$$;
