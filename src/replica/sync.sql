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
local_shard AS (
    SELECT * FROM shard_assignment WHERE local
),
slot_schema AS (
    SELECT DISTINCT slot_schema_name FROM shard_assignment
),
template_schema AS (
    SELECT DISTINCT template_schema_name FROM shard_assignment
),
shard_structure AS MATERIALIZED (
    SELECT * FROM fdw_shard_structure
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
        shard_server_user
    FROM
        shard_assignment
    WHERE
        shard_server_name IS NOT NULL
),
shard_server_schema AS (
    SELECT DISTINCT shard_server_schema_name
    FROM shard_assignment
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
-- subscribed_publication AS (
--     SELECT
--         subname, pub.name AS subpubname
--     FROM
--         owned_subscription, unnest(subpublications) AS pub(name)
-- ),
-- unsuscribed_local_shard AS (
--     SELECT
--         *
--     FROM
--         local_shard
--     WHERE
--         NOT EXISTS (SELECT 1 FROM subscribed_publication WHERE subpubname = pubname)
-- ),
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
ready_remote_shard AS (
    SELECT
        *
    FROM
        remote_shard
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
        ) s
    WHERE NOT EXISTS (SELECT 1 FROM
        pg_namespace
        WHERE nspname = schema_name
    )
    GROUP BY 1, 2

    UNION ALL
    SELECT
        FALSE,
        TRUE,
        format('Found tables [%s] to create.',
            string_agg(format('%I.%I', schema_name, table_name), ', ')),
        array_agg(create_table ORDER BY level)
        ||
        array_agg(add_ext_dependency((schema_name, table_name)))
    FROM
        shard_structure s JOIN pg_namespace n ON nspname = s.schema_name
    WHERE
        NOT EXISTS (SELECT 1 FROM local_rel WHERE (schema_name, table_name) = (s.schema_name, s.table_name))
    GROUP BY 1, 2 -- make sure we produce empty set when no results
    UNION ALL
    -- Subscriptions
    -- TODO decide if we need to implement multiple subscriptions - having hardcoded single subscription would simplify a bit
    SELECT
        FALSE,
        TRUE,
        format('Record subscription %I to create', subname),
        ARRAY[
            format('INSERT INTO "@extschema@".shard_subscription (subname, modulus, remainder) VALUES (%L, %s, %s) ON CONFLICT DO NOTHING', subname, sub_modulus, sub_remainder)
        ]
    FROM
        (
            SELECT DISTINCT subname, sub_modulus, sub_remainder
            FROM local_shard ls
                WHERE NOT EXISTS (SELECT 1 FROM
                    shard_subscription
                    WHERE subname = ls.subname
                )
        ) s

    UNION ALL
    -- Make sure there exists a subscription for all locally stored shards
    SELECT
        FALSE,
        FALSE,
        format('Creating replication subscription %s', subname),
        ARRAY[
            format('TRUNCATE %s',
                string_agg((sc).reg_class::text, ', ')
            ),
            format('CREATE SUBSCRIPTION %I CONNECTION %L PUBLICATION pgwrh_controller_publication,%s WITH (%s)',
                subname,
                -- always connecto to the primary
                format('host=%s port=%s user=%s password=%s dbname=%s target_session_attrs=primary',
                    s.host, s.port, cred.user, cred.pass, current_database()),
                -- compute subscription options
                -- this is verbose as it is sql :-)
                -- the full set of options depends on Pg version
                -- we always add slot_name
                -- and if Pg version >= 17 - failover = 'true'
                string_agg(quote_ident(sc.pubname), ', '),
                -- options
                (
                    SELECT string_agg(format('%s = %L', key, val), ', ') FROM (
                        SELECT
                            'slot_name' AS key,
                            cred.user || '_' || (random() * 10000000)::bigint::text AS val-- random slot_name
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
        ]
    FROM
        local_shard sc JOIN shard_subscription USING (subname)
            JOIN server_host_port s ON srvname = 'replica_controller'
            JOIN pg_user_mappings um ON um.srvid = s.oid AND (um.umuser = 0) -- PUBLIC
            CROSS JOIN LATERAL (
                SELECT
                    u.value AS "user",
                    p.value AS pass
                FROM
                    opts(um.umoptions) AS u,
                    opts(um.umoptions) AS p
                WHERE
                    u.key = 'user' AND p.key = 'password'
            ) AS cred
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM pg_subscription
            WHERE subname = sc.subname
        )
    GROUP BY
        subname, s.host, s.port, cred.user, cred.pass

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
            SELECT 1 FROM shard_assignment WHERE n.nspname IN (shard_server_schema_name, retained_shard_server_schema)
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
    -- Create pgwrh_replica role if not exists
    -- TODO should this be moved to extension intallation script
    -- so that it fails if there is conflicting role already present?
    SELECT
        FALSE,
        TRUE,
        format('Creating %I role', format('pgwrh_replica_%s', current_database())),
        ARRAY [
            format('CREATE ROLE %I', format('pgwrh_replica_%s', current_database()))
        ]
    WHERE
        NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = format('pgwrh_replica_%s', current_database()))

    UNION ALL
    -- Make sure user accounts for local shards are created
    SELECT
        FALSE,
        TRUE,
        format('User accounts [%s] to access local shards need to be created.', string_agg(username, ', ')),
        array_agg(format('CREATE USER %I PASSWORD %L IN ROLE %I', username, password, format('pgwrh_replica_%s', current_database())))
    FROM
        roles
    WHERE
                NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = username)
            AND EXISTS (SELECT 1 FROM pg_roles WHERE rolname = format('pgwrh_replica_%s', current_database()))
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
            JOIN pg_roles gr ON gr.oid = roleid AND gr.rolname = format('pgwrh_replica_%s', current_database())
    WHERE
            NOT EXISTS (SELECT 1 FROM roles WHERE u.rolname = username)
    GROUP BY 1, 2 -- make sure we produce empty set when no results

    UNION ALL
    -- Grant USAGE on local shards schemas
    SELECT
        FALSE,
        TRUE,
        format('Found local shard schemas [%s] without proper access rights for other replicas', string_agg(schema_name, ', ')),
        ARRAY[
            format('GRANT USAGE ON SCHEMA %s TO %I', string_agg(quote_ident(schema_name), ', '), format('pgwrh_replica_%s', current_database()))
        ]
    FROM
        pg_roles
            JOIN (SELECT DISTINCT (rel_id).schema_name FROM local_shard) s ON
                    rolname = format('pgwrh_replica_%s', current_database())
                AND NOT has_schema_privilege(rolname, schema_name, 'USAGE')
    GROUP BY 1, 2

    UNION ALL
    -- Grant SELECT on local shards
    SELECT
        FALSE,
        TRUE,
        format('Found local shard [%s] without proper access rights for other replicas', string_agg(reg_class::text, ', ')),
        ARRAY[
            format('GRANT SELECT ON %s TO %I', string_agg(reg_class::text, ', '), format('pgwrh_replica_%s', current_database()))
        ]
    FROM
        local_shard JOIN pg_roles ON
                rolname = format('pgwrh_replica_%s', current_database())
            AND NOT has_table_privilege(rolname, reg_class, 'SELECT')
    GROUP BY rolname

    UNION ALL
    -- Create single table infrastructure: slot and template tables
    SELECT
        FALSE,
        TRUE,
        format('Found new shards [%s]. Preparing slot tables.', string_agg(reg_class::text, ', ')),
        array_agg(
            format('ALTER TABLE %s DETACH PARTITION %s',
                (parent).reg_class,
                reg_class
            )
        )
        ||
        array_agg(
            format('CREATE TABLE %s PARTITION OF %s %s PARTITION BY %s',
                fqn(slot_rel_id),
                (parent).reg_class,
                (lr).bound,
                pg_get_partkeydef((parent).pc.oid)
            )
        )
        ||
        array_agg(add_ext_dependency(slot_rel_id))
        ||
        array_agg(
            format('CREATE TABLE %s PARTITION OF %s %s PARTITION BY %s',
                fqn(template_rel_id),
                fqn(slot_rel_id),
                (lr).bound,
                pg_get_partkeydef((parent).pc.oid)
            )
        )
        ||
        array_agg(add_ext_dependency(template_rel_id))
    FROM
        shard_assignment sc
            JOIN pg_namespace sns ON sns.nspname = slot_schema_name
            JOIN pg_namespace tns ON tns.nspname = template_schema_name
    WHERE
            parent IS NOT NULL
        AND (parent).pn.oid <> sns.oid
    GROUP BY
        1, 2

    UNION ALL
    -- Attach ready local shards to slots replacing existing attachments if necessary
    -- TODO partition check constraints handling to speed up attaching local shards
    SELECT
        FALSE,
        TRUE,
        format('Attaching local shards [%s] to slots', string_agg(format('%s', ready_shard.reg_class), ', ')),
        array_agg(format('ALTER TABLE %s DETACH PARTITION %s',
                slot.reg_class,
                i.inhrelid::regclass
            )
        ) FILTER (WHERE i IS NOT NULL)
        ||
        array_agg(format('ALTER TABLE %s ATTACH PARTITION %s %s',
                slot.reg_class,
                ready_shard.reg_class,
                slot.bound
            )
        )
    FROM
        shard_assignment sa
            JOIN local_rel slot ON slot.rel_id = sa.slot_rel_id
            JOIN ready_local_shard ready_shard ON sa.rel_id = ready_shard.rel_id
            LEFT JOIN pg_inherits i ON i.inhparent = slot.reg_class
    WHERE
            ready_shard.reg_class IS DISTINCT FROM i.inhrelid
        AND sa.local
        AND NOT sa.connect_remote
    GROUP BY 1, 2

    UNION ALL
    -- Attach ready remote shards to slots replacing
    -- existing attachments if necessary
    SELECT
        FALSE,
        TRUE,
        format('Attaching remote shards [%s] to slots', string_agg(format('%s', ready_shard.reg_class), ', ')),
        array_agg(format('ALTER TABLE %s DETACH PARTITION %s',
                slot.reg_class,
                i.inhrelid::regclass
            )
        ) FILTER (WHERE i IS NOT NULL)
        ||
        array_agg(format('ALTER TABLE %s ATTACH PARTITION %s %s',
                slot.reg_class,
                ready_shard.reg_class,
                slot.bound
            )
        )
    FROM
        shard_assignment sa
            JOIN local_rel slot ON slot.rel_id = sa.slot_rel_id
            JOIN ready_remote_shard ready_shard ON sa.remote_rel_id = ready_shard.rel_id
            LEFT JOIN pg_inherits i ON i.inhparent = slot.reg_class
    WHERE
            ready_shard.reg_class IS DISTINCT FROM i.inhrelid
        AND
            sa.connect_remote
    GROUP BY 1, 2

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
            format('ALTER SUBSCRIPTION %I ADD PUBLICATION %s',
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
        AND pub.name NOT IN ('pgwrh_controller_publication') -- FIXME
    GROUP BY
        s.oid, s.subname

----- REMOTE SHARDS ------
    UNION ALL
    -- create missing foreign servers
    SELECT
        FALSE,
        TRUE,
        format('Found foreign servers [%s] to create.', string_agg(format('%I', shard_server_name), ', ')),
        array_agg(
            format('CREATE SERVER IF NOT EXISTS %I FOREIGN DATA WRAPPER postgres_fdw OPTIONS
                    ( host %L, port %L, dbname %L,
                    load_balance_hosts ''random'',
                    async_capable ''true'',
                    updatable ''false'',
                    truncatable ''false'',
                    extensions %L,
                    fdw_tuple_cost ''99999'',
                    analyze_sampling ''system'')',
                shard_server_name,
                host, port,
                dbname,
                (SELECT string_agg(extname, ', ') FROM pg_extension) -- assume remote server has all the same extensions
            )
        )
        ||
        array_agg(
            format('CREATE USER MAPPING FOR PUBLIC SERVER %I OPTIONS (user %L, password %L)',
                shard_server_name,
                username,
                password
            ))
        ||
        array_agg(select_add_ext_dependency('pg_foreign_server'::regclass, 'srvname', shard_server_name))
    FROM
        shard_server
            JOIN roles ON shard_server_user = username
    WHERE
        NOT EXISTS (SELECT 1 FROM pg_foreign_server WHERE srvname = shard_server_name)
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
                slot.bound,
                shard_server_name,
                (sa).rel_id.schema_name
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
        shard_assignment sa
            JOIN local_rel template ON template.rel_id = sa.template_rel_id
            JOIN local_rel slot ON slot.rel_id = sa.slot_rel_id
            JOIN pg_namespace ns ON ns.nspname = shard_server_schema_name
            JOIN pg_foreign_server fs ON fs.srvname = shard_server_name
    WHERE
        NOT EXISTS (SELECT 1 FROM
            rel
            WHERE rel_id = remote_rel_id
        )
    GROUP BY 1, 2

    UNION ALL
    -- Analyze remote shards in parallel
    SELECT
        TRUE,
        TRUE,
        format('Analyze remote shards [%s]', reg_class),
        ARRAY [
            format('ANALYZE %s', reg_class),
            format('INSERT INTO "@extschema@".analyzed_remote_pg_class (oid) VALUES (%s) ON CONFLICT DO NOTHING', reg_class::oid)
        ]
    FROM (
        SELECT
            rs.reg_class
        FROM
            remote_shard rs
                JOIN shard_assignment ON rs.rel_id IN (remote_rel_id, retained_remote_rel_id)
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
            shard_assignment
            WHERE
                rs.rel_id IN (remote_rel_id, retained_remote_rel_id)
        )
    GROUP BY 1, 2

    UNION ALL
    -- Update foreign servers with updated host/port if changed
    SELECT
        FALSE,
        TRUE,
        format('Found modified host and port for server %I', srvname),
        ARRAY[
            cmd
        ]
    FROM
        owned_server
            JOIN shard_server ON srvname = shard_server_name,
            update_server_options(srvname, srvoptions, host, port) AS cmd

    UNION ALL
    -- Update user mapping with updated user/pass if changed
    SELECT
        FALSE,
        TRUE,
        format('Found modified user and pass for server %I', s.srvname),
        ARRAY[
            cmd
        ]
    FROM
        owned_server s
            JOIN pg_user_mappings um ON um.srvid = s.oid AND um.umuser = 0
            JOIN shard_server ON s.srvname = shard_server_name
            JOIN roles ON shard_server_user = username,
            update_user_mapping(s.srvname, umoptions, username, password) AS cmd

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
            fs.srvname <> 'replica_controller'
        AND NOT EXISTS (SELECT 1 FROM
            shard_assignment WHERE fs.srvname IN (shard_server_name, retained_shard_server_name)
        )
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
