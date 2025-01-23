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
        shard_server_user,
        shard_server_password
    FROM
        shard_assignment
),
table_with_slot AS (
    SELECT lr.*
    FROM
        local_rel lr
            JOIN local_rel slot ON slot.rel_id = lr.slot_rel_id
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
owned_obj AS (
    SELECT
        classid,
        objid
    FROM
        pg_depend d JOIN pg_extension e ON refclassid = 'pg_extension'::regclass AND refobjid = e.oid
    WHERE
        d.deptype = 'n'
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
subscribed_publication AS (
    SELECT
        subname, pub.name AS subpubname
    FROM
        owned_subscription, unnest(subpublications) AS pub(name)
),
unsuscribed_local_shard AS (
    SELECT
        *
    FROM
        local_shard
    WHERE
        NOT EXISTS (SELECT 1 FROM subscribed_publication WHERE subpubname = publication_name)
),
shard_index AS (
    SELECT
        reg_class,
        si.*
    FROM
        fdw_shard_index si
            JOIN local_shard ls ON (schema_name, table_name) = ((rel_id).schema_name, (rel_id).table_name)
    WHERE
        NOT EXISTS (
            SELECT 1 FROM pg_index i JOIN pg_class ic ON i.indexrelid = ic.oid
            WHERE
                i.indrelid = ls.reg_class AND
                ic.relname = si.index_name
        )
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
)
SELECT
    FALSE,
    TRUE,
    format('Found schema %I to create. Creating.', schema_name),
    ARRAY[
        format('CREATE SCHEMA IF NOT EXISTS %I', schema_name),
        select_add_ext_dependency('pg_namespace'::regclass, 'nspname', schema_name)
    ]
FROM
    (SELECT DISTINCT schema_name FROM fdw_shard_structure) s
WHERE
    NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = s.schema_name)
UNION ALL
SELECT * FROM (
    SELECT
        FALSE,
        TRUE,
        format('Found tables [%s] to create. Creating.', string_agg(format('%I.%I', schema_name, table_name), ', ')),
        array_agg(create_table ORDER BY level) AS commands
    FROM
        fdw_shard_structure s JOIN pg_namespace ON nspname = s.schema_name
    WHERE
        NOT EXISTS (SELECT 1 FROM local_rel WHERE (schema_name, table_name) = (s.schema_name, s.table_name))
)
WHERE array_length(commands, 1) > 0
UNION ALL
-- subscriptions
SELECT
    FALSE,
    TRUE,
    format('Record subscription %I to create', subname),
    ARRAY[
        format('INSERT INTO @extschema@.shard_subscription (subname, modulus, remainder) VALUES (%L, %s, %s) ON CONFLICT DO NOTHING', subname, sub_modulus, sub_remainder)
    ]
FROM
    (SELECT DISTINCT subname, sub_modulus, sub_remainder FROM local_shard ls WHERE NOT EXISTS (SELECT 1 FROM shard_subscription WHERE subname = ls.subname)) s

UNION ALL
-- Make sure there exists a subscription for all locally stored shards
SELECT
    FALSE,
    FALSE,
    format('Creating replication subscription %s', subname),
    ARRAY[
        format('CREATE SUBSCRIPTION %I CONNECTION %L PUBLICATION %s WITH (slot_name = %L)',
            subname,
            format('host=%s port=%s user=%s password=%s dbname=%s',
                s.host, s.port, cred.user, cred.pass, current_database()),
            string_agg(quote_ident(sc.publication_name), ', '),
            cred.user || '_' || (random() * 10000000)::bigint::text -- random slot_name
        )
    ]
FROM
    local_shard sc JOIN shard_subscription USING (subname)
        JOIN server_host_port s ON srvname = 'replica_controller'
        JOIN pg_user_mappings um ON um.srvid = s.oid AND (um.umuser = 0) -- PUBLIC
        CROSS JOIN LATERAL (
            SELECT
                u.value AS user,
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
-- -- Make sure there exists foreign server for all remote shards
SELECT
    FALSE,
    TRUE,
    format('Creating shard server for [%s:%s]', s.host, s.port),
    ARRAY[
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
        ),
        format('CREATE USER MAPPING FOR PUBLIC SERVER %I OPTIONS (user %L, password %L)',
            shard_server_name,
            shard_server_user,
            shard_server_password
        ),
        select_add_ext_dependency('pg_foreign_server'::regclass, 'srvname', shard_server_name)
    ]
FROM
    shard_server s
WHERE
    NOT EXISTS (
        SELECT 1 FROM pg_foreign_server WHERE srvname = s.shard_server_name
    )

UNION ALL
-- DROP remote servers (and all dependent objects) for non-existent remote shards
SELECT
    FALSE,
    TRUE,
    format('Found server %I for non-existent shard. Dropping.',
        srvname
    ),
    ARRAY[
        format('DROP SERVER IF EXISTS %I CASCADE', srvname)
    ]
FROM
    pg_foreign_server fs
        JOIN owned_obj ON classid = 'pg_foreign_server'::regclass AND objid = fs.oid
WHERE
    fs.srvname <> 'replica_controller'
    AND NOT EXISTS (
        SELECT 1 FROM shard_server WHERE shard_server_name = fs.srvname
    )

UNION ALL
-- Create missing schemas for each remote server
-- For each table [schema_name.table_name] we create schema [schema_name_serverid] that will hold foreign tables
SELECT
    FALSE,
    TRUE,
    format('Creating missing remote schema %I for server %I', shard_server_schema_name, shard_server_name),
    ARRAY[
        format('CREATE SCHEMA IF NOT EXISTS %I', shard_server_schema_name),
        select_add_ext_dependency('pg_namespace'::regclass, 'nspname', shard_server_schema_name)
    ]
FROM
    shard_server
WHERE
    NOT EXISTS (
        SELECT 1 FROM
            owned_namespace
        WHERE
            nspname = shard_server_schema_name
    )

UNION ALL
-- Create missing schemas for slots
SELECT
    FALSE,
    TRUE,
    format('Creating missing slot schema %I',
        slot_schema_name
    ),
    ARRAY[
        format('CREATE SCHEMA IF NOT EXISTS %I', slot_schema_name),
        select_add_ext_dependency('pg_namespace'::regclass, 'nspname', slot_schema_name)
    ]
FROM
    slot_schema
WHERE
    NOT EXISTS (
        SELECT 1
        FROM
            pg_namespace WHERE nspname = slot_schema_name
    )

UNION ALL
-- CLEANUP: DROP unnecessary slot and remote (per shard server) schemas
SELECT
    FALSE,
    TRUE,
    format('Removing unused schema %I', nspname),
    ARRAY[
        format('DROP SCHEMA IF EXISTS %I CASCADE', nspname)
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
        SELECT 1 FROM shard_server WHERE n.nspname = shard_server_schema_name
    )
    

UNION ALL
-- Make sure user accounts for local shards is created
SELECT
    FALSE,
    TRUE,
    format('User account %I to access local shards needs to be created.', shard_server_user),
    ARRAY[
        format('CREATE USER %I PASSWORD %L', shard_server_user, shard_server_password)
    ]
FROM
    (
        SELECT DISTINCT shard_server_user, shard_server_password
        FROM local_shard WHERE
            NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = shard_server_user)
    )

UNION ALL
-- Grant USAGE on local shards schemas
SELECT
    FALSE,
    TRUE,
    format('Found local shard schema [%I] without proper access rights for other replicas', schema_name),
    ARRAY[
        format('GRANT USAGE ON SCHEMA %I TO %I', schema_name, rolname)
    ]
FROM
    pg_roles JOIN (SELECT DISTINCT (rel_id).schema_name, shard_server_user FROM local_shard) s ON rolname = shard_server_user
WHERE
    NOT has_schema_privilege(rolname, schema_name, 'USAGE')

UNION ALL
-- Grant SELECT on local shards
SELECT
    FALSE,
    TRUE,
    format('Found local shard [%s] without proper access rights for other replicas', reg_class),
    ARRAY[
        format('GRANT SELECT ON %s TO %I', reg_class, rolname)
    ]
FROM
    local_shard JOIN pg_roles ON shard_server_user = rolname
WHERE
    NOT has_table_privilege(rolname, reg_class, 'SELECT')

UNION ALL
-- Create single table infrastructure: slot and remote tables
SELECT
    FALSE,
    TRUE,
    format('Found new shard %s. Preparing slot and remote tables.', reg_class),
    ARRAY[
        format('ALTER TABLE %s DETACH PARTITION %s',
            (parent).reg_class,
            reg_class
        ),
        format('CREATE TABLE %I.%I PARTITION OF %s %s PARTITION BY %s',
            (slot_rel_id).schema_name,
            (slot_rel_id).table_name,
            (parent).reg_class,
            (lr).bound,
            pg_get_partkeydef((parent).pc.oid)
        ),
        select_add_ext_dependency('pg_class', format('%L::regclass', format('%s.%s', (slot_rel_id).schema_name, (slot_rel_id).table_name))),
        format('CREATE FOREIGN TABLE %I.%I PARTITION OF %I.%I %s SERVER %I OPTIONS (schema_name %L)',
            (remote_rel_id).schema_name,
            (remote_rel_id).table_name,
            (slot_rel_id).schema_name,
            (slot_rel_id).table_name,
            (lr).bound,
            shard_server_name,
            (rel_id).schema_name
        ),
        select_add_ext_dependency('pg_class', format('%L::regclass', format('%s.%s', (remote_rel_id).schema_name, (remote_rel_id).table_name)))
    ]
FROM
    shard_assignment sc
        JOIN pg_namespace sns ON sns.nspname = slot_schema_name
        JOIN pg_namespace rns ON rns.nspname = (remote_rel_id).schema_name
WHERE
    parent IS NOT NULL
    AND (parent).pn.oid <> sns.oid
    AND EXISTS (
        SELECT 1 FROM pg_foreign_server
        WHERE srvname = shard_server_name
    )

UNION ALL
-- Make sure the right foreign table is attached to slot
SELECT
    FALSE,
    TRUE,
    format('Found mismatched remote table %s connected to slot. Replacing.', ft.ftrelid::regclass),
    ARRAY[
        format('ALTER TABLE %s DETACH PARTITION %s',
            slot.reg_class,
            ft.ftrelid::regclass
        ),
        format('DROP FOREIGN TABLE %s CASCADE',
            ft.ftrelid::regclass
        ),
        format('CREATE FOREIGN TABLE %I.%I PARTITION OF %s %s SERVER %I OPTIONS (schema_name %L)',
            (rs).remote_rel_id.schema_name,
            (rs).remote_rel_id.table_name,
            slot.reg_class,
            (slot).bound,
            shard_server_name,
            (rs).rel_id.schema_name
        ),
        select_add_ext_dependency('pg_class'::regclass,
            format('%L::regclass', format('%s.%s',
                (rs).remote_rel_id.schema_name,
                (rs).remote_rel_id.table_name)))
    ]
FROM
    shard_assignment rs
        JOIN pg_foreign_server fs ON srvname = shard_server_name
        JOIN pg_namespace n ON n.nspname = rs.shard_server_schema_name
        JOIN local_rel slot ON slot.rel_id = rs.slot_rel_id
        JOIN pg_inherits i ON i.inhparent = slot.reg_class
        JOIN pg_foreign_table ft ON ft.ftrelid = i.inhrelid AND ft.ftserver <> fs.oid

UNION ALL
-- Shard sharing
-- If a local shard is ready (ie. synchronized and indexed)
-- drop any remote table attached to the slot and attach the local shard
-- TODO partition check constraints handling to speed up attaching local shards
SELECT
    FALSE,
    TRUE,
    format('Found local shard [%s] ready to expose. Attaching to target table.',
        (sa).reg_class
    ),
    ARRAY[
        format('ALTER TABLE %s DETACH PARTITION %s',
            slot.reg_class,
            ft.ftrelid::regclass
        ),
        format('DROP FOREIGN TABLE %s CASCADE',
            ft.ftrelid::regclass
        ),
        format('ALTER TABLE %s ATTACH PARTITION %s %s',
            slot.reg_class,
            (sa).reg_class,
            slot.bound
        )
    ]
FROM
    local_shard sa
        JOIN local_rel slot ON slot.rel_id = sa.slot_rel_id
        JOIN pg_subscription_rel sr ON srrelid = (sa).reg_class AND srsubstate = 'r'
        JOIN owned_subscription s ON s.oid = srsubid
        JOIN pg_inherits i ON i.inhparent = slot.reg_class
        JOIN pg_foreign_table ft ON ft.ftrelid = i.inhrelid
WHERE
    -- it is not already attached
    (sa).parent IS NULL
    -- all "ready" indexes must be created
    AND NOT EXISTS (
        SELECT 1 FROM missing_index WHERE reg_class = sa.reg_class AND NOT pending
    )

UNION ALL
-- Detach shards with missing indexes and replace it with a new remote shard
SELECT
    FALSE,
    TRUE,
    format('Found exposed shard %s that is missing indexes. Detaching.', (lr).reg_class),
    ARRAY[
        format('ALTER TABLE %s DETACH PARTITION %s',
            (lr).parent.reg_class,
            (lr).reg_class
        ),
        format('CREATE FOREIGN TABLE %I.%I PARTITION OF %s %s SERVER %I OPTIONS (schema_name %L)',
            (remote_rel_id).schema_name,
            (remote_rel_id).table_name,
            (parent).reg_class,
            (lr).bound,
            shard_server_name,
            (rel_id).schema_name
        )
    ]
FROM
    shard_assignment a
        JOIN pg_foreign_server fs ON srvname = shard_server_name
        JOIN pg_namespace ON nspname = shard_server_schema_name
WHERE
    local AND connected
    AND EXISTS (
        SELECT 1 FROM missing_index WHERE reg_class = a.reg_class AND NOT pending
    )

UNION ALL
-- Detach non-local shards and replace them with remote tables
SELECT
    FALSE,
    TRUE,
    format('Found no longer local shard [%s] attached to slot. Detaching', reg_class),
    ARRAY[
        format('ALTER TABLE %s DETACH PARTITION %s',
            (parent).reg_class,
            reg_class
        ),
        format('CREATE FOREIGN TABLE %I.%I PARTITION OF %s %s SERVER %I OPTIONS (schema_name %L)',
            (remote_rel_id).schema_name,
            (remote_rel_id).table_name,
            (parent).reg_class,
            (lr).bound,
            shard_server_name,
            (rel_id).schema_name
        )
    ]
FROM
    shard_assignment rs
        JOIN pg_foreign_server fs ON fs.srvname = shard_server_name
        JOIN pg_namespace n ON n.nspname = rs.shard_server_schema_name
WHERE
    NOT local AND connected

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
            string_agg(quote_ident(sc.publication_name), ', ')
        )
    ]
FROM
    local_shard sc JOIN owned_subscription s USING (subname)
WHERE
    NOT EXISTS (
        SELECT 1 FROM unnest(s.subpublications) AS pub(name)
        WHERE pub.name = sc.publication_name
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
            )
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
SELECT
    FALSE,
    TRUE,
    format('Dropping unnecessary indexes [%s] on %s', string_agg(i.indexrelid::regclass::text, ', '), reg_class),
    ARRAY[
        format('DROP INDEX %s', string_agg(i.indexrelid::regclass::text, ', '))
    ]
FROM
    pg_index i
        JOIN pg_class ic ON ic.oid = i.indexrelid
        JOIN shard_assignment sa ON sa.reg_class = i.indrelid
WHERE
    NOT EXISTS (
        SELECT 1 FROM shard_index t
        WHERE ic.relname = t.index_name AND i.indrelid = reg_class
    )
    AND NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conindid = i.indexrelid
    )
GROUP BY
    reg_class

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
        (
            SELECT format('TRUNCATE %s', string_agg(srrelid::regclass::text, ', '))
            FROM pg_subscription_rel
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
        SELECT 1 FROM local_shard WHERE publication_name = pub.name
    )
    AND pub.name NOT IN ('replication_controller') -- FIXME
GROUP BY
    s.oid, s.subname

UNION ALL
-- DROP indexes of remote shards
SELECT
    FALSE,
    TRUE,
    format('Dropping indexes of no longer hosted shards [%s]', string_agg(DISTINCT i.indrelid::regclass::text, ', ')),
    ARRAY[
        format('DROP INDEX %s',
            string_agg(DISTINCT i.indrelid::regclass::text, ', ')
        )
    ]
FROM
    table_with_slot ls
        JOIN pg_index i ON i.indrelid = ls.reg_class
WHERE
    NOT EXISTS (
        SELECT 1 FROM local_shard WHERE reg_class = ls.reg_class
    )
    -- DO NOT drop constraint indexes
    AND NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conindid = i.indexrelid
    )
-- need group by to produce empty set when no results
GROUP BY 1
;
-- -- FIXME???
GRANT SELECT ON sync TO PUBLIC;
