GRANT USAGE ON SCHEMA @extschema@ TO PUBLIC;

ALTER DEFAULT PRIVILEGES GRANT EXECUTE ON ROUTINES TO PUBLIC;

--------------------
-- Global (controller and replica) helpers
--------------------
CREATE OR REPLACE FUNCTION add_ext_dependency(_classid regclass, _objid oid) RETURNS void LANGUAGE sql AS
$$INSERT INTO pg_depend (classid, objid, refclassid, refobjid, deptype, objsubid, refobjsubid)
SELECT _classid, _objid, 'pg_extension'::regclass, e.oid, 'n', 0 ,0
FROM pg_extension e WHERE e.extname = 'pgwrh'$$;

CREATE OR REPLACE FUNCTION select_add_ext_dependency(_classid regclass, oidexpr text) RETURNS text LANGUAGE sql AS
$$SELECT format('SELECT @extschema@.add_ext_dependency(%L, %s)', _classid, oidexpr)$$;

CREATE OR REPLACE FUNCTION select_add_ext_dependency(_classid regclass, name_attr text, name text) RETURNS text LANGUAGE sql AS
$$SELECT format('SELECT @extschema@.add_ext_dependency(%1$L, (SELECT oid FROM %1$s WHERE %I = %L))', _classid, name_attr, name)$$;

-------------------
-- End Global (controller and replica) helpers
-------------------

CREATE TYPE config_version AS ENUM ('FLIP', 'FLOP');

CREATE OR REPLACE FUNCTION next_version(version config_version) RETURNS config_version
IMMUTABLE
SET SEARCH_PATH FROM CURRENT
LANGUAGE sql AS
$$SELECT CASE version WHEN 'FLIP' THEN 'FLOP' ELSE 'FLIP' END::config_version$$;

CREATE TABLE IF NOT EXISTS replication_group (
    replication_group_id text NOT NULL PRIMARY KEY,
    username text NOT NULL,
    password text NOT NULL
);
SELECT pg_catalog.pg_extension_config_dump('replication_group', '');
COMMENT ON TABLE replication_group IS
'Represents a specific cluster (replica group) configuration.
A single sever may be a source of data for multiple groups of replicas.
Each group may have different configuration, in particular:
* what tables should be sharded
* number of desired copies per shard
* member servers and shard hosts topology

Username and password are credentials shared between cluster members and used
to access remote shards (ie. they are used in USER MAPPINGs created on cluster members).
';
COMMENT ON COLUMN replication_group.replication_group_id IS
'Unique identifier of a replication group.';

CREATE TABLE IF NOT EXISTS replication_group_config (
    replication_group_id text NOT NULL REFERENCES replication_group(replication_group_id),
    version config_version NOT NULL,
    pending boolean NOT NULL,

    PRIMARY KEY (replication_group_id, version),
    UNIQUE (replication_group_id, pending)
);
SELECT pg_catalog.pg_extension_config_dump('replication_group_config', '');
COMMENT ON TABLE replication_group_config IS
'Represents a version of configuration of a replication group.

Each cluster (replication group) configuration is versioned to make sure
changes in cluster topology and shards configuration does not cause any downtime.

There may be two versions of configuration present at the same time.
A configuration version might be "pending" or "ready".

Version marked as "ready" (pending = false) is a configuration version that all
replicas installed and configured successfully. The shards assigned to replicas in that version are copied, indexed and available to use.

Version marked as "pending" (pending = true) is a configuration version that is under installaction/configuration by the replicas.

A replica keeps all shards from "ready" configuration even if a shard might be no longer assigned to it in "pending" configuration version.';

CREATE OR REPLACE FUNCTION next_pending_version(group_id text) RETURNS config_version
SET SEARCH_PATH FROM CURRENT
LANGUAGE sql AS
$$
    -- calculate next version
    WITH v AS (
        SELECT g.replication_group_id, coalesce(pending.version, next_version(ready.version), 'FLIP') AS pending_version, ready.version AS version, TRUE
        FROM
            replication_group g
                LEFT JOIN replication_group_config pending ON g.replication_group_id = pending.replication_group_id AND pending.pending
                LEFT JOIN replication_group_config ready ON g.replication_group_id = ready.replication_group_id AND NOT ready.pending
        WHERE
            g.replication_group_id = group_id
    ),
    -- insert next config if necessary
    _ AS (
        INSERT INTO replication_group_config (replication_group_id, version, pending)
        SELECT replication_group_id, pending_version, TRUE FROM v
        ON CONFLICT DO NOTHING
    ),
    -- clone weights if necessary
    __ AS (
        INSERT INTO shard_host_weight (replication_group_id, host_id, version, weight)
        SELECT
            replication_group_id, host_id, pending_version, weight
        FROM
            shard_host_weight w
                JOIN v USING (replication_group_id, version)
        ON CONFLICT DO NOTHING
    ),
    -- clone shards if necessary
    ___ AS (
        INSERT INTO sharded_table (replication_group_id, sharded_table_schema, sharded_table_name, version, replica_count)
        SELECT replication_group_id, sharded_table_schema, sharded_table_name, pending_version, replica_count
        FROM
            sharded_table JOIN v USING (replication_group_id, version)
        ON CONFLICT DO NOTHING
    ),
    -- clone inex templates if necessary
    ____ AS (
        INSERT INTO shard_index_template (replication_group_id, version, schema_name, table_name, index_name, index_template)
        SELECT replication_group_id, pending_version, schema_name, table_name, index_name, index_template
        FROM
            shard_index_template JOIN v USING (replication_group_id, version)
        ON CONFLICT DO NOTHING
    )
    SELECT pending_version FROM v
$$;
COMMENT ON FUNCTION next_pending_version(group_id text) IS
'Inserts next pending version into replication_group_config and returns it.

Clones existing non-pending configuration.';

CREATE OR REPLACE FUNCTION next_pending_version_trigger() RETURNS TRIGGER
SET SEARCH_PATH FROM CURRENT
LANGUAGE plpgsql AS
$$BEGIN
    NEW.version := @extschema@.next_pending_version(NEW.replication_group_id);
    RETURN NEW;
END$$;

CREATE TABLE IF NOT EXISTS replication_group_member (
    replication_group_id text NOT NULL REFERENCES replication_group(replication_group_id),
    host_id text NOT NULL,
    member_role text NOT NULL UNIQUE,
    availability_zone text NOT NULL,
    same_zone_multiplier smallint NOT NULL CHECK ( same_zone_multiplier BETWEEN 1 AND 5 ) DEFAULT 2,

    PRIMARY KEY (replication_group_id, host_id)
);
SELECT pg_catalog.pg_extension_config_dump('replication_group_member', '');
COMMENT ON TABLE replication_group_member IS
'Represents a node in a cluster (replication group).

A cluster consists of two types of nodes:

* shard hosts - nodes that replicate and serve data
* non replicating members - nodes that act only as proxies (ie. not hosting any shards)';

CREATE TABLE IF NOT EXISTS shard_host (
    replication_group_id text NOT NULL,
    host_id text NOT NULL,
    host_name text NOT NULL,
    port int NOT NULL CHECK ( port > 0 ),

    online boolean NOT NULL DEFAULT TRUE,

    PRIMARY KEY (replication_group_id, host_id),
    FOREIGN KEY (replication_group_id, host_id) REFERENCES replication_group_member(replication_group_id, host_id),
    UNIQUE (host_name, port)
);
SELECT pg_catalog.pg_extension_config_dump('shard_host', '');
COMMENT ON TABLE shard_host IS
'Represents a data replicating node in a cluster (replication group).';
COMMENT ON COLUMN shard_host.online IS
'Shard host marked as offline is not going to receive any requests for data from other nodes.
It is still replicating shards assigned to it.

This flag is supposed to be used in situation when a particular node must be
temporarily disconnected from a cluster for maintenance purposes.';

CREATE TABLE IF NOT EXISTS shard_host_weight (
    replication_group_id text NOT NULL,
    host_id text NOT NULL,
    version config_version NOT NULL,
    weight int NOT NULL CHECK ( weight >= 0 ),

    PRIMARY KEY (replication_group_id, host_id, version),
    FOREIGN KEY (replication_group_id, host_id) REFERENCES shard_host(replication_group_id, host_id),
    FOREIGN KEY (replication_group_id, version) REFERENCES replication_group_config(replication_group_id, version)
);
SELECT pg_catalog.pg_extension_config_dump('shard_host_weight', '');
COMMENT ON TABLE shard_host_weight IS
'Weight of a shard host in a specific configuration version';

CREATE OR REPLACE TRIGGER shard_host_weight_version BEFORE INSERT ON shard_host_weight
FOR EACH ROW EXECUTE FUNCTION next_pending_version_trigger();

CREATE OR REPLACE FUNCTION add_shard_host(_replication_group_id text, _host_id text, _host_name text, _port int, _member_role regrole DEFAULT NULL, _availability_zone text DEFAULT 'default', _weight int DEFAULT 100) RETURNS void
SET SEARCH_PATH FROM CURRENT
LANGUAGE sql AS
$$
    WITH m AS (
        INSERT INTO replication_group_member (replication_group_id, host_id, member_role, availability_zone)
        VALUES (_replication_group_id, _host_id, coalesce(_member_role::text, _host_id::regrole::text), _availability_zone)
    ),
    h AS (
        INSERT INTO shard_host (replication_group_id, host_id, host_name, port)
        VALUES (_replication_group_id, _host_id, _host_name, _port)
    )
    INSERT INTO shard_host_weight (replication_group_id, host_id, weight)
    VALUES (_replication_group_id, _host_id, _weight)
$$;

CREATE TABLE IF NOT EXISTS sharded_table (
    replication_group_id text NOT NULL,
    sharded_table_schema text NOT NULL,
    sharded_table_name text NOT NULL,
    version config_version NOT NULL,
    replica_count smallint NOT NULL CHECK ( replica_count >= 0 ),

    PRIMARY KEY (replication_group_id, sharded_table_schema, sharded_table_name, version),
    FOREIGN KEY (replication_group_id, version) REFERENCES replication_group_config(replication_group_id, version)
);
SELECT pg_catalog.pg_extension_config_dump('sharded_table', '');

CREATE OR REPLACE TRIGGER sharded_table_version BEFORE INSERT ON sharded_table
FOR EACH ROW EXECUTE FUNCTION next_pending_version_trigger();

CREATE TABLE IF NOT EXISTS shard_index_template (
    replication_group_id text NOT NULL,
    version config_version,
    schema_name text NOT NULL,
    table_name text NOT NULL,
    index_name name NOT NULL,
    index_template text NOT NULL,

    PRIMARY KEY (replication_group_id, schema_name, table_name, index_name),
    FOREIGN KEY (replication_group_id, version) REFERENCES replication_group_config(replication_group_id, version)
);
SELECT pg_catalog.pg_extension_config_dump('shard_index_template', '');

CREATE TABLE IF NOT EXISTS pg_wrh_publication (
    publication_name text NOT NULL PRIMARY KEY,
    published_shard oid NOT NULL UNIQUE
);

CREATE OR REPLACE FUNCTION to_regclass(st @extschema@.sharded_table) RETURNS regclass STABLE LANGUAGE sql AS
$$SELECT to_regclass(st.sharded_table_schema || '.' || st.sharded_table_name)$$;

CREATE OR REPLACE FUNCTION stable_hash(VARIADIC text[]) RETURNS int IMMUTABLE LANGUAGE sql AS
$$SELECT ('x' || substr(md5(array_to_string($1, '', '')), 1, 8))::bit(32)::int$$;

CREATE OR REPLACE FUNCTION score(weight int, VARIADIC text[]) RETURNS double precision IMMUTABLE LANGUAGE sql AS
$$SELECT weight / -ln(stable_hash(VARIADIC $2)::double precision / ((2147483649)::bigint - (-2147483648)::bigint) + 0.5::double precision)$$;


CREATE OR REPLACE FUNCTION scores(group_id text, schema_name text, table_name text)
RETURNS TABLE (
    host_id text,
    availability_zone text,
    member_role text,
    az_score double precision,
    max_pending_score double precision,
    min_pending_score double precision,
    ready_score double precision)
STABLE
SECURITY DEFINER -- TODO try to somehow make it usable in views without this
SET SEARCH_PATH FROM CURRENT
LANGUAGE sql AS
$$WITH shv AS (
    SELECT
        host_id,
        max(weight) AS max_pending_weight,
        min(weight) AS min_pending_weight,
        coalesce(min(weight) FILTER ( WHERE NOT pending ), 0) AS ready_weight
    FROM
        shard_host_weight JOIN replication_group_config USING (replication_group_id, version)
    WHERE
        replication_group_id = group_id
    GROUP BY
    	host_id
),
rgm AS (
    SELECT
        *
    FROM
        replication_group_member
    WHERE
        replication_group_id = group_id
)
SELECT
    host_id,
    availability_zone,
    member_role,
    score(100, schema_name, table_name, availability_zone) AS az_score,
    score(max_pending_weight, schema_name, table_name, host_id) AS max_pending_score,
    score(min_pending_weight, schema_name, table_name, host_id) AS min_pending_score,
    score(ready_weight, schema_name, table_name, host_id) AS ready_score
FROM
    shv JOIN rgm USING (host_id)
$$;

CREATE OR REPLACE VIEW shard_counts AS
WITH sharded_pg_class AS (
    SELECT
        st.replication_group_id,
        c.oid::regclass,
        version,
        replica_count
    FROM
        pg_class c
            JOIN pg_namespace n ON relnamespace = n.oid
            JOIN sharded_table st ON (nspname, relname) = (sharded_table_schema, sharded_table_name)
)
SELECT
    replication_group_id,
    nspname AS schema_name,
    relname AS table_name,
    max(replica_count) AS pending_count,
    coalesce(min(replica_count) FILTER ( WHERE NOT pending ), 0) AS ready_count,
    publication_name
FROM
    pg_class c
        JOIN pg_wrh_publication pwp ON c.oid = pwp.published_shard
        JOIN pg_publication_rel pr ON c.oid = prrelid
        JOIN pg_publication pub ON pub.oid = prpubid AND pub.pubname = pwp.publication_name
        JOIN pg_namespace n ON n.oid = relnamespace
        JOIN sharded_pg_class st ON st.oid = ANY (
            SELECT * FROM pg_partition_ancestors(c.oid)
        ) AND NOT EXISTS (
            SELECT 1
            FROM sharded_pg_class des
            WHERE
                des.oid <> st.oid AND
                st.oid = ANY (SELECT * FROM pg_partition_ancestors(des.oid)) 
        )
        JOIN replication_group_config USING (replication_group_id, version)
WHERE
    c.relkind = 'r'
GROUP BY
    replication_group_id, nspname, relname, publication_name;

COMMENT ON VIEW shard_counts IS
'Provides shards and their number of pending and ready copies based on configuration in sharded_table.
A shard is a non-partitioned table which ancestor (can be the table iself) is in sharded_table.
The desired number of copies is specified per the whole hierarchy (ie. all partitions of a given table).

Only shards for which there is a publication are present here.';

CREATE OR REPLACE VIEW shard_assignment AS
SELECT
    schema_name,
    table_name,
    (loc.host_id IS NOT NULL) AS local,
    shard_server_name,
    host,
    port,
    current_database() AS dbname,
    username AS shard_server_user,
    password AS shard_server_password,
    publication_name
FROM
    replication_group_member m
        JOIN replication_group g USING (replication_group_id)
        JOIN shard_counts sc USING (replication_group_id)
        -- all hosts of a particular shard
        -- from the point of view of m
        LEFT JOIN LATERAL (
            SELECT
                host_id,
                row_number() OVER (PARTITION BY s.availability_zone ORDER BY (CASE WHEN s.host_id = m.host_id THEN max_pending_score ELSE min_pending_score END) DESC) AS group_rank
            FROM
                scores(replication_group_id, sc.schema_name, sc.table_name) s
            ORDER BY
                group_rank, az_score
            LIMIT
                sc.pending_count
        ) loc USING (host_id)
        -- remote server of a particular shard
        -- from the point of view of m
        -- take "ready" values of weight and replica_count
        CROSS JOIN LATERAL (
            SELECT
                md5(coalesce(string_agg(host_name || ':' || port, ',') FILTER ( WHERE online ), '')) AS shard_server_name,
                coalesce(string_agg(host_name, ',') FILTER ( WHERE online ), '') AS host,
                coalesce(string_agg(port::text, ',') FILTER ( WHERE online ), '') AS port
            FROM (
                SELECT
                    sc.replication_group_id,
                    host_id,
                    availability_zone,
                    row_number() OVER (PARTITION BY availability_zone ORDER BY ready_score DESC) AS group_rank
                FROM
                    scores(replication_group_id, sc.schema_name, sc.table_name)
                ORDER BY
                    group_rank, az_score
                LIMIT
                    sc.ready_count
            ) AS top
                JOIN shard_host USING (replication_group_id, host_id),
                generate_series(1, CASE WHEN m.availability_zone = top.availability_zone THEN m.same_zone_multiplier ELSE 1 END)
            WHERE host_id <> m.host_id
        ) rem
WHERE member_role = CURRENT_ROLE;

GRANT SELECT ON shard_assignment TO PUBLIC;

COMMENT ON VIEW shard_assignment IS
'Main view implementing shard assignment logic.

Presents a particular replication_group_member (as identified by member_role) view of the cluster (replicaton_group).
Each member sees all shards with the following information for each shard:
* "local" flag saying if this shard should be replicated to this member
* information on how to connect to remote replicas for this shard: host, port, dbname, user, password';

CREATE OR REPLACE VIEW shard_index AS
WITH parent_class AS (
    SELECT DISTINCT
        replication_group_id,
        c.oid::regclass,
        it.index_name,
        it.index_template,
        pending
    FROM
        pg_class c
            JOIN pg_namespace n ON relnamespace = n.oid
            JOIN shard_index_template it ON (nspname, relname) = (schema_name, table_name)
            JOIN replication_group_config USING (replication_group_id, version)
)
SELECT
    nspname AS schema_name,
    relname AS table_name,
    format('%s_%s_%s', relname, index_name, substring(md5(index_template), 0, 16)) AS index_name,
    index_template,
    pending
FROM
    pg_class c
        JOIN pg_wrh_publication pwp ON c.oid = pwp.published_shard
        JOIN pg_publication_rel pr ON c.oid = prrelid
        JOIN pg_publication pub ON pub.oid = prpubid AND pub.pubname = pwp.publication_name
        JOIN pg_namespace n ON n.oid = relnamespace
        JOIN parent_class p ON p.oid = ANY (
            SELECT * FROM pg_partition_ancestors(c.oid)
        )
        JOIN replication_group_member m USING (replication_group_id)
WHERE
    c.relkind = 'r'
    AND m.member_role = CURRENT_ROLE;
GRANT SELECT ON shard_index TO PUBLIC;
COMMENT ON VIEW shard_index IS
'Provides definitions of indexes that should be created for each shard.';

-------- metadata

-- 
CREATE OR REPLACE FUNCTION sync_publications() RETURNS void
SET SEARCH_PATH FROM CURRENT
LANGUAGE plpgsql AS
$$DECLARE
    r record;
BEGIN
    INSERT INTO pg_wrh_publication (publication_name, published_shard)
    SELECT
            gen_random_uuid()::text,
            c.oid::regclass
        FROM
            pg_class c
                JOIN pg_namespace n ON n.oid = relnamespace
        WHERE
            EXISTS (
                SELECT 1 FROM pg_partition_ancestors(c.oid) a JOIN sharded_table st ON a.oid = to_regclass(st)
            )
            AND relkind = 'r'
    ON CONFLICT DO NOTHING;
    FOR r IN
        WITH deleted AS (
            DELETE FROM pg_wrh_publication p WHERE NOT EXISTS (
                SELECT 1 FROM pg_partition_ancestors(p.published_shard) a JOIN sharded_table st ON a.oid = to_regclass(st)
            )
            RETURNING *
        )
        SELECT
            format('DROP PUBLICATION %I CASCADE', d.publication_name) stmt
        FROM deleted d JOIN pg_publication p ON d.publication_name = p.pubname
    LOOP
        EXECUTE r.stmt;
    END LOOP;
    FOR r IN
        SELECT format('CREATE PUBLICATION %I FOR TABLE %s WITH ( publish = %L )',
                        p.publication_name,
                        p.published_shard::regclass,
                        'insert,update,delete') stmt,
                p.publication_name AS publication_name
        FROM pg_wrh_publication p JOIN pg_class ON published_shard = oid
        WHERE NOT EXISTS (
                SELECT 1 FROM pg_publication WHERE pubname = p.publication_name
            )
    LOOP
        EXECUTE r.stmt;
        PERFORM add_ext_dependency('pg_publication'::regclass, (SELECT oid FROM pg_publication WHERE pubname = r.publication_name));
    END LOOP;
    RETURN;
END
$$;

CREATE OR REPLACE FUNCTION sync_publications_trigger() RETURNS TRIGGER
SET SEARCH_PATH FROM CURRENT
LANGUAGE plpgsql AS
$$BEGIN
    PERFORM sync_publications();
    RETURN NULL;
END$$;

-- CREATE OR REPLACE FUNCTION sync_publications_event_trigger RETURNS event_trigger LANGUAGE pgsql AS
-- $$BEGIN
--     PERFORM sync_publications();
-- END$$;

CREATE OR REPLACE TRIGGER sync_publications AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON sharded_table
FOR EACH STATEMENT EXECUTE FUNCTION sync_publications_trigger();

-- CREATE EVENT TRIGGER sync_publications ON ddl_command_end
-- WHEN TAG IN ('CREATE TABLE', 'ALTER TABLE', 'DROP TABLE')
-- EXECUTE FUNCTION sync_publications_event_trigger();


-- -- API

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
    roots r JOIN replication_group_member m USING (replication_group_id), pg_partition_tree(oid) t
        JOIN pg_class c ON t.relid = c.oid JOIN pg_namespace n ON c.relnamespace = n.oid
        LEFT JOIN pg_class p ON t.parentrelid = p.oid LEFT JOIN pg_namespace pn ON p.relnamespace = pn.oid
WHERE
    member_role = CURRENT_ROLE;

GRANT SELECT ON shard_structure TO PUBLIC;


----------------- REPLICA -------------------

CREATE SERVER IF NOT EXISTS replica_controller FOREIGN DATA WRAPPER postgres_fdw;
CREATE USER MAPPING FOR PUBLIC SERVER replica_controller;

CREATE OR REPLACE FUNCTION sub_num_modulus_exponent() RETURNS int LANGUAGE sql AS
$$SELECT 2$$; -- FIXME GUC
CREATE OR REPLACE FUNCTION sub_num() RETURNS int LANGUAGE sql AS
$$SELECT (2 ^ @extschema@.sub_num_modulus_exponent())$$;

CREATE TABLE IF NOT EXISTS dependent_subscription (
    subname text NOT NULL PRIMARY KEY
);
SELECT pg_catalog.pg_extension_config_dump('dependent_subscription', '');
CREATE TABLE IF NOT EXISTS shard_subscription (
    subname text NOT NULL PRIMARY KEY REFERENCES dependent_subscription(subname),
    modulus int NOT NULL CHECK (modulus > 0),
    remainder int NOT NULL CHECK (remainder >= 0 AND remainder < modulus)
);
SELECT pg_catalog.pg_extension_config_dump('shard_subscription', '');

CREATE OR REPLACE FUNCTION insert_dependent_subscription_trigger() RETURNS trigger LANGUAGE plpgsql AS
$$BEGIN
    INSERT INTO @extschema@.dependent_subscription VALUES (NEW.subname);
    RETURN NEW;
END$$;
CREATE OR REPLACE TRIGGER insert_dependent_subscription BEFORE INSERT ON shard_subscription
FOR EACH ROW EXECUTE FUNCTION insert_dependent_subscription_trigger();


CREATE TABLE IF NOT EXISTS config_change (
    config_change_seq_number bigint PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS applied_config_change (
    config_change_seq_number bigint NOT NULL PRIMARY KEY REFERENCES config_change(config_change_seq_number)
);

-- -- types.sql

CREATE TYPE  rel_id AS (schema_name text, table_name text);

----------------
-- Helpers
----------------

-- options parsing
CREATE OR REPLACE FUNCTION opts(arr text[]) RETURNS TABLE(key text, value text, vals text[]) LANGUAGE sql AS
$$
SELECT kv[1] AS key, kv[2] AS value, vals FROM unnest(arr) AS o(val), string_to_array(o.val, '=') AS kv, string_to_array(kv[2], ',') vals
$$;

CREATE OR REPLACE FUNCTION update_server_options(server_name text, host text, port text, dbname text DEFAULT current_database())
RETURNS SETOF text
STABLE
LANGUAGE sql AS
$$
SELECT format('ALTER SERVER %I OPTIONS (%s)', srvname, string_agg(opts.cmd, ', '))
FROM
    pg_foreign_server pfs CROSS JOIN LATERAL (
        SELECT
            CASE
                WHEN opt.key IS NOT NULL THEN format('SET %s %L', toset.key, toset.val)
                ELSE format('ADD %s %L', toset.key, toset.val)
            END
        FROM
            unnest(ARRAY['host', 'port', 'dbname'], ARRAY[host, port, dbname]) AS toset(key, val)
                LEFT JOIN (SELECT * FROM opts(pfs.srvoptions)) AS opt USING (key)
        WHERE
            opt.key IS NULL OR toset.val <> opt.value
    ) AS opts(cmd)
WHERE
    srvname = server_name
GROUP BY
    srvname
$$;

CREATE OR REPLACE FUNCTION update_user_mapping(server_name text, username text, password text)
RETURNS SETOF text
STABLE
LANGUAGE sql AS
$$
SELECT format('ALTER USER MAPPING FOR PUBLIC SERVER %I OPTIONS (%s)', srvname, string_agg(opts.cmd, ', '))
FROM
    pg_user_mappings pum CROSS JOIN LATERAL (
        SELECT
            CASE
                WHEN opt.key IS NOT NULL THEN format('SET %s %L', toset.key, toset.val)
                ELSE format('ADD %s %L', toset.key, toset.val)
            END
        FROM
            unnest(ARRAY['user', 'password'], ARRAY[username, password]) AS toset(key, val)
                LEFT JOIN (SELECT * FROM opts(pum.umoptions)) AS opt USING (key)
        WHERE
            opt.key IS NULL OR toset.val <> opt.value
    ) AS opts(cmd)
WHERE
    srvname = server_name
GROUP BY
    srvname
$$;

-- rel_id functions
CREATE OR REPLACE VIEW rel AS
    SELECT
        pc, pn,
        nspname AS schema_name,
        relname AS table_name,
        (nspname, relname)::rel_id AS rel_id,
        pc.oid::regclass AS reg_class
    FROM
        pg_class pc
            JOIN pg_namespace pn ON pn.oid = pc.relnamespace;
CREATE OR REPLACE VIEW local_rel AS
SELECT
    r.*,
    pg_get_expr((r).pc.relpartbound, (r).pc.oid) AS bound,
    parent,
    ((r.rel_id).schema_name || '_' || 'slot', (r.rel_id).table_name)::rel_id AS slot_rel_id
FROM
    rel r
        LEFT JOIN pg_inherits pi ON (r).pc.oid = pi.inhrelid
        LEFT JOIN rel AS parent ON (parent).pc.oid = pi.inhparent;

-- CREATE OR REPLACE VIEW server_host_port AS
-- SELECT
--     s.*,
--     host,
--     port
-- FROM
--     pg_foreign_server s,
--     LATERAL (
--         SELECT h.value AS host, p.value AS port
--         FROM opts(srvoptions) AS h, opts(srvoptions) AS p
--         WHERE h.key = 'host' AND p.key = 'port'
--     ) AS opts;

CREATE FOREIGN TABLE IF NOT EXISTS fdw_shard_assignment (
    schema_name text,
    table_name text,
    local boolean,
    shard_server_name text,
    host text,
    port text,
    dbname text,
    shard_server_user text,
    shard_server_password text,
    publication_name text
)
SERVER replica_controller
OPTIONS (table_name 'shard_assignment');

CREATE FOREIGN TABLE IF NOT EXISTS fdw_shard_index (
    schema_name text,
    table_name text,
    index_name text,
    index_template text,
    pending boolean
)
SERVER replica_controller
OPTIONS (table_name 'shard_index');

CREATE FOREIGN TABLE IF NOT EXISTS fdw_shard_structure (
    schema_name text,
    table_name text,
    level int,
    create_table text
)
SERVER replica_controller
OPTIONS (table_name 'shard_structure');

-- TODO rename
CREATE OR REPLACE VIEW shard_assignment_r AS
SELECT
    lr.rel_id AS rel_id,
    lr.slot_rel_id AS slot_rel_id,
    (lr).slot_rel_id.schema_name AS slot_schema_name,
    (shard_server_schema, (rel_id).table_name)::rel_id AS remote_rel_id,
    shard_server_name,
    (rel_id).schema_name || '_' || shard_server_name AS shard_server_schema_name,
    sa.local,
    format('pgwrh_shard_subscription_%s_%s', sub_num(), (stable_hash(sa.schema_name, sa.table_name) % sub_num() + sub_num()) % sub_num()) AS subname,
    sub_num() AS sub_modulus,
    (stable_hash(sa.schema_name, sa.table_name) % sub_num() + sub_num()) % sub_num() AS sub_remainder,
    sa.publication_name,
    sa.shard_server_user,
    sa.shard_server_password,
    sa.dbname,
    host,
    port,
    lr.reg_class,
    lr.parent,
    lr,
    parent IS NOT NULL AND (parent).rel_id = slot_rel_id AS connected
FROM
    fdw_shard_assignment sa
        JOIN local_rel lr ON (sa.schema_name, sa.table_name) = ((lr).rel_id.schema_name, (lr).rel_id.table_name),
        format('%s_%s', sa.schema_name, shard_server_name) AS shard_server_schema;

-- -- commands to execute
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
            string_agg(sc.publication_name, ', ')
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
            string_agg(pub.name, ', ')
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

CREATE OR REPLACE FUNCTION launch_sync() RETURNS void LANGUAGE sql AS
$$
    SELECT pg_background_detach(pg_background_launch('
        CAll @extschema@.sync_replica_worker();
    '));
$$;

CREATE OR REPLACE FUNCTION exec_script(script text) RETURNS boolean LANGUAGE plpgsql AS
$$BEGIN
    PERFORM * FROM pg_background_result(pg_background_launch(script)) AS discarded(result text);
    RETURN TRUE;
EXCEPTION
    WHEN OTHERS THEN
        RETURN FALSE;
END$$;

CREATE OR REPLACE FUNCTION exec_non_tx_scripts(script text[]) RETURNS boolean LANGUAGE plpgsql AS
$$DECLARE
    cmd text;
    err text;
BEGIN
    FOREACH cmd IN script LOOP
        PERFORM * FROM pg_background_result(pg_background_launch(cmd)) AS discarded(result text);
    END LOOP;
EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS err = MESSAGE_TEXT;
        raise NOTICE '%', err;
        RETURN FALSE;
END$$;

CREATE OR REPLACE FUNCTION sync_step() RETURNS boolean LANGUAGE plpgsql AS
$$
DECLARE
    r record;
    cmd text;
    err text;
BEGIN
    FOR r IN SELECT * FROM @extschema@.sync LOOP
        RAISE NOTICE '%', r.description;
        IF r.transactional THEN
            IF r.async THEN
                PERFORM pg_background_detach(pg_background_launch(array_to_string(r.commands, ';')));
            ELSE
                PERFORM @extschema@.exec_script(array_to_string(r.commands, ';'));
            END IF;
        ELSE
            IF r.async THEN
                IF array_length(r.commands, 1) > 1 THEN
                    PERFORM pg_background_detach(pg_background_launch(format('SELECT @extschema@.exec_non_tx_scripts(ARRAY[%s])', (SELECT string_agg(format('%L', c), ',') FROM unnest(r.commands) AS c))));
                ELSE
                    PERFORM pg_background_detach(pg_background_launch(r.commands[1]));
                END IF;
            ELSE
                FOREACH cmd IN ARRAY r.commands LOOP
                    PERFORM @extschema@.exec_script(cmd);
                END LOOP;
            END IF;
        END IF;
    END LOOP;
    RETURN FOUND;
EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS err = MESSAGE_TEXT;
        raise NOTICE '%', err;
        PERFORM pg_sleep(1);
        RETURN TRUE;
END
$$;

CREATE OR REPLACE PROCEDURE sync_replica_worker() LANGUAGE plpgsql AS
$$
BEGIN
    IF pg_try_advisory_lock(2895359559) THEN
        WHILE r FROM pg_background_result(pg_background_launch('SELECT @extschema@.sync_step()')) AS r(r boolean) LOOP
        END LOOP;
    END IF;
END
$$;


-- -- CREATE OR REPLACE FUNCTION sync_trigger() RETURNS trigger LANGUAGE plpgsql AS
-- -- $$BEGIN
-- --     PERFORM @extschema@.launch_sync();
-- --     RETURN NULL;
-- -- END$$;
-- -- CREATE OR REPLACE TRIGGER sync_trigger AFTER INSERT ON config_change FOR EACH ROW EXECUTE FUNCTION sync_trigger();
-- -- ALTER TABLE config_change ENABLE REPLICA TRIGGER sync_trigger;


-- -- -- API

CREATE OR REPLACE FUNCTION configure_controller(host text, port text, username text, password text)
RETURNS void
SET SEARCH_PATH FROM CURRENT
LANGUAGE plpgsql AS
$$
DECLARE
    r record;
BEGIN
    FOR r IN SELECT * FROM @extschema@.update_server_options('replica_controller', host, port) AS u(cmd) LOOP
        EXECUTE r.cmd;
    END LOOP;
    FOR r IN SELECT * FROM @extschema@.update_user_mapping('replica_controller', username, password) AS u(cmd) LOOP
        EXECUTE r.cmd;
    END LOOP;
END
$$;
