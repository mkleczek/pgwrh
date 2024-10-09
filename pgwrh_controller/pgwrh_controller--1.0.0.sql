GRANT USAGE ON SCHEMA @extschema@ TO PUBLIC;

CREATE TYPE config_version AS ENUM ('FLIP', 'FLOP');

CREATE OR REPLACE FUNCTION next_version(version config_version) RETURNS config_version IMMUTABLE LANGUAGE sql AS
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

    offline boolean NOT NULL DEFAULT FALSE,

    PRIMARY KEY (replication_group_id, host_id),
    FOREIGN KEY (replication_group_id, host_id) REFERENCES replication_group_member(replication_group_id, host_id),
    UNIQUE (host_name, port)
);
SELECT pg_catalog.pg_extension_config_dump('shard_host', '');
COMMENT ON TABLE shard_host IS
'Represents a data replicating node in a cluster (replication group).';
COMMENT ON COLUMN shard_host.offline IS
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

CREATE TABLE IF NOT EXISTS index_template (
    replication_group_id text NOT NULL,
    schema_name text NOT NULL,
    table_name text NOT NULL,
    index_name name NOT NULL,
    index_template text NOT NULL,

    PRIMARY KEY (replication_group_id, schema_name, table_name, index_name)
);
GRANT SELECT ON index_template TO PUBLIC;

CREATE TABLE IF NOT EXISTS pg_wrh_publication (
    publication_name text NOT NULL PRIMARY KEY,
    published_shard oid NOT NULL UNIQUE
);

CREATE OR REPLACE FUNCTION to_regclass(st sharded_table) RETURNS regclass STABLE LANGUAGE sql AS
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
LANGUAGE sql AS
$$WITH shv AS (
    SELECT
        host_id,
        max(weight) AS max_pending_weight,
        least(min(weight), coalesce(min(weight) FILTER ( WHERE NOT pending ), 0)) AS min_pending_weight,
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
    coalesce(min(replica_count) FILTER ( WHERE NOT pending ), 0) AS ready_count
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
    replication_group_id, nspname, relname;

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
    username AS shard_server_user,
    password AS shard_server_password
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
                md5(string_agg(host_name || ':' || port, ',') FILTER ( WHERE NOT offline )) AS shard_server_name,
                string_agg(host_name, ',') FILTER ( WHERE NOT offline ) AS host,
                string_agg(port::text, ',') FILTER ( WHERE NOT offline ) AS port
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
COMMENT ON VIEW shard_assignment IS
'Main view implementing shard assignment logic.

Presents a particular replication_group_member (as identified by member_role) view of the cluster (replicaton_group).
Each member sees all shards with the following information for each shard:
* "local" flag saying if this shard should be replicated to this member
* information on how to connect to remote replicas for this shard: host, port, dbname, user, password';

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
                        p.published_shard,
                        'insert,update,delete') stmt
        FROM pg_wrh_publication p
        WHERE NOT EXISTS (
                SELECT 1 FROM pg_publication WHERE pubname = p.publication_name
            )
    LOOP
        EXECUTE r.stmt;
    END LOOP;
    RETURN;
END
$$;

CREATE OR REPLACE FUNCTION sync_publications_trigger() RETURNS TRIGGER LANGUAGE plpgsql AS
$$BEGIN
    PERFORM sync_publications();
END$$;

-- CREATE OR REPLACE FUNCTION sync_publications_event_trigger RETURNS event_trigger LANGUAGE pgsql AS
-- $$BEGIN
--     PERFORM sync_publications();
-- END$$;

-- CREATE OR REPLACE TRIGGER sync_publications AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON sharded_table
-- FOR EACH STATEMENT EXECUTE FUNCTION sync_publications_trigger();

-- CREATE EVENT TRIGGER sync_publications ON ddl_command_end
-- WHEN TAG IN ('CREATE TABLE', 'ALTER TABLE', 'DROP TABLE')
-- EXECUTE FUNCTION sync_publications_event_trigger();


-- -- Impl helper
-- CREATE OR REPLACE FUNTION version_snapshot(group_id text) RETURNS config_version LANGUAGE sql AS
-- $$
-- config AS (
--     SELECT
--         replication_group_id,
--         coalesce(pending.version, next_version(ready.version), 'FLIP') AS version
--     FROM
--         replication_group g
--             LEFT JOIN replication_group_config pending ON g.replication_group_id = pending.replication_group_id AND pending.pending
--             LEFT JOIN replication_group_config ready ON g.replication_group_id = ready.replication_group_id AND NOT ready.pending
-- ),
-- _01 AS (
--     INSERT INTO replication_group_config (replication_group_id, version, pending)
--     SELECT group_id, version, TRUE
--     FROM  config
--     ON CONFLICT DO NOTHING
-- ),
-- _02 AS (
--     INSERT INTO shard_host_weight
--     SELECT replication_group_id, pending.version, weight
--     FROM
--         shard_host_weight
--             JOIN replication_group_config USING (replication_group_id)
--             JOIN config pending USING (replication_group_id)
--     WHERE
--         replication_group_id = group_id
--         AND NOT pending
--     ON CONFLICT DO NOTHING
-- ),
-- _03 AS (
--     INSERT INTO sharded_table (replication_group_id, version, sharded_table_schema, sharded_table_name, replica_count)
--     SELECT replication_group_id, pending.version, sharded_table_schema, sharded_table_name, replica_count
--     FROM
--         shard_host_weight
--             JOIN replication_group_config USING (replication_group_id)
--             JOIN config pending USING (replication_group_id)
--     WHERE
--         replication_group_id = group_id
--         AND NOT pending
--     ON CONFLICT DO NOTHING
-- )
-- SELECT version FROM config WHERE replication_group_id = group_id
-- $$;


-- -- API

-- CREATE OR REPLACE FUNCTION create_replication_group(id text, username text, password text) RETURNS void LANGUAGE sql AS
-- $$
-- WITH versions AS (
--     INSERT INTO replication_group_config VALUES (id, 'FLIP', TRUE);
-- )
-- INSERT INTO replication_group VALUES (id, username, password);
-- $$;

-- CREATE OR REPLACE FUNCTION create_shard_host(group_id text, id text, hostname text, port int, member_role regrole, az text DEFAULT 'default', weight int DEFAULT 100)
-- RETURNS void LANGUAGE sql AS
-- $$
-- WITH rgm AS (
--     INSERT INTO replication_group_member VALUES (group_id, id, member_role, az)
-- ),
-- sh AS (
--     INSERT INTO shard_host VALUES (group_id, id, hostname, port)
-- ),
-- config AS (
--     SELECT
--         coalesce(pending.version, next_version(ready.version), 'FLIP') AS version
--     FROM
--         replication_group g
--             LEFT JOIN replication_group_config pending ON g.replication_group_id = pending.replication_group_id AND pending.pending
--             LEFT JOIN replication_group_config ready ON g.replication_group_id = ready.replication_group_id AND NOT ready.pending
--     WHERE g.replication_group_id = group_id
-- ),
-- new_config AS (
--     INSERT INTO replication_group_config SELECT group_id, verstion, TRUE FROM config
--     RETURNING 1
-- )
-- INSERT INTO shard_host_weight
-- SELECT
--     group_id, id, version, weight
-- FROM
--     config
-- $$;