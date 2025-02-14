-- name: replica-helpers

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

CREATE OR REPLACE FUNCTION sub_num_modulus_exponent() RETURNS int LANGUAGE sql AS
$$
    SELECT 0
$$; -- FIXME GUC
CREATE OR REPLACE FUNCTION sub_num() RETURNS int LANGUAGE sql AS
$$
    SELECT (2 ^ "@extschema@".sub_num_modulus_exponent())
$$;

-- options parsing
CREATE OR REPLACE FUNCTION opts(arr text[]) RETURNS TABLE(key text, value text, vals text[]) LANGUAGE sql AS
$$
SELECT kv[1] AS key, kv[2] AS value, vals FROM unnest(arr) AS o(val), string_to_array(o.val, '=') AS kv, string_to_array(kv[2], ',') vals
$$;

CREATE OR REPLACE FUNCTION update_server_options(_srvname text, srvoptions text[], host text, port text, dbname text DEFAULT current_database())
RETURNS SETOF text
STABLE
LANGUAGE sql AS
$$
SELECT format('ALTER SERVER %I OPTIONS (%s)', srvname, string_agg(opts.cmd, ', '))
FROM
    (
        SELECT
            _srvname,
            CASE
                WHEN opt.key IS NOT NULL THEN format('SET %s %L', toset.key, toset.val)
                ELSE format('ADD %s %L', toset.key, toset.val)
            END
        FROM
            unnest(ARRAY['host', 'port', 'dbname'], ARRAY[host, port, dbname]) AS toset(key, val)
                LEFT JOIN (SELECT * FROM opts(srvoptions)) AS opt USING (key)
        WHERE
            opt.key IS NULL OR toset.val <> opt.value
    ) AS opts(srvname, cmd)
GROUP BY
    srvname
$$;
CREATE OR REPLACE FUNCTION update_server_options(_srvname text, host text, port text, dbname text DEFAULT current_database())
RETURNS SETOF text
STABLE
LANGUAGE sql
AS
$$
    SELECT update_server_options(srvname, srvoptions, host, port, dbname)
    FROM
        pg_foreign_server
    WHERE
        srvname = _srvname;
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

CREATE TYPE  rel_id AS (schema_name text, table_name text);

CREATE FUNCTION fqn(rel_id) RETURNS text LANGUAGE sql AS
$$
    SELECT format('%I.%I', $1.schema_name, $1.table_name)
$$;
CREATE FUNCTION add_ext_dependency(rel_id) RETURNS text LANGUAGE sql AS
$$
    SELECT "@extschema@".select_add_ext_dependency('pg_class'::regclass, format('%L::regclass', "@extschema@".fqn($1)))
$$;

-- rel_id functions
CREATE VIEW rel AS
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

CREATE OR REPLACE VIEW shard_assignment_r AS
SELECT
    lr.rel_id AS rel_id,
    lr.slot_rel_id AS slot_rel_id,
    (lr).slot_rel_id.schema_name AS slot_schema_name,
    remote_rel_id,
    shard_server_name,
    shard_server_schema AS shard_server_schema_name,
    template_rel_id,
    shard_template_schema AS template_schema_name,
    sa.local,
    CASE WHEN local THEN rel_id ELSE remote_rel_id END AS shard_rel_id,
    format('pgwrh_shard_subscription_%s_%s', sub_num(), (stable_hash(sa.schema_name, sa.table_name) % sub_num() + sub_num()) % sub_num()) AS subname,
    sub_num() AS sub_modulus,
    (stable_hash(sa.schema_name, sa.table_name) % sub_num() + sub_num()) % sub_num() AS sub_remainder,
    sa.pubname,
    sa.shard_server_user,
    sa.shard_server_password,
    sa.dbname,
    host,
    port,
    retained_shard_server_name,
    retained_shard_server_schema,
    retained_remote_rel_id,
    lr.reg_class,
    lr.parent,
    lr,
    parent IS NOT NULL AND (parent).rel_id = slot_rel_id AS connected
FROM
    fdw_shard_assignment sa
        JOIN local_rel lr ON (sa.schema_name, sa.table_name) = ((lr).rel_id.schema_name, (lr).rel_id.table_name),
        format('%s_%s', sa.schema_name, shard_server_name) AS shard_server_schema,
        format('%s_%s', sa.schema_name, retained_shard_server_name) AS retained_shard_server_schema,
        format('%s_template', sa.schema_name) AS shard_template_schema
        CROSS JOIN LATERAL (
            SELECT
                (shard_server_schema, (rel_id).table_name)::rel_id AS remote_rel_id,
                (shard_template_schema, (rel_id).table_name)::rel_id AS template_rel_id,
                (retained_shard_server_schema, (rel_id).table_name)::rel_id AS retained_remote_rel_id
        ) AS rels;

CREATE VIEW subscribed_local_shard AS
    SELECT
        *
    FROM
        local_rel
    WHERE
        EXISTS (SELECT 1 FROM
            pg_subscription_rel sr
                JOIN pg_subscription s ON srsubid = s.oid
                JOIN shard_subscription USING (subname)
            WHERE srrelid = reg_class AND srsubstate = 'r'
        )
;

CREATE VIEW created_index AS
    SELECT
        schema_name,
        table_name AS index_name
    FROM
        pg_index i
            JOIN rel r ON i.indexrelid = r.reg_class
    WHERE
            schema_name <> '@extschema@'
        AND is_dependent_object('pg_class'::regclass, i.indexrelid)
;

CREATE VIEW remote_shard AS
    SELECT
        lr.*,
        s.srvname
    FROM
        local_rel lr
            JOIN pg_foreign_table ft ON ft.ftrelid = reg_class
            JOIN owned_server s ON
                    s.oid = ft.ftserver
;
