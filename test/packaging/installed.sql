\set ON_ERROR_STOP on
-- The wait API must work on a plain PostgreSQL subscriber.
CREATE EXTENSION pgwrh_wait;
DO $$
BEGIN
    ASSERT (SELECT array_agg(extname ORDER BY extname) = ARRAY['pgwrh_wait']::name[]
        FROM pg_extension WHERE extname <> 'plpgsql'),
        'Standalone pgwrh_wait installed other extensions';
END $$;
-- The GiST extension is independently installable and requires only btree_gist.
CREATE EXTENSION pgwrh_gist_extra CASCADE;
DO $$
BEGIN
    ASSERT (SELECT array_agg(extname ORDER BY extname) =
        ARRAY['btree_gist','pgwrh_gist_extra','pgwrh_wait']::name[]
        FROM pg_extension WHERE extname <> 'plpgsql'),
        'Standalone GiST extension pulled in pgwrh';
END $$;
CREATE TABLE gist_probe(account text);
INSERT INTO gist_probe VALUES ('one'), ('two'), ('three');
CREATE INDEX gist_probe_idx ON gist_probe USING gist (account pgwrh_gist_text_ops);
SET enable_seqscan = off;
DO $$
BEGIN
    ASSERT (SELECT array_agg(account ORDER BY account) FROM gist_probe
        WHERE account ||= ARRAY['one','three']) = ARRAY['one','three'],
        'Packaged GiST operators or support functions did not load';
END $$;
RESET enable_seqscan;
DROP TABLE gist_probe;
CREATE TABLE gist_order_probe(d date NOT NULL, id bigint NOT NULL);
INSERT INTO gist_order_probe VALUES
    ('2100-12-31', 9223372036854775807),
    ('2100-12-31', 9223372036854775806),
    ('2000-01-01', '-9223372036854775808');
CREATE INDEX ON gist_order_probe USING gist
    (d pgwrh_gist_date_order_ops, id pgwrh_gist_int8_order_ops);
SET enable_seqscan=off;
SET enable_sort=off;
DO $$
DECLARE
    query_plan json;
BEGIN
    ASSERT (SELECT array_agg(id) FROM
        (SELECT id FROM gist_order_probe ORDER BY d DESC,id DESC LIMIT 2) page)
        = ARRAY[9223372036854775807,9223372036854775806]::bigint[],
        'Packaged GiST ordering lost bigint precision';
    EXECUTE 'EXPLAIN (FORMAT JSON) SELECT * FROM gist_order_probe ORDER BY d DESC,id DESC LIMIT 2'
        INTO query_plan;
    ASSERT query_plan::text LIKE '%pgwrh GiST ordered scan%',
        'Packaged GiST planner hook did not produce an ordered scan';
END $$;
RESET enable_seqscan;
RESET enable_sort;
DROP TABLE gist_order_probe;
CREATE SUBSCRIPTION packaging_probe CONNECTION 'host=localhost dbname=postgres'
    PUBLICATION packaging_probe WITH (connect = false);
SELECT pgwrh.applied_lsn('packaging_probe');

-- Install the core after the wait API, reusing the pgwrh schema.
CREATE EXTENSION pgwrh CASCADE;
CREATE EXTENSION pgwrh_ui;
DO $$
DECLARE
    extension_name text;
BEGIN
    ASSERT current_setting('server_version_num')::int / 10000 IN (18, 19),
        'The release requires PostgreSQL 18 or 19';
    ASSERT NOT EXISTS (SELECT FROM pg_extension WHERE extname = 'postgres_fdw'),
        'The bundle must not require the stock postgres_fdw extension';
    ASSERT (SELECT f.fdwname = 'pgwrh_fdw' FROM pg_foreign_server s
        JOIN pg_foreign_data_wrapper f ON f.oid = s.srvfdw
        WHERE s.srvname = 'replica_controller'),
        'The controller connection must use the bundled FDW';
    FOREACH extension_name IN ARRAY ARRAY['pgwrh', 'pgwrh_ui', 'pgwrh_fdw', 'pgwrh_wait', 'pgwrh_gist_extra'] LOOP
        ASSERT (SELECT extversion = '1.0.0-alpha1' FROM pg_extension WHERE extname = extension_name),
            'Installed extension version differs from the release';
        ASSERT (SELECT array_agg(version ORDER BY version) = ARRAY['1.0.0-alpha1']
            FROM pg_available_extension_versions WHERE name = extension_name),
            'Unexpected historical installation scripts';
        ASSERT NOT EXISTS (SELECT FROM pg_extension_update_paths(extension_name) WHERE path IS NOT NULL),
            'Unexpected upgrade scripts';
    END LOOP;
    ASSERT pgwrh_ui.index()::text LIKE '<!doctype html>%',
        'The packaged UI must render its controller page';
    ASSERT octet_length(pgwrh_ui.htmx()::bytea) > 50000,
        'The packaged UI must include the vendored htmx asset';
    ASSERT position(':root' IN convert_from(pgwrh_ui.style()::bytea, 'UTF8')) > 0,
        'The packaged UI must include its stylesheet';
    ASSERT octet_length(pgwrh_ui.script()::bytea) > 0,
        'The packaged UI must include its application script';
END $$;
-- Execute native code, rather than checking only the control and SQL files.
SELECT * FROM pgwrh_fdw_get_connections();

-- Removing the core must leave the independent wait API usable.
DROP EXTENSION pgwrh CASCADE;
SELECT pgwrh.applied_lsn('packaging_probe');
SELECT 'one' ||= ARRAY['one'];
DROP EXTENSION pgwrh_gist_extra;
DROP EXTENSION btree_gist;
DROP EXTENSION pgwrh_wait;

-- Also cover the installation order used by the container and guides.
CREATE EXTENSION pgwrh CASCADE;
CREATE EXTENSION pgwrh_wait;
CREATE EXTENSION pgwrh_ui;
DO $$
BEGIN
    ASSERT NOT EXISTS (SELECT FROM pg_extension WHERE extname='pgwrh_gist_extra'),
        'The core must not require pgwrh_gist_extra';
END $$;
SELECT * FROM pgwrh_fdw_get_connections();
SELECT pgwrh.applied_lsn('packaging_probe');
ALTER SUBSCRIPTION packaging_probe SET (slot_name = NONE);
DROP SUBSCRIPTION packaging_probe;
