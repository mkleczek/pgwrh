\set ON_ERROR_STOP on
-- The wait API must work on a plain PostgreSQL subscriber.
CREATE EXTENSION pgwrh_wait;
DO $$
BEGIN
    ASSERT (SELECT array_agg(extname ORDER BY extname) = ARRAY['pgwrh_wait']::name[]
        FROM pg_extension WHERE extname <> 'plpgsql'),
        'Standalone pgwrh_wait installed other extensions';
END $$;
CREATE SUBSCRIPTION packaging_probe CONNECTION 'host=localhost dbname=postgres'
    PUBLICATION packaging_probe WITH (connect = false);
SELECT pgwrh.applied_lsn('packaging_probe');

-- Install the core after the wait API, reusing the pgwrh schema.
CREATE EXTENSION pgwrh CASCADE;
DO $$
DECLARE
    extension_name text;
BEGIN
    ASSERT current_setting('server_version_num')::int / 10000 = 18,
        'The release requires PostgreSQL 18';
    ASSERT NOT EXISTS (SELECT FROM pg_extension WHERE extname = 'postgres_fdw'),
        'The bundle must not require the stock postgres_fdw extension';
    ASSERT (SELECT f.fdwname = 'pgwrh_fdw' FROM pg_foreign_server s
        JOIN pg_foreign_data_wrapper f ON f.oid = s.srvfdw
        WHERE s.srvname = 'replica_controller'),
        'The controller connection must use the bundled FDW';
    FOREACH extension_name IN ARRAY ARRAY['pgwrh', 'pgwrh_fdw', 'pgwrh_wait'] LOOP
        ASSERT (SELECT extversion = '0.3.0' FROM pg_extension WHERE extname = extension_name),
            'Installed extension version differs from the release';
        ASSERT (SELECT array_agg(version ORDER BY version) = ARRAY['0.3.0']
            FROM pg_available_extension_versions WHERE name = extension_name),
            'Unexpected historical installation scripts';
        ASSERT NOT EXISTS (SELECT FROM pg_extension_update_paths(extension_name) WHERE path IS NOT NULL),
            'Unexpected upgrade scripts';
    END LOOP;
END $$;
-- Execute native code, rather than checking only the control and SQL files.
SELECT * FROM pgwrh_fdw_get_connections();

-- Removing the core must leave the independent wait API usable.
DROP EXTENSION pgwrh CASCADE;
SELECT pgwrh.applied_lsn('packaging_probe');
DROP EXTENSION pgwrh_wait;

-- Also cover the installation order used by the container and guides.
CREATE EXTENSION pgwrh CASCADE;
CREATE EXTENSION pgwrh_wait;
SELECT * FROM pgwrh_fdw_get_connections();
SELECT pgwrh.applied_lsn('packaging_probe');
ALTER SUBSCRIPTION packaging_probe SET (slot_name = NONE);
DROP SUBSCRIPTION packaging_probe;
