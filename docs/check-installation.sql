\set ON_ERROR_STOP on
-- Read-only diagnosis of the full bundle, including the optional wait API.
DO $$
DECLARE
    check_result record;
    failures integer := 0;
BEGIN
    FOR check_result IN
        SELECT * FROM (VALUES
            ('PostgreSQL 18', current_setting('server_version_num')::int / 10000 = 18),
            ('pgwrh 1.0.0-alpha1', EXISTS (SELECT FROM pg_extension WHERE extname = 'pgwrh' AND extversion = '1.0.0-alpha1')),
            ('pgwrh_fdw 1.0.0-alpha1', EXISTS (SELECT FROM pg_extension WHERE extname = 'pgwrh_fdw' AND extversion = '1.0.0-alpha1')),
            ('pgwrh_wait 1.0.0-alpha1', EXISTS (SELECT FROM pg_extension WHERE extname = 'pgwrh_wait' AND extversion = '1.0.0-alpha1')),
            ('pg_background v2 API', (SELECT count(DISTINCT p.proname) = 4
                FROM pg_proc p JOIN pg_extension e ON p.pronamespace = e.extnamespace
                WHERE e.extname = 'pg_background' AND p.proname IN
                    ('pg_background_launch_v2', 'pg_background_submit_v2', 'pg_background_result_v2', 'pg_background_detach_v2'))),
            ('pgwrh_wait preloaded after server restart', EXISTS
                (SELECT FROM pg_settings WHERE name = 'pgwrh.max_tracked_subscriptions')),
            ('logical WAL enabled', current_setting('wal_level') = 'logical')
        ) AS checks(description, ok)
    LOOP
        RAISE NOTICE '%: %', CASE WHEN check_result.ok THEN 'OK' ELSE 'MISSING' END, check_result.description;
        IF NOT check_result.ok THEN failures := failures + 1; END IF;
    END LOOP;
    IF failures > 0 THEN
        RAISE EXCEPTION '% installation prerequisites are missing; see docs/packages.md', failures;
    END IF;
END $$;
SELECT name, setting FROM pg_settings WHERE name IN
    ('max_worker_processes', 'max_replication_slots', 'max_wal_senders', 'max_logical_replication_workers');
