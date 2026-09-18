-- SPDX-License-Identifier: AGPL-3.0-or-later
\echo Use "CREATE EXTENSION pgwrh_ui" to load this file. \quit

-- All application objects belong to this extension. No controller tables,
-- triggers, grants, or replica-side objects are changed by installation.
CREATE DOMAIN "text/html" AS text;
-- Script tags request */*: force raw assets rather than JSON scalar output.
CREATE DOMAIN "*/*" AS bytea;

CREATE FUNCTION check_session() RETURNS void
LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
BEGIN
    -- Some existing pgwrh management functions set their own search_path.
    -- Reject sessions with a temporary namespace before entering those APIs:
    -- a direct SQL caller must not shadow a controller table in pg_temp while
    -- a UI endpoint is running with its owner's privileges. PostgREST's UI
    -- requests do not create temporary objects.
    IF pg_my_temp_schema() <> 0 THEN
        RAISE sqlstate 'PT403' USING MESSAGE = 'Use a fresh database session without temporary objects for the controller console';
    END IF;
END
$$;
