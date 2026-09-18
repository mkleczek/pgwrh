-- No helper or endpoint is public by default. Deployment grants specific
-- endpoints to a dedicated PostgREST role; never expose the pgwrh schema.
REVOKE ALL ON SCHEMA pgwrh_ui FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA pgwrh_ui FROM PUBLIC;
