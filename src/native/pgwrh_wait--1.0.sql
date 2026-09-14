CREATE FUNCTION applied_lsn(subscription_name text)
RETURNS pg_lsn
AS 'MODULE_PATHNAME', 'pgwrh_applied_lsn'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;

COMMENT ON FUNCTION applied_lsn(text) IS
'Last publisher LSN known visible by an apply-worker commit callback; NULL until initialized. This does not wait or refresh a snapshot.';
