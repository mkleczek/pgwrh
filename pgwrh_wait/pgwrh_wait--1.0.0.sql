CREATE FUNCTION applied_lsn(subscription_name text)
RETURNS pg_lsn
AS 'MODULE_PATHNAME', 'pgwrh_applied_lsn'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;

COMMENT ON FUNCTION applied_lsn(text) IS
'Last publisher LSN known visible by an apply-worker commit callback; NULL until initialized. This does not wait or refresh a snapshot.';

CREATE FUNCTION wait_for_lsn(subscription_name text, target_lsn pg_lsn,
                             timeout_ms integer DEFAULT 10000)
RETURNS void
AS 'MODULE_PATHNAME', 'pgwrh_wait_for_lsn'
LANGUAGE C VOLATILE PARALLEL UNSAFE;

COMMENT ON FUNCTION wait_for_lsn(text, pg_lsn, integer) IS
'Wait for committed logical apply progress. Only a subsequent Read Committed statement gets a fresh snapshot; use SET LOCAL pgwrh.read_after_lsn for Repeatable Read/Serializable.';
