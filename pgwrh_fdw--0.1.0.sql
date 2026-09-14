/*
 * pgwrh_fdw 0.1.0 installation script.
 * Derived from PostgreSQL's postgres_fdw installation and upgrade scripts.
 * Upstream permissions: COPYRIGHT. Fork modifications: AGPL-3.0-only, LICENSE.
 */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pgwrh_fdw" to load this file. \quit

CREATE FUNCTION pgwrh_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION pgwrh_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER pgwrh_fdw
  HANDLER pgwrh_fdw_handler
  VALIDATOR pgwrh_fdw_validator;

CREATE FUNCTION pgwrh_fdw_get_connections (
    IN check_conn boolean DEFAULT false, OUT server_name text,
    OUT user_name text, OUT valid boolean, OUT used_in_xact boolean,
    OUT closed boolean, OUT remote_backend_pid int4)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pgwrh_fdw_get_connections_1_2'
LANGUAGE C STRICT PARALLEL RESTRICTED;

CREATE FUNCTION pgwrh_fdw_disconnect (text)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT PARALLEL RESTRICTED;

CREATE FUNCTION pgwrh_fdw_disconnect_all ()
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT PARALLEL RESTRICTED;
