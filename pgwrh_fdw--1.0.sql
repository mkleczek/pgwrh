/* contrib/pgwrh_fdw/pgwrh_fdw--1.0.sql */

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
