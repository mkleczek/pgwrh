/* pgwrh_gist_extra--1.0.0-alpha1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pgwrh_gist_extra" to load this file. \quit

-- CREATE OR REPLACE FUNCTION pgwrh_gist_text_in_array(text, text[]) RETURNS boolean IMMUTABLE LANGUAGE sql AS
-- $$SELECT $1 = ANY ($2)$$;

CREATE FUNCTION pgwrh_gist_text_any_eq_array(text, text[])
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION pgwrh_gist_text_all_eq_array(text, text[])
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION pgwrh_gist_text_consistent(internal,anyelement,int2,oid,internal)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION pgwrh_gist_options(internal)
RETURNS void
AS 'MODULE_PATHNAME', 'pgwrh_gist_options'
LANGUAGE C IMMUTABLE PARALLEL SAFE;


CREATE OPERATOR ||= (
	LEFTARG = text,
	RIGHTARG = text[],
	PROCEDURE = pgwrh_gist_text_any_eq_array
);

CREATE OPERATOR &&= (
	LEFTARG = text,
	RIGHTARG = text[],
	PROCEDURE = pgwrh_gist_text_all_eq_array
);

-- Create the operator class
CREATE OPERATOR CLASS pgwrh_gist_text_ops
FOR TYPE text USING gist
AS
	OPERATOR	1	<  ,
	OPERATOR	2	<= ,
	OPERATOR	3	=  ,
	OPERATOR	4	>= ,
	OPERATOR	5	>  ,
	FUNCTION	1	pgwrh_gist_text_consistent (internal, anyelement, int2, oid, internal),
	FUNCTION	2	gbt_text_union (internal, internal),
	FUNCTION	3	gbt_text_compress (internal),
	FUNCTION	4	gbt_var_decompress (internal),
	FUNCTION	5	gbt_text_penalty (internal, internal, internal),
	FUNCTION	6	gbt_text_picksplit (internal, internal),
	FUNCTION	7	gbt_text_same (gbtreekey_var, gbtreekey_var, internal),
	STORAGE			gbtreekey_var;

ALTER OPERATOR FAMILY pgwrh_gist_text_ops USING gist ADD
	OPERATOR	6	<> (text, text),
	OPERATOR	7	||= (text, text[]),
	OPERATOR	8	&&= (text, text[]),
	FUNCTION	9 (text, text) gbt_var_fetch (internal),
	FUNCTION	10 (text) pgwrh_gist_options (internal);

-- Exact ordering keys; 64-bit values use high and low 32-bit components.

CREATE FUNCTION pgwrh_gist_int2_order(smallint, smallint) RETURNS float8
AS 'MODULE_PATHNAME', 'pgwrh_gist_order_int2' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OPERATOR <# (LEFTARG=smallint, RIGHTARG=smallint, PROCEDURE=pgwrh_gist_int2_order);
CREATE FUNCTION pgwrh_gist_int2_order_distance(internal, smallint, smallint, oid, internal)
RETURNS float8 AS 'MODULE_PATHNAME', 'pgwrh_gist_order_int2_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR CLASS pgwrh_gist_int2_order_ops FOR TYPE smallint USING gist AS
    OPERATOR 1 <, OPERATOR 2 <=, OPERATOR 3 =,
    OPERATOR 4 >=, OPERATOR 5 >, OPERATOR 6 <>,
    OPERATOR 15 <# (smallint, smallint) FOR ORDER BY pg_catalog.float_ops,
    FUNCTION 1 gbt_int2_consistent(internal, smallint, smallint, oid, internal),
    FUNCTION 2 gbt_int2_union(internal, internal),
    FUNCTION 3 gbt_int2_compress(internal),
    FUNCTION 4 gbt_decompress(internal),
    FUNCTION 5 gbt_int2_penalty(internal, internal, internal),
    FUNCTION 6 gbt_int2_picksplit(internal, internal),
    FUNCTION 7 gbt_int2_same(gbtreekey4, gbtreekey4, internal),
    FUNCTION 8 pgwrh_gist_int2_order_distance(internal, smallint, smallint, oid, internal),
    FUNCTION 9 gbt_int2_fetch(internal),
    STORAGE gbtreekey4;

CREATE FUNCTION pgwrh_gist_int4_order(integer, smallint) RETURNS float8
AS 'MODULE_PATHNAME', 'pgwrh_gist_order_int4' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OPERATOR <# (LEFTARG=integer, RIGHTARG=smallint, PROCEDURE=pgwrh_gist_int4_order);
CREATE FUNCTION pgwrh_gist_int4_order_distance(internal, integer, smallint, oid, internal)
RETURNS float8 AS 'MODULE_PATHNAME', 'pgwrh_gist_order_int4_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR CLASS pgwrh_gist_int4_order_ops FOR TYPE integer USING gist AS
    OPERATOR 1 <, OPERATOR 2 <=, OPERATOR 3 =,
    OPERATOR 4 >=, OPERATOR 5 >, OPERATOR 6 <>,
    OPERATOR 15 <# (integer, smallint) FOR ORDER BY pg_catalog.float_ops,
    FUNCTION 1 gbt_int4_consistent(internal, integer, smallint, oid, internal),
    FUNCTION 2 gbt_int4_union(internal, internal),
    FUNCTION 3 gbt_int4_compress(internal),
    FUNCTION 4 gbt_decompress(internal),
    FUNCTION 5 gbt_int4_penalty(internal, internal, internal),
    FUNCTION 6 gbt_int4_picksplit(internal, internal),
    FUNCTION 7 gbt_int4_same(gbtreekey8, gbtreekey8, internal),
    FUNCTION 8 pgwrh_gist_int4_order_distance(internal, integer, smallint, oid, internal),
    FUNCTION 9 gbt_int4_fetch(internal),
    STORAGE gbtreekey8;

CREATE FUNCTION pgwrh_gist_int8_order(bigint, smallint) RETURNS float8
AS 'MODULE_PATHNAME', 'pgwrh_gist_order64' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OPERATOR <# (LEFTARG=bigint, RIGHTARG=smallint, PROCEDURE=pgwrh_gist_int8_order);
CREATE FUNCTION pgwrh_gist_int8_order_distance(internal, bigint, smallint, oid, internal)
RETURNS float8 AS 'MODULE_PATHNAME', 'pgwrh_gist_order64_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR CLASS pgwrh_gist_int8_order_ops FOR TYPE bigint USING gist AS
    OPERATOR 1 <, OPERATOR 2 <=, OPERATOR 3 =,
    OPERATOR 4 >=, OPERATOR 5 >, OPERATOR 6 <>,
    OPERATOR 15 <# (bigint, smallint) FOR ORDER BY pg_catalog.float_ops,
    FUNCTION 1 gbt_int8_consistent(internal, bigint, smallint, oid, internal),
    FUNCTION 2 gbt_int8_union(internal, internal),
    FUNCTION 3 gbt_int8_compress(internal),
    FUNCTION 4 gbt_decompress(internal),
    FUNCTION 5 gbt_int8_penalty(internal, internal, internal),
    FUNCTION 6 gbt_int8_picksplit(internal, internal),
    FUNCTION 7 gbt_int8_same(gbtreekey16, gbtreekey16, internal),
    FUNCTION 8 pgwrh_gist_int8_order_distance(internal, bigint, smallint, oid, internal),
    FUNCTION 9 gbt_int8_fetch(internal),
    STORAGE gbtreekey16;

CREATE FUNCTION pgwrh_gist_date_order(date, smallint) RETURNS float8
AS 'MODULE_PATHNAME', 'pgwrh_gist_order_int4' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OPERATOR <# (LEFTARG=date, RIGHTARG=smallint, PROCEDURE=pgwrh_gist_date_order);
CREATE FUNCTION pgwrh_gist_date_order_distance(internal, date, smallint, oid, internal)
RETURNS float8 AS 'MODULE_PATHNAME', 'pgwrh_gist_order_int4_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- Date/time Datums have the same ordered integer representation, including infinities.
CREATE FUNCTION pgwrh_gist_date_order_consistent(internal, date, smallint, oid, internal)
RETURNS bool AS '$libdir/btree_gist', 'gbt_int4_consistent'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR CLASS pgwrh_gist_date_order_ops FOR TYPE date USING gist AS
    OPERATOR 1 <, OPERATOR 2 <=, OPERATOR 3 =,
    OPERATOR 4 >=, OPERATOR 5 >, OPERATOR 6 <>,
    OPERATOR 15 <# (date, smallint) FOR ORDER BY pg_catalog.float_ops,
    FUNCTION 1 pgwrh_gist_date_order_consistent(internal, date, smallint, oid, internal),
    FUNCTION 2 gbt_int4_union(internal, internal),
    FUNCTION 3 gbt_int4_compress(internal),
    FUNCTION 4 gbt_decompress(internal),
    FUNCTION 5 gbt_int4_penalty(internal, internal, internal),
    FUNCTION 6 gbt_int4_picksplit(internal, internal),
    FUNCTION 7 gbt_int4_same(gbtreekey8, gbtreekey8, internal),
    FUNCTION 8 pgwrh_gist_date_order_distance(internal, date, smallint, oid, internal),
    FUNCTION 9 gbt_int4_fetch(internal),
    STORAGE gbtreekey8;

CREATE FUNCTION pgwrh_gist_timestamp_order(timestamp, smallint) RETURNS float8
AS 'MODULE_PATHNAME', 'pgwrh_gist_order64' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OPERATOR <# (LEFTARG=timestamp, RIGHTARG=smallint, PROCEDURE=pgwrh_gist_timestamp_order);
CREATE FUNCTION pgwrh_gist_timestamp_order_distance(internal, timestamp, smallint, oid, internal)
RETURNS float8 AS 'MODULE_PATHNAME', 'pgwrh_gist_order64_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- Date/time Datums have the same ordered integer representation, including infinities.
CREATE FUNCTION pgwrh_gist_timestamp_order_consistent(internal, timestamp, smallint, oid, internal)
RETURNS bool AS '$libdir/btree_gist', 'gbt_int8_consistent'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR CLASS pgwrh_gist_timestamp_order_ops FOR TYPE timestamp USING gist AS
    OPERATOR 1 <, OPERATOR 2 <=, OPERATOR 3 =,
    OPERATOR 4 >=, OPERATOR 5 >, OPERATOR 6 <>,
    OPERATOR 15 <# (timestamp, smallint) FOR ORDER BY pg_catalog.float_ops,
    FUNCTION 1 pgwrh_gist_timestamp_order_consistent(internal, timestamp, smallint, oid, internal),
    FUNCTION 2 gbt_int8_union(internal, internal),
    FUNCTION 3 gbt_int8_compress(internal),
    FUNCTION 4 gbt_decompress(internal),
    FUNCTION 5 gbt_int8_penalty(internal, internal, internal),
    FUNCTION 6 gbt_int8_picksplit(internal, internal),
    FUNCTION 7 gbt_int8_same(gbtreekey16, gbtreekey16, internal),
    FUNCTION 8 pgwrh_gist_timestamp_order_distance(internal, timestamp, smallint, oid, internal),
    FUNCTION 9 gbt_int8_fetch(internal),
    STORAGE gbtreekey16;

CREATE FUNCTION pgwrh_gist_timestamptz_order(timestamptz, smallint) RETURNS float8
AS 'MODULE_PATHNAME', 'pgwrh_gist_order64' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OPERATOR <# (LEFTARG=timestamptz, RIGHTARG=smallint, PROCEDURE=pgwrh_gist_timestamptz_order);
CREATE FUNCTION pgwrh_gist_timestamptz_order_distance(internal, timestamptz, smallint, oid, internal)
RETURNS float8 AS 'MODULE_PATHNAME', 'pgwrh_gist_order64_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- Date/time Datums have the same ordered integer representation, including infinities.
CREATE FUNCTION pgwrh_gist_timestamptz_order_consistent(internal, timestamptz, smallint, oid, internal)
RETURNS bool AS '$libdir/btree_gist', 'gbt_int8_consistent'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR CLASS pgwrh_gist_timestamptz_order_ops FOR TYPE timestamptz USING gist AS
    OPERATOR 1 <, OPERATOR 2 <=, OPERATOR 3 =,
    OPERATOR 4 >=, OPERATOR 5 >, OPERATOR 6 <>,
    OPERATOR 15 <# (timestamptz, smallint) FOR ORDER BY pg_catalog.float_ops,
    FUNCTION 1 pgwrh_gist_timestamptz_order_consistent(internal, timestamptz, smallint, oid, internal),
    FUNCTION 2 gbt_int8_union(internal, internal),
    FUNCTION 3 gbt_int8_compress(internal),
    FUNCTION 4 gbt_decompress(internal),
    FUNCTION 5 gbt_int8_penalty(internal, internal, internal),
    FUNCTION 6 gbt_int8_picksplit(internal, internal),
    FUNCTION 7 gbt_int8_same(gbtreekey16, gbtreekey16, internal),
    FUNCTION 8 pgwrh_gist_timestamptz_order_distance(internal, timestamptz, smallint, oid, internal),
    FUNCTION 9 gbt_int8_fetch(internal),
    STORAGE gbtreekey16;
