/* SPDX-License-Identifier: GPL-3.0-only */
#include "postgres.h"

#include "access/gist.h"
#include "fmgr.h"

/*
 * These opclasses reuse btree_gist's fixed-size [lower, upper] storage on
 * PostgreSQL 18/19. Read with memcpy so no additional alignment is required.
 *
 * A signed 64-bit value is ordered exactly by (signed high32, unsigned low32).
 * Both components fit in float8 without rounding. GiST compares its ordering
 * terms lexicographically, so it never needs a rounded 64-bit distance.
 *
 * Selectors +1/-1 request ascending/descending high (or 32-bit scalar) order;
 * +2/-2 request ascending/descending low order. Low-word bounds must also be
 * valid when that operator is used alone: if a range crosses a high-word
 * boundary, its conservative low-word bounds are [0, UINT32_MAX].
 */
static int
order_selector(FunctionCallInfo fcinfo)
{
    int selector = PG_GETARG_INT16(1);

    if (selector != 1 && selector != -1 && selector != 2 && selector != -2)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("GiST order selector must be 1, -1, 2 or -2")));
    return selector;
}

static double
rank64(int64 value, int selector)
{
    double rank = (selector == 1 || selector == -1)
        ? (double) (int32) ((uint64) value >> 32)
        : (double) (uint32) value;

    return selector < 0 ? -rank : rank;
}

PG_FUNCTION_INFO_V1(pgwrh_gist_order64);
Datum
pgwrh_gist_order64(PG_FUNCTION_ARGS)
{
    PG_RETURN_FLOAT8(rank64(PG_GETARG_INT64(0), order_selector(fcinfo)));
}

PG_FUNCTION_INFO_V1(pgwrh_gist_order64_distance);
Datum
pgwrh_gist_order64_distance(PG_FUNCTION_ARGS)
{
    GISTENTRY *entry = (GISTENTRY *) PG_GETARG_POINTER(0);
    int selector = order_selector(fcinfo);
    int64 lower, upper;

    memcpy(&lower, DatumGetPointer(entry->key), sizeof(lower));
    memcpy(&upper, (char *) DatumGetPointer(entry->key) + sizeof(lower), sizeof(upper));
    *(bool *) PG_GETARG_POINTER(4) = false;
    if ((selector == 2 || selector == -2) &&
        ((uint64) lower >> 32) != ((uint64) upper >> 32))
        PG_RETURN_FLOAT8(selector < 0 ? -(double) PG_UINT32_MAX : 0.0);
    PG_RETURN_FLOAT8(rank64(selector < 0 ? upper : lower, selector));
}

#define DEFINE_ORDER32(name, ctype, getdatum) \
    PG_FUNCTION_INFO_V1(pgwrh_gist_order_##name); \
    Datum pgwrh_gist_order_##name(PG_FUNCTION_ARGS) \
    { \
        int selector = order_selector(fcinfo); \
        double value = (double) getdatum(PG_GETARG_DATUM(0)); \
        if (selector != 1 && selector != -1) \
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), \
                           errmsg("GiST order selector for this type must be 1 or -1"))); \
        PG_RETURN_FLOAT8(selector < 0 ? -value : value); \
    } \
    PG_FUNCTION_INFO_V1(pgwrh_gist_order_##name##_distance); \
    Datum pgwrh_gist_order_##name##_distance(PG_FUNCTION_ARGS) \
    { \
        GISTENTRY *entry = (GISTENTRY *) PG_GETARG_POINTER(0); \
        int selector = order_selector(fcinfo); \
        ctype value; \
        if (selector != 1 && selector != -1) \
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), \
                           errmsg("GiST order selector for this type must be 1 or -1"))); \
        memcpy(&value, (char *) DatumGetPointer(entry->key) + \
               (selector < 0 ? sizeof(ctype) : 0), sizeof(ctype)); \
        *(bool *) PG_GETARG_POINTER(4) = false; \
        PG_RETURN_FLOAT8(selector < 0 ? -(double) value : (double) value); \
    }

DEFINE_ORDER32(int2, int16, DatumGetInt16)
DEFINE_ORDER32(int4, int32, DatumGetInt32)
