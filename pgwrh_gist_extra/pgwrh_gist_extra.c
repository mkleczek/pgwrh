/* SPDX-License-Identifier: GPL-3.0-only */
#include <postgres.h>
#include <utils/builtins.h>
#include <access/gist.h>
#include <access/stratnum.h>
#include <utils/array.h>
#include <utils/lsyscache.h>
#include <utils/varlena.h>
#include <utils/rel.h>
#include <utils/partcache.h>
#include <partitioning/partdesc.h>
#include <partitioning/partbounds.h>
#include <catalog/partition.h>
#include <catalog/index.h>
#include <common/hashfn.h>
#include <access/reloptions.h>
#include <utils/memutils.h>
#include <access/genam.h>
#include <access/table.h>
#include <catalog/pg_operator_d.h>

#include "ordered_scan.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(pgwrh_gist_text_any_eq_array);
PG_FUNCTION_INFO_V1(pgwrh_gist_text_all_eq_array);

PG_FUNCTION_INFO_V1(pgwrh_gist_text_consistent);
PG_FUNCTION_INFO_V1(pgwrh_gist_options);

static PGFunction btree_text_consistent;

void _PG_init(void);

void
_PG_init(void)
{
    /* Resolve the prerequisite module through PostgreSQL on every platform. */
    btree_text_consistent = load_external_function("$libdir/btree_gist",
                                                  "gbt_text_consistent", true, NULL);
    pgwrh_gist_ordered_scan_init();
}

#define GbtExtraAnyEqStrategyNumber RTContainsStrategyNumber
#define GbtExtraAllEqStrategyNumber (GbtExtraAnyEqStrategyNumber + 1)

static Datum array_equality(FunctionCallInfo fcinfo, bool require_all);
static Datum any_consistent(PG_FUNCTION_ARGS);
static Datum all_consistent(PG_FUNCTION_ARGS);

typedef struct
{
    int32 vl_len_;    /* varlena header (do not touch directly!) */
    int attno; /* index column number (for consistent function to lookup metadata) */
} GbteGistOptions;

Datum pgwrh_gist_text_any_eq_array(PG_FUNCTION_ARGS)
{
    return array_equality(fcinfo, false);
}

Datum pgwrh_gist_text_all_eq_array(PG_FUNCTION_ARGS)
{
    return array_equality(fcinfo, true);
}

Datum pgwrh_gist_text_consistent(PG_FUNCTION_ARGS)
{
    StrategyNumber strategy = (StrategyNumber)PG_GETARG_UINT16(2);
    switch (strategy)
    {
    case GbtExtraAnyEqStrategyNumber:
        return any_consistent(fcinfo);
    case GbtExtraAllEqStrategyNumber:
        return all_consistent(fcinfo);
    default:
        /*
         * Do not bother to use any DirectFunctionCall macros
         */
        return btree_text_consistent(fcinfo);
    }
}

Datum pgwrh_gist_options(PG_FUNCTION_ARGS)
{
    local_relopts *relopts = (local_relopts *)PG_GETARG_POINTER(0);

    init_local_reloptions(relopts, sizeof(GbteGistOptions));
    add_local_int_reloption(relopts, "attno",
                            "Index attribute number (starts from 1)",
                            0, 0, INDEX_MAX_KEYS,
                            offsetof(GbteGistOptions, attno));

    PG_RETURN_VOID();
}

/* Strict on the scalar and array; NULL elements follow SQL ANY/ALL logic. */
static Datum
array_equality(FunctionCallInfo fcinfo, bool require_all)
{
    Datum elem = PG_GETARG_DATUM(0);
    ArrayType *array = PG_GETARG_ARRAYTYPE_P(1);
    ArrayIterator it = array_create_iterator(array, 0, NULL);
    Datum next;
    bool is_null;
    bool saw_null = false;
    bool result = require_all;

    while (array_iterate(it, &next, &is_null))
    {
        if (is_null)
            saw_null = true;
        else if (DatumGetBool(DirectFunctionCall2Coll(texteq, PG_GET_COLLATION(),
                                                      elem, next)) != require_all)
        {
            result = !require_all;
            saw_null = false;
            break;
        }
    }
    array_free_iterator(it);
    PG_FREE_IF_COPY(array, 1);
    if (saw_null)
        PG_RETURN_NULL();
    PG_RETURN_BOOL(result);
}

struct part_bound_check_info
{
    FmgrInfo hash_function;
    Oid collation;
    int modulus;
    int remainder;
};

/*
 * attno is an explicit opt-in. Validate every use of this support function so
 * a misnumbered option cannot silently prune values for another index key.
 */
static List *
get_part_bounds_info(Relation index_rel, FunctionCallInfo fcinfo)
{
    GbteGistOptions *options = (GbteGistOptions *)PG_GET_OPCLASS_OPTIONS();
    bytea **attoptions = RelationGetIndexAttOptions(index_rel, false);
    AttrNumber key_att_no;
    Relation rel;
    char *key_name;
    List *result = NIL;
    int i;

    if (options->attno == 0)
        return NIL;
    for (i = 0; i < IndexRelationGetNumberOfKeyAttributes(index_rel); i++)
    {
        if (index_getprocid(index_rel, i + 1, GIST_CONSISTENT_PROC) == fcinfo->flinfo->fn_oid &&
            attoptions[i] != NULL)
        {
            GbteGistOptions *other = (GbteGistOptions *)attoptions[i];

            if (other->attno != 0 && other->attno != i + 1)
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                               errmsg("GiST attno option must match its index column position")));
        }
    }
    if (options->attno < 1 || options->attno > IndexRelationGetNumberOfKeyAttributes(index_rel))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("GiST attno option is outside the index key columns")));
    key_att_no = index_rel->rd_index->indkey.values[options->attno - 1];
    /* Expressions need an expression-aware mapping; conservatively skip them. */
    if (key_att_no <= 0)
        return NIL;

    rel = table_open(IndexGetRelation(RelationGetRelid(index_rel), false), AccessShareLock);
    key_name = pstrdup(NameStr(TupleDescAttr(RelationGetDescr(rel), key_att_no - 1)->attname));
    while (rel->rd_rel->relispartition)
    {
        Relation parent = table_open(get_partition_parent(RelationGetRelid(rel), false), AccessShareLock);
        PartitionKey key = RelationGetPartitionKey(parent);
        AttrNumber parent_attno = get_attnum(RelationGetRelid(parent), key_name);

        /* Attached tables may have a different physical attribute order. */
        if (key->strategy == PARTITION_STRATEGY_HASH && key->partnatts == 1 &&
            parent_attno > 0 && key->partattrs[0] == parent_attno &&
            key->parttypid[0] == TEXTOID && key->partcollation[0] == PG_GET_COLLATION() &&
            get_opfamily_member(key->partopfamily[0], TEXTOID, TEXTOID,
                                HTEqualStrategyNumber) == TextEqualOperator)
        {
            PartitionDesc desc = RelationGetPartitionDesc(parent, false);
            int partition_index = -1;

            for (i = 0; i < desc->nparts; i++)
                if (desc->oids[i] == RelationGetRelid(rel))
                    partition_index = i;
            if (partition_index >= 0)
            {
                struct part_bound_check_info *info = palloc(sizeof(*info));

                fmgr_info_copy(&info->hash_function, &key->partsupfunc[0], CurrentMemoryContext);
                info->collation = key->partcollation[0];
                /* Hash bounds are sorted in the same order as desc->oids. */
                info->modulus = DatumGetInt32(desc->boundinfo->datums[partition_index][0]);
                info->remainder = DatumGetInt32(desc->boundinfo->datums[partition_index][1]);
                result = lappend(result, info);
            }
        }
        table_close(rel, AccessShareLock);
        rel = parent;
    }
    table_close(rel, AccessShareLock);
    pfree(key_name);
    return result;
}

static bool
value_part_bounds_consistent(List *part_bounds_info, Datum value)
{
    bool is_null = false;

    foreach_ptr(struct part_bound_check_info, info, part_bounds_info)
    {
        uint64 hash = compute_partition_hash_value(1, &info->hash_function,
                                                   &info->collation, &value, &is_null);
        if (hash % info->modulus != info->remainder)
            return false;
    }
    return true;
}

struct filtered_array_cache
{
    MemoryContext values_context;
    ArrayType *original_array;
    ArrayType *filtered_array;
};

static ArrayType *get_cached_array_query(PG_FUNCTION_ARGS)
{
    GISTENTRY *entry = (GISTENTRY *)PG_GETARG_POINTER(0);
    ArrayType *array = DatumGetArrayTypeP(PG_GETARG_DATUM(1));
    struct filtered_array_cache *cache = (struct filtered_array_cache *)fcinfo->flinfo->fn_extra;
    if (cache == NULL)
    {
        cache = MemoryContextAllocZero(fcinfo->flinfo->fn_mcxt, sizeof(*cache));
        cache->values_context = AllocSetContextCreate(fcinfo->flinfo->fn_mcxt,
                                                      "pgwrh GiST array values",
                                                      ALLOCSET_SMALL_SIZES);
        fcinfo->flinfo->fn_extra = cache;
    }

    /*
     * GiST preserves fn_extra across rescans, but scan-key arrays belong to
     * the executor. Their addresses can be reused for different parameters.
     * Compare an owned flat copy and bound retained memory to one query value.
     */
    if (cache->original_array == NULL ||
        VARSIZE(cache->original_array) != VARSIZE(array) ||
        memcmp(cache->original_array, array, VARSIZE(array)) != 0)
    {
        MemoryContext old_context;

        MemoryContextReset(cache->values_context);
        cache->original_array = NULL;
        cache->filtered_array = NULL;
        old_context = MemoryContextSwitchTo(cache->values_context);
        cache->original_array = DatumGetArrayTypePCopy(PointerGetDatum(array));
        cache->filtered_array = cache->original_array;
        if (PG_HAS_OPCLASS_OPTIONS() && ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array)) > 1)
        {
            List *part_bounds_info = get_part_bounds_info(entry->rel, fcinfo);
            ArrayBuildState *build_state = initArrayResult(array->elemtype, CurrentMemoryContext, false);
            ArrayIterator it = array_create_iterator(array, 0, NULL);
            Datum next_array_elem;
            bool is_null;

            while (array_iterate(it, &next_array_elem, &is_null))
            {
                if (!is_null && value_part_bounds_consistent(part_bounds_info, next_array_elem))
                    build_state = accumArrayResult(build_state, next_array_elem, false,
                                                   array->elemtype, CurrentMemoryContext);
            }
            array_free_iterator(it);
            cache->filtered_array = DatumGetArrayTypeP(makeArrayResult(build_state, CurrentMemoryContext));
        }
        MemoryContextSwitchTo(old_context);
    }

    return cache->filtered_array;
}

Datum any_consistent(PG_FUNCTION_ARGS)
{
    ArrayType *array = get_cached_array_query(fcinfo);
    ArrayIterator it = array_create_iterator(array, 0, NULL);
    bool *recheck = (bool *)PG_GETARG_POINTER(4);
    Datum next;
    bool is_null;
    bool found = false;

    *recheck = false;
    while (array_iterate(it, &next, &is_null))
    {
        bool scalar_recheck = false;

        if (!is_null && DatumGetBool(DirectFunctionCall5Coll(
                btree_text_consistent, PG_GET_COLLATION(), PG_GETARG_DATUM(0),
                next, UInt16GetDatum(BTEqualStrategyNumber), PG_GETARG_DATUM(3),
                PointerGetDatum(&scalar_recheck))))
        {
            found = true;
            *recheck = scalar_recheck;
            /* One exact match proves the disjunction, even after lossy ones. */
            if (!scalar_recheck)
                break;
        }
    }
    array_free_iterator(it);
    PG_RETURN_BOOL(found);
}

Datum all_consistent(PG_FUNCTION_ARGS)
{
    ArrayIterator it = array_create_iterator(PG_GETARG_ARRAYTYPE_P(1), 0, NULL);
    bool *recheck = (bool *)PG_GETARG_POINTER(4);
    Datum next;
    bool is_null;
    bool found = true;

    *recheck = false;
    while (found && array_iterate(it, &next, &is_null))
    {
        bool scalar_recheck = false;

        found = !is_null && DatumGetBool(DirectFunctionCall5Coll(
                btree_text_consistent, PG_GET_COLLATION(), PG_GETARG_DATUM(0),
                next, UInt16GetDatum(BTEqualStrategyNumber), PG_GETARG_DATUM(3),
                PointerGetDatum(&scalar_recheck)));
        *recheck |= scalar_recheck;
    }
    array_free_iterator(it);
    PG_RETURN_BOOL(found);
}
