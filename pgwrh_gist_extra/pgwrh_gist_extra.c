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
}

#define GbtExtraAnyEqStrategyNumber RTContainsStrategyNumber
#define GbtExtraAllEqStrategyNumber (GbtExtraAnyEqStrategyNumber + 1)

static Datum find_any(Datum elem, ArrayType *array, Oid colation, PGFunction compare);
static Datum check_all(Datum elem, ArrayType *array, Oid colation, PGFunction compare);
static Datum any_consistent(PG_FUNCTION_ARGS);
static Datum all_consistent(PG_FUNCTION_ARGS);

typedef struct
{
    int32 vl_len_;    /* varlena header (do not touch directly!) */
    AttrNumber attno; /* index column number (for consistent function to lookup metadata) */
} GbteGistOptions;

Datum pgwrh_gist_text_any_eq_array(PG_FUNCTION_ARGS)
{
    Datum elem = PG_GETARG_DATUM(0);
    ArrayType *arrayval = DatumGetArrayTypeP(PG_GETARG_DATUM(1));

    return find_any(elem, arrayval, PG_GET_COLLATION(), texteq);
}

Datum pgwrh_gist_text_all_eq_array(PG_FUNCTION_ARGS)
{
    Datum elem = PG_GETARG_DATUM(0);
    ArrayType *arrayval = PG_GETARG_ARRAYTYPE_P(1);

    return check_all(elem, arrayval, PG_GET_COLLATION(), texteq);
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
                            0, 1, 32, /* TODO check max number of index attributes */
                            offsetof(GbteGistOptions, attno));

    PG_RETURN_VOID();
}

Datum find_any(Datum elem, ArrayType *array, Oid colation, PGFunction compare)
{
    ArrayIterator it = array_create_iterator(array, 0, NULL);
    Datum next_array_elem;
    bool is_null;
    bool found = false;

    while (!found && array_iterate(it, &next_array_elem, &is_null))
    {
        found = !is_null && DatumGetBool(DirectFunctionCall2Coll(compare, colation, next_array_elem, elem));
    }

    array_free_iterator(it);

    PG_RETURN_BOOL(found);
}
Datum check_all(Datum elem, ArrayType *array, Oid colation, PGFunction compare)
{
    ArrayIterator it = array_create_iterator(array, 0, NULL);
    Datum next_array_elem;
    bool is_null;
    bool found = true;

    while (found && array_iterate(it, &next_array_elem, &is_null))
    {
        found = !is_null && DatumGetBool(DirectFunctionCall2Coll(compare, colation, next_array_elem, elem));
    }

    array_free_iterator(it);

    PG_RETURN_BOOL(found);
}
struct part_bound_check_info
{
    PartitionKey part_key;
    PartitionDesc part_desc;

    Oid expected_part_oid;
};

struct cached_part_info
{
    List *hash_part_infos; /* List of (struct part_bound_check_info*) */
};
static struct cached_part_info *get_part_bounds_info(Relation index_rel, AttrNumber index_att_no)
{
    struct cached_part_info *result;
    AttrNumber key_att_no = index_rel->rd_index->indkey.values[index_att_no - 1];

    Assert(index_att_no > 0);

    result = palloc0(sizeof(struct cached_part_info));

    /* We do not handle expression indexes (for now) */
    if (key_att_no > 0)
    {
        /* Find table indexed by index_rel */
        Relation rel = RelationIdGetRelation(IndexGetRelation(index_rel->rd_id, false));
        Assert(rel);

        /* Walk up through partition hierarchy */
        while (rel->rd_rel->relispartition)
        {
            /* Find the actual parent and partition descriptor */
            Relation parent = RelationIdGetRelation(get_partition_parent(rel->rd_id, false));
            PartitionKey part_key;

            Assert(parent);

            part_key = RelationGetPartitionKey(parent);

            Assert(part_key);

            /* Append partition info for the right partition key */
            if (part_key->partnatts == 1 && part_key->partattrs[0] == key_att_no)
            {
                /* We only handle hash partitions for now */
                if (part_key->strategy == PARTITION_STRATEGY_HASH)
                {
                    struct part_bound_check_info *hash_part_info = palloc(sizeof(struct part_bound_check_info));
                    hash_part_info->part_key = part_key;
                    hash_part_info->part_desc = RelationGetPartitionDesc(parent, false);
                    hash_part_info->expected_part_oid = rel->rd_id;

                    result->hash_part_infos = lappend(result->hash_part_infos, hash_part_info);
                }
            }

            RelationClose(rel);
            rel = parent;
        }

        /* rel points to the root of partition hierarchy */
        RelationClose(rel);
    }

    return result;
}
static bool value_part_bounds_consistent(struct cached_part_info *part_bounds_info, Datum value)
{
    bool value_null = false;

    Assert(part_bounds_info);

    /* Check hash partitions */
    foreach_ptr(struct part_bound_check_info, part_info, part_bounds_info->hash_part_infos)
    {
        PartitionBoundInfo boundinfo = part_info->part_desc->boundinfo;
        uint64 hash;
        int idx;

        /* Only a single Datum is checked. We can only handle single attribute partition keys */
        Assert(part_info->part_key->partnatts == 1);
        hash = compute_partition_hash_value(
            1,
            part_info->part_key->partsupfunc,
            part_info->part_key->partcollation,
            &value, &value_null);
        idx = boundinfo->indexes[hash % boundinfo->nindexes];
        if (idx >= 0 && part_info->part_desc->oids[idx] != part_info->expected_part_oid)
        {
            /* The value is in another partition so we can safely exclude it */
            return false;
        }
    }

    /* Cannot exclude */
    return true;
}

struct filtered_array_cache
{
    ArrayType *original_array;
    ArrayType *filtered_array;
};

static ArrayType *get_cached_array_query(PG_FUNCTION_ARGS)
{
    GISTENTRY *entry = (GISTENTRY *)PG_GETARG_POINTER(0);
    ArrayType *array = DatumGetArrayTypeP(PG_GETARG_DATUM(1));
    struct filtered_array_cache *cache = (struct filtered_array_cache *)fcinfo->flinfo->fn_extra;
    if (!cache || cache->original_array != array)
    {
        cache = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt, sizeof(struct filtered_array_cache));
        cache->original_array = array;
        cache->filtered_array = array;
        fcinfo->flinfo->fn_extra = cache;
        if (PG_HAS_OPCLASS_OPTIONS() && ArrayGetNItems(ARR_NDIM(cache->original_array), ARR_DIMS(cache->original_array)) > 1)
        {
            GbteGistOptions *options = (GbteGistOptions *)PG_GET_OPCLASS_OPTIONS();
            struct cached_part_info *part_bounds_info = get_part_bounds_info(entry->rel, options->attno);
            ArrayBuildState *build_state = initArrayResult(array->elemtype, fcinfo->flinfo->fn_mcxt, true);
            ArrayIterator it = array_create_iterator(array, 0, NULL);
            Datum next_array_elem;
            bool is_null;

            while (array_iterate(it, &next_array_elem, &is_null))
            {
                if (!is_null && value_part_bounds_consistent(part_bounds_info, next_array_elem))
                {
                    build_state = accumArrayResult(build_state, next_array_elem, false, array->elemtype, fcinfo->flinfo->fn_mcxt);
                }
            }
            cache->filtered_array = DatumGetArrayTypeP(makeArrayResult(build_state, fcinfo->flinfo->fn_mcxt));

            // elog(NOTICE, "Elems original: %d", ArrayGetNItems( ARR_NDIM(cache->original_array), ARR_DIMS(cache->original_array)));
            // elog(NOTICE, "Elems filtered: %d", ArrayGetNItems( ARR_NDIM(cache->filtered_array), ARR_DIMS(cache->filtered_array)));
        }
    }

    return cache->filtered_array;
}

Datum any_consistent(PG_FUNCTION_ARGS)
{
    ArrayType *array = get_cached_array_query(fcinfo);
    ArrayIterator it = array_create_iterator(array, 0, NULL);
    bool *recheck = (bool *)PG_GETARG_POINTER(4);
    Datum next_array_elem;
    bool is_null;
    bool found = false;

    *recheck = false;
    while ((!found || *recheck) && array_iterate(it, &next_array_elem, &is_null))
    {
        found = (!is_null && DatumGetBool(DirectFunctionCall5Coll(
                                 btree_text_consistent,
                                 PG_GET_COLLATION(),
                                 PG_GETARG_DATUM(0),
                                 next_array_elem,
                                 BTEqualStrategyNumber,
                                 PG_GETARG_DATUM(3),
                                 PG_GETARG_DATUM(4)))) ||
                found;
    }

    array_free_iterator(it);

    PG_RETURN_BOOL(found);
}

Datum all_consistent(PG_FUNCTION_ARGS)
{
    ArrayIterator it = array_create_iterator(DatumGetArrayTypeP(PG_GETARG_DATUM(1)), 0, NULL);
    Datum next_array_elem;
    bool is_null;
    bool found = true;

    while (found && array_iterate(it, &next_array_elem, &is_null))
    {
        found = !is_null && DatumGetBool(DirectFunctionCall5Coll(
                                btree_text_consistent,
                                PG_GET_COLLATION(),
                                PG_GETARG_DATUM(0),
                                next_array_elem,
                                BTEqualStrategyNumber,
                                PG_GETARG_DATUM(3),
                                PG_GETARG_DATUM(4)));
    }

    array_free_iterator(it);

    PG_RETURN_BOOL(found);
}
