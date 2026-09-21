/* SPDX-License-Identifier: GPL-3.0-only */
#include "postgres.h"

#include "access/stratnum.h"
#include "catalog/dependency.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_operator_d.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_type_d.h"
#include "commands/extension.h"
#include "executor/executor.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "optimizer/optimizer.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

#include "ordered_scan.h"

static bool enable_ordered_scan = true;
static set_rel_pathlist_hook_type previous_pathlist_hook;

static Plan *plan_ordered_scan(PlannerInfo *, RelOptInfo *, CustomPath *, List *, List *, List *);
static Node *create_ordered_scan(CustomScan *);
static void begin_ordered_scan(CustomScanState *, EState *, int);
static TupleTableSlot *exec_ordered_scan(CustomScanState *);
static void end_ordered_scan(CustomScanState *);
static void rescan_ordered_scan(CustomScanState *);

static const CustomPathMethods ordered_path_methods = {
    .CustomName = "pgwrh GiST ordered scan",
    .PlanCustomPath = plan_ordered_scan
};
static const CustomScanMethods ordered_scan_methods = {
    .CustomName = "pgwrh GiST ordered scan",
    .CreateCustomScanState = create_ordered_scan
};
static const CustomExecMethods ordered_exec_methods = {
    .CustomName = "pgwrh GiST ordered scan",
    .BeginCustomScan = begin_ordered_scan,
    .ExecCustomScan = exec_ordered_scan,
    .EndCustomScan = end_ordered_scan,
    .ReScanCustomScan = rescan_ordered_scan
};

static int
ordering_parts(Oid type)
{
    switch (type)
    {
        case INT2OID:
        case INT4OID:
        case DATEOID:
            return 1;
        case INT8OID:
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
            return 2;
        default:
            return 0;
    }
}

/* Match plain columns and native btree semantics, including partition members. */
static Var *
match_ordering_column(PathKey *key, RelOptInfo *rel, IndexOptInfo *index,
                      Oid extension, Oid *operator)
{
    EquivalenceMemberIterator iterator;
    EquivalenceMember *member;

    if (key->pk_eclass->ec_has_volatile ||
        (key->pk_cmptype != COMPARE_LT && key->pk_cmptype != COMPARE_GT))
        return NULL;

    setup_eclass_member_iterator(&iterator, key->pk_eclass, rel->relids);
    while ((member = eclass_member_iterator_next(&iterator)) != NULL)
    {
        Var *var;
        TypeCacheEntry *type;
        int i;

        if (!IsA(member->em_expr, Var) || !bms_equal(member->em_relids, rel->relids))
            continue;
        var = (Var *)member->em_expr;
        if (var->varno != rel->relid || var->varlevelsup != 0 || var->varattno <= 0 ||
            var->varnullingrels != NULL || ordering_parts(var->vartype) == 0)
            continue;
        if (key->pk_nulls_first && !bms_is_member(var->varattno, rel->notnullattnums))
            continue;
        type = lookup_type_cache(var->vartype, TYPECACHE_BTREE_OPFAMILY);
        if (key->pk_opfamily != type->btree_opf)
            continue;

        for (i = 0; i < index->nkeycolumns; i++)
        {
            if (index->indexkeys[i] != var->varattno ||
                index->opcintype[i] != var->vartype ||
                getExtensionOfObject(OperatorFamilyRelationId, index->opfamily[i]) != extension)
                continue;
            *operator = get_opfamily_member(index->opfamily[i], var->vartype,
                                            INT2OID, RTKNNSearchStrategyNumber);
            if (OidIsValid(*operator))
                return var;
        }
    }
    return NULL;
}

/*
 * Ask the core index planner to match the real KNN expressions. It retains all
 * normal index restrictions, partial-index proofs, security checks, costing,
 * and parameterized paths. Only collect the resulting ordered IndexPaths;
 * neither the SQL query nor its native pathkeys or pruning quals are changed.
 */
static void
add_ordered_paths(PlannerInfo *root, RelOptInfo *rel, IndexOptInfo *index,
                  List *distance_keys, List *native_keys)
{
    List *saved_query_keys = root->query_pathkeys;
    List *saved_indexes = rel->indexlist;
    List *saved_paths = rel->pathlist;
    List *saved_partial_paths = rel->partial_pathlist;
    List *candidates;
    ListCell *cell;

    root->query_pathkeys = distance_keys;
    rel->indexlist = list_make1(index);
    rel->pathlist = NIL;
    rel->partial_pathlist = NIL;
    create_index_paths(root, rel);
    candidates = rel->pathlist;
    root->query_pathkeys = saved_query_keys;
    rel->indexlist = saved_indexes;
    rel->pathlist = saved_paths;
    rel->partial_pathlist = saved_partial_paths;

    foreach(cell, candidates)
    {
        Path *child = lfirst(cell);
        CustomPath *path;

        if (!IsA(child, IndexPath) || !pathkeys_contained_in(distance_keys, child->pathkeys))
            continue;
        path = makeNode(CustomPath);
        path->path = *child;
        NodeSetTag(path, T_CustomPath);
        path->path.pathtype = T_CustomScan;
        path->path.pathkeys = native_keys;
        path->path.parallel_aware = false;
        path->path.parallel_safe = false;
        path->path.parallel_workers = 0;
        path->path.total_cost += cpu_operator_cost * child->rows;
        path->flags = CUSTOMPATH_SUPPORT_PROJECTION;
        path->custom_paths = list_make1(child);
        path->methods = &ordered_path_methods;
        add_path(rel, &path->path);
    }
}

static void
ordered_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte)
{
    Oid extension;
    ListCell *icell;

    if (previous_pathlist_hook)
        previous_pathlist_hook(root, rel, rti, rte);
    if (!enable_ordered_scan || !enable_indexscan || root->query_pathkeys == NIL ||
        root->parse->commandType != CMD_SELECT || root->parse->rowMarks != NIL ||
        rte->rtekind != RTE_RELATION || rte->tablesample != NULL || IS_DUMMY_REL(rel))
        return;
    extension = get_extension_oid("pgwrh_gist_extra", true);
    if (!OidIsValid(extension))
        return;

    foreach(icell, rel->indexlist)
    {
        IndexOptInfo *index = lfirst_node(IndexOptInfo, icell);
        List *distance_keys = NIL;
        List *native_keys = NIL;
        ListCell *kcell;

        if (index->relam != GIST_AM_OID || !index->amcanorderbyop)
            continue;
        foreach(kcell, root->query_pathkeys)
        {
            PathKey *key = lfirst_node(PathKey, kcell);
            Oid operator = InvalidOid;
            Var *var = match_ordering_column(key, rel, index, extension, &operator);
            int part;

            if (var == NULL)
                break;
            for (part = 1; part <= ordering_parts(var->vartype); part++)
            {
                int selector = key->pk_cmptype == COMPARE_GT ? -part : part;
                Const *argument = makeConst(INT2OID, -1, InvalidOid, sizeof(int16),
                                             Int16GetDatum(selector), false, true);
                Expr *expr = make_opclause(operator, FLOAT8OID, false,
                                           (Expr *)copyObject(var), (Expr *)argument,
                                           InvalidOid, InvalidOid);
                distance_keys = list_concat(distance_keys,
                    build_expression_pathkey(root, expr, Float8LessOperator, rel->relids, true));
            }
            native_keys = lappend(native_keys, key);
        }
        if (native_keys != NIL)
            add_ordered_paths(root, rel, index, distance_keys, native_keys);
    }
}

void
pgwrh_gist_ordered_scan_init(void)
{
    DefineCustomBoolVariable("pgwrh_gist_extra.enable_ordered_scan",
                             "Offer exact native column ordering through GiST.", NULL,
                             &enable_ordered_scan, true, PGC_USERSET, 0, NULL, NULL, NULL);
    RegisterCustomScanMethods(&ordered_scan_methods);
    previous_pathlist_hook = set_rel_pathlist_hook;
    set_rel_pathlist_hook = ordered_pathlist;
}

static Plan *
plan_ordered_scan(PlannerInfo *root, RelOptInfo *rel, CustomPath *best_path,
                   List *tlist, List *clauses, List *custom_plans)
{
    CustomScan *scan = makeNode(CustomScan);
    Plan *child = (Plan *)linitial(custom_plans);

    scan->scan.plan.targetlist = tlist;
    /* All filtering, visibility checks and lossy rechecks belong to the child. */
    scan->scan.plan.qual = NIL;
    scan->scan.scanrelid = 0;
    scan->flags = best_path->flags;
    scan->custom_plans = custom_plans;
    scan->custom_scan_tlist = copyObject(child->targetlist);
    scan->custom_relids = bms_copy(rel->relids);
    scan->methods = &ordered_scan_methods;
    return &scan->scan.plan;
}

static Node *
create_ordered_scan(CustomScan *scan)
{
    CustomScanState *state = makeNode(CustomScanState);

    state->methods = &ordered_exec_methods;
    return (Node *)state;
}

static void
begin_ordered_scan(CustomScanState *state, EState *estate, int eflags)
{
    CustomScan *scan = (CustomScan *)state->ss.ps.plan;

    state->custom_ps = list_make1(ExecInitNode(linitial(scan->custom_plans), estate, eflags));
}

static TupleTableSlot *
next_ordered_tuple(ScanState *state)
{
    TupleTableSlot *child = ExecProcNode(linitial(((CustomScanState *)state)->custom_ps));

    /* The child can use heap, buffer-heap, or virtual slots. Projection above
     * this node is compiled for our virtual scan slot, so deform/copy first. */
    if (TupIsNull(child))
        return ExecClearTuple(state->ss_ScanTupleSlot);
    return ExecCopySlot(state->ss_ScanTupleSlot, child);
}

static bool
recheck_ordered_tuple(ScanState *state, TupleTableSlot *slot)
{
    /* Row-locking queries are excluded; the child owns all scan qualifications. */
    return true;
}

static TupleTableSlot *
exec_ordered_scan(CustomScanState *state)
{
    return ExecScan(&state->ss, next_ordered_tuple, recheck_ordered_tuple);
}

static void
end_ordered_scan(CustomScanState *state)
{
    ExecEndNode(linitial(state->custom_ps));
    ExecClearTuple(state->ss.ss_ScanTupleSlot);
    ExecClearTuple(state->ss.ps.ps_ResultTupleSlot);
}

static void
rescan_ordered_scan(CustomScanState *state)
{
    PlanState *child = linitial(state->custom_ps);

    ExecScanReScan(&state->ss);
    if (child->chgParam == NULL)
        ExecReScan(child);
}
