/* SPDX-License-Identifier: AGPL-3.0-only */
/* A local, occurrence-preserving lookup relation driving remote joins. */
#include "postgres.h"

#include "access/heaptoast.h"
#include "access/table.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type_d.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/explain_format.h"
#include "optimizer/cost.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/appendinfo.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "partitioning/partbounds.h"
#include "partitioning/partdesc.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/partcache.h"
#include "utils/rel.h"
#include "utils/tuplestore.h"
#include "utils/typcache.h"

#include "postgres_fdw.h"
#include "lookup_join.h"

static bool enable_lookup_join = true;
static int lookup_max_rows = 10000;
static int lookup_max_memory = 8192;
static set_join_pathlist_hook_type previous_hook;
static GetForeignJoinPaths_function foreign_callback;

/* All finished-plan private data consists of copyable nodes. */
enum { LKind, LParent, LKey, LShardCols, LShipCols, LShipTypes, LDestinations };
/* Destination: ordinary child index, remote child index (-1 for local), OID. */
enum { DOrdinary, DRemote, DOid };

typedef struct LookupPath
{
    CustomPath path;
    RelOptInfo *shard;
    RelOptInfo *lookup;
    List *shardvars;
    List *lookupvars;
    List *shipvars;
    List *routeoids;
    Var *lookupkey;
    Oid parentoid;
    JoinType kind;
} LookupPath;

typedef struct LookupState
{
    CustomScanState css;
    int eflags;
    int nshard;
    int nparams;
    bool semi;
    bool ready;
    bool overflow;
    bool row_active;
    int destination;
    bool destination_started;
    int ndest;
    uint64 nrows;
    uint64 bytes;
    uint64 executions;
    uint64 skipped;
    uint64 remote_rows;
    int capacity;
    HeapTuple *rows;
    int *row_dest;
    uint64 *dest_rows;
    const char ***payloads;
    Tuplestorestate *store;
    TupleTableSlot *lookupslot;
    TupleTableSlot *spoolslot;
    TupleTableSlot *shardslot;
    ExprState *joinqual;
    MemoryContext fastcxt;
    Relation parent;
    PartitionDesc partdesc;
    PartitionKey partkey;
} LookupState;

static Plan *plan_lookup(PlannerInfo *, RelOptInfo *, CustomPath *, List *, List *, List *);
static Node *create_lookup(CustomScan *);
static void begin_lookup(CustomScanState *, EState *, int);
static TupleTableSlot *exec_lookup(CustomScanState *);
static void end_lookup(CustomScanState *);
static void rescan_lookup(CustomScanState *);
static void explain_lookup(CustomScanState *, List *, ExplainState *);

static const CustomPathMethods path_methods = {
    .CustomName = "Pgwrh Remote Lookup Join", .PlanCustomPath = plan_lookup
};
static const CustomScanMethods scan_methods = {
    .CustomName = "Pgwrh Remote Lookup Join", .CreateCustomScanState = create_lookup
};
static const CustomExecMethods exec_methods = {
    .CustomName = "Pgwrh Remote Lookup Join", .BeginCustomScan = begin_lookup,
    .ExecCustomScan = exec_lookup, .EndCustomScan = end_lookup,
    .ReScanCustomScan = rescan_lookup, .ExplainCustomScan = explain_lookup
};

static bool
integer_type(Oid type)
{
    return type == INT2OID || type == INT4OID || type == INT8OID;
}

/* Output-only values never need to pass this test. No OID-bearing user types. */
static bool
payload_type(Oid type)
{
    switch (type)
    {
        case BOOLOID: case INT2OID: case INT4OID: case INT8OID:
        case FLOAT4OID: case FLOAT8OID: case NUMERICOID:
        case TEXTOID: case VARCHAROID: case BPCHAROID: case BYTEAOID:
        case DATEOID: case TIMEOID: case TIMETZOID: case TIMESTAMPOID:
        case TIMESTAMPTZOID: case INTERVALOID: case UUIDOID:
        case JSONOID: case JSONBOID: case BITOID: case VARBITOID:
            return true;
        default:
            return false;
    }
}

static Path *
plain_path(RelOptInfo *rel)
{
    Path *best = NULL;
    ListCell *lc;

    foreach(lc, rel->pathlist)
    {
        Path *path = lfirst(lc);
        if (path->param_info || path->parallel_aware || path->parallel_workers ||
            IsA(path, GatherPath) || IsA(path, GatherMergePath))
            continue;
        if (!best || path->total_cost < best->total_cost)
            best = path;
    }
    return best;
}

static bool
our_foreign(RelOptInfo *rel)
{
    return rel->fdwroutine &&
        rel->fdwroutine->GetForeignJoinPaths == foreign_callback && rel->fdw_private;
}

/* Collect only ordinary scalar Vars; placeholders/whole rows require more work. */
static bool
collect_vars(Node *expr, Index shardid, Index lookupid, List **svars, List **lvars)
{
    List *vars = pull_var_clause(expr, PVC_RECURSE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS);
    ListCell *lc;

    foreach(lc, vars)
    {
        Var *var = lfirst(lc);
        if (!IsA(var, Var) || var->varlevelsup || var->varattno <= 0 ||
            !bms_is_empty(var->varnullingrels))
            return false;
        if (var->varno == shardid)
            *svars = list_append_unique(*svars, var);
        else if (var->varno == lookupid)
            *lvars = list_append_unique(*lvars, var);
        else
            return false;
    }
    return true;
}

static List *
child_exprs(PlannerInfo *root, RelOptInfo *parent, RelOptInfo *child, List *exprs)
{
    if (child == parent)
        return copyObject(exprs);
    return (List *) adjust_appendrel_attrs_multilevel(root, (Node *) exprs,
                                                      child, parent);
}

static List *
vars_tlist(List *vars)
{
    List *result = NIL;
    ListCell *lc;
    foreach(lc, vars)
        result = lappend(result, makeTargetEntry(copyObject(lfirst(lc)),
                         list_length(result) + 1, NULL, false));
    return result;
}

static int
var_position(List *vars, Var *var)
{
    ListCell *lc;
    int pos = 0;
    foreach(lc, vars)
    {
        if (equal(lfirst(lc), var))
            return pos;
        pos++;
    }
    elog(ERROR, "lookup column is missing from materialization");
    return -1;
}

static void
consider_lookup(PlannerInfo *root, RelOptInfo *joinrel, RelOptInfo *shard,
                RelOptInfo *lookup, JoinType kind, JoinPathExtraData *extra)
{
    RangeTblEntry *lrte;
    RangeTblEntry *srte;
    List *leaves = NIL, *routeoids = NIL;
    List *svars = NIL, *lvars = NIL, *shipvars = NIL, *unused = NIL;
    List *quals = extract_actual_clauses(extra->restrictlist, false);
    List *paths = NIL;
    ListCell *lc;
    Path *lp;
    Var *key = NULL;
    AttrNumber partatt = 0;
    Oid parentoid = InvalidOid;
    int remotes = 0, disabled_nodes = 0;
    double cost = 0, transfer = 0;
    LookupPath *result;

    if (!IS_SIMPLE_REL(shard) || !IS_SIMPLE_REL(lookup) ||
        shard->reloptkind != RELOPT_BASEREL || lookup->reloptkind != RELOPT_BASEREL ||
        shard->lateral_relids || lookup->lateral_relids ||
        lookup->part_scheme ||
        lookup->fdwroutine || lookup->rows > lookup_max_rows)
        return;
    lrte = planner_rt_fetch(lookup->relid, root);
    srte = planner_rt_fetch(shard->relid, root);
    if (lrte->rtekind != RTE_RELATION || lrte->relkind != RELKIND_RELATION ||
        srte->rtekind != RTE_RELATION ||
        lrte->securityQuals || srte->securityQuals || lrte->tablesample || srte->tablesample ||
        contain_volatile_functions((Node *) lookup->baserestrictinfo) ||
        contain_volatile_functions((Node *) shard->baserestrictinfo) ||
        contain_volatile_functions((Node *) quals))
        return;
    if (srte->relkind == RELKIND_PARTITIONED_TABLE)
    {
        Relation rel = table_open(srte->relid, NoLock);
        PartitionKey pk = RelationGetPartitionKey(rel);
        bool supported = pk && pk->partnatts == 1 && pk->partattrs[0] > 0 &&
            integer_type(pk->parttypid[0]) &&
            pk->partopfamily[0] < FirstGenbkiObjectId;
        if (supported)
            partatt = pk->partattrs[0];
        table_close(rel, NoLock);
        if (!supported || !shard->part_scheme || !shard->part_rels)
            return;
        parentoid = srte->relid;
        for (int i = 0; i < shard->nparts; i++)
        {
            RelOptInfo *leaf = shard->part_rels[i];
            Oid routeoid;
            if (!leaf || IS_DUMMY_REL(leaf))
                continue;
            routeoid = planner_rt_fetch(leaf->relid, root)->relid;
            /* pgwrh wraps a leaf in one partitioned slot using the same key.
             * The root routes to the slot; one leaf can be scanned directly.
             * More general nested trees require another pruning analysis. */
            if (leaf->part_scheme)
            {
                Relation slotrel = table_open(routeoid, NoLock);
                PartitionKey slotkey = RelationGetPartitionKey(slotrel);
                bool same = slotkey && slotkey->partnatts == 1 &&
                    slotkey->partattrs[0] == partatt &&
                    slotkey->parttypid[0] == shard->part_scheme->partopcintype[0];
                table_close(slotrel, NoLock);
                if (!same || leaf->nparts != 1 || !leaf->part_rels || !leaf->part_rels[0])
                    return;
                leaf = leaf->part_rels[0];
                if (IS_DUMMY_REL(leaf))
                    continue;
            }
            /* No additional nested shapes or UNION ALL inputs. */
            if (leaf->part_scheme || (planner_rt_fetch(leaf->relid, root)->relkind != RELKIND_RELATION &&
                                     planner_rt_fetch(leaf->relid, root)->relkind != RELKIND_FOREIGN_TABLE))
                return;
            leaves = lappend(leaves, leaf);
            routeoids = lappend_oid(routeoids, routeoid);
        }
    }
    else if (srte->relkind == RELKIND_FOREIGN_TABLE && our_foreign(shard))
    {
        leaves = list_make1(shard);
        routeoids = list_make1_oid(srte->relid);
    }
    else
        return;

    foreach(lc, quals)
    {
        OpExpr *op = lfirst(lc);
        Var *a, *b;
        if (!IsA(op, OpExpr) || list_length(op->args) != 2)
            continue;
        a = linitial(op->args); b = lsecond(op->args);
        if (!IsA(a, Var) || !IsA(b, Var))
            continue;
        if (a->varno == lookup->relid)
        { Var *swap = a; a = b; b = swap; }
        if (a->varno == shard->relid && b->varno == lookup->relid &&
            a->vartype == b->vartype && integer_type(a->vartype) &&
            (!partatt || a->varattno == partatt) &&
            op->opno == lookup_type_cache(a->vartype, TYPECACHE_EQ_OPR)->eq_opr &&
            op_strict(op->opno))
            key = b;
    }
    if (!key || !collect_vars((Node *) quals, shard->relid, lookup->relid, &svars, &shipvars) ||
        !collect_vars((Node *) joinrel->reltarget->exprs, shard->relid, lookup->relid,
                      &svars, &lvars))
        return;
    lvars = list_union(lvars, shipvars);
    /* A true SEMI path has no lookup-side output. */
    if (kind == JOIN_SEMI &&
        !collect_vars((Node *) joinrel->reltarget->exprs, shard->relid, 0, &unused, &unused))
        return;
    foreach(lc, shipvars)
        if (!payload_type(((Var *) lfirst(lc))->vartype))
            return;
    lp = plain_path(lookup);
    if (!lp || (lp->pathtype != T_SeqScan && lp->pathtype != T_IndexScan &&
                lp->pathtype != T_IndexOnlyScan && lp->pathtype != T_BitmapHeapScan &&
                lp->pathtype != T_TidScan && lp->pathtype != T_TidRangeScan) || lookup->rows * (lookup->reltarget->width * 2.0 + 128) >
               lookup_max_memory * 1024.0)
        return;
    paths = list_make1(create_projection_path(root, lookup, lp,
                               create_pathtarget(root, vars_tlist(lvars))));
    foreach(lc, leaves)
    {
        RelOptInfo *leaf = lfirst(lc);
        Path *path = plain_path(leaf);
        List *leafquals = child_exprs(root, shard, leaf, quals);
        ListCell *qc;
        RangeTblEntry *rte = planner_rt_fetch(leaf->relid, root);
        if (!path || rte->securityQuals || rte->tablesample)
            return;
        disabled_nodes += path->disabled_nodes;
        if (rte->relkind == RELKIND_FOREIGN_TABLE)
        {
            PgFdwRelationInfo *fpinfo;
            if (!our_foreign(leaf))
                return;
            fpinfo = leaf->fdw_private;
            if (fpinfo->local_conds ||
                (OidIsValid(leaf->userid) ? leaf->userid : GetUserId()) !=
                (OidIsValid(lookup->userid) ? lookup->userid : GetUserId()))
                return;
            foreach(qc, fpinfo->table->options)
            {
                DefElem *option = lfirst(qc);
                if (strcmp(option->defname, "lookup_join") == 0 && !defGetBoolean(option))
                    return;
            }
            foreach(qc, leafquals)
                if (!is_foreign_expr(root, leaf, lfirst(qc)))
                    return;
            remotes++;
            /* Retain remote scan work; replace transfer with joined rows. */
            cost += Max(fpinfo->fdw_startup_cost,
                        path->total_cost - leaf->rows * fpinfo->fdw_tuple_cost);
            transfer += fpinfo->fdw_tuple_cost;
        }
        else
            cost += path->total_cost + leaf->rows * lookup->rows * cpu_operator_cost;
        paths = lappend(paths, create_projection_path(root, leaf, path,
                       create_pathtarget(root, vars_tlist(child_exprs(root, shard, leaf, svars)))));
    }
    if (!remotes)
        return;
    result = palloc0(sizeof(LookupPath));
    NodeSetTag(&result->path, T_CustomPath);
    result->path.path.pathtype = T_CustomScan;
    result->path.path.parent = joinrel;
    result->path.path.pathtarget = joinrel->reltarget;
    result->path.path.rows = joinrel->rows;
    result->path.path.startup_cost = lp->total_cost + lookup->rows * cpu_tuple_cost;
    result->path.path.total_cost = result->path.path.startup_cost + cost +
        joinrel->rows * (transfer / remotes + cpu_tuple_cost) +
        lookup->rows * (list_length(shipvars) + remotes) * cpu_operator_cost;
    result->path.path.disabled_nodes = lp->disabled_nodes + disabled_nodes;
    result->path.flags = CUSTOMPATH_SUPPORT_PROJECTION;
    result->path.custom_paths = paths;
    result->path.custom_restrictinfo = extra->restrictlist;
    result->path.methods = &path_methods;
    result->shard = shard; result->lookup = lookup;
    result->shardvars = svars; result->lookupvars = lvars; result->shipvars = shipvars;
    result->lookupkey = key; result->parentoid = parentoid; result->kind = kind;
    result->routeoids = routeoids;
    add_path(joinrel, &result->path.path);
}

static void
lookup_paths(PlannerInfo *root, RelOptInfo *joinrel, RelOptInfo *outer,
             RelOptInfo *inner, JoinType kind, JoinPathExtraData *extra)
{
    if (previous_hook)
        previous_hook(root, joinrel, outer, inner, kind, extra);
    if (!foreign_callback || !enable_lookup_join || root->parse->commandType != CMD_SELECT ||
        root->parse->rowMarks || root->parse->hasModifyingCTE ||
        root->parse->hasRowSecurity || root->hasLateralRTEs || root->query_level > 1 ||
        contain_volatile_functions((Node *) root->parse->targetList))
        return;
    if (kind == JOIN_INNER || kind == JOIN_SEMI)
        consider_lookup(root, joinrel, outer, inner, kind, extra);
    if (kind == JOIN_INNER)
        consider_lookup(root, joinrel, inner, outer, kind, extra);
}

void
pgwrh_fdw_lookup_init(GetForeignJoinPaths_function callback)
{
    static bool initialized = false;

    if (callback)
        foreign_callback = callback;
    if (initialized)
        return;
    initialized = true;
    DefineCustomBoolVariable("pgwrh_fdw.enable_lookup_join",
        "Offer remote joins driven by a materialized local lookup.", NULL,
        &enable_lookup_join, true, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pgwrh_fdw.lookup_join_max_rows",
        "Maximum rows in the optimized lookup input.", NULL,
        &lookup_max_rows, 10000, 1, 1000000, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pgwrh_fdw.lookup_join_max_memory",
        "Maximum accounted lookup and payload storage before local fallback.", NULL,
        &lookup_max_memory, 8192, 1, 65536, PGC_USERSET, GUC_UNIT_KB, NULL, NULL, NULL);
    RegisterCustomScanMethods(&scan_methods);
    previous_hook = set_join_pathlist_hook;
    set_join_pathlist_hook = lookup_paths;
}

static Plan *
plan_lookup(PlannerInfo *root, RelOptInfo *rel, CustomPath *path,
            List *tlist, List *clauses, List *plans)
{
    LookupPath *best = (LookupPath *) path;
    CustomScan *scan = makeNode(CustomScan);
    List *quals = extract_actual_clauses(path->custom_restrictinfo, false);
    List *destinations = NIL, *shipcols = NIL, *shiptypes = NIL;
    ListCell *lc;
    int ordinary = 1;

    scan->scan.plan.targetlist = tlist;
    scan->custom_scan_tlist = vars_tlist(list_concat_copy(best->shardvars, best->lookupvars));
    scan->custom_exprs = quals;
    scan->methods = &scan_methods;
    scan->flags = path->flags;
    foreach(lc, best->shipvars)
    {
        Var *var = lfirst(lc);
        shipcols = lappend_int(shipcols, list_member(best->lookupvars, var) ?
                              var_position(best->lookupvars, var) + 1 : 0);
        shiptypes = lappend_oid(shiptypes, var->vartype);
    }
    for_each_from(lc, path->custom_paths, 1)
    {
        Path *child = lfirst(lc);
        RelOptInfo *leaf = child->parent;
        Oid oid = list_nth_oid(best->routeoids, ordinary - 1);
        int remote = -1;
        if (our_foreign(leaf))
        {
            PgFdwRelationInfo *fpinfo = leaf->fdw_private;
            List *params, *attrs = NIL;
            List *stlist = vars_tlist(child_exprs(root, best->shard, leaf, best->shardvars));
            List *scantlist = copyObject(stlist);
            ForeignScan *fs;
            StringInfoData sql;
            initStringInfo(&sql);
            pgwrh_fdw_deparse_lookup(&sql, root, leaf, best->lookup->relid,
                best->shipvars, stlist, child_exprs(root, best->shard, leaf, quals),
                best->kind == JOIN_SEMI, &params);
            /* Descriptor only: the last field is supplied by remote SQL. */
            if (best->kind != JOIN_SEMI)
                scantlist = lappend(scantlist, makeTargetEntry((Expr *) makeNullConst(
                    INT8OID, -1, InvalidOid), list_length(scantlist) + 1,
                    pstrdup("lookup_rowno"), false));
            for (int i = 1; i <= list_length(scantlist); i++)
                attrs = lappend_int(attrs, i);
            fs = make_foreignscan(stlist, NIL, 0, params,
                list_make5(makeString(sql.data), attrs, makeInteger(fpinfo->fetch_size),
                           makeString("remote lookup"), list_make1_oid(leaf->serverid)),
                scantlist, NIL, NULL);
            fs->fs_server = leaf->serverid;
            fs->checkAsUser = leaf->userid;
            fs->fs_relids = bms_copy(leaf->relids);
            fs->fs_base_relids = bms_copy(leaf->relids);
            if (leaf->useridiscurrent)
                root->glob->dependsOnRole = true;
            fs->scan.plan.plan_rows = rel->rows;
            fs->scan.plan.plan_width = child->pathtarget->width;
            remote = list_length(plans);
            plans = lappend(plans, fs);
        }
        destinations = lappend(destinations, list_make3(makeInteger(ordinary++),
                                          makeInteger(remote), makeInteger(oid)));
    }
    scan->custom_plans = plans;
    scan->custom_private = list_make5(makeInteger(best->kind), makeInteger(best->parentoid),
        makeInteger(var_position(best->lookupvars, best->lookupkey) + 1),
        makeInteger(list_length(best->shardvars)), shipcols);
    scan->custom_private = lappend(scan->custom_private, shiptypes);
    scan->custom_private = lappend(scan->custom_private, destinations);
    return &scan->scan.plan;
}

static Node *
create_lookup(CustomScan *scan)
{
    LookupState *state = palloc0(sizeof(LookupState));
    NodeSetTag(&state->css, T_CustomScanState);
    state->css.methods = &exec_methods;
    return (Node *) state;
}

/* A projection may surround the leaf; these paths cannot contain other joins. */
static ForeignScanState *
foreign_state(PlanState *state)
{
    while (state && IsA(state, ResultState))
        state = outerPlanState(state);
    return state && IsA(state, ForeignScanState) ? (ForeignScanState *) state : NULL;
}

static bool
foreign_plan(Plan *plan)
{
    while (plan && IsA(plan, Result))
        plan = outerPlan(plan);
    return plan && IsA(plan, ForeignScan);
}

static void
begin_lookup(CustomScanState *css, EState *estate, int eflags)
{
    LookupState *s = (LookupState *) css;
    CustomScan *plan = (CustomScan *) css->ss.ps.plan;
    List *priv = plan->custom_private;
    ListCell *lc;
    Oid parentoid = intVal(list_nth(priv, LParent));

    s->eflags = eflags;
    s->semi = intVal(list_nth(priv, LKind)) == JOIN_SEMI;
    s->nshard = intVal(list_nth(priv, LShardCols));
    s->nparams = list_length(list_nth(priv, LShipCols)) + (s->semi ? 0 : 1);
    s->ndest = list_length(list_nth(priv, LDestinations));
    s->fastcxt = AllocSetContextCreate(estate->es_query_cxt,
                                      "pgwrh lookup rows and payloads", ALLOCSET_DEFAULT_SIZES);
    foreach(lc, plan->custom_plans)
    {
        Plan *child = lfirst(lc);
        /* EXPLAIN_ONLY suppresses FDW connection acquisition, not local quals. */
        css->custom_ps = lappend(css->custom_ps,
            ExecInitNode(child, estate, eflags | (foreign_plan(child) ? EXEC_FLAG_EXPLAIN_ONLY : 0)));
    }
    s->lookupslot = ExecInitExtraTupleSlot(estate,
        ExecGetResultType(linitial(css->custom_ps)), &TTSOpsHeapTuple);
    s->spoolslot = ExecInitExtraTupleSlot(estate,
        ExecGetResultType(linitial(css->custom_ps)), &TTSOpsMinimalTuple);
    s->joinqual = ExecInitQual(plan->custom_exprs, &css->ss.ps);
    if (OidIsValid(parentoid))
    {
        s->parent = table_open(parentoid, NoLock);
        s->partkey = RelationGetPartitionKey(s->parent);
        s->partdesc = RelationGetPartitionDesc(s->parent, true);
    }
    s->dest_rows = palloc0(s->ndest * sizeof(uint64));
}

static int
route_row(LookupState *s, Datum value, bool isnull)
{
    CustomScan *plan = (CustomScan *) s->css.ss.ps.plan;
    PartitionKey key = s->partkey;
    PartitionBoundInfo bounds;
    int index = -1;
    bool equal;
    ListCell *lc;
    int dest = 0;

    if (!s->parent)
        return 0;
    if (isnull)
        return -1;                  /* the required equality is strict */
    bounds = s->partdesc->boundinfo;
    switch (key->strategy)
    {
        case PARTITION_STRATEGY_HASH:
            index = bounds->indexes[compute_partition_hash_value(1, key->partsupfunc,
                key->partcollation, &value, &isnull) % bounds->nindexes];
            break;
        case PARTITION_STRATEGY_LIST:
        {
            int offset = partition_list_bsearch(key->partsupfunc, key->partcollation,
                                               bounds, value, &equal);
            if (offset >= 0 && equal)
                index = bounds->indexes[offset];
            break;
        }
        case PARTITION_STRATEGY_RANGE:
        {
            int offset = partition_range_datum_bsearch(key->partsupfunc, key->partcollation,
                                                      bounds, 1, &value, &equal);
            index = bounds->indexes[offset + 1];
            break;
        }
        default:
            elog(ERROR, "unsupported lookup partition strategy");
    }
    if (index < 0)
        index = bounds->default_index;
    if (index < 0)
        return -1;
    foreach(lc, (List *) list_nth(plan->custom_private, LDestinations))
    {
        List *d = lfirst(lc);
        if ((Oid) intVal(list_nth(d, DOid)) == s->partdesc->oids[index])
            return dest;
        dest++;
    }
    return -1;                      /* already pruned by the ordinary planner */
}

static void
materialize_lookup(LookupState *s)
{
    CustomScan *plan = (CustomScan *) s->css.ss.ps.plan;
    PlanState *lookup = linitial(s->css.custom_ps);
    TupleDesc desc = ExecGetResultType(lookup);
    int keypos = intVal(list_nth(plan->custom_private, LKey));
    MemoryContext scratch = AllocSetContextCreate(CurrentMemoryContext,
                                "pgwrh lookup flatten", ALLOCSET_SMALL_SIZES);
    TupleTableSlot *slot;
    uint64 bound = (uint64) lookup_max_memory * 1024;

    s->bytes = s->ndest * sizeof(uint64);
    s->store = tuplestore_begin_heap(true, false, work_mem);
    for (;;)
    {
        MemoryContext old;
        HeapTuple tuple, flat;
        bool isnull;
        Datum key;
        int dest;

        slot = ExecProcNode(lookup);
        if (TupIsNull(slot))
            break;
        old = MemoryContextSwitchTo(scratch);
        tuple = ExecCopySlotHeapTuple(slot);
        flat = toast_flatten_tuple(tuple, desc);
        key = heap_getattr(flat, keypos, desc, &isnull);
        dest = route_row(s, key, isnull);

        CHECK_FOR_INTERRUPTS();
        tuplestore_puttuple(s->store, flat);
        s->nrows++;
        /* Include retained outputs, spool copy and allocation overhead. */
        s->bytes += 2 * (MAXALIGN(flat->t_len) + HEAPTUPLESIZE) + 64;
        if (dest >= 0)
            s->dest_rows[dest]++;
        if (s->nrows > lookup_max_rows || s->bytes > bound)
            s->overflow = true;
        if (!s->overflow && s->nrows > s->capacity)
        {
            int capacity = s->capacity ? s->capacity * 2 : 64;

            /* Charge allocated index capacity, including its unused entries. */
            s->bytes += (capacity - s->capacity) * (sizeof(HeapTuple) + sizeof(int));
            if (s->bytes > bound)
                s->overflow = true;
            else
            {
                MemoryContextSwitchTo(s->fastcxt);
                s->capacity = capacity;
                s->rows = s->rows ? repalloc(s->rows, capacity * sizeof(HeapTuple)) :
                                   palloc(capacity * sizeof(HeapTuple));
                s->row_dest = s->row_dest ? repalloc(s->row_dest, capacity * sizeof(int)) :
                                           palloc(capacity * sizeof(int));
            }
        }
        if (!s->overflow)
        {
            MemoryContextSwitchTo(s->fastcxt);
            s->rows[s->nrows - 1] = heap_copytuple(flat);
            s->row_dest[s->nrows - 1] = dest;
        }
        MemoryContextSwitchTo(old);
        MemoryContextReset(scratch);
    }
    MemoryContextDelete(scratch);
    if (s->overflow)
    {
        MemoryContextReset(s->fastcxt);
        s->rows = NULL; s->row_dest = NULL;
    }
}

/* Build every bounded request before emitting any result or opening a server. */
static void
build_payloads(LookupState *s)
{
    CustomScan *plan = (CustomScan *) s->css.ss.ps.plan;
    List *positions = list_nth(plan->custom_private, LShipCols);
    List *types = list_nth(plan->custom_private, LShipTypes);
    List *destinations = list_nth(plan->custom_private, LDestinations);
    TupleDesc desc = s->lookupslot->tts_tupleDescriptor;
    MemoryContext old = MemoryContextSwitchTo(s->fastcxt);
    int dest = 0;
    ListCell *lc;

    s->bytes += s->ndest * (sizeof(char **) + s->nparams * sizeof(char *));
    s->payloads = palloc0(s->ndest * sizeof(char **));
    foreach(lc, destinations)
    {
        List *d = lfirst(lc);
        if (intVal(list_nth(d, DRemote)) >= 0 && s->dest_rows[dest])
        {
            int count = s->dest_rows[dest];
            Datum *values = palloc(count * sizeof(Datum));
            bool *nulls = palloc(count * sizeof(bool));
            s->payloads[dest] = palloc0(s->nparams * sizeof(char *));
            for (int col = 0; col < s->nparams; col++)
            {
                bool rowno = col == list_length(positions);
                Oid type = rowno ? INT8OID : list_nth_oid(types, col);
                int pos = rowno ? 0 : list_nth_int(positions, col);
                int n = 0, dims[1], lbs[1] = {1}, nestlevel;
                int16 len;
                bool byval, varlena;
                char align;
                Oid output;
                ArrayType *array;
                for (uint64 row = 0; row < s->nrows; row++)
                {
                    if (s->row_dest[row] != dest)
                        continue;
                    nulls[n] = false;
                    values[n] = rowno ? Int64GetDatum(row + 1) :
                        heap_getattr(s->rows[row], pos, desc, &nulls[n]);
                    n++;
                }
                Assert(n == count);
                dims[0] = n;
                get_typlenbyvalalign(type, &len, &byval, &align);
                array = construct_md_array(values, nulls, 1, dims, lbs,
                                           type, len, byval, align);
                getTypeOutputInfo(get_array_type(type), &output, &varlena);
                nestlevel = set_transmission_modes();
                s->payloads[dest][col] = OidOutputFunctionCall(output, PointerGetDatum(array));
                reset_transmission_modes(nestlevel);
                /* Binary array plus textual conversion and array workspace peak. */
                s->bytes += VARSIZE(array) + strlen(s->payloads[dest][col]) + 1 +
                            count * (sizeof(Datum) + sizeof(bool));
                pfree(array);
                if (s->bytes > (uint64) lookup_max_memory * 1024)
                {
                    s->overflow = true;
                    MemoryContextSwitchTo(old);
                    MemoryContextReset(s->fastcxt);
                    s->rows = NULL; s->payloads = NULL; s->row_dest = NULL;
                    return;
                }
            }
            pfree(values); pfree(nulls);
        }
        dest++;
    }
    MemoryContextSwitchTo(old);
}

static void
combine_slots(LookupState *s, TupleTableSlot *shard, TupleTableSlot *lookup)
{
    TupleTableSlot *out = s->css.ss.ss_ScanTupleSlot;
    int nlookup = out->tts_tupleDescriptor->natts - s->nshard;
    ExecClearTuple(out);
    slot_getallattrs(shard);
    memcpy(out->tts_values, shard->tts_values, s->nshard * sizeof(Datum));
    memcpy(out->tts_isnull, shard->tts_isnull, s->nshard * sizeof(bool));
    if (lookup)
    {
        slot_getallattrs(lookup);
        memcpy(out->tts_values + s->nshard, lookup->tts_values, nlookup * sizeof(Datum));
        memcpy(out->tts_isnull + s->nshard, lookup->tts_isnull, nlookup * sizeof(bool));
    }
    else
        memset(out->tts_isnull + s->nshard, true, nlookup * sizeof(bool));
    ExecStoreVirtualTuple(out);
}

static TupleTableSlot *
next_lookup(ScanState *ss)
{
    LookupState *s = (LookupState *) ss;
    CustomScan *plan = (CustomScan *) ss->ps.plan;
    List *destinations = list_nth(plan->custom_private, LDestinations);
    TupleTableSlot *out = ss->ss_ScanTupleSlot;

    if (!s->ready)
    {
        materialize_lookup(s);
        if (!s->overflow && s->nrows)
            build_payloads(s);
        s->ready = true;
    }
    while (s->destination < s->ndest)
    {
        List *d = list_nth(destinations, s->destination);
        int remoteidx = intVal(list_nth(d, DRemote));
        bool remote = remoteidx >= 0 && !s->overflow;
        PlanState *child = list_nth(s->css.custom_ps,
            remote ? remoteidx : intVal(list_nth(d, DOrdinary)));
        ForeignScanState *fs = foreign_state(child);
        if (!s->dest_rows[s->destination])
        {
            if (remoteidx >= 0)
                s->skipped++;
            s->destination++;
            s->destination_started = false;
            continue;
        }
        if (!s->row_active)
        {
            if (fs && !fs->fdw_state)
            {
                pgwrh_fdw_lookup_start(fs, remote ? s->nparams : 0,
                    remote ? s->payloads[s->destination] : NULL);
            }
            /* Set the current execution's payload even on a reused scan. */
            if (fs)
                pgwrh_fdw_lookup_start(fs, remote ? s->nparams : 0,
                    remote ? s->payloads[s->destination] : NULL);
            if (!s->destination_started)
            {
                if (fs)
                    s->executions++;
                s->destination_started = true;
            }
            s->shardslot = ExecProcNode(child);
            if (TupIsNull(s->shardslot))
            {
                s->destination++;
                s->destination_started = false;
                continue;
            }
            if (fs)
                s->remote_rows++;
            if (remote)
            {
                if (!s->semi)
                {
                    bool isnull;
                    int64 row = DatumGetInt64(slot_getattr(fs->ss.ss_ScanTupleSlot,
                                                         s->nshard + 1, &isnull));
                    if (isnull || row < 1 || row > s->nrows ||
                        s->row_dest[row - 1] != s->destination)
                        elog(ERROR, "invalid remote lookup row identifier");
                    ExecStoreHeapTuple(s->rows[row - 1], s->lookupslot, false);
                }
                combine_slots(s, s->shardslot, s->semi ? NULL : s->lookupslot);
                return out;          /* matching belongs entirely to PostgreSQL remotely */
            }
            tuplestore_rescan(s->store);
            s->row_active = true;
        }
        while (tuplestore_gettupleslot(s->store, true, false, s->spoolslot))
        {
            CHECK_FOR_INTERRUPTS();
            combine_slots(s, s->shardslot, s->spoolslot);
            ss->ps.ps_ExprContext->ecxt_scantuple = out;
            if (ExecQual(s->joinqual, ss->ps.ps_ExprContext))
            {
                if (s->semi)
                    s->row_active = false;
                return out;
            }
            ResetExprContext(ss->ps.ps_ExprContext);
        }
        s->row_active = false;
    }
    return ExecClearTuple(out);
}

static bool
recheck_lookup(ScanState *ss, TupleTableSlot *slot)
{
    elog(ERROR, "EPQ is not supported by remote lookup joins");
    return false;
}

static TupleTableSlot *
exec_lookup(CustomScanState *css)
{
    return ExecScan(&css->ss, next_lookup, recheck_lookup);
}

static void
rescan_lookup(CustomScanState *css)
{
    LookupState *s = (LookupState *) css;
    bool changed = css->ss.ps.chgParam != NULL;
    ListCell *lc;

    ExecScanReScan(&css->ss);
    foreach(lc, css->custom_ps)
    {
        PlanState *child = lfirst(lc);
        ForeignScanState *fs = foreign_state(child);
        if (lc == list_head(css->custom_ps) && !changed)
            continue;
        if (fs)
        {
            pgwrh_fdw_lookup_reset(fs);
            /* An unstarted ForeignScan has no FDW rescan state yet. */
            if (!fs->fdw_state)
                continue;
        }
        ExecReScan(child);
    }
    s->destination = 0; s->row_active = false; s->destination_started = false;
    if (changed)
    {
        if (s->store)
            tuplestore_end(s->store);
        s->store = NULL;
        ExecClearTuple(s->lookupslot);
        ExecClearTuple(s->spoolslot);
        MemoryContextReset(s->fastcxt);
        s->rows = NULL; s->row_dest = NULL; s->payloads = NULL;
        s->capacity = 0; s->nrows = 0; s->bytes = 0;
        memset(s->dest_rows, 0, s->ndest * sizeof(uint64));
        s->ready = s->overflow = false;
    }
}

static void
end_lookup(CustomScanState *css)
{
    LookupState *s = (LookupState *) css;
    ListCell *lc;
    foreach(lc, css->custom_ps)
        ExecEndNode(lfirst(lc));
    ExecClearTuple(css->ss.ss_ScanTupleSlot);
    ExecClearTuple(css->ss.ps.ps_ResultTupleSlot);
    ExecClearTuple(s->lookupslot);
    ExecClearTuple(s->spoolslot);
    if (s->store)
        tuplestore_end(s->store);
    if (s->parent)
        table_close(s->parent, NoLock);
    MemoryContextDelete(s->fastcxt);
}

static void
explain_lookup(CustomScanState *css, List *ancestors, ExplainState *es)
{
    LookupState *s = (LookupState *) css;
    ExplainPropertyText("Lookup Join", s->semi ? "Remote EXISTS" : "Remote INNER", es);
    ExplainPropertyInteger("Lookup Condition Columns", NULL,
                            s->nparams - (s->semi ? 0 : 1), es);
    if (es->analyze)
    {
        ExplainPropertyText("Lookup Execution", s->overflow ? "Local overflow fallback" :
                            "Parameterized unnest", es);
        ExplainPropertyUInteger("Lookup Rows", NULL, s->nrows, es);
        ExplainPropertyUInteger("Lookup Bytes", NULL, s->bytes, es);
        ExplainPropertyUInteger("Remote Executions", NULL, s->executions, es);
        ExplainPropertyUInteger("Remote Rows", NULL, s->remote_rows, es);
        ExplainPropertyUInteger("Skipped Shards", NULL, s->skipped, es);
    }
}
