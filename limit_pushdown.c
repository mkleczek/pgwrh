/* SPDX-License-Identifier: AGPL-3.0-only */
/* Propagate an enclosing LIMIT's demand to pgwrh foreign append inputs. */
#include "postgres.h"

#include "catalog/pg_type_d.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "optimizer/paths.h"
#include "optimizer/planner.h"
#include "utils/guc.h"

#include "limit_pushdown.h"

static create_upper_paths_hook_type previous_upper_paths_hook;
static bool enable_limit_pushdown = true;

/*
 * Clone only the supported portion of the tree. Paths are shared by other
 * alternatives, and the bound is valid only underneath the enclosing Limit.
 * In particular, never put these truncated paths in a base relation pathlist.
 *
 * Keep the existing cost/row estimates: the enclosing Limit already accounts
 * for fetching only a prefix. We communicate that demand to the remote planner
 * without guessing the savings from a different remote execution strategy.
 * This also preserves Append's partition-pruning and async/parallel metadata.
 */
static Path *
limit_append_inputs(PlannerInfo *root, Path *path)
{
	check_stack_depth();

	/* Partial/parameterized paths require a separate demand analysis. */
	if (path->parallel_aware || path->param_info != NULL)
		return path;

	if (IsA(path, AppendPath))
	{
		AppendPath *append = (AppendPath *) path;
		AppendPath *result;
		List	   *children = NIL;
		ListCell   *lc;
		bool		changed = false;

		if (append->first_partial_path < list_length(append->subpaths))
			return path;
		foreach(lc, append->subpaths)
		{
			Path	   *child = lfirst(lc);
			/* create_append_plan may insert a Sort above an unsorted child. */
			Path	   *limited = pathkeys_contained_in(path->pathkeys, child->pathkeys)
				? limit_append_inputs(root, child) : child;

			children = lappend(children, limited);
			changed |= limited != child;
		}
		if (!changed)
			return path;
		result = makeNode(AppendPath);
		*result = *append;
		result->subpaths = children;
		return &result->path;
	}
	if (IsA(path, MergeAppendPath))
	{
		MergeAppendPath *merge = (MergeAppendPath *) path;
		MergeAppendPath *result;
		List	   *children = NIL;
		ListCell   *lc;
		bool		changed = false;

		foreach(lc, merge->subpaths)
		{
			Path	   *child = lfirst(lc);
			/* A Sort inserted by create_merge_append_plan is also a barrier. */
			Path	   *limited = pathkeys_contained_in(path->pathkeys, child->pathkeys)
				? limit_append_inputs(root, child) : child;

			children = lappend(children, limited);
			changed |= limited != child;
		}
		if (!changed)
			return path;
		result = makeNode(MergeAppendPath);
		*result = *merge;
		result->subpaths = children;
		return &result->path;
	}
	if (IsA(path, ProjectionPath))
	{
		ProjectionPath *projection = (ProjectionPath *) path;
		ProjectionPath *result;
		Path	   *child;

		if (expression_returns_set((Node *) path->pathtarget->exprs) ||
			contain_volatile_functions((Node *) path->pathtarget->exprs))
			return path;
		child = limit_append_inputs(root, projection->subpath);
		if (child == projection->subpath)
			return path;
		result = makeNode(ProjectionPath);
		*result = *projection;
		result->subpath = child;
		return &result->path;
	}
	if (IsA(path, ForeignPath))
		return pgwrh_fdw_limit_foreign_path(root, (ForeignPath *) path);

	/* Filters, joins, sorts, subqueries, aggregates and custom paths stop us. */
	return path;
}

static void
limit_upper_paths(PlannerInfo *root, UpperRelationKind stage,
				  RelOptInfo *input_rel, RelOptInfo *output_rel, void *extra)
{
	Query	   *query = root->parse;
	ListCell   *lc;

	if (previous_upper_paths_hook)
		previous_upper_paths_hook(root, stage, input_rel, output_rel, extra);

	if (!enable_limit_pushdown || stage != UPPERREL_FINAL ||
		query->commandType != CMD_SELECT || query->rowMarks ||
		query->hasTargetSRFs || query->limitOffset || !query->limitCount ||
		query->limitOption != LIMIT_OPTION_COUNT ||
		contain_volatile_functions((Node *) query->targetList))
		return;

	/*
	 * Constants and external parameters have the same value at every input.
	 * Do not duplicate expression evaluation, initplans or PARAM_EXEC values.
	 * The real expression is deparsed; limit_tuples/count_est are only estimates.
	 */
	if (IsA(query->limitCount, Const))
	{
		Const	   *count = (Const *) query->limitCount;

		if (count->consttype != INT8OID || count->constisnull ||
			DatumGetInt64(count->constvalue) <= 0)
			return;
	}
	else if (IsA(query->limitCount, Param))
	{
		Param	   *count = (Param *) query->limitCount;

		if (count->paramkind != PARAM_EXTERN || count->paramtype != INT8OID)
			return;
	}
	else
		return;

	foreach(lc, output_rel->pathlist)
	{
		Path	   *path = lfirst(lc);
		LimitPath  *limit;
		LimitPath  *result;
		Path	   *child;

		if (!IsA(path, LimitPath))
			continue;
		limit = (LimitPath *) path;
		/* Another hook may have supplied a different Limit or an OFFSET. */
		if (limit->limitOffset || limit->limitOption != LIMIT_OPTION_COUNT ||
			!equal(limit->limitCount, query->limitCount))
			continue;
		child = limit_append_inputs(root, limit->subpath);
		if (child == limit->subpath)
			continue;
		result = makeNode(LimitPath);
		*result = *limit;
		result->subpath = child;
		lfirst(lc) = result;
	}
}

void
pgwrh_fdw_init_limit_pushdown(void)
{
	DefineCustomBoolVariable("pgwrh_fdw.enable_limit_pushdown",
						 "Push safe enclosing limits into foreign append inputs.",
						 NULL, &enable_limit_pushdown, true, PGC_USERSET, 0,
						 NULL, NULL, NULL);
	previous_upper_paths_hook = create_upper_paths_hook;
	create_upper_paths_hook = limit_upper_paths;
}
