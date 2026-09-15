/* SPDX-License-Identifier: AGPL-3.0-only */
/* Allow the FDW to consider joins that PostgreSQL's server-OID gate skips. */
#include "postgres.h"

#include "catalog/pg_class.h"
#include "catalog/pg_inherits.h"
#include "join.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/paths.h"
#include "parser/parsetree.h"
#include "pgwrh_fdw.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "virtual.h"

static set_join_pathlist_hook_type previous_join_hook;
static GetForeignJoinPaths_function foreign_join_callback;

typedef struct ReferenceCount
{
	List *servers;
	int *remaining;              /* NULL when collecting statement references */
} ReferenceCount;

typedef struct QueryReferences
{
	PlannerInfo *root;
	List *servers;
	MemoryContextCallback reset;
	struct QueryReferences *next;
} QueryReferences;

static QueryReferences *query_references;

static bool
count_reference(Oid relid, ReferenceCount *count)
{
	ListCell *lc;
	int index = 0;
	Oid serverid;

	if (get_rel_relkind(relid) != RELKIND_FOREIGN_TABLE)
		return false;
	serverid = GetForeignTable(relid)->serverid;
	if (!count->remaining)
	{
		count->servers = lappend_oid(count->servers, serverid);
		return false;
	}
	foreach(lc, count->servers)
	{
		if (lfirst_oid(lc) == serverid && --count->remaining[index] < 0)
			return true;
		index++;
	}
	return false;
}

/* Count references in sibling subqueries too, including unexpanded partitions. */
static bool
outside_reference(Node *node, ReferenceCount *count)
{
	if (!node)
		return false;
	if (IsA(node, Query))
	{
		Query *query = (Query *) node;
		ListCell *lc;

		foreach(lc, query->rtable)
		{
			RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);

			/* A flattened UNION ALL parent duplicates its first append child. */
			if (rte->rtekind == RTE_SUBQUERY && !rte->inh &&
				outside_reference((Node *) rte->subquery, count))
				return true;
			if (rte->rtekind != RTE_RELATION)
				continue;
			if (count_reference(rte->relid, count))
				return true;
			if (rte->inh && get_rel_relkind(rte->relid) != RELKIND_FOREIGN_TABLE)
			{
				List *children = find_all_inheritors(rte->relid, NoLock, NULL);
				ListCell *child;
				ListCell *other;
				bool expanded = false;

				/* Expanded children already have their own range-table entries. */
				foreach(other, query->rtable)
				{
					RangeTblEntry *entry = lfirst_node(RangeTblEntry, other);

					if (entry->rtekind == RTE_RELATION && entry->relid != rte->relid &&
						list_member_oid(children, entry->relid))
						expanded = true;
				}
				if (!expanded)
					foreach(child, children)
						if (count_reference(lfirst_oid(child), count))
						{
							list_free(children);
							return true;
						}
				list_free(children);
			}
		}
		return query_tree_walker(query, outside_reference, count,
								QTW_IGNORE_RT_SUBQUERIES);
	}
	return expression_tree_walker(node, outside_reference, count);
}

static void
forget_query_references(void *arg)
{
	QueryReferences **link = &query_references;

	while (*link != arg)
		link = &(*link)->next;
	*link = (*link)->next;
}

/*
 * Expansion changes planner range tables, but not the statement's original
 * references. Collect once, expanding still-unplanned inheritance parents.
 * Keep the cache in planner memory, including cleanup after planning errors.
 */
static List *
statement_references(PlannerInfo *root)
{
	QueryReferences *entry;
	ReferenceCount count = {NIL, NULL};
	MemoryContext oldcontext;

	while (root->parent_root)
		root = root->parent_root;
	for (entry = query_references; entry; entry = entry->next)
		if (entry->root == root)
			return entry->servers;

	oldcontext = MemoryContextSwitchTo(root->planner_cxt);
	(void) outside_reference((Node *) root->parse, &count);
	entry = palloc0(sizeof(QueryReferences));
	entry->root = root;
	entry->servers = count.servers;
	entry->next = query_references;
	entry->reset.func = forget_query_references;
	entry->reset.arg = entry;
	MemoryContextRegisterResetCallback(root->planner_cxt, &entry->reset);
	query_references = entry;
	MemoryContextSwitchTo(oldcontext);
	return entry->servers;
}

/*
 * An independently executed scan of the same virtual server could bind it to a
 * member outside this join's intersection. Only offer the cross-server path if
 * it contains every reference to each virtual input in the statement. Ordinary
 * actual servers cannot move, so repeated references to them are harmless.
 */
bool
pgwrh_fdw_join_isolated(PlannerInfo *root, RelOptInfo *joinrel, List *servers)
{
	ReferenceCount count;
	ListCell *lc;
	int relid = -1;
	bool outside = false;

	count.servers = NIL;
	foreach(lc, servers)
		if (pgwrh_fdw_is_virtual_server(lfirst_oid(lc)))
			count.servers = lappend_oid(count.servers, lfirst_oid(lc));
	count.remaining = palloc0(sizeof(int) * list_length(count.servers));
	/* Counting downward from zero first computes the negative allowed counts. */
	while ((relid = bms_next_member(joinrel->relids, relid)) >= 0)
	{
		RangeTblEntry *rte;

		if (bms_is_member(relid, root->outer_join_rels))
			continue;
		rte = planner_rt_fetch(relid, root);
		if (rte->rtekind == RTE_RELATION)
			(void) count_reference(rte->relid, &count);
	}
	for (int i = 0; i < list_length(count.servers); i++)
		count.remaining[i] = -count.remaining[i];
	foreach(lc, statement_references(root))
	{
		ListCell *input;
		int index = 0;

		foreach(input, count.servers)
		{
			if (lfirst_oid(input) == lfirst_oid(lc) && --count.remaining[index] < 0)
				outside = true;
			index++;
		}
		if (outside)
			break;
	}
	list_free(count.servers);
	pfree(count.remaining);
	return !outside;
}

static void
virtual_join_paths(PlannerInfo *root, RelOptInfo *joinrel,
				   RelOptInfo *outerrel, RelOptInfo *innerrel,
				   JoinType jointype, JoinPathExtraData *extra)
{
	PgFdwRelationInfo *outer;
	PgFdwRelationInfo *inner;
	Oid outeruser;
	Oid inneruser;
	List *servers;
	List *targets;

	if (previous_join_hook)
		previous_join_hook(root, joinrel, outerrel, innerrel, jointype, extra);

	/* Never reinterpret another wrapper's private data or redo core's call. */
	if (joinrel->fdw_private || !outerrel->fdwroutine || !innerrel->fdwroutine ||
		outerrel->fdwroutine->GetForeignJoinPaths != foreign_join_callback ||
		innerrel->fdwroutine->GetForeignJoinPaths != foreign_join_callback)
		return;
	outer = outerrel->fdw_private;
	inner = innerrel->fdw_private;
	if (!outer || !inner || !outer->pushdown_safe || !inner->pushdown_safe ||
		outer->server->fdwid != inner->server->fdwid)
		return;

	/* Keep PostgreSQL's effective-user rule, including view-owner access. */
	outeruser = OidIsValid(outerrel->userid) ? outerrel->userid : GetUserId();
	inneruser = OidIsValid(innerrel->userid) ? innerrel->userid : GetUserId();
	if (outeruser != inneruser)
		return;
	servers = list_union_oid(outer->relation_serverids, inner->relation_serverids);
	targets = pgwrh_fdw_common_targets(servers, outeruser);
	list_free(servers);
	if (targets == NIL)
		return;
	list_free(targets);

	/* Representative identity only; execution carries every input server. */
	joinrel->serverid = outerrel->serverid;
	joinrel->userid = outerrel->userid;
	joinrel->useridiscurrent = outerrel->useridiscurrent || innerrel->useridiscurrent ||
		!OidIsValid(outerrel->userid) || !OidIsValid(innerrel->userid);
	joinrel->fdwroutine = outerrel->fdwroutine;
	foreign_join_callback(root, joinrel, outerrel, innerrel, jointype, extra);
}

void
pgwrh_fdw_join_init(GetForeignJoinPaths_function callback)
{
	if (foreign_join_callback)
		return;
	foreign_join_callback = callback;
	previous_join_hook = set_join_pathlist_hook;
	set_join_pathlist_hook = virtual_join_paths;
}
