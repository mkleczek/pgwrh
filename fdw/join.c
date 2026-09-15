/* SPDX-License-Identifier: AGPL-3.0-only */
/* Allow the FDW to consider joins that PostgreSQL's server-OID gate skips. */
#include "postgres.h"

#include "join.h"
#include "miscadmin.h"
#include "optimizer/paths.h"
#include "pgwrh_fdw.h"
#include "virtual.h"

static set_join_pathlist_hook_type previous_join_hook;
static GetForeignJoinPaths_function foreign_join_callback;

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
