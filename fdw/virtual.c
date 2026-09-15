/*-------------------------------------------------------------------------
 * virtual.c
 *   Resolve virtual servers to transaction-pinned target user mappings.
 *
 * Copyright (c) 2026, pgwrh_fdw contributors. GNU AGPL version 3 only; see LICENSE.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_foreign_server.h"
#include "commands/defrem.h"
#include "common/hashfn.h"
#include "common/pg_prng.h"
#include "pgwrh_fdw.h"
#include "utils/acl.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/varlena.h"
#include "virtual.h"

typedef struct VirtualKey
{
	Oid			serverid;
	Oid			userid;
} VirtualKey;

typedef struct RoutingKey
{
	Oid			userid;
	List	   *members;		/* sorted actual server OIDs, before eligibility checks */
} RoutingKey;

struct PgwrhFdwVirtualBinding
{
	RoutingKey	key;			/* hash key, must be first */
	Oid			serverid;		/* selected actual server */
	Oid			umid;			/* selected actual mapping */
	bool		failed;			/* acquisition did not finish successfully */
	PGconn	   *conn;			/* physical connection, never owned here */
	int			backend_pid;	/* also detect replacement of a PGconn */
};

typedef struct VirtualAlias
{
	VirtualKey	key;			/* hash key, must be first */
	PgwrhFdwVirtualBinding *binding;
} VirtualAlias;

typedef struct VirtualState
{
	HTAB	   *aliases;
	HTAB	   *groups;
	MemoryContextCallback reset;
} VirtualState;

static VirtualState *virtual_state = NULL;

static const char *
members_option(List *options)
{
	ListCell   *lc;

	foreach(lc, options)
	{
		DefElem    *def = lfirst_node(DefElem, lc);

		if (strcmp(def->defname, "members") == 0)
			return defGetString(def);
	}
	return NULL;
}

/* Return separately allocated SQL identifiers, including quoted names. */
static List *
parse_members(const char *value)
{
	char	   *raw = pstrdup(value);
	List	   *names;
	List	   *members = NIL;
	ListCell   *lc;

	if (!SplitIdentifierString(raw, ',', &names) || names == NIL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("members must be a nonempty list of foreign server names")));

	foreach(lc, names)
	{
		char	   *name = lfirst(lc);
		ListCell   *prev;

		foreach(prev, members)
		{
			if (strcmp(lfirst(prev), name) == 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("duplicate virtual server member \"%s\"", name)));
		}
		members = lappend(members, pstrdup(name));
	}
	list_free(names);
	pfree(raw);
	return members;
}

/* Virtual servers configure planning; targets configure physical sessions. */
void
pgwrh_fdw_validate_virtual_options(List *options, Oid catalog)
{
	const char *members;
	ListCell   *lc;

	if (catalog != ForeignServerRelationId ||
		(members = members_option(options)) == NULL)
		return;

	list_free_deep(parse_members(members));
	foreach(lc, options)
	{
		DefElem    *def = lfirst_node(DefElem, lc);
		const char *name = def->defname;

		if (strcmp(name, "members") != 0 &&
			strcmp(name, "use_remote_estimate") != 0 &&
			strcmp(name, "fdw_startup_cost") != 0 &&
			strcmp(name, "fdw_tuple_cost") != 0 &&
			strcmp(name, "extensions") != 0 &&
			strcmp(name, "updatable") != 0 &&
			strcmp(name, "truncatable") != 0 &&
			strcmp(name, "fetch_size") != 0 &&
			strcmp(name, "batch_size") != 0 &&
			strcmp(name, "async_capable") != 0 &&
			strcmp(name, "analyze_sampling") != 0)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("option \"%s\" is not allowed on a virtual server", name),
					 errhint("Configure connection and transaction options on the member servers.")));
	}
}

static void
reset_virtual_state(void *arg)
{
	virtual_state = NULL;
}

static uint32
routing_hash(const void *key, Size keysize)
{
	const RoutingKey *route = key;
	uint32 hash = hash_uint32(route->userid);
	ListCell *lc;

	foreach(lc, route->members)
		hash = hash_combine(hash, hash_uint32(lfirst_oid(lc)));
	return hash;
}

static int
routing_match(const void *left, const void *right, Size keysize)
{
	const RoutingKey *a = left;
	const RoutingKey *b = right;

	return a->userid != b->userid || !equal(a->members, b->members);
}

static void
init_virtual_state(void)
{
	MemoryContext oldcontext;
	VirtualState *state;
	HASHCTL		ctl = {0};

	oldcontext = MemoryContextSwitchTo(TopTransactionContext);
	state = palloc0(sizeof(VirtualState));
	ctl.keysize = sizeof(VirtualKey);
	ctl.entrysize = sizeof(VirtualAlias);
	ctl.hcxt = TopTransactionContext;
	state->aliases = hash_create("pgwrh_fdw virtual aliases", 8, &ctl,
								 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	ctl.keysize = sizeof(RoutingKey);
	ctl.entrysize = sizeof(PgwrhFdwVirtualBinding);
	ctl.hash = routing_hash;
	ctl.match = routing_match;
	state->groups = hash_create("pgwrh_fdw routing groups", 8, &ctl,
							   HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);
	state->reset.func = reset_virtual_state;
	MemoryContextRegisterResetCallback(TopTransactionContext, &state->reset);
	MemoryContextSwitchTo(oldcontext);
	virtual_state = state;
}

/* A PUBLIC virtual mapping still carries the caller's effective userid. */
static bool
can_use_member(Oid userid, Oid serverid)
{
	if (object_aclcheck(ForeignServerRelationId, serverid, userid, ACL_USAGE) != ACLCHECK_OK)
		return false;

	return SearchSysCacheExists2(USERMAPPINGUSERSERVER,
								ObjectIdGetDatum(userid), ObjectIdGetDatum(serverid)) ||
		SearchSysCacheExists2(USERMAPPINGUSERSERVER,
							 ObjectIdGetDatum(InvalidOid), ObjectIdGetDatum(serverid));
}

static void
check_member(ForeignServer *server, Oid fdwid)
{
	if (server->fdwid != fdwid || members_option(server->options) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
				 errmsg("virtual server member \"%s\" must be an ordinary server of the same foreign-data wrapper",
						server->servername)));
}

static PgwrhFdwVirtualBinding *
alias_binding(Oid serverid, Oid userid)
{
	VirtualKey key = {serverid, userid};
	VirtualAlias *alias = virtual_state ?
		hash_search(virtual_state->aliases, &key, HASH_FIND, NULL) : NULL;

	return alias ? alias->binding : NULL;
}

/* Group identity never depends on ordering, cache state, or accessible subsets. */
List *
pgwrh_fdw_routing_members(Oid serverid, Oid userid)
{
	PgwrhFdwVirtualBinding *binding = alias_binding(serverid, userid);
	ForeignServer *server;
	const char *members;
	List *names;
	List *result = NIL;
	ListCell *lc;

	/* A used alias retains its original group across topology changes. */
	if (binding)
		return list_copy(binding->key.members);
	server = GetForeignServer(serverid);
	members = members_option(server->options);
	if (!members)
		return NIL;
	names = parse_members(members);
	foreach(lc, names)
	{
		ForeignServer *member = GetForeignServerByName(lfirst(lc), false);

		check_member(member, server->fdwid);
		result = lappend_oid(result, member->serverid);
	}
	list_free_deep(names);
	list_sort(result, list_oid_cmp);
	return result;
}

/* Read-only: planning must see peer bindings without binding a new alias. */
static PgwrhFdwVirtualBinding *
find_binding(Oid serverid, Oid userid)
{
	PgwrhFdwVirtualBinding *binding = alias_binding(serverid, userid);
	RoutingKey key;

	if (binding || !virtual_state)
		return binding;
	key.userid = userid;
	key.members = pgwrh_fdw_routing_members(serverid, userid);
	if (key.members)
		binding = hash_search(virtual_state->groups, &key, HASH_FIND, NULL);
	list_free(key.members);
	return binding;
}

/* Attach only aliases actually acquired, preserving their topology at first use. */
static PgwrhFdwVirtualBinding *
bind_alias(Oid serverid, Oid userid, UserMapping *target)
{
	VirtualKey key = {serverid, userid};
	PgwrhFdwVirtualBinding *binding = find_binding(serverid, userid);
	VirtualAlias *alias;
	MemoryContext oldcontext;
	bool found;

	if (!virtual_state)
		init_virtual_state();
	oldcontext = MemoryContextSwitchTo(TopTransactionContext);
	if (!binding)
	{
		RoutingKey route = {userid, pgwrh_fdw_routing_members(serverid, userid)};

		Assert(route.members != NIL && target != NULL);
		binding = hash_search(virtual_state->groups, &route, HASH_ENTER, &found);
		Assert(!found);
		binding->serverid = target->serverid;
		binding->umid = target->umid;
		binding->conn = NULL;
		binding->backend_pid = 0;
		/* Even allocation failure while attaching an alias must leave it failed. */
		binding->failed = true;
	}
	alias = hash_search(virtual_state->aliases, &key, HASH_ENTER, &found);
	alias->binding = binding;
	MemoryContextSwitchTo(oldcontext);
	return binding;
}

UserMapping *
pgwrh_fdw_resolve_virtual_mapping(UserMapping *user,
								  PgwrhFdwRankConnection rank_connection,
								  PgwrhFdwVirtualBinding **binding)
{
	ForeignServer *server;
	UserMapping *target;
	const char *members;
	List	   *memberids;
	List	   *candidates = NIL;
	ListCell   *lc;
	bool		have_access = false;
	PgwrhFdwConnectionRank best_rank = PGWRH_FDW_CONNECTION_UNUSABLE;

	*binding = find_binding(user->serverid, user->userid);
	server = GetForeignServer(user->serverid);
	members = members_option(server->options);
	if (*binding == NULL && members == NULL)
		return user;

	if (user->options != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
				 errmsg("user mapping for virtual server \"%s\" must have no options", server->servername),
				 errhint("Configure credentials on the member servers' user mappings.")));

	if (*binding != NULL)
	{
		PgwrhFdwVirtualBinding *entry = bind_alias(user->serverid, user->userid, NULL);

		if (entry->failed)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("previous connection acquisition for virtual server \"%s\" failed", server->servername),
					 errhint("Roll back the local transaction before retrying.")));

		/* Keep the selected OID even if membership changes or a name is reused. */
		entry->failed = true;
		check_member(GetForeignServer(entry->serverid), server->fdwid);
		if (!can_use_member(user->userid, entry->serverid))
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("selected member of virtual server \"%s\" is no longer accessible", server->servername)));
		target = GetUserMapping(user->userid, entry->serverid);
		if (target->umid != entry->umid)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("selected user mapping for virtual server \"%s\" changed during the transaction", server->servername)));
		return target;
	}

	memberids = pgwrh_fdw_routing_members(user->serverid, user->userid);
	foreach(lc, memberids)
	{
		Oid memberid = lfirst_oid(lc);
		UserMapping *candidate;
		PgwrhFdwConnectionRank rank;

		if (!can_use_member(user->userid, memberid))
			continue;
		have_access = true;
		candidate = GetUserMapping(user->userid, memberid);
		rank = rank_connection(candidate->umid);
		if (rank == PGWRH_FDW_CONNECTION_UNUSABLE)
			continue;
		if (rank > best_rank)
		{
			list_free(candidates);
			candidates = NIL;
			best_rank = rank;
		}
		if (rank == best_rank)
			candidates = lappend(candidates, candidate);
	}
	list_free(memberids);
	if (candidates == NIL)
	{
		if (have_access)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("no usable member connections for virtual server \"%s\"", server->servername),
					 errhint("Roll back the local transaction before retrying.")));
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("no accessible members for virtual server \"%s\"", server->servername),
				 errhint("The effective local user needs USAGE and a user mapping on at least one member server.")));
	}

	target = list_nth(candidates,
					 pg_prng_uint64_range(&pg_global_prng_state, 0,
										  list_length(candidates) - 1));
	list_free(candidates);

	*binding = bind_alias(user->serverid, user->userid, target);
	return target;
}

void
pgwrh_fdw_check_virtual_connection(PgwrhFdwVirtualBinding *binding,
								 PGconn *conn, int xact_depth)
{
	if (binding && binding->conn &&
		(conn != binding->conn || xact_depth == 0 ||
		 PQstatus(conn) != CONNECTION_OK ||
		 PQbackendPID(conn) != binding->backend_pid))
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
				 errmsg("connection selected by virtual server was lost"),
				 errhint("Roll back the local transaction before retrying.")));
}

void
pgwrh_fdw_virtual_connected(PgwrhFdwVirtualBinding *binding, PGconn *conn)
{
	if (binding)
	{
		binding->conn = conn;
		binding->backend_pid = PQbackendPID(conn);
		binding->failed = false;
	}
}

bool
pgwrh_fdw_is_virtual_server(Oid serverid)
{
	return members_option(GetForeignServer(serverid)->options) != NULL;
}

/* Inspect eligibility without selecting a target or opening a connection. */
static List *
server_targets(Oid serverid, Oid userid)
{
	ForeignServer *server = GetForeignServer(serverid);
	const char *members = members_option(server->options);
	PgwrhFdwVirtualBinding *binding = find_binding(serverid, userid);
	UserMapping *user;
	List *memberids;
	List *targets = NIL;
	ListCell *lc;

	if (!members && !binding)
		return list_make1_oid(serverid);
	user = GetUserMapping(userid, serverid);
	if (user->options != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
				 errmsg("user mapping for virtual server \"%s\" must have no options",
						server->servername)));
	if (binding)
	{
		if (binding->failed)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("previous connection acquisition for virtual server \"%s\" failed",
							server->servername),
					 errhint("Roll back the local transaction before retrying.")));
		check_member(GetForeignServer(binding->serverid), server->fdwid);
		if (!can_use_member(userid, binding->serverid))
			return NIL;
		if (GetUserMapping(userid, binding->serverid)->umid != binding->umid)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("selected user mapping for virtual server \"%s\" changed during the transaction",
							server->servername)));
		return list_make1_oid(binding->serverid);
	}

	memberids = pgwrh_fdw_routing_members(serverid, userid);
	foreach(lc, memberids)
	{
		Oid memberid = lfirst_oid(lc);

		if (can_use_member(userid, memberid) &&
			pgwrh_fdw_rank_cached_connection(GetUserMapping(userid, memberid)->umid) !=
			PGWRH_FDW_CONNECTION_UNUSABLE)
			targets = lappend_oid(targets, memberid);
	}
	list_free(memberids);
	return targets;
}

/* Intersect all inputs, including transaction bindings, not just pairs. */
List *
pgwrh_fdw_common_targets(List *serverids, Oid userid)
{
	List *common = NIL;
	ListCell *lc;
	bool first = true;

	foreach(lc, serverids)
	{
		List *targets = server_targets(lfirst_oid(lc), userid);

		if (first)
			common = targets;
		else
		{
			List *intersection = NIL;
			ListCell *candidate;

			foreach(candidate, common)
			{
				if (list_member_oid(targets, lfirst_oid(candidate)))
					intersection = lappend_oid(intersection, lfirst_oid(candidate));
			}

			list_free(common);
			list_free(targets);
			common = intersection;
		}
		first = false;
	}
	return common;
}

/*
 * Acquire one physical connection for a whole remote expression. Estimation
 * uses bind=false: EXPLAIN must not commit individual shards to replicas before
 * join planning has found their intersection. Actual sessions still use the
 * ordinary cache and transaction setup, including transaction_parameters.
 */
PGconn *
pgwrh_fdw_group_connection(List *serverids, Oid userid,
						   PgFdwConnState **state, bool bind)
{
	List *targets = pgwrh_fdw_common_targets(serverids, userid);
	List *best = NIL;
	List *bindings = NIL;
	ListCell *lc;
	PgwrhFdwConnectionRank best_rank = PGWRH_FDW_CONNECTION_UNUSABLE;
	UserMapping *target;
	PGconn *conn = NULL;

	foreach(lc, targets)
	{
		Oid serverid = lfirst_oid(lc);
		PgwrhFdwConnectionRank rank = pgwrh_fdw_rank_cached_connection(
			GetUserMapping(userid, serverid)->umid);

		/* A bound, invalidated transaction may finish on its old connection. */
		if (rank > best_rank)
		{
			list_free(best);
			best = NIL;
			best_rank = rank;
		}
		if (rank == best_rank)
			best = lappend_oid(best, serverid);
	}
	if (best == NIL)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
				 errmsg("no common target for virtual foreign servers"),
				 errhint("Replan the query with compatible transaction bindings and server membership.")));
	target = GetUserMapping(userid, list_nth_oid(best,
		pg_prng_uint64_range(&pg_global_prng_state, 0, list_length(best) - 1)));
	list_free(best);
	list_free(targets);

	if (!bind)
		return GetConnection(target, false, state);

	/* Reserve every virtual input before any connection or transaction work. */
	foreach(lc, serverids)
	{
		Oid serverid = lfirst_oid(lc);
		PgwrhFdwVirtualBinding *binding = find_binding(serverid, userid);

		if (binding || pgwrh_fdw_is_virtual_server(serverid))
		{
			binding = bind_alias(serverid, userid, target);
			Assert(binding->serverid == target->serverid && binding->umid == target->umid);
			bindings = list_append_unique_ptr(bindings, binding);
		}
	}

	PG_TRY();
	{
		/* Reservations are complete; any acquisition error below poisons them all. */
		foreach(lc, bindings)
			((PgwrhFdwVirtualBinding *) lfirst(lc))->failed = false;
		foreach(lc, serverids)
		{
			PGconn *next = GetConnection(GetUserMapping(userid, lfirst_oid(lc)),
										false, state);

			Assert(conn == NULL || conn == next);
			conn = next;
		}
	}
	PG_CATCH();
	{
		/* A caught acquisition error must not let any input change replicas. */
		foreach(lc, bindings)
			((PgwrhFdwVirtualBinding *) lfirst(lc))->failed = true;
		PG_RE_THROW();
	}
	PG_END_TRY();
	list_free(bindings);
	return conn;
}
