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
#include "executor/spi.h"
#include "miscadmin.h"
#include "postgres_fdw.h"
#include "storage/lmgr.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
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

typedef struct RoutingUse
{
	Oid			serverid;		/* hash key, must be first */
	bool		locked;			/* reader lock owned by the top transaction */
	bool		changed;		/* managed update attempted in this transaction */
} RoutingUse;

typedef struct VirtualState
{
	HTAB	   *aliases;
	HTAB	   *groups;
	HTAB	   *uses;
	MemoryContextCallback reset;
} VirtualState;

static VirtualState *virtual_state = NULL;

extern Datum pgwrh_fdw_handler(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pgwrh_fdw_set_members);

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
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(RoutingUse);
	state->uses = hash_create("pgwrh_fdw routing locks", 8, &ctl,
							 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	state->reset.func = reset_virtual_state;
	MemoryContextRegisterResetCallback(TopTransactionContext, &state->reset);
	MemoryContextSwitchTo(oldcontext);
	virtual_state = state;
}

static RoutingUse *
routing_use(Oid serverid)
{
	RoutingUse *use;
	bool		found;

	if (!virtual_state)
		init_virtual_state();
	use = hash_search(virtual_state->uses, &serverid, HASH_ENTER, &found);
	if (!found)
	{
		use->locked = false;
		use->changed = false;
	}
	return use;
}

/*
 * Protect membership before inspecting it, including planning and IMPORT.
 * The object lock covers every effective user and table of this virtual server.
 * Top-level ownership matches bindings that survive subtransaction abort.
 */
static ForeignServer *
routing_server(Oid serverid)
{
	ForeignServer *server = GetForeignServer(serverid);
	RoutingUse *use;
	ResourceOwner saved_owner;

	if (members_option(server->options) == NULL)
		return server;
	use = routing_use(serverid);
	if (use->changed)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot use virtual server \"%s\" after updating its members in this transaction",
						server->servername),
				 errhint("Commit or roll back the transaction before using the server.")));
	if (use->locked)
		return server;

	saved_owner = CurrentResourceOwner;
	PG_TRY();
	{
		CurrentResourceOwner = TopTransactionResourceOwner;
		LockDatabaseObject(ForeignServerRelationId, serverid, 0, AccessShareLock);
		CurrentResourceOwner = saved_owner;
	}
	PG_CATCH();
	{
		CurrentResourceOwner = saved_owner;
		PG_RE_THROW();
	}
	PG_END_TRY();
	use->locked = true;

	/* LockDatabaseObject accepts invalidations; discard the pre-wait copy. */
	return GetForeignServer(serverid);
}

/*
 * The virtual server's owner authorizes the route; the effective query user
 * supplies the mapping. Reading an existing foreign table needs no server USAGE.
 */
static bool
can_use_member(Oid ownerid, Oid userid, Oid serverid)
{
	if (object_aclcheck(ForeignServerRelationId, serverid, ownerid, ACL_USAGE) != ACLCHECK_OK)
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
	server = routing_server(serverid);
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

/*
 * Weighted reservoir selection within the best reuse rank. Actual servers own
 * the preference so overlapping virtual servers agree about each target.
 * The validator bounds weights by INT_MAX; even an INT_MAX-length candidate
 * list cannot overflow the uint64 total. Weights never change routing groups
 * or an existing transaction binding.
 */
static Oid
choose_target(List *candidates)
{
	ListCell   *lc;
	uint64		total = 0;
	Oid			selected = InvalidOid;

	Assert(candidates != NIL);
	foreach(lc, candidates)
	{
		Oid			serverid = lfirst_oid(lc);
		ForeignServer *server = GetForeignServer(serverid);
		ListCell   *option;
		uint64		weight = 1;

		foreach(option, server->options)
		{
			DefElem    *def = lfirst_node(DefElem, option);

			if (strcmp(def->defname, "load_balance_weight") == 0)
				weight = strtoul(defGetString(def), NULL, 10);
		}
		total += weight;
		if (!OidIsValid(selected) ||
			pg_prng_uint64_range(&pg_global_prng_state, 1, total) <= weight)
			selected = serverid;
	}
	return selected;
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
	server = routing_server(user->serverid);
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
		if (!can_use_member(server->owner, user->userid, entry->serverid))
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

		if (!can_use_member(server->owner, user->userid, memberid))
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
			candidates = lappend_oid(candidates, memberid);
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
				 errhint("The virtual server owner needs USAGE and the effective local user needs a user mapping on at least one member server.")));
	}

	target = GetUserMapping(user->userid, choose_target(candidates));
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
	return members_option(routing_server(serverid)->options) != NULL;
}

/* Inspect eligibility without selecting a target or opening a connection. */
static List *
server_targets(Oid serverid, Oid userid)
{
	ForeignServer *server = routing_server(serverid);
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
		if (!can_use_member(server->owner, userid, binding->serverid))
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

		if (can_use_member(server->owner, userid, memberid) &&
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
	List *ordered = list_copy(serverids);
	ListCell *lc;
	bool first = true;

	/* Lock all inputs before inspecting their intersection. */
	list_sort(ordered, list_oid_cmp);
	foreach(lc, ordered)
		(void) routing_server(lfirst_oid(lc));
	list_free(ordered);

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

/* Choose within the best reuse tier, both initially and after a failed dial. */
static UserMapping *
choose_mapping(List *targets, Oid userid)
{
	List *best = NIL;
	ListCell *lc;
	PgwrhFdwConnectionRank best_rank = PGWRH_FDW_CONNECTION_UNUSABLE;
	UserMapping *target;

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
	target = GetUserMapping(userid, choose_target(best));
	list_free(best);
	return target;
}

/*
 * Acquire a physical session before publishing successful routing pins. Retry
 * only a failed initial connection (08001) whose cache entry is still empty.
 * Context, query, transaction-start and cancellation errors are not retried.
 * A coordinated expression must move all its provisional bindings together.
 */
static PGconn *
acquire_targets(UserMapping *target, List *targets, List *bindings,
                bool will_prep_stmt, PgFdwConnState **state)
{
	MemoryContext context = CurrentMemoryContext;
	ListCell *lc;
	PGconn *conn = NULL;

	for (;;)
	{
		volatile bool retry = false;
		bool pinned = false;

		foreach(lc, bindings)
			((PgwrhFdwVirtualBinding *) lfirst(lc))->failed = true;
		foreach(lc, bindings)
		{
			PgwrhFdwVirtualBinding *binding = lfirst(lc);

			pinned |= binding->conn != NULL;
			pgwrh_fdw_check_cached_virtual_connection(binding, target->umid);
			Assert(binding->conn == NULL || binding->serverid == target->serverid);
			binding->serverid = target->serverid;
			binding->umid = target->umid;
		}

		PG_TRY();
		{
			conn = GetConnection(target, will_prep_stmt, state);
		}
		PG_CATCH();
		{
			MemoryContext error_context = MemoryContextSwitchTo(context);
			ErrorData *error = CopyErrorData();

			/* NEW after 08001 means connect_pg_server left an empty entry. */
			if (pinned || error->sqlerrcode != ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION ||
				pgwrh_fdw_rank_cached_connection(target->umid) != PGWRH_FDW_CONNECTION_NEW ||
				list_length(targets) <= 1)
			{
				MemoryContextSwitchTo(error_context);
				PG_RE_THROW();
			}
			FlushErrorState();
			FreeErrorData(error);
			retry = true;
		}
		PG_END_TRY();

		if (!retry)
			break;
		targets = list_delete_oid(targets, target->serverid);
		target = choose_mapping(targets, target->userid);
	}

	foreach(lc, bindings)
		pgwrh_fdw_virtual_connected(lfirst(lc), conn);
	list_free(targets);
	return conn;
}

/* GetConnection keeps its signature and delegates only its virtual case here. */
PGconn *
pgwrh_fdw_acquire_virtual_connection(Oid virtual_serverid, UserMapping *target,
                                   PgwrhFdwVirtualBinding *binding,
                                   bool will_prep_stmt, PgFdwConnState **state)
{
	List *targets = list_make1_oid(target->serverid);
	List *bindings = list_make1(binding);
	ListCell *lc;
	PGconn *conn;

	if (binding->conn == NULL)
	{
		ForeignServer *server = routing_server(virtual_serverid);

		foreach(lc, binding->key.members)
		{
			Oid memberid = lfirst_oid(lc);

			if (can_use_member(server->owner, target->userid, memberid) &&
				pgwrh_fdw_rank_cached_connection(GetUserMapping(target->userid, memberid)->umid) !=
				PGWRH_FDW_CONNECTION_UNUSABLE)
				targets = list_append_unique_oid(targets, memberid);
		}
	}
	conn = acquire_targets(target, targets, bindings, will_prep_stmt, state);
	list_free(bindings);
	return conn;
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
	List *bindings = NIL;
	ListCell *lc;
	UserMapping *target = choose_mapping(targets, userid);
	PGconn *conn;

	if (bind)
	{
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
	}
	conn = acquire_targets(target, targets, bindings, false, state);
	list_free(bindings);
	return conn;
}

/* Validate again after waiting: ownership and the object may have changed. */
static void
check_updatable_virtual_server(ForeignServer *server)
{
	ForeignDataWrapper *fdw = GetForeignDataWrapper(server->fdwid);
	FmgrInfo	handler;

	if (!object_ownercheck(ForeignServerRelationId, server->serverid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_FOREIGN_SERVER, server->servername);
	if (OidIsValid(fdw->fdwhandler))
		fmgr_info(fdw->fdwhandler, &handler);
	if (!OidIsValid(fdw->fdwhandler) || handler.fn_addr != pgwrh_fdw_handler ||
		members_option(server->options) == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("server \"%s\" is not a pgwrh_fdw virtual server",
						server->servername)));
}

/*
 * SQL entry point for synchronized membership updates. Invoke ordinary DDL
 * through SPI to preserve permission checks, validation and event triggers.
 * The exclusive lock belongs to the updating (sub)transaction and remains
 * held after this function returns. Raw ALTER SERVER is deliberately unchanged.
 */
Datum
pgwrh_fdw_set_members(PG_FUNCTION_ARGS)
{
	ForeignServer *server;
	RoutingUse *use;
	ArrayType  *members;
	Datum	   *values;
	bool	   *nulls;
	int			count;
	StringInfoData option;
	char	   *command;
	int			i;

	PreventCommandIfReadOnly("pgwrh_fdw_set_members()");
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("server name and members must not be null")));
	server = GetForeignServerByName(text_to_cstring(PG_GETARG_TEXT_PP(0)), false);
	check_updatable_virtual_server(server);
	use = routing_use(server->serverid);
	if (use->locked)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot update members of virtual server \"%s\" after using it in this transaction",
						server->servername),
				 errhint("Update members in a separate transaction from queries using the server.")));

	members = PG_GETARG_ARRAYTYPE_P(1);
	if (ARR_NDIM(members) != 1 || ArrayGetNItems(ARR_NDIM(members), ARR_DIMS(members)) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("members must be a nonempty one-dimensional array of server names")));
	deconstruct_array(members, TEXTOID, -1, false, TYPALIGN_INT,
					  &values, &nulls, &count);
	initStringInfo(&option);
	for (i = 0; i < count; i++)
	{
		char *name;

		if (nulls[i])
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("member names must not be null")));
		name = TextDatumGetCString(values[i]);
		if (name[0] == '\0' || strlen(name) >= NAMEDATALEN)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid member server name \"%s\"", name)));
		if (i > 0)
			appendStringInfoChar(&option, ',');
		appendStringInfoString(&option, quote_identifier(name));
	}
	/* Apply the same identifier and duplicate validation as CREATE SERVER. */
	list_free_deep(parse_members(option.data));

	LockDatabaseObject(ForeignServerRelationId, server->serverid, 0, AccessExclusiveLock);
	server = GetForeignServer(server->serverid);
	check_updatable_virtual_server(server);
	for (i = 0; i < count; i++)
		check_member(GetForeignServerByName(TextDatumGetCString(values[i]), false),
					 server->fdwid);

	/*
	 * Do not let this transaction route through its own uncommitted membership.
	 * Such a binding would outlive a rollback of this update to a savepoint.
	 * Keep the guard even if DDL/event-trigger execution fails and is caught.
	 */
	use->changed = true;
	command = psprintf("ALTER SERVER %s OPTIONS (SET members %s)",
					   quote_identifier(server->servername), quote_literal_cstr(option.data));
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");
	if (SPI_execute(command, false, 0) != SPI_OK_UTILITY)
		elog(ERROR, "could not update virtual server members");
	SPI_finish();
	PG_RETURN_VOID();
}
