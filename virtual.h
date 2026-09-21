/* SPDX-License-Identifier: AGPL-3.0-only */
#ifndef PGWRH_FDW_VIRTUAL_H
#define PGWRH_FDW_VIRTUAL_H

#include "foreign/foreign.h"
#include "libpq-fe.h"

typedef struct PgwrhFdwVirtualBinding PgwrhFdwVirtualBinding;

/* Higher ranks are preferred; negative ranks cannot accept a new binding. */
typedef enum PgwrhFdwConnectionRank
{
	PGWRH_FDW_CONNECTION_UNUSABLE = -1,
	PGWRH_FDW_CONNECTION_NEW,
	PGWRH_FDW_CONNECTION_IDLE,
	PGWRH_FDW_CONNECTION_ACTIVE
} PgwrhFdwConnectionRank;

typedef PgwrhFdwConnectionRank (*PgwrhFdwRankConnection) (Oid umid);

struct PgFdwConnState;

extern PgwrhFdwConnectionRank pgwrh_fdw_rank_cached_connection(Oid umid);
extern bool pgwrh_fdw_is_virtual_server(Oid serverid);
extern List *pgwrh_fdw_routing_members(Oid serverid, Oid userid);
extern List *pgwrh_fdw_common_targets(List *serverids, Oid userid);
extern PGconn *pgwrh_fdw_group_connection(List *serverids, Oid userid,
										struct PgFdwConnState **state, bool bind);

extern void pgwrh_fdw_validate_virtual_options(List *options, Oid catalog);
extern UserMapping *pgwrh_fdw_resolve_virtual_mapping(UserMapping *user,
												   PgwrhFdwRankConnection rank_connection,
												   PgwrhFdwVirtualBinding **binding);
extern void pgwrh_fdw_check_virtual_connection(PgwrhFdwVirtualBinding *binding,
											 PGconn *conn, int xact_depth);
extern void pgwrh_fdw_virtual_connected(PgwrhFdwVirtualBinding *binding,
										  PGconn *conn);

#endif
