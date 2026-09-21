/* SPDX-License-Identifier: AGPL-3.0-only */
#ifndef PGWRH_FDW_VIRTUAL_H
#define PGWRH_FDW_VIRTUAL_H

#include "foreign/foreign.h"
#include "libpq-fe.h"

typedef struct PgwrhFdwVirtualBinding PgwrhFdwVirtualBinding;

extern void pgwrh_fdw_validate_virtual_options(List *options, Oid catalog);
extern UserMapping *pgwrh_fdw_resolve_virtual_mapping(UserMapping *user,
												   PgwrhFdwVirtualBinding **binding);
extern void pgwrh_fdw_check_virtual_connection(PgwrhFdwVirtualBinding *binding,
											 PGconn *conn, int xact_depth);
extern void pgwrh_fdw_virtual_connected(PgwrhFdwVirtualBinding *binding,
										  PGconn *conn);

#endif
