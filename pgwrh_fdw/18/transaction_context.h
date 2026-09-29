/* Copyright (c) 2026, pgwrh_fdw contributors. GNU AGPL version 3 only; see LICENSE. */
#ifndef PGWRH_FDW_TRANSACTION_CONTEXT_H
#define PGWRH_FDW_TRANSACTION_CONTEXT_H

#include "nodes/pg_list.h"
#include "libpq-fe.h"

extern void pgwrh_fdw_context_init(void);
extern List *pgwrh_fdw_parse_parameters(const char *value);
extern List *pgwrh_fdw_transaction_parameters(Oid serverid);
extern void pgwrh_fdw_apply_parameters(PGconn *conn, List *parameters);

#endif
