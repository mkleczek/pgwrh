/* Copyright (C) 2026 Michal Kleczek. SPDX-License-Identifier: AGPL-3.0-or-later */
#ifndef PGWRH_WAIT_COMPAT_H
#define PGWRH_WAIT_COMPAT_H

#include "catalog/pg_subscription_rel.h"
#include "replication/origin.h"
#include "storage/shmem.h"
#include "utils/pg_lsn.h"
#if PG_VERSION_NUM >= 190000
#include "nodes/miscnodes.h"
#endif

#if PG_VERSION_NUM < 180000 || PG_VERSION_NUM >= 200000
#error "pgwrh_wait supports PostgreSQL 18 and 19; use WITH_LSN_WAIT=0 for SQL-only pgwrh"
#endif

static inline XLogRecPtr
pgwrh_parse_lsn(const char *value, bool *invalid)
{
#if PG_VERSION_NUM >= 190000
	ErrorSaveContext context = {T_ErrorSaveContext};
	XLogRecPtr result = pg_lsn_in_safe(value, (Node *) &context);

	*invalid = context.error_occurred;
	return result;
#else
	return pg_lsn_in_internal(value, invalid);
#endif
}

static inline bool
pgwrh_has_current_origin(void)
{
#if PG_VERSION_NUM >= 190000
	return replorigin_xact_state.origin != InvalidReplOriginId;
#else
	return replorigin_session_origin != InvalidRepOriginId;
#endif
}

static inline HTAB *
pgwrh_init_progress_hash(const char *name, long capacity, HASHCTL *ctl, int flags)
{
#if PG_VERSION_NUM >= 190000
	return ShmemInitHash(name, capacity, ctl, flags);
#else
	return ShmemInitHash(name, capacity, capacity, ctl, flags);
#endif
}

static inline XLogRecPtr
pgwrh_current_origin_lsn(void)
{
#if PG_VERSION_NUM >= 190000
	return replorigin_xact_state.origin_lsn;
#else
	return replorigin_session_origin_lsn;
#endif
}

static inline bool
pgwrh_has_unready_tables(Oid subid)
{
#if PG_VERSION_NUM >= 190000
	/* Sequence synchronization does not establish table-read visibility. */
	return GetSubscriptionRelations(subid, true, false, true) != NIL;
#else
	return GetSubscriptionRelations(subid, true) != NIL;
#endif
}

#endif
