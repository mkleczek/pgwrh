/* Copyright (C) 2026 Michal Kleczek. SPDX-License-Identifier: AGPL-3.0-or-later */
#include "postgres.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "fmgr.h"
#include "utils/pg_lsn.h"
#include "monitor.h"

PG_FUNCTION_INFO_V1(pgwrh_last_commit_lsn);

/* Backend-local; inherited as InvalidXLogRecPtr from the postmaster. */
static XLogRecPtr last_commit_lsn = InvalidXLogRecPtr;

static void
remember_commit(XactEvent event, void *arg)
{
	/*
	 * RecordTransactionCommit has set XactLastCommitEnd before this callback.
	 * Without an XID it can instead point to maintenance WAL (e.g. pruning),
	 * for which no commit record was emitted. Preserve our previous token.
	 * Aborts, subtransactions and PREPARE do not establish a new commit.
	 */
	if (event == XACT_EVENT_COMMIT &&
		TransactionIdIsValid(GetTopTransactionIdIfAny()))
		last_commit_lsn = XactLastCommitEnd;
}

Datum
pgwrh_last_commit_lsn(PG_FUNCTION_ARGS)
{
	/* Preloading ensures the callback observed all commits in this session. */
	pgwrh_require_monitor();
	if (XLogRecPtrIsInvalid(last_commit_lsn))
		PG_RETURN_NULL();
	PG_RETURN_LSN(last_commit_lsn);
}

void
pgwrh_init_commit(void)
{
	RegisterXactCallback(remember_commit, NULL);
}
