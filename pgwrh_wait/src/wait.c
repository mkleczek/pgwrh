/* Copyright (C) 2026 Michal Kleczek. SPDX-License-Identifier: AGPL-3.0-or-later */
#include "postgres.h"
#include "access/xact.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "portability/instr_time.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/pg_lsn.h"
#include "utils/snapmgr.h"
#include "utils/wait_event.h"
#include "monitor.h"
#include "compat.h"

PG_FUNCTION_INFO_V1(pgwrh_wait_for_lsn);

static char *read_after_lsn;
static char *read_after_subscription;
static int wait_timeout_ms;
static bool accepting_barrier;
static uint32 wait_event;
static ProcessUtility_hook_type previous_utility;

static bool
check_read_after_lsn(char **newval, void **extra, GucSource source)
{
	bool invalid;
	XLogRecPtr lsn;

	if (**newval == '\0')
		return true;
	if (!accepting_barrier)
	{
		GUC_check_errdetail("Use an explicit SET LOCAL pgwrh.read_after_lsn before the first query in a transaction.");
		return false;
	}
	lsn = pgwrh_parse_lsn(*newval, &invalid);
	if (invalid || XLogRecPtrIsInvalid(lsn))
	{
		GUC_check_errdetail("A nonzero PostgreSQL LSN is required.");
		return false;
	}
	return true;
}

static void
wait_for_progress(const char *subscription, XLogRecPtr target, int timeout_ms)
{
	Oid subid;
	instr_time started;
	ConditionVariable *cv;

	if (XLogRecPtrIsInvalid(target) || timeout_ms < 0)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("a nonzero LSN and nonnegative timeout are required")));
	INSTR_TIME_SET_CURRENT(started);
	subid = pgwrh_subscription(subscription, true);
	if (!wait_event)
		wait_event = WaitEventExtensionNew("PgwrhWaitForLSN");
	cv = pgwrh_progress_changed();
	ConditionVariablePrepareToSleep(cv);
	PG_TRY();
	{
		for (;;)
		{
			long remaining;
			XLogRecPtr visible;
			instr_time elapsed;

			CHECK_FOR_INTERRUPTS();
			visible = pgwrh_progress(subid);
			if (visible >= target)
				break;
			INSTR_TIME_SET_CURRENT(elapsed);
			INSTR_TIME_SUBTRACT(elapsed, started);
			remaining = timeout_ms - (long) INSTR_TIME_GET_MILLISEC(elapsed);
			if (remaining <= 0)
				ereport(ERROR,
						(errcode(ERRCODE_QUERY_CANCELED),
						 errmsg("timed out waiting for subscription \"%s\" to reach %X/%X",
								subscription, LSN_FORMAT_ARGS(target)),
						 errdetail("Last visible publisher LSN: %X/%X.",
								   LSN_FORMAT_ARGS(visible))));
			ConditionVariableTimedSleep(cv, remaining, wait_event);
		}
	}
	PG_FINALLY();
	{
		ConditionVariableCancelSleep();
	}
	PG_END_TRY();
}

static void
process_utility(PlannedStmt *pstmt, const char *query_string,
				bool read_only_tree, ProcessUtilityContext context,
				ParamListInfo params, QueryEnvironment *query_env,
				DestReceiver *dest, QueryCompletion *qc)
{
	Node *node = pstmt->utilityStmt;
	bool barrier = false;

	if (IsA(node, VariableSetStmt))
	{
		VariableSetStmt *stmt = (VariableSetStmt *) node;

		barrier = stmt->name && strcmp(stmt->name, "pgwrh.read_after_lsn") == 0 &&
			stmt->kind == VAR_SET_VALUE;
		if (barrier)
		{
			pgwrh_require_monitor();
			if (!stmt->is_local || !IsTransactionBlock() ||
				context != PROCESS_UTILITY_TOPLEVEL || IsSubTransaction())
				ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
								errmsg("read watermark requires top-level SET LOCAL inside an explicit transaction")));
			if (FirstSnapshotSet || ActiveSnapshotSet())
				ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
								errmsg("read watermark must be set before the first transaction snapshot"),
								errhint("Begin a new transaction and set the watermark before executing a query.")));
		}
	}

	/* GUC assignment/restoration itself never blocks or changes shared state. */
	accepting_barrier = barrier;
	PG_TRY();
	{
		if (previous_utility)
			previous_utility(pstmt, query_string, read_only_tree, context,
							 params, query_env, dest, qc);
		else
			standard_ProcessUtility(pstmt, query_string, read_only_tree, context,
									params, query_env, dest, qc);
	}
	PG_FINALLY();
	{
		accepting_barrier = false;
	}
	PG_END_TRY();
	if (barrier && read_after_lsn[0])
	{
		bool invalid;
		XLogRecPtr target = pgwrh_parse_lsn(read_after_lsn, &invalid);

		/* Another utility hook must not have established a snapshot either. */
		if (FirstSnapshotSet || ActiveSnapshotSet())
			ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
							errmsg("a utility hook acquired a snapshot before the read watermark wait")));
		Assert(!invalid);
		wait_for_progress(read_after_subscription, target, wait_timeout_ms);
		Assert(!FirstSnapshotSet && !ActiveSnapshotSet());
	}
}

Datum
pgwrh_wait_for_lsn(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("wait_for_lsn arguments must not be null")));
	/* A SQL function cannot refresh its calling statement's snapshot. */
	if (IsolationUsesXactSnapshot())
		ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
						errmsg("wait_for_lsn cannot refresh a Repeatable Read or Serializable snapshot"),
						errhint("Use SET LOCAL pgwrh.read_after_lsn before the first query instead.")));
	wait_for_progress(text_to_cstring(PG_GETARG_TEXT_PP(0)), PG_GETARG_LSN(1),
					  PG_GETARG_INT32(2));
	PG_RETURN_VOID();
}

void
pgwrh_init_wait(void)
{
	DefineCustomStringVariable("pgwrh.read_after_subscription",
							   "Local subscription used by the read watermark barrier.",
							   NULL, &read_after_subscription, "pgwrh_replica_subscription",
							   PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("pgwrh.wait_timeout_ms", "Read watermark wait timeout.",
							NULL, &wait_timeout_ms, 10000, 0, PG_INT32_MAX,
							PGC_USERSET, GUC_UNIT_MS, NULL, NULL, NULL);
	DefineCustomStringVariable("pgwrh.read_after_lsn",
							   "Wait for this publisher LSN before the first read snapshot.",
							   NULL, &read_after_lsn, "", PGC_USERSET,
							   GUC_DISALLOW_IN_FILE | GUC_NO_RESET_ALL,
							   check_read_after_lsn, NULL, NULL);
	previous_utility = ProcessUtility_hook;
	ProcessUtility_hook = process_utility;
}
