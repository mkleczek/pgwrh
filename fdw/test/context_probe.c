/* SPDX-License-Identifier: AGPL-3.0-only */
/* Test-only receiver: validate utility ordering, types and privileges. */
#include "postgres.h"
#include "fmgr.h"
#include "tcop/utility.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"

PG_MODULE_MAGIC;

static ProcessUtility_hook_type previous_hook;
static int level;
static char *secret;
static char *token;

static bool
check_token(char **newval, void **extra, GucSource source)
{
	if (strcmp(*newval, "raise08001") == 0)
	{
		GUC_check_errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION);
		GUC_check_errmsg("context_probe: transaction context raised 08001");
		return false;
	}
	return true;
}

static void
probe_utility(PlannedStmt *pstmt, const char *queryString,
			  bool readOnlyTree, ProcessUtilityContext context,
			  ParamListInfo params, QueryEnvironment *queryEnv,
			  DestReceiver *dest, QueryCompletion *qc)
{
	if (IsA(pstmt->utilityStmt, VariableSetStmt))
	{
		VariableSetStmt *stmt = (VariableSetStmt *) pstmt->utilityStmt;

		if (stmt->is_local && strcmp(stmt->name, "app.request_id") == 0 &&
			FirstSnapshotSet)
			elog(ERROR, "context_probe: SET LOCAL arrived after snapshot acquisition");
	}
	if (previous_hook)
		previous_hook(pstmt, queryString, readOnlyTree, context, params,
					  queryEnv, dest, qc);
	else
		standard_ProcessUtility(pstmt, queryString, readOnlyTree, context,
								params, queryEnv, dest, qc);
}

void
_PG_init(void)
{
	DefineCustomIntVariable("ctxprobe.level", "Test integer", NULL,
							&level, 7, 0, 100, PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomStringVariable("ctxprobe.secret", "Test privileged string", NULL,
							   &secret, "remote-secret", PGC_SUSET,
							   GUC_SUPERUSER_ONLY, NULL, NULL, NULL);
	DefineCustomStringVariable("ctxprobe.token", "Test default", NULL,
							   &token, "default-token", PGC_USERSET,
							   0, check_token, NULL, NULL);
	MarkGUCPrefixReserved("ctxprobe");
	previous_hook = ProcessUtility_hook;
	ProcessUtility_hook = probe_utility;
}
