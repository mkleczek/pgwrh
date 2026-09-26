/*
 * Restart database-local SQL daemons after server/worker failure.
 * Copyright (C) 2026 Michal Kleczek. SPDX-License-Identifier: AGPL-3.0-or-later
 */
#include "postgres.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timeout.h"
#include "utils/wait_event.h"

PG_MODULE_MAGIC;
void _PG_init(void);
PGDLLEXPORT void pgwrh_supervisor_main(Datum arg);
PGDLLEXPORT void pgwrh_database_check_main(Datum arg);

static char *supervisor_database = NULL;

void
_PG_init(void)
{
	BackgroundWorker worker = {0};

	/* lock_managed_object() also loads this library on demand. Only a preload
	 * may define a PGC_POSTMASTER variable: doing so later is a FATAL error. */
	if (!process_shared_preload_libraries_in_progress)
		return;
	DefineCustomStringVariable("pgwrh.supervisor_database",
		"Database used to discover databases needing pgwrh supervision.",
		NULL, &supervisor_database, "postgres", PGC_POSTMASTER, 0,
		NULL, NULL, NULL);
	if (IsBinaryUpgrade)
		return;

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 1;
	strlcpy(worker.bgw_library_name, "pgwrh", BGW_MAXLEN);
	strlcpy(worker.bgw_function_name, "pgwrh_supervisor_main", BGW_MAXLEN);
	strlcpy(worker.bgw_name, "pgwrh supervisor", BGW_MAXLEN);
	strlcpy(worker.bgw_type, "pgwrh supervisor", BGW_MAXLEN);
	RegisterBackgroundWorker(&worker);
}

void
pgwrh_supervisor_main(Datum arg)
{
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();
	BackgroundWorkerInitializeConnection(supervisor_database, NULL, 0);
	SetConfigOption("search_path", "pg_catalog", PGC_SUSET, PGC_S_SESSION);

	for (;;)
	{
		MemoryContext cycle = AllocSetContextCreate(TopMemoryContext,
												   "pgwrh discovery", ALLOCSET_SMALL_SIZES);
		Oid *databases;
		uint64 count;

		CHECK_FOR_INTERRUPTS();
		StartTransactionCommand();
		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());
		SPI_execute("SELECT oid FROM pg_catalog.pg_database "
					"WHERE datallowconn AND NOT datistemplate ORDER BY oid", true, 0);
		count = SPI_processed;
		databases = MemoryContextAlloc(cycle, sizeof(Oid) * count);
		for (uint64 i = 0; i < count; i++)
		{
			bool isnull;
			databases[i] = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[i],
														SPI_tuptable->tupdesc, 1, &isnull));
		}
		PopActiveSnapshot();
		SPI_finish();
		CommitTransactionCommand();

		/* One short-lived check at a time: no permanent worker slot is consumed
		 * by each empty database. Failed checks are retried on the next scan. */
		for (uint64 i = 0; i < count; i++)
		{
			BackgroundWorker worker = {0};
			BackgroundWorkerHandle *handle;

			CHECK_FOR_INTERRUPTS();
			worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
			worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
			worker.bgw_restart_time = BGW_NEVER_RESTART;
			worker.bgw_main_arg = ObjectIdGetDatum(databases[i]);
			worker.bgw_notify_pid = MyProcPid;
			strlcpy(worker.bgw_library_name, "pgwrh", BGW_MAXLEN);
			strlcpy(worker.bgw_function_name, "pgwrh_database_check_main", BGW_MAXLEN);
			snprintf(worker.bgw_name, BGW_MAXLEN, "pgwrh check database %u", databases[i]);
			strlcpy(worker.bgw_type, "pgwrh database check", BGW_MAXLEN);
			if (!RegisterDynamicBackgroundWorker(&worker, &handle))
				break; /* Worker-slot exhaustion must not kill the launcher. */
			WaitForBackgroundWorkerShutdown(handle);
			pfree(handle);
		}
		MemoryContextDelete(cycle);
		(void) WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 1000L, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
	}
}

void
pgwrh_database_check_main(Datum arg)
{
	Oid owner = InvalidOid;
	bool isnull;

	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();
	BackgroundWorkerInitializeConnectionByOid(DatumGetObjectId(arg), InvalidOid, 0);
	/* This runs as a superuser in databases whose owners may be untrusted:
	 * resolve operators and functions in pg_catalog only, never in public. */
	SetConfigOption("search_path", "pg_catalog", PGC_SUSET, PGC_S_SESSION);
	StartTransactionCommand();
	/* Bound the check with a timer, not the statement_timeout setting:
	 * pg_background copies settings into a daemon started below. */
	enable_timeout_after(STATEMENT_TIMEOUT, 5000);
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	/* Never execute a same-named function planted by an untrusted DB owner.
	 * The entry point must belong to pgwrh, installed by a superuser. */
	SPI_execute("SELECT e.extowner FROM pg_catalog.pg_extension e "
				"JOIN pg_catalog.pg_roles r ON r.oid = e.extowner AND r.rolsuper "
				"JOIN pg_catalog.pg_depend d ON d.refclassid = 'pg_catalog.pg_extension'::regclass::oid "
				"AND d.refobjid = e.oid AND d.deptype = 'e' "
				"JOIN pg_catalog.pg_proc p ON d.classid = 'pg_catalog.pg_proc'::regclass::oid AND d.objid = p.oid "
				"JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace "
				"WHERE e.extname = 'pgwrh' AND n.nspname = 'pgwrh' "
				"AND p.proname = 'supervise_sync_daemon' AND p.pronargs = 0", true, 1);
	if (SPI_processed)
		owner = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[0],
												 SPI_tuptable->tupdesc, 1, &isnull));
	if (OidIsValid(owner))
	{
		Oid saved_user;
		int saved_context;

		GetUserIdAndSecContext(&saved_user, &saved_context);
		SetUserIdAndSecContext(owner, saved_context | SECURITY_LOCAL_USERID_CHANGE);
		SPI_execute("SELECT pgwrh.supervise_sync_daemon()", false, 0);
		SetUserIdAndSecContext(saved_user, saved_context);
	}
	PopActiveSnapshot();
	SPI_finish();
	CommitTransactionCommand();
	proc_exit(0);
}
