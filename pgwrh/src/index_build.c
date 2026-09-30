/*
 * Server-wide admission and top-level concurrent index workers.
 * Copyright (C) 2026 Michal Kleczek. SPDX-License-Identifier: AGPL-3.0-or-later
 */
#include "postgres.h"
#include "access/htup_details.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_class.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/dsm_registry.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lmgr.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/tuplestore.h"
#include "utils/wait_event.h"
#include "pgstat.h"

PG_FUNCTION_INFO_V1(pgwrh_launch_index_build);
PG_FUNCTION_INFO_V1(pgwrh_index_build_tasks);
PGDLLEXPORT void pgwrh_index_build_main(Datum arg);

typedef struct IndexSlot
{
	uint64 generation;
	Oid database;
	Oid relation;
	int64 job;
	int pid;
	ProcNumber proc;
	bool starting;
} IndexSlot;

typedef struct IndexAdmission
{
	LWLock lock;
	uint64 generation;
	IndexSlot slots[FLEXIBLE_ARRAY_MEMBER];
} IndexAdmission;

typedef struct IndexWorkerArgs
{
	int slot;
	uint64 generation;
	Oid database;
	Oid user;
	Oid relation;
	TransactionId xid;
	int64 job;
} IndexWorkerArgs;

static IndexAdmission *admission;
static IndexWorkerArgs worker_args;
static object_access_hook_type previous_object_hook;
static bool registering_index;

static int
slot_count(void)
{
	return max_worker_processes / 2;
}

static void
#if PG_VERSION_NUM >= 190000
init_admission(void *ptr, void *arg)
#else
init_admission(void *ptr)
#endif
{
	IndexAdmission *state = ptr;

#if PG_VERSION_NUM >= 190000
	LWLockInitialize(&state->lock, LWLockNewTrancheId("pgwrh index admission"));
#else
	LWLockInitialize(&state->lock, LWLockNewTrancheId());
#endif
}

static void
attach_admission(void)
{
	bool found;

	if (!admission)
	{
		admission = GetNamedDSMSegment("pgwrh index admission v1",
			offsetof(IndexAdmission, slots) + sizeof(IndexSlot) * slot_count(),
			init_admission, &found
#if PG_VERSION_NUM >= 190000
			, NULL
#endif
			);
#if PG_VERSION_NUM < 190000
		LWLockRegisterTranche(admission->lock.tranche, "pgwrh index admission");
#endif
	}
}

/* A generation fences workers whose launcher died before they could claim a
 * reservation. ProcNumber + PID also catches exits before our exit callback.
 * The DSM registry is reset on postmaster restart; durable jobs are SQL data. */
static void
reap_slots(void)
{
	for (int i = 0; i < slot_count(); i++)
	{
		IndexSlot *slot = &admission->slots[i];

		if (slot->pid && GetPGProcByNumber(slot->proc)->pid != slot->pid)
			slot->pid = 0;
	}
}

static void
release_slot(int code, Datum arg)
{
	IndexSlot *slot = &admission->slots[worker_args.slot];

	LWLockAcquire(&admission->lock, LW_EXCLUSIVE);
	if (slot->generation == worker_args.generation)
		slot->pid = 0;
	LWLockRelease(&admission->lock);
}

Datum
pgwrh_launch_index_build(PG_FUNCTION_ARGS)
{
	IndexWorkerArgs args = {0};
	BackgroundWorker worker = {0};
	BackgroundWorkerHandle *handle = NULL;
	int free_slot = -1;
	bool busy = false;

	args.job = PG_GETARG_INT64(0);
	args.database = MyDatabaseId;
	args.user = GetUserId();
	args.relation = PG_GETARG_OID(1);
	args.xid = GetCurrentTransactionId();
	attach_admission();
	LWLockAcquire(&admission->lock, LW_EXCLUSIVE);
	reap_slots();
	for (int i = 0; i < slot_count(); i++)
	{
		IndexSlot *slot = &admission->slots[i];

		if (!slot->pid)
			free_slot = i;
		else if (slot->database == MyDatabaseId &&
				 (slot->relation == args.relation || slot->job == args.job))
			busy = true;
	}
	if (busy || free_slot < 0)
	{
		LWLockRelease(&admission->lock);
		PG_RETURN_BOOL(false);
	}
	args.slot = free_slot;
	args.generation = ++admission->generation;
	admission->slots[free_slot] = (IndexSlot) {
		.generation = args.generation, .database = MyDatabaseId,
		.relation = args.relation, .job = args.job,
		.pid = MyProcPid, .proc = MyProcNumber, .starting = true
	};
	LWLockRelease(&admission->lock);

	PG_TRY();
	{
		worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
		worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
		worker.bgw_restart_time = BGW_NEVER_RESTART;
		worker.bgw_notify_pid = MyProcPid;
		strlcpy(worker.bgw_library_name, "pgwrh", BGW_MAXLEN);
		strlcpy(worker.bgw_function_name, "pgwrh_index_build_main", BGW_MAXLEN);
		strlcpy(worker.bgw_type, "pgwrh index build", BGW_MAXLEN);
		snprintf(worker.bgw_name, BGW_MAXLEN, "pgwrh index build %u/" INT64_FORMAT,
				 MyDatabaseId, args.job);
		StaticAssertStmt(sizeof(args) <= BGW_EXTRALEN, "index worker arguments too large");
		memcpy(worker.bgw_extra, &args, sizeof(args));
		if (!RegisterDynamicBackgroundWorker(&worker, &handle))
			ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
							errmsg("no background worker slot available for index build")));

		/* Return only after the worker owns the reservation. It will wait for
		 * our transaction before reading the job, including on caller rollback. */
		for (;;)
		{
			bool claimed;
			pid_t pid;

			CHECK_FOR_INTERRUPTS();
			LWLockAcquire(&admission->lock, LW_SHARED);
			claimed = admission->slots[free_slot].generation != args.generation ||
				admission->slots[free_slot].pid != MyProcPid;
			LWLockRelease(&admission->lock);
			if (claimed)
				break;
			if (GetBackgroundWorkerPid(handle, &pid) == BGWH_STOPPED)
				ereport(ERROR, (errmsg("index worker exited before claiming its reservation")));
			(void) WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
							 10L, PG_WAIT_EXTENSION);
			ResetLatch(MyLatch);
		}
		pfree(handle);
	}
	PG_CATCH();
	{
		LWLockAcquire(&admission->lock, LW_EXCLUSIVE);
		if (admission->slots[free_slot].generation == args.generation &&
			admission->slots[free_slot].pid == MyProcPid)
			admission->slots[free_slot].pid = 0;
		LWLockRelease(&admission->lock);
		PG_RE_THROW();
	}
	PG_END_TRY();
	PG_RETURN_BOOL(true);
}

Datum
pgwrh_index_build_tasks(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	IndexSlot *slots;

	InitMaterializedSRF(fcinfo, 0);
	attach_admission();
	slots = palloc(sizeof(IndexSlot) * slot_count());
	LWLockAcquire(&admission->lock, LW_EXCLUSIVE);
	reap_slots();
	memcpy(slots, admission->slots, sizeof(IndexSlot) * slot_count());
	LWLockRelease(&admission->lock);
	for (int i = 0; i < slot_count(); i++)
	{
		IndexSlot *slot = &slots[i];
		Datum values[5];
		bool nulls[5] = {false};

		if (!slot->pid)
			continue;
		values[0] = ObjectIdGetDatum(slot->database);
		values[1] = ObjectIdGetDatum(slot->relation);
		values[2] = Int64GetDatum(slot->job);
		values[3] = Int32GetDatum(slot->pid);
		values[4] = BoolGetDatum(slot->starting);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}
	PG_RETURN_NULL();
}

/* Capture provenance in the FIRST catalog transaction of CIC. Successful and
 * failed builds are both registered atomically with their catalog entry. There
 * is no name-based adoption window between creation and registration. This
 * hook is installed only in our worker while executing its CREATE INDEX. */
static void
register_created_index(ObjectAccessType access, Oid classid, Oid objid, int subid, void *arg)
{
	if (previous_object_hook)
		previous_object_hook(access, classid, objid, subid, arg);
	if (registering_index && access == OAT_POST_CREATE &&
		classid == RelationRelationId && subid == 0)
	{
		Oid types[] = {INT8OID, OIDOID};
		Datum values[] = {Int64GetDatum(worker_args.job), ObjectIdGetDatum(objid)};

		CommandCounterIncrement();
		SPI_connect();
		SPI_execute_with_args("SELECT pgwrh.register_index_build($1, $2)",
							  2, types, values, NULL, false, 0);
		SPI_finish();
	}
}

/* Unlike SPI or a multi-command pg_background script, a portal is a top-level
 * command and lets PostgreSQL commit the phases of CREATE/DROP CONCURRENTLY. */
static void
run_index_utility(const char *command)
{
	MemoryContext context = AllocSetContextCreate(TopMemoryContext,
													 "pgwrh index command", ALLOCSET_DEFAULT_SIZES);
	MemoryContext old;
	List *raw;
	List *plans;
	Portal portal;
	QueryCompletion qc;
	DestReceiver *dest = CreateDestReceiver(DestNone);

	old = MemoryContextSwitchTo(context);
	raw = pg_parse_query(command);
	if (list_length(raw) != 1 ||
		(!IsA(linitial_node(RawStmt, raw)->stmt, IndexStmt) &&
		 !IsA(linitial_node(RawStmt, raw)->stmt, DropStmt)))
		elog(ERROR, "expected a single index utility command");
	plans = pg_plan_queries(pg_analyze_and_rewrite_fixedparams(linitial(raw), command,
																NULL, 0, NULL), command, 0, NULL);
	portal = CreatePortal("", true, true);
	portal->visible = false;
	PortalDefineQuery(portal, NULL, command, CreateCommandTag(linitial_node(RawStmt, raw)->stmt), plans, NULL);
	PortalStart(portal, NULL, 0, InvalidSnapshot);
	MemoryContextSwitchTo(old);
	pgstat_report_activity(STATE_RUNNING, command);
	PortalRun(portal, FETCH_ALL, true, dest, dest, &qc);
	PortalDrop(portal, false);
	dest->rDestroy(dest);
	CommitTransactionCommand();
	MemoryContextDelete(context);
}

static char *
job_query(const char *sql, bool keep_transaction)
{
	Oid types[] = {INT8OID, OIDOID};
	Datum values[] = {Int64GetDatum(worker_args.job), ObjectIdGetDatum(worker_args.relation)};
	char *result = NULL;

	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	SPI_execute_with_args(sql, 2, types, values, NULL, false, 1);
	if (SPI_processed && SPI_tuptable)
	{
		char *value = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);

		if (value)
			result = MemoryContextStrdup(TopMemoryContext, value);
	}
	PopActiveSnapshot();
	SPI_finish();
	if (!keep_transaction || !result)
		CommitTransactionCommand();
	return result;
}

void
pgwrh_index_build_main(Datum arg)
{
	IndexSlot *slot;

	memcpy(&worker_args, MyBgworkerEntry->bgw_extra, sizeof(worker_args));
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();
	attach_admission();
	slot = &admission->slots[worker_args.slot];
	LWLockAcquire(&admission->lock, LW_EXCLUSIVE);
	if (!slot->pid || slot->generation != worker_args.generation)
	{
		LWLockRelease(&admission->lock);
		proc_exit(0);
	}
	slot->pid = MyProcPid;
	slot->proc = MyProcNumber;
	slot->starting = false;
	LWLockRelease(&admission->lock);
	before_shmem_exit(release_slot, (Datum) 0);
	BackgroundWorkerInitializeConnectionByOid(worker_args.database, worker_args.user, 0);
	SetConfigOption("search_path", "pg_catalog", PGC_SUSET, PGC_S_SESSION);
	SetConfigOption("application_name", "pgwrh index build", PGC_USERSET, PGC_S_SESSION);
	SetConfigOption("max_parallel_maintenance_workers", "0", PGC_USERSET, PGC_S_SESSION);
	StartTransactionCommand();
	XactLockTableWait(worker_args.xid, NULL, NULL, XLTW_None);
	CommitTransactionCommand();
	if (!TransactionIdDidCommit(worker_args.xid))
		proc_exit(0);

	PG_TRY();
	{
		char *command;

		command = job_query("SELECT pgwrh.index_build_command($1, $2, true)", true);
		if (command)
			run_index_utility(command);
		command = job_query("SELECT pgwrh.index_build_command($1, $2, false)", true);
		if (command)
		{
			previous_object_hook = object_access_hook;
			object_access_hook = register_created_index;
			registering_index = true;
			run_index_utility(command);
			registering_index = false;
			object_access_hook = previous_object_hook;
		}
		job_query("SELECT pgwrh.finish_index_build($1)", false);
	}
	PG_CATCH();
	{
		ErrorData *error;
		Oid types[] = {INT8OID, TEXTOID, TEXTOID};
		Datum values[3];

		MemoryContextSwitchTo(TopMemoryContext);
		error = CopyErrorData();
		FlushErrorState();
		registering_index = false;
		AbortCurrentTransaction();
		values[0] = Int64GetDatum(worker_args.job);
		values[1] = CStringGetTextDatum(unpack_sql_state(error->sqlerrcode));
		values[2] = CStringGetTextDatum(error->message);
		StartTransactionCommand();
		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());
		SPI_execute_with_args("SELECT pgwrh.fail_index_build($1, $2, $3)",
							  3, types, values, NULL, false, 0);
		PopActiveSnapshot();
		SPI_finish();
		CommitTransactionCommand();
		ereport(LOG, (errmsg("pgwrh index build " INT64_FORMAT " failed: %s",
							worker_args.job, error->message)));
		FreeErrorData(error);
	}
	PG_END_TRY();
	proc_exit(0);
}
