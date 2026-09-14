/*
 * Commit visibility monitoring for built-in logical replication.
 * Copyright (C) 2026 Michal Kleczek. SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Never publish from PRE_COMMIT: origin progress advances before the applying
 * transaction leaves ProcArray. COMMIT is the first safe transaction callback.
 */
#include "postgres.h"
#include "access/xact.h"
#include "catalog/pg_subscription.h"
#include "catalog/pg_subscription_rel.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "replication/origin.h"
#include "replication/worker_internal.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/pg_lsn.h"
#include "utils/syscache.h"
#include "monitor.h"

#if PG_VERSION_NUM < 180000 || PG_VERSION_NUM >= 190000
#error "pgwrh_wait supports PostgreSQL 18; use WITH_LSN_WAIT=0 for SQL-only pgwrh"
#endif

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(pgwrh_applied_lsn);
void _PG_init(void);

typedef struct ProgressKey
{
	Oid dbid;
	Oid subid;
} ProgressKey;

typedef struct ProgressEntry
{
	ProgressKey key;
	XLogRecPtr lsn;
} ProgressEntry;

static int max_subscriptions = 256;
static HTAB *progress_table;
static LWLock *progress_lock;
static ConditionVariable *progress_cv;
static shmem_request_hook_type previous_request;
static shmem_startup_hook_type previous_startup;

/* Backend-local, prepared while errors and allocations are still safe. */
static ProgressEntry *pending_entry;
static XLogRecPtr pending_lsn;
static bool pending_bootstrap;
static bool origin_initialized;

static void
request_shared_memory(void)
{
	if (previous_request)
		previous_request();
	RequestAddinShmemSpace(hash_estimate_size(max_subscriptions,
											 sizeof(ProgressEntry)) +
						  MAXALIGN(sizeof(ConditionVariable)));
	RequestNamedLWLockTranche("pgwrh_wait", 1);
}

static void
startup_shared_memory(void)
{
	HASHCTL ctl;
	bool found;

	if (previous_startup)
		previous_startup();
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	progress_cv = ShmemInitStruct("pgwrh_wait condition", sizeof(ConditionVariable),
								 &found);
	if (!found)
		ConditionVariableInit(progress_cv);
	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(ProgressKey);
	ctl.entrysize = sizeof(ProgressEntry);
	progress_table = ShmemInitHash("pgwrh_wait progress", max_subscriptions,
								  max_subscriptions, &ctl,
								  HASH_ELEM | HASH_BLOBS | HASH_FIXED_SIZE);
	progress_lock = &GetNamedLWLockTranche("pgwrh_wait")[0].lock;
	LWLockRelease(AddinShmemInitLock);
}

static void
transaction_callback(XactEvent event, void *arg)
{
	if (event == XACT_EVENT_PRE_COMMIT)
	{
		ProgressKey key;
		bool found;

		pending_entry = NULL;
		pending_bootstrap = false;
		if (!MyLogicalRepWorker || !MySubscription ||
			am_tablesync_worker() ||
			MySubscription->twophasestate != LOGICALREP_TWOPHASE_STATE_DISABLED ||
			replorigin_session_origin == InvalidRepOriginId)
			return;

		/*
		 * run_apply_worker() commits origin setup before connecting upstream
		 * or launching parallel workers. The leader exclusively acquired the
		 * origin. Only a NEW shared entry may use recovered progress: on a
		 * worker restart an old parallel worker might still be completing
		 * commit after advancing the origin. Existing entries retain their
		 * callback-confirmed position. Never bootstrap in a parallel worker.
		 */
		if (!origin_initialized && am_leader_apply_worker() &&
			LogRepWorkerWalRcvConn == NULL)
		{
			pending_lsn = replorigin_session_get_progress(false);
			pending_bootstrap = true;
		}
		else
			pending_lsn = replorigin_session_origin_lsn;

		/* Spooling and worker housekeeping are not applied data commits. */
		if (!pending_bootstrap && !TransactionIdIsValid(GetTopTransactionIdIfAny()))
			return;
		if (XLogRecPtrIsInvalid(pending_lsn) && !pending_bootstrap)
			return;
		key.dbid = MyDatabaseId;
		key.subid = MyLogicalRepWorker->subid;
		LWLockAcquire(progress_lock, LW_EXCLUSIVE);
		pending_entry = hash_search(progress_table, &key, HASH_FIND, NULL);
		if (pending_entry && pending_bootstrap)
			pending_entry = NULL;
		else if (!pending_entry && hash_get_num_entries(progress_table) < max_subscriptions)
		{
			pending_entry = hash_search(progress_table, &key, HASH_ENTER_NULL, &found);
			if (pending_entry && !found)
				pending_entry->lsn = InvalidXLogRecPtr;
		}
		LWLockRelease(progress_lock);
		/* A full monitor must not break logical replication. Readers error. */
	}
	else if (event == XACT_EVENT_COMMIT)
	{
		if (pending_entry)
		{
			/* No catalog access, allocation or error reporting after commit. */
			LWLockAcquire(progress_lock, LW_EXCLUSIVE);
			if (pending_bootstrap || pending_lsn > pending_entry->lsn)
				pending_entry->lsn = pending_lsn;
			LWLockRelease(progress_lock);
			ConditionVariableBroadcast(progress_cv);
		}
		if (pending_bootstrap)
			origin_initialized = true;
		pending_entry = NULL;
		pending_bootstrap = false;
	}
	else if (event == XACT_EVENT_ABORT || event == XACT_EVENT_PREPARE)
	{
		pending_entry = NULL;
		pending_bootstrap = false;
	}
}

void
pgwrh_require_monitor(void)
{
	if (progress_table == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pgwrh_wait must be loaded via shared_preload_libraries"),
				 errhint("Add pgwrh_wait to shared_preload_libraries and restart PostgreSQL.")));
}

Oid
pgwrh_subscription(const char *name, bool require_ready)
{
	HeapTuple tuple;
	Form_pg_subscription sub;
	Oid subid;

	pgwrh_require_monitor();
	subid = get_subscription_oid(name, false);
	/* Keep subscription identity/options stable until the reader commits. */
	LockSharedObject(SubscriptionRelationId, subid, 0, AccessShareLock);
	tuple = SearchSysCache1(SUBSCRIPTIONOID, ObjectIdGetDatum(subid));
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("subscription \"%s\" no longer exists", name)));
	sub = (Form_pg_subscription) GETSTRUCT(tuple);
	if (sub->subdbid != MyDatabaseId ||
		sub->subtwophasestate != LOGICALREP_TWOPHASE_STATE_DISABLED)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pgwrh_wait requires a local subscription with two_phase disabled")));
	if (!XLogRecPtrIsInvalid(sub->subskiplsn))
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("cannot wait on a subscription with a pending skipped transaction")));
	ReleaseSysCache(tuple);
	if (require_ready && GetSubscriptionRelations(subid, true) != NIL)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("subscription \"%s\" has tables that are not ready", name),
						errhint("Finish initial table synchronization before waiting for a read watermark.")));
	return subid;
}

XLogRecPtr
pgwrh_progress(Oid subid)
{
	ProgressKey key = {MyDatabaseId, subid};
	ProgressEntry *entry;
	XLogRecPtr result = InvalidXLogRecPtr;
	bool full;

	LWLockAcquire(progress_lock, LW_SHARED);
	entry = hash_search(progress_table, &key, HASH_FIND, NULL);
	if (entry)
		result = entry->lsn;
	full = !entry && hash_get_num_entries(progress_table) >= max_subscriptions;
	LWLockRelease(progress_lock);
	if (full)
		ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
						errmsg("pgwrh_wait subscription capacity exhausted"),
						errhint("Increase pgwrh.max_tracked_subscriptions and restart PostgreSQL.")));
	return result;
}

ConditionVariable *
pgwrh_progress_changed(void)
{
	return progress_cv;
}

Datum
pgwrh_applied_lsn(PG_FUNCTION_ARGS)
{
	Oid subid = pgwrh_subscription(text_to_cstring(PG_GETARG_TEXT_PP(0)), false);
	XLogRecPtr lsn = pgwrh_progress(subid);

	if (XLogRecPtrIsInvalid(lsn))
		PG_RETURN_NULL();
	PG_RETURN_LSN(lsn);
}

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;
	DefineCustomIntVariable("pgwrh.max_tracked_subscriptions",
							"Maximum subscription identities tracked until restart.",
							NULL, &max_subscriptions, 256, 1, 65536,
							PGC_POSTMASTER, 0, NULL, NULL, NULL);
	previous_request = shmem_request_hook;
	shmem_request_hook = request_shared_memory;
	previous_startup = shmem_startup_hook;
	shmem_startup_hook = startup_shared_memory;
	RegisterXactCallback(transaction_callback, NULL);
}
