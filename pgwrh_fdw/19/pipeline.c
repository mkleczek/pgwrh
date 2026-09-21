/*
 * Copyright (c) 2026, pgwrh_fdw contributors.
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * Connection-owned, ordered libpq operations.  This layer knows nothing about
 * executor callbacks or tuple conversion.  A result can become ready while a
 * different scan is using the connection; only its owner may take it.
 */
#include "postgres.h"
#include "varatt.h"

#include "access/xact.h"
#include "catalog/pg_type_d.h"
#include "executor/tuptable.h"
#include "lib/ilist.h"
#include "libpq/libpq-be-fe-helpers.h"
#include "miscadmin.h"
#include "postgres_fdw.h"
#include "storage/latch.h"
#include "utils/memutils.h"
#include "utils/builtins.h"
#include "utils/resowner.h"
#include "utils/tuplestore.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

struct PgFdwPendingOperation
{
	dlist_node all_node;
	dlist_node wire_node;
	PgFdwPipeline *pipeline;
	char	   *sql;
	ExecStatusType expected;
	PGresult   *result;
	int			nestlevel;
	bool		done;
	bool		failed;
	int			chunk_size; /* zero for ordinary operations */
	bool		row_mode_set;
	bool		discard_rows;
	PGresult   *chunk;
	PGresult   *row_description;
	Tuplestorestate *store;
	MemoryContext store_context;
	ResourceOwner store_owner;
	TupleDesc	store_desc;
	TupleTableSlot *store_slot;
	uint64		stored_rows;
};

struct PgFdwPipeline
{
	MemoryContext context;
	MemoryContextCallback cleanup;
	PgFdwConnState *state;
	PGconn	   *conn;
	dlist_head operations; /* includes completed, unclaimed results */
	dlist_head wire;       /* commands and Syncs awaiting protocol completion */
	int			fetches;
	bool		active;
	bool		flush_pending;
	bool		broken;
	int			old_nonblocking;
};

static uint32 pipeline_wait_event;

static void
pipeline_cleanup(void *arg)
{
	PgFdwPipeline *pipeline = arg;
	/* PG19 result wrappers register their own reset callbacks. */
	/* A disconnected/replaced cache entry may already own another queue. */
	if (pipeline->state->pipeline == pipeline)
		pipeline->state->pipeline = NULL;
}

static PgFdwPipeline *
pipeline_get(PGconn *conn, PgFdwConnState *state)
{
	PgFdwPipeline *pipeline = state->pipeline;

	if (pipeline == NULL)
	{
		MemoryContext context;

		context = AllocSetContextCreate(TopTransactionContext,
									   "postgres_fdw pipeline",
									   ALLOCSET_DEFAULT_SIZES);
		pipeline = MemoryContextAllocZero(context, sizeof(*pipeline));
		pipeline->context = context;
		pipeline->state = state;
		pipeline->conn = conn;
		dlist_init(&pipeline->operations);
		dlist_init(&pipeline->wire);
		pipeline->cleanup.func = pipeline_cleanup;
		pipeline->cleanup.arg = pipeline;
		MemoryContextRegisterResetCallback(context, &pipeline->cleanup);
		state->pipeline = pipeline;
	}
	Assert(pipeline->conn == conn);
	if (pipeline_wait_event == 0)
		pipeline_wait_event = WaitEventExtensionNew("PostgresFdwPipeline");
	return pipeline;
}

static void
pipeline_error(PgFdwPipeline *pipeline)
{
	pgfdw_report_error(NULL, pipeline->conn, NULL);
}

static void
pipeline_enter(PgFdwPipeline *pipeline)
{
	if (pipeline->active)
		return;
	pipeline->old_nonblocking = PQisnonblocking(pipeline->conn);
	if (!PQenterPipelineMode(pipeline->conn))
		pipeline_error(pipeline);
	pipeline->active = true;
	if (PQsetnonblocking(pipeline->conn, 1) != 0)
		pipeline_error(pipeline);
}

static PgFdwPendingOperation *
pipeline_operation(PgFdwPipeline *pipeline, const char *sql,
				   ExecStatusType expected)
{
	PgFdwPendingOperation *op;

	op = MemoryContextAllocZero(pipeline->context, sizeof(*op));
	op->pipeline = pipeline;
	op->sql = sql ? MemoryContextStrdup(pipeline->context, sql) : NULL;
	op->expected = expected;
	op->nestlevel = GetCurrentTransactionNestLevel();
	dlist_push_tail(&pipeline->operations, &op->all_node);
	dlist_push_tail(&pipeline->wire, &op->wire_node);
	if (expected == PGRES_TUPLES_OK)
		pipeline->fetches++;
	return op;
}

/* No user-defined input functions or executor callbacks run in this layer. */
static void
pipeline_clear_stream(PgFdwPendingOperation *op)
{
	PQclear(op->chunk);
	op->chunk = NULL;
	PQclear(op->row_description);
	op->row_description = NULL;
	/*
	 * BufFileClose flushes dirty buffers, which can fail again after a spill
	 * error (notably temp_file_limit).  Release the file resource directly,
	 * then its private memory, without trying to write discarded rows.
	 */
	if (op->store_owner)
	{
		ResourceOwnerRelease(op->store_owner, RESOURCE_RELEASE_BEFORE_LOCKS, false, false);
		ResourceOwnerRelease(op->store_owner, RESOURCE_RELEASE_LOCKS, false, false);
		ResourceOwnerRelease(op->store_owner, RESOURCE_RELEASE_AFTER_LOCKS, false, false);
		ResourceOwnerDelete(op->store_owner);
		op->store_owner = NULL;
	}
	if (op->store_context)
		MemoryContextDelete(op->store_context);
	op->store_context = NULL;
	op->store = NULL;
	op->store_slot = NULL;
	op->store_desc = NULL;
	op->stored_rows = 0;
}

/*
 * Save wire-format text, not locally converted Datums.  Spilling must belong
 * to the top-level transaction: a nested query can drain an outer scan, then
 * roll back its savepoint while the outer scan still needs these rows.
 */
static void
pipeline_store_chunk(PgFdwPendingOperation *op)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(op->pipeline->context);
	ResourceOwner oldowner = CurrentResourceOwner;
	PGresult   *res = op->chunk;
	int			nfields = PQnfields(res);
	Datum	   *values;
	bool	   *nulls;
	bool		was_failed = op->failed;

	/* A partial write must never be mistaken for a complete result. */
	op->failed = true;
	if (!op->store_context)
		op->store_context = AllocSetContextCreate(op->pipeline->context,
												   "postgres_fdw stream buffer", ALLOCSET_DEFAULT_SIZES);
	if (!op->store_owner)
		op->store_owner = ResourceOwnerCreate(TopTransactionResourceOwner,
												 "postgres_fdw stream buffer");
	CurrentResourceOwner = op->store_owner;
	MemoryContextSwitchTo(op->store_context);
	PG_TRY();
	{
		if (!op->store)
		{
			op->row_description = libpqsrv_PQwrap(PQcopyResult(res->res, PG_COPYRES_ATTRS));
			if (!op->row_description)
				ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
			op->store_desc = CreateTemplateTupleDesc(nfields);
			for (int col = 0; col < nfields; col++)
				TupleDescInitEntry(op->store_desc, col + 1, NULL, TEXTOID, -1, 0);
			TupleDescFinalize(op->store_desc);
			op->store = tuplestore_begin_heap(false, false, work_mem);
			tuplestore_set_eflags(op->store, 0);
			op->store_slot = MakeSingleTupleTableSlot(op->store_desc, &TTSOpsMinimalTuple);
		}
		values = palloc(sizeof(Datum) * nfields);
		nulls = palloc(sizeof(bool) * nfields);
		for (int row = 0; row < PQntuples(res); row++)
		{
			CHECK_FOR_INTERRUPTS();
			for (int col = 0; col < nfields; col++)
			{
				nulls[col] = PQgetisnull(res, row, col);
				values[col] = nulls[col] ? (Datum) 0 :
					PointerGetDatum(cstring_to_text_with_len(PQgetvalue(res, row, col),
													 PQgetlength(res, row, col)));
			}
			tuplestore_putvalues(op->store, op->store_desc, values, nulls);
			for (int col = 0; col < nfields; col++)
				if (!nulls[col])
					pfree(DatumGetPointer(values[col]));
			op->stored_rows++;
		}
		pfree(values);
		pfree(nulls);
		PQclear(op->chunk);
		op->chunk = NULL;
		op->failed = was_failed;
	}
	PG_FINALLY();
	{
		CurrentResourceOwner = oldowner;
		MemoryContextSwitchTo(oldcontext);
	}
	PG_END_TRY();
}

static void
pipeline_free_operation(PgFdwPendingOperation *op)
{
	Assert(op->done);
	dlist_delete(&op->all_node);
	pipeline_clear_stream(op);
	PQclear(op->result);
	if (op->sql)
		pfree(op->sql);
	pfree(op);
}

PgFdwPendingOperation *
pgfdw_pipeline_submit(PGconn *conn, PgFdwConnState *state, const char *sql,
					  int nparams, const char *const *values,
					  ExecStatusType expected, const char *error_sql)
{
	PgFdwPipeline *pipeline = pipeline_get(conn, state);
	PgFdwPendingOperation *op;

	Assert(state->pendingAreq == NULL);
	pipeline_enter(pipeline);
	op = pipeline_operation(pipeline, error_sql ? error_sql : sql, expected);
	/* On a send failure we cannot assume the protocol queue is intact. */
	pipeline->broken = true;
	if (!PQsendQueryParams(conn, sql, nparams, NULL, values, NULL, NULL, 0))
		pipeline_error(pipeline);
	pipeline->broken = false;
	pipeline->flush_pending = true;
	return op;
}

/* Chunk mode is selected when this operation reaches the wire head. */
PgFdwPendingOperation *
pgfdw_pipeline_stream_submit(PGconn *conn, PgFdwConnState *state,
							 const char *sql, int nparams,
							 const char *const *values, int chunk_size)
{
	PgFdwPendingOperation *op = pgfdw_pipeline_submit(conn, state, sql,
															 nparams, values, PGRES_TUPLES_OK, sql);

	Assert(chunk_size > 0);
	op->chunk_size = chunk_size;
	pgfdw_pipeline_sync(conn, state);
	return op;
}

bool
pgfdw_pipeline_stream_has_room(PgFdwConnState *state)
{
	/* Depth zero still permits streaming, but never queues a second query. */
	return !state->pipeline || state->pipeline->fetches < Max(1, state->pipeline_depth);
}

void
pgfdw_pipeline_sync(PGconn *conn, PgFdwConnState *state)
{
	PgFdwPipeline *pipeline = pipeline_get(conn, state);

	Assert(pipeline->active);
	pipeline_operation(pipeline, NULL, PGRES_PIPELINE_SYNC);
	pipeline->broken = true;
	if (!PQsendPipelineSync(conn))
		pipeline_error(pipeline);
	pipeline->broken = false;
	pipeline->flush_pending = true;
}

/*
 * Consume only immediately available protocol messages.  NULL ends one query,
 * not the pipeline.  A Sync result has no following NULL.  Never run executor
 * code here: it could recurse through a subplan using this same connection.
 * During abort, discard results without reporting secondary remote errors.
 */
static bool
pipeline_process(PgFdwPipeline *pipeline, bool discard,
				 PgFdwPendingOperation *wanted)
{
	PGconn	   *conn = pipeline->conn;
	int			flush;

	if (!pipeline->active)
		return true;
	if (pipeline->broken)
		return false;
	/* Select row mode before any libpq call that might parse this query. */
	if (!dlist_is_empty(&pipeline->wire))
	{
		PgFdwPendingOperation *head =
			dlist_head_element(PgFdwPendingOperation, wire_node, &pipeline->wire);
		if (head->chunk_size && !head->row_mode_set)
		{
			if (!PQsetChunkedRowsMode(conn, head->chunk_size))
				return false;
			head->row_mode_set = true;
		}
	}
	flush = PQflush(conn);
	if (flush < 0 || !PQconsumeInput(conn))
		return false;
	pipeline->flush_pending = (flush != 0);
	while (!dlist_is_empty(&pipeline->wire))
	{
		PgFdwPendingOperation *op =
			dlist_head_element(PgFdwPendingOperation, wire_node, &pipeline->wire);
		PGresult   *res;
		MemoryContext oldcontext;

		if (!discard)
			CHECK_FOR_INTERRUPTS();
		if (op->chunk_size && !op->row_mode_set)
		{
			if (!PQsetChunkedRowsMode(conn, op->chunk_size))
				return false;
			op->row_mode_set = true;
		}
		if (op->chunk)
		{
			if (discard || op->discard_rows)
			{
				PQclear(op->chunk);
				op->chunk = NULL;
			}
			else if (op == wanted)
				return true;
			else
				pipeline_store_chunk(op);
		}
		if (PQisBusy(conn))
			break;
		/* PG19's result wrappers must outlive the current subtransaction. */
		oldcontext = MemoryContextSwitchTo(pipeline->context);
		res = PQgetResult(conn);
		MemoryContextSwitchTo(oldcontext);
		if (res && op->chunk_size && PQresultStatus(res) == PGRES_TUPLES_CHUNK)
		{
			op->chunk = res;
			if (discard || op->discard_rows)
			{
				PQclear(op->chunk);
				op->chunk = NULL;
			}
			else if (op == wanted)
			{
				SetLatch(MyLatch);
				return true;
			}
			else
				pipeline_store_chunk(op);
			continue;
		}
		if (res && op->expected != PGRES_PIPELINE_SYNC)
		{
			if (op->result != NULL)
			{
				PQclear(res);
				pipeline->broken = true;
				return false;
			}
			op->result = res;
			if (PQresultStatus(res) != op->expected)
			{
				op->failed = true;
			}
			continue;
		}
		if (op->expected == PGRES_PIPELINE_SYNC)
		{
			if (!res || PQresultStatus(res) != PGRES_PIPELINE_SYNC)
			{
				PQclear(res);
				pipeline->broken = true;
				return false;
			}
			PQclear(res);
		}
		else if (op->result == NULL)
		{
			pipeline->broken = true;
			return false;
		}
		dlist_delete(&op->wire_node);
		op->done = true;
		/* A nested synchronous user may have consumed a registered event. */
		if (!discard)
			SetLatch(MyLatch);
		if (op->expected == PGRES_TUPLES_OK)
			pipeline->fetches--;
		if (discard)
		{
			op->failed = true;
			pipeline_clear_stream(op);
			PQclear(op->result);
			op->result = NULL;
		}
		if (op->expected == PGRES_PIPELINE_SYNC)
			pipeline_free_operation(op);
		else if (op->failed && !discard)
			pgfdw_report_error(op->result, conn, op->sql);
	}
	return PQstatus(conn) == CONNECTION_OK;
}

void
pgfdw_pipeline_process(PgFdwConnState *state)
{
	if (state->pipeline && !pipeline_process(state->pipeline, false, NULL))
		pipeline_error(state->pipeline);
}

bool
pgfdw_pipeline_has_room(PgFdwConnState *state)
{
	return state->pipeline_depth > 0 &&
		(!state->pipeline || state->pipeline->fetches < state->pipeline_depth);
}

bool
pgfdw_pipeline_ready(PgFdwPendingOperation *op)
{
	return op->done || op->failed;
}

uint32
pgfdw_pipeline_events(PgFdwConnState *state)
{
	PgFdwPipeline *pipeline = state->pipeline;

	if (!pipeline || !pipeline->active || dlist_is_empty(&pipeline->wire))
		return 0;
	return WL_SOCKET_READABLE |
		(pipeline->flush_pending ? WL_SOCKET_WRITEABLE : 0);
}

static void
pipeline_wait(PgFdwPipeline *pipeline, long timeout)
{
	int			events;

	events = WaitLatchOrSocket(MyLatch,
							   WL_EXIT_ON_PM_DEATH | WL_LATCH_SET | WL_TIMEOUT |
							   pgfdw_pipeline_events(pipeline->state),
							   PQsocket(pipeline->conn), timeout,
							   pipeline_wait_event);
	if (events & WL_LATCH_SET)
	{
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
	}
}

static void
pipeline_check_stream(PgFdwPendingOperation *op)
{
	if (op->failed)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_ERROR),
				 errmsg("remote streaming operation was aborted"),
				 errcontext("remote SQL command: %s", op->sql)));
}

bool
pgfdw_pipeline_stream_ready(PgFdwPendingOperation *op)
{
	pipeline_check_stream(op);
	if (op->chunk || op->stored_rows || op->done)
		return true;
	if (!pipeline_process(op->pipeline, false, op))
		pipeline_error(op->pipeline);
	pipeline_check_stream(op);
	return op->chunk || op->stored_rows || op->done;
}

/* Returns one chunk, or NULL only after successful protocol completion. */
PGresult *
pgfdw_pipeline_stream_take(PgFdwPendingOperation *op)
{
	PGresult   *res;

	while (!pgfdw_pipeline_stream_ready(op))
		pipeline_wait(op->pipeline, 1000);
	if (op->stored_rows)
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(op->pipeline->context);
		bool was_failed = op->failed;

		/* A failed replay cannot leave a silently shortened outer scan. */
		op->failed = true;
		Assert(op->chunk == NULL);
		op->chunk = libpqsrv_PQwrap(PQcopyResult(op->row_description->res, PG_COPYRES_ATTRS));
		if (!op->chunk)
			ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
		for (int row = 0; row < op->chunk_size && op->stored_rows; row++)
		{
			TupleTableSlot *slot = op->store_slot;

			CHECK_FOR_INTERRUPTS();
			if (!tuplestore_gettupleslot(op->store, true, false, slot))
				elog(ERROR, "missing buffered remote row");
			slot_getallattrs(slot);
			for (int col = 0; col < slot->tts_tupleDescriptor->natts; col++)
			{
				text *value = slot->tts_isnull[col] ? NULL : DatumGetTextPP(slot->tts_values[col]);
				if (!PQsetvalue(op->chunk->res, row, col,
								value ? VARDATA_ANY(value) : NULL,
								value ? VARSIZE_ANY_EXHDR(value) : -1))
					ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
			}
			ExecClearTuple(slot);
			op->stored_rows--;
		}
		tuplestore_trim(op->store);
		op->failed = was_failed;
		MemoryContextSwitchTo(oldcontext);
	}
	res = op->chunk;
	op->chunk = NULL;
	return res;
}

/* Early stop drains without cancelling the shared remote transaction. */
void
pgfdw_pipeline_stream_release(PgFdwPendingOperation *op)
{
	pipeline_check_stream(op);
	op->discard_rows = true;
	pipeline_clear_stream(op);
	while (!op->done)
	{
		if (!pipeline_process(op->pipeline, false, NULL))
			pipeline_error(op->pipeline);
		if (!op->done)
			pipeline_wait(op->pipeline, 1000);
	}
	pipeline_check_stream(op);
	pipeline_free_operation(op);
}

PGresult *
pgfdw_pipeline_take(PgFdwPendingOperation *op)
{
	PGresult   *result;

	while (!pgfdw_pipeline_ready(op))
	{
		pgfdw_pipeline_process(op->pipeline->state);
		if (!pgfdw_pipeline_ready(op))
			pipeline_wait(op->pipeline, 1000);
	}
	if (op->failed)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_ERROR),
				 errmsg("remote pipeline operation was aborted"),
				 errcontext("remote SQL command: %s", op->sql)));
	result = op->result;
	result = libpqsrv_PGresultSetParent(result, CurrentMemoryContext);
	op->result = NULL;
	pipeline_free_operation(op);
	return result;
}

/* Leave the wire idle without discarding any completed scan's result. */
void
pgfdw_pipeline_drain(PgFdwConnState *state)
{
	PgFdwPipeline *pipeline = state->pipeline;

	if (!pipeline || !pipeline->active)
		return;
	while (!dlist_is_empty(&pipeline->wire))
	{
		pgfdw_pipeline_process(state);
		if (!dlist_is_empty(&pipeline->wire))
			pipeline_wait(pipeline, 1000);
	}
	if (!PQexitPipelineMode(pipeline->conn) ||
		PQsetnonblocking(pipeline->conn, pipeline->old_nonblocking) != 0)
		pipeline_error(pipeline);
	pipeline->active = false;
}

/*
 * Abort recovery does not follow scan pointers, convert tuples, or invoke
 * callbacks.  In-flight operations become failed tombstones until their owner
 * releases them or the top-level transaction ends.  Older completed results
 * survive savepoint rollback; a failed operation can never look like EOF.
 */
bool
pgfdw_pipeline_abort(PgFdwConnState *state)
{
	PgFdwPipeline *pipeline = state->pipeline;
	TimestampTz deadline;
	TimestampTz next_cancel = 0;
	dlist_iter iter;
	int			level = GetCurrentTransactionNestLevel();

	if (!pipeline)
		return true;
	if (pipeline->broken)
		return false;
	deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), 30000);
	if (pipeline->active)
	{
		/* Consume query terminators before appending to an aborted pipeline. */
		if (!pipeline_process(pipeline, true, NULL))
			return false;
		/* An interrupt may have occurred before the submitter sent its Sync. */
		pipeline_operation(pipeline, NULL, PGRES_PIPELINE_SYNC);
		if (!PQsendPipelineSync(pipeline->conn))
			return false;
		pipeline->flush_pending = true;
		while (!dlist_is_empty(&pipeline->wire))
		{
			TimestampTz now = GetCurrentTimestamp();

			if (now >= deadline)
				return false;
			if (now >= next_cancel &&
				dlist_head_element(PgFdwPendingOperation, wire_node,
								   &pipeline->wire)->expected != PGRES_PIPELINE_SYNC)
			{
				if (libpqsrv_cancel(pipeline->conn, deadline) != NULL)
					return false;
				next_cancel = TimestampTzPlusMilliseconds(now, 1000);
			}
			if (!pipeline_process(pipeline, true, NULL))
				return false;
			if (!dlist_is_empty(&pipeline->wire))
				pipeline_wait(pipeline, 100);
		}
		if (!PQexitPipelineMode(pipeline->conn) ||
			PQsetnonblocking(pipeline->conn, pipeline->old_nonblocking) != 0)
			return false;
		pipeline->active = false;
	}
	dlist_foreach(iter, &pipeline->operations)
	{
		PgFdwPendingOperation *op =
			dlist_container(PgFdwPendingOperation, all_node, iter.cur);

		if (op->nestlevel >= level)
		{
			op->failed = true;
			pipeline_clear_stream(op);
			PQclear(op->result);
			op->result = NULL;
		}
	}
	return true;
}

/* Completed requests and their owners survive release of their savepoint. */
void
pgfdw_pipeline_subcommit(PgFdwConnState *state)
{
	dlist_iter iter;
	int level = GetCurrentTransactionNestLevel();

	if (!state->pipeline)
		return;
	dlist_foreach(iter, &state->pipeline->operations)
	{
		PgFdwPendingOperation *op =
			dlist_container(PgFdwPendingOperation, all_node, iter.cur);

		if (op->nestlevel == level)
			op->nestlevel--;
	}
}

/* Drop protocol state before disconnect; outstanding owners must fail. */
void
pgfdw_pipeline_disconnect(PgFdwConnState *state)
{
	PgFdwPipeline *pipeline = state->pipeline;
	dlist_iter iter;

	if (!pipeline)
		return;
	dlist_foreach(iter, &pipeline->operations)
	{
		PgFdwPendingOperation *op =
			dlist_container(PgFdwPendingOperation, all_node, iter.cur);

		op->failed = true;
		pipeline_clear_stream(op);
		PQclear(op->result);
		op->result = NULL;
	}
	pipeline->active = false;
	pipeline->broken = true;
	state->pipeline = NULL;
}
