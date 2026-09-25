/*
 * Copyright (c) 2026, pgwrh_fdw contributors.
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * Connection-owned, ordered libpq operations.  This layer knows nothing about
 * executor callbacks or tuple conversion.  A result can become ready while a
 * different scan is using the connection; only its owner may take it.
 */
#include "postgres.h"

#include "access/xact.h"
#include "lib/ilist.h"
#include "libpq/libpq-be-fe-helpers.h"
#include "miscadmin.h"
#include "postgres_fdw.h"
#include "storage/latch.h"
#include "utils/memutils.h"
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
	dlist_iter iter;

	dlist_foreach(iter, &pipeline->operations)
	{
		PgFdwPendingOperation *op =
			dlist_container(PgFdwPendingOperation, all_node, iter.cur);

		PQclear(op->result);
		op->result = NULL;
	}
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
	pgfdw_report_error(ERROR, NULL, pipeline->conn, false, NULL);
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

static void
pipeline_free_operation(PgFdwPendingOperation *op)
{
	Assert(op->done);
	dlist_delete(&op->all_node);
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
pipeline_process(PgFdwPipeline *pipeline, bool discard)
{
	PGconn	   *conn = pipeline->conn;
	int			flush;

	if (!pipeline->active)
		return true;
	if (pipeline->broken)
		return false;
	flush = PQflush(conn);
	if (flush < 0 || !PQconsumeInput(conn))
		return false;
	pipeline->flush_pending = (flush != 0);
	while (!dlist_is_empty(&pipeline->wire) && !PQisBusy(conn))
	{
		PgFdwPendingOperation *op =
			dlist_head_element(PgFdwPendingOperation, wire_node, &pipeline->wire);
		PGresult   *res;
		MemoryContext oldcontext;

		if (!discard)
			CHECK_FOR_INTERRUPTS();
		/* PG19's result wrappers must outlive the current subtransaction. */
		oldcontext = MemoryContextSwitchTo(pipeline->context);
		res = PQgetResult(conn);
		MemoryContextSwitchTo(oldcontext);
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
			PQclear(op->result);
			op->result = NULL;
		}
		if (op->expected == PGRES_PIPELINE_SYNC)
			pipeline_free_operation(op);
		else if (op->failed && !discard)
			pgfdw_report_error(ERROR, op->result, conn, false, op->sql);
	}
	return PQstatus(conn) == CONNECTION_OK;
}

void
pgfdw_pipeline_process(PgFdwConnState *state)
{
	if (state->pipeline && !pipeline_process(state->pipeline, false))
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
		if (!pipeline_process(pipeline, true))
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
			if (!pipeline_process(pipeline, true))
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
			PQclear(op->result);
			op->result = NULL;
		}
	}
	return true;
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
		PQclear(op->result);
		op->result = NULL;
	}
	pipeline->active = false;
	pipeline->broken = true;
	state->pipeline = NULL;
}
