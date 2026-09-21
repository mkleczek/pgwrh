/*-------------------------------------------------------------------------
 * transaction_context.c
 *   Frozen custom GUC context for remote transaction initialization.
 *
 * Copyright (c) 2026, pgwrh_fdw contributors. GNU AGPL version 3 only; see LICENSE.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "commands/defrem.h"
#include "postgres_fdw.h"
#include "transaction_context.h"
#include "utils/guc.h"
#include "utils/guc_tables.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

typedef struct FrozenParameter
{
	char		name[NAMEDATALEN]; /* hash key, canonical lowercase ASCII */
	char	   *value;            /* NULL if not visible at capture */
} FrozenParameter;

static HTAB *frozen_parameters = NULL;

/* Deliberately narrower than PostgreSQL's general identifier syntax. */
static bool
supported_name(const char *name)
{
	bool		start = true;
	bool		dotted = false;
	const char *p;

	if (strlen(name) >= NAMEDATALEN ||
		strncmp(name, "pgwrh_fdw.", 10) == 0 ||
		strncmp(name, "postgres_fdw.", 13) == 0)
		return false;
	for (p = name; *p; p++)
	{
		if (*p == '.' && !start)
		{
			start = true;
			dotted = true;
		}
		else if ((*p >= 'a' && *p <= 'z') || *p == '_' ||
				 (!start && *p >= '0' && *p <= '9'))
			start = false;
		else
			return false;
	}
	return dotted && !start;
}

static char *
canonical_name(const char *name)
{
	char	   *result = pstrdup(name);
	char	   *p;

	for (p = result; *p; p++)
		if (*p >= 'A' && *p <= 'Z')
			*p += 'a' - 'A';
	return result;
}

static bool
list_space(char c)
{
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' ||
		c == '\f' || c == '\v';
}

List *
pgwrh_fdw_parse_parameters(const char *value)
{
	List	   *names = NIL;
	const char *p = value;

	for (;;)
	{
		const char *start;
		const char *end;
		char	   *raw;
		char	   *name;
		ListCell   *lc;

		while (list_space(*p))
			p++;
		start = p;
		while (*p && *p != ',')
			p++;
		end = p;
		while (end > start && list_space(end[-1]))
			end--;
		raw = pnstrdup(start, end - start);
		name = canonical_name(raw);
		pfree(raw);
		if (!supported_name(name))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid transaction_parameters name \"%s\"", name),
					 errhint("Use a nonempty comma-separated list of custom GUC names, such as app.request_id. Names must be ASCII identifiers separated by dots, at most 63 bytes; postgres_fdw and pgwrh_fdw prefixes are reserved.")));
		foreach(lc, names)
			if (strcmp(name, lfirst(lc)) == 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("duplicate transaction_parameters name \"%s\"", name)));
		names = lappend(names, name);
		if (!*p)
			return names;
		p++;
	}
}

static void
context_xact_callback(XactEvent event, void *arg)
{
	/* TopTransactionContext owns the data, including across subxact aborts. */
	if (event == XACT_EVENT_COMMIT || event == XACT_EVENT_ABORT ||
		event == XACT_EVENT_PARALLEL_COMMIT || event == XACT_EVENT_PARALLEL_ABORT ||
		event == XACT_EVENT_PREPARE)
		frozen_parameters = NULL;
}

void
pgwrh_fdw_context_init(void)
{
	RegisterXactCallback(context_xact_callback, NULL);
}

static void
capture_parameters(void)
{
	MemoryContext oldcontext;
	HASHCTL		ctl = {0};
	HTAB	   *captured;
	struct config_generic **variables;
	int			count;
	int			i;

	oldcontext = MemoryContextSwitchTo(TopTransactionContext);
	ctl.keysize = NAMEDATALEN;
	ctl.entrysize = sizeof(FrozenParameter);
	ctl.hcxt = TopTransactionContext;
	captured = hash_create("pgwrh_fdw frozen parameters", 32, &ctl,
						   HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);
	variables = get_guc_variables(&count);
	for (i = 0; i < count; i++)
	{
		char	   *name = canonical_name(variables[i]->name);

		if (supported_name(name))
		{
			FrozenParameter *parameter = hash_search(captured, name, HASH_ENTER, NULL);

			/* Never capture a value the initiating role cannot examine. */
			parameter->value = ConfigOptionIsVisible(variables[i]) ?
				pstrdup(GetConfigOption(name, false, true)) : NULL;
		}
		pfree(name);
	}
	pfree(variables);
	MemoryContextSwitchTo(oldcontext);
	/* Publish only a complete capture. No GUC show hooks are invoked. */
	frozen_parameters = captured;
}

List *
pgwrh_fdw_transaction_parameters(Oid serverid)
{
	ForeignServer *server = GetForeignServer(serverid);
	List	   *names = NIL;
	List	   *parameters = NIL;
	ListCell   *lc;

	foreach(lc, server->options)
	{
		DefElem    *def = lfirst_node(DefElem, lc);

		if (strcmp(def->defname, "transaction_parameters") == 0)
			names = pgwrh_fdw_parse_parameters(defGetString(def));
	}
	if (names == NIL)
		return NIL;
	if (frozen_parameters == NULL)
		capture_parameters();
	foreach(lc, names)
	{
		char	   *name = lfirst(lc);
		FrozenParameter *parameter;

		/* Recheck visibility under the role opening this participant. */
		(void) GetConfigOption(name, true, true);
		parameter = hash_search(frozen_parameters, name, HASH_FIND, NULL);
		if (parameter == NULL || parameter->value == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("transaction parameter \"%s\" was not available when pgwrh_fdw context was frozen", name),
					 errhint("Define and set every required parameter before the first access to a server with transaction_parameters in this transaction.")));
		parameters = lappend(parameters, parameter);
	}
	list_free_deep(names);
	return parameters;
}

void
pgwrh_fdw_apply_parameters(PGconn *conn, List *parameters)
{
	ListCell   *lc;

	foreach(lc, parameters)
	{
		FrozenParameter *parameter = lfirst(lc);
		char	   *name;
		char	   *value;
		StringInfoData sql;

		name = PQescapeIdentifier(conn, parameter->name, strlen(parameter->name));
		if (name == NULL)
			pgfdw_report_error(NULL, conn, NULL);
		value = PQescapeLiteral(conn, parameter->value, strlen(parameter->value));
		if (value == NULL)
		{
			PQfreemem(name);
			pgfdw_report_error(NULL, conn, NULL);
		}
		initStringInfo(&sql);
		appendStringInfo(&sql, "SET LOCAL %s = %s", name, value);
		PQfreemem(name);
		PQfreemem(value);
		/* Synchronous, PGRES_COMMAND_OK required; never SELECT set_config(). */
		do_sql_command(conn, sql.data);
		pfree(sql.data);
	}
}
