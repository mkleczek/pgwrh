/* Copyright (C) 2026 Michal Kleczek. SPDX-License-Identifier: AGPL-3.0-or-later */
#include "postgres.h"
#include "catalog/pg_class.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_publication.h"
#include "fmgr.h"
#include "storage/lmgr.h"
#include "utils/syscache.h"

PG_FUNCTION_INFO_V1(pgwrh_lock_managed_object);

Datum
pgwrh_lock_managed_object(PG_FUNCTION_ARGS)
{
	Oid classid = PG_GETARG_OID(0);
	Oid objid = PG_GETARG_OID(1);
	int cacheid;
	bool locked;

	switch (classid)
	{
		case RelationRelationId: cacheid = RELOID; break;
		case NamespaceRelationId: cacheid = NAMESPACEOID; break;
		case ForeignServerRelationId: cacheid = FOREIGNSERVEROID; break;
		case PublicationRelationId: cacheid = PUBLICATIONOID; break;
		default: elog(ERROR, "unsupported managed object catalog: %u", classid);
	}
	/* DROP holds the object lock before its event trigger updates the registry.
	 * Repair holds the registry lock first: fail promptly rather than deadlock.
	 * These locks are retained until transaction end, including for indexes. */
	locked = classid == RelationRelationId
		? ConditionalLockRelationOid(objid, AccessShareLock)
		: ConditionalLockDatabaseObject(classid, objid, 0, AccessShareLock);
	if (!locked)
		ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
						errmsg("managed object is being changed concurrently"),
						errhint("Retry managed-object registration or repair after the DDL transaction finishes.")));
	/* Check the live catalog even for a caller using an older MVCC snapshot. */
	PG_RETURN_BOOL(SearchSysCacheExists1(cacheid, ObjectIdGetDatum(objid)));
}
