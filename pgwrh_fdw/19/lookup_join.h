/* SPDX-License-Identifier: AGPL-3.0-only */
#ifndef PGWRH_FDW_LOOKUP_JOIN_H
#define PGWRH_FDW_LOOKUP_JOIN_H
#include "foreign/fdwapi.h"
#include "lib/stringinfo.h"

extern void pgwrh_fdw_lookup_init(GetForeignJoinPaths_function callback);
extern Oid pgwrh_fdw_lookup_array_type(Oid type);
extern void pgwrh_fdw_lookup_start(ForeignScanState *node, int nparams,
                                  const char **values);
extern void pgwrh_fdw_lookup_reset(ForeignScanState *node);
extern void pgwrh_fdw_deparse_lookup(StringInfo buf, PlannerInfo *root,
                                   RelOptInfo *rel, Index lookupid,
                                   List *shipvars, List *tlist, List *quals,
                                   bool semi, List **params);
#endif
