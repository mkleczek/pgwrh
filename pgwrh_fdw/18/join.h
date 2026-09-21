/* SPDX-License-Identifier: AGPL-3.0-only */
#ifndef PGWRH_FDW_JOIN_H
#define PGWRH_FDW_JOIN_H

#include "foreign/fdwapi.h"

extern void pgwrh_fdw_join_init(GetForeignJoinPaths_function callback);
extern bool pgwrh_fdw_join_isolated(PlannerInfo *root, RelOptInfo *joinrel,
								   List *servers);

#endif
