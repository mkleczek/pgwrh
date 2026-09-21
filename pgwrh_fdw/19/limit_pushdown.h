/* SPDX-License-Identifier: AGPL-3.0-only */
#ifndef PGWRH_FDW_LIMIT_PUSHDOWN_H
#define PGWRH_FDW_LIMIT_PUSHDOWN_H

#include "nodes/pathnodes.h"

extern void pgwrh_fdw_init_limit_pushdown(void);
extern Path *pgwrh_fdw_limit_foreign_path(PlannerInfo *root, ForeignPath *path);

#endif
