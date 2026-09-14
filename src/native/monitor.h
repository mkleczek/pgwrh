/* Copyright (C) 2026 Michal Kleczek. SPDX-License-Identifier: AGPL-3.0-or-later */
#ifndef PGWRH_MONITOR_H
#define PGWRH_MONITOR_H
#include "postgres.h"
#include "access/xlogdefs.h"
#include "storage/condition_variable.h"
extern void pgwrh_require_monitor(void);
extern Oid pgwrh_subscription(const char *name, bool require_ready);
extern XLogRecPtr pgwrh_progress(Oid subid);
extern ConditionVariable *pgwrh_progress_changed(void);
#endif
