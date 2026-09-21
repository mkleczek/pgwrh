MODULES = context_probe
REGRESS = pgwrh_fdw query_cancel
ISOLATION = eval_plan_qual
ISOLATION_OPTS = --load-extension=pgwrh_fdw
TAP_TESTS = 1

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
