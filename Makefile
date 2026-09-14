# contrib/pgwrh_fdw/Makefile

MODULE_big = pgwrh_fdw
OBJS = \
	$(WIN32RES) \
	connection.o \
	deparse.o \
	option.o \
	transaction_context.o \
	pgwrh_fdw.o \
	shippable.o
PGFILEDESC = "pgwrh_fdw - foreign data wrapper for PostgreSQL"

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = $(libpq)

EXTENSION = pgwrh_fdw
DATA = pgwrh_fdw--1.0.sql pgwrh_fdw--1.0--1.1.sql pgwrh_fdw--1.1--1.2.sql

REGRESS = pgwrh_fdw query_cancel
ISOLATION = eval_plan_qual
ISOLATION_OPTS = --load-extension=pgwrh_fdw
TAP_TESTS = 1

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Export only PostgreSQL loader/SQL entry points; helpers also have unique names.
PG_CFLAGS += -fvisibility=hidden
