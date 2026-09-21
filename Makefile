# Standalone pgwrh_fdw build and regression tests.

MODULE_big = pgwrh_fdw
OBJS = \
	$(WIN32RES) \
	connection.o \
	deparse.o \
	option.o \
	transaction_context.o \
	virtual.o \
	postgres_fdw.o \
	shippable.o
PGFILEDESC = "pgwrh_fdw - foreign data wrapper for PostgreSQL"

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = $(libpq)

EXTENSION = pgwrh_fdw
DATA = pgwrh_fdw--1.0.0-alpha1.sql

MODULES = context_probe
REGRESS = pgwrh_fdw query_cancel
ISOLATION = eval_plan_qual
ISOLATION_OPTS = --load-extension=pgwrh_fdw
TAP_TESTS = 1

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Export only PostgreSQL loader/SQL entry points; helpers also have unique names.
PG_CFLAGS += -fvisibility=hidden

connection.o join.o option.o virtual.o postgres_fdw.o: virtual.h
$(OBJS): postgres_fdw.h namespace.h

STAGE_DIR ?= $(abspath .build/testgres-ext)
stage: all
	mkdir -p "$(STAGE_DIR)/extension"
	cp $(DATA) "$(STAGE_DIR)/extension/"
	sed 's|\$$libdir/pgwrh_fdw|pgwrh_fdw|' pgwrh_fdw.control > "$(STAGE_DIR)/extension/pgwrh_fdw.control"
	cp $(shlib) "$(STAGE_DIR)/"

.PHONY: stage
