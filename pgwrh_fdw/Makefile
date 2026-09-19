# pgwrh_fdw extension build; regression tests live in ../test/pgwrh_fdw.

MODULE_big = pgwrh_fdw
OBJS = \
	$(WIN32RES) \
	connection.o \
	credentials.o \
	deparse.o \
	join.o \
	option.o \
	transaction_context.o \
	virtual.o \
	pgwrh_fdw.o \
	shippable.o
PGFILEDESC = "pgwrh_fdw - foreign data wrapper for PostgreSQL"

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = $(libpq)

EXTENSION = pgwrh_fdw
DATA = pgwrh_fdw--1.0.0.sql

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Export only PostgreSQL loader/SQL entry points; helpers also have unique names.
PG_CFLAGS += -fvisibility=hidden

connection.o join.o option.o virtual.o pgwrh_fdw.o: virtual.h
join.o pgwrh_fdw.o: join.h
$(OBJS): pgwrh_fdw.h

STAGE_DIR ?= $(abspath ../.build/testgres-ext)
stage: all
	mkdir -p "$(STAGE_DIR)/extension"
	cp $(DATA) "$(STAGE_DIR)/extension/"
	sed 's|\$$libdir/pgwrh_fdw|pgwrh_fdw|' pgwrh_fdw.control > "$(STAGE_DIR)/extension/pgwrh_fdw.control"
	cp $(shlib) "$(STAGE_DIR)/"

.PHONY: stage
