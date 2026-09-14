# pgwrh
# Copyright (C) 2024  Michal Kleczek

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.

# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

EXTENSION = pgwrh
.DEFAULT_GOAL := all
EXTVERSION = $(shell grep default_version pgwrh.control | \
               sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")
BUILD = .build
DATA = $(BUILD)/pgwrh/pgwrh--$(EXTVERSION).sql $(wildcard src/updates/*.sql)
EXTRA_CLEAN = $(BUILD)

# Keep the original SQL-only installation available.
ifdef NO_PGXS
WITH_LSN_WAIT ?= 0
else
WITH_LSN_WAIT ?= 1
endif
ifeq ($(WITH_LSN_WAIT),1)
ifdef NO_PGXS
$(error WITH_LSN_WAIT=1 requires PGXS; omit NO_PGXS)
endif
EXTENSION += pgwrh_wait
MODULE_big = pgwrh_wait
OBJS = src/native/monitor.o src/native/wait.o
DATA += src/native/pgwrh_wait--1.0.sql
endif

MASTER = $(shell tsort src/master/deps.txt | sed -e 's/^/src\/master\//' -e 's/$$/\.sql/'  | xargs echo)
REPLICA = $(shell tsort src/replica/deps.txt | sed -e 's/^/src\/replica\//' -e 's/$$/\.sql/'  | xargs echo)

PG_CONFIG ?= pg_config

ifdef NO_PGXS
# Simple install for systems without pgxs
# RedHat packages pgxs in postgresql-devel
# which has a lot of dependencies (compilers etc.)
# need to make it possible to use make to install
# pgwrh on such systems
EXTDIR := $(shell $(PG_CONFIG) --sharedir)/extension

clean:
	rm -rf $(BUILD)

install: all
	install -c -m 644 ./pgwrh.control $(EXTDIR)
	install -c -m 644 $(wildcard $(BUILD)/pgwrh/*.sql) $(EXTDIR)

else # NO_PGXS
# Standard pgxs makefile
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

endif # NO_PGXS

$(BUILD)/pgwrh/pgwrh--$(EXTVERSION).sql: src/common.sql $(MASTER) $(REPLICA) | prepare
	cat $^ > $@

updates: $(wildcard src/updates/*.sql) | prepare
	cp $^ $(BUILD)/pgwrh

all: prepare pgwrh.control $(BUILD)/pgwrh/pgwrh--$(EXTVERSION).sql updates
prepare:
	mkdir -p ${BUILD}/pgwrh

.PHONY: all prepare updates

src/native/monitor.o src/native/wait.o: src/native/monitor.h

# PostgreSQL 18 can load extension files from a writable staging directory.
test-stage: all
	mkdir -p $(BUILD)/test-stage/extension
	cp pgwrh.control pgwrh_wait.control $(BUILD)/test-stage/extension/
	cp $(DATA) $(BUILD)/test-stage/extension/
	cp $(shlib) $(BUILD)/test-stage/

test-wait: test-stage
	python3 -m pytest test/native -v

.PHONY: test-stage test-wait
