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
DATA_built = $(BUILD)/pgwrh/pgwrh--$(EXTVERSION).sql
DATA = $(wildcard src/updates/*.sql)
EXTRA_CLEAN = $(BUILD)

# Keep the original SQL-only installation available.
ifdef NO_PGXS
WITH_LSN_WAIT ?= 0
WITH_FDW ?= 0
else
WITH_LSN_WAIT ?= 1
WITH_FDW ?= 1
endif
ifeq ($(WITH_FDW),1)
ifdef NO_PGXS
$(error WITH_FDW=1 requires PGXS; omit NO_PGXS)
endif
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
	install -d "$(DESTDIR)$(EXTDIR)"
	install -c -m 644 ./pgwrh.control $(DATA_built) $(DATA) "$(DESTDIR)$(EXTDIR)"

uninstall:
	rm -f $(addprefix "$(DESTDIR)$(EXTDIR)/",$(notdir pgwrh.control $(DATA_built) $(DATA)))

else # NO_PGXS
# Standard pgxs makefile
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

endif # NO_PGXS

# Keep each shared library in its own PGXS build. Command-line build flags
# propagate through recursive make; both installs share the same staging root.
ifeq ($(WITH_FDW),1)
.PHONY: fdw-all fdw-install fdw-clean fdw-uninstall
all: fdw-all
install: fdw-install
clean: fdw-clean
uninstall: fdw-uninstall

fdw-all:
	$(MAKE) -C fdw all PG_CONFIG="$(PG_CONFIG)"

fdw-install: fdw-all
	$(MAKE) -C fdw install PG_CONFIG="$(PG_CONFIG)" DESTDIR="$(DESTDIR)"

fdw-clean:
	$(MAKE) -C fdw clean PG_CONFIG="$(PG_CONFIG)"
	$(MAKE) -C fdw/test clean PG_CONFIG="$(PG_CONFIG)"
	rm -rf fdw/.build

fdw-uninstall:
	$(MAKE) -C fdw uninstall PG_CONFIG="$(PG_CONFIG)" DESTDIR="$(DESTDIR)"
endif

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
ifeq ($(WITH_LSN_WAIT),1)
test-stage: all
	mkdir -p $(BUILD)/test-stage/extension
	cp pgwrh.control pgwrh_wait.control $(BUILD)/test-stage/extension/
	cp $(DATA_built) $(DATA) $(BUILD)/test-stage/extension/
	cp $(shlib) $(BUILD)/test-stage/

test-wait: test-stage
	$(PYTHON) -m pytest test/native -v
else
test-stage test-wait:
	$(error test-wait requires WITH_LSN_WAIT=1)
endif

# PGXS can define an empty PYTHON when PostgreSQL was built without PL/Python.
ifeq ($(strip $(PYTHON)),)
PYTHON = python3
endif

ifeq ($(WITH_FDW),1)
test-fdw: fdw-all
	PG_CONFIG="$(PG_CONFIG)" $(PYTHON) fdw/test/test_context.py
	PG_CONFIG="$(PG_CONFIG)" $(PYTHON) fdw/tools/run-upstream.py
	$(PYTHON) fdw/tools/check-symbols.py
else
test-fdw:
	$(error test-fdw requires WITH_FDW=1)
endif

test-packaging:
	PG_CONFIG="$(PG_CONFIG)" $(PYTHON) tools/check-install.py

.PHONY: test-stage test-wait test-fdw test-packaging
