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
EXTVERSION = $(shell grep default_version $(EXTENSION).control | \
               sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")
BUILD = .build
TESTGRES_EXT_ROOT = $(BUILD)/testgres-ext
TESTGRES_EXT_DIR = $(TESTGRES_EXT_ROOT)/extension
DATA = $(wildcard $(BUILD)/pgwrh/*.sql)
EXTRA_CLEAN = $(BUILD)

MASTER = $(shell tsort src/master/deps.txt | sed -e 's/^/src\/master\//' -e 's/$$/\.sql/'  | xargs echo)
REPLICA = $(shell tsort src/replica/deps.txt | sed -e 's/^/src\/replica\//' -e 's/$$/\.sql/'  | xargs echo)

# Reuse the current definitions instead of maintaining a second copy of the SQL.
UPGRADE_022 = src/updates/pgwrh--0.2.1--0.2.2.sql.in \
	src/master/api-management.sql src/master/implementation-views.sql src/master/api-replica.sql \
	src/master/triggers.sql src/replica/helpers.sql src/replica/aggregation.sql \
	src/replica/status.sql src/replica/sync.sql

PG_CONFIG = pg_config

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

$(BUILD)/pgwrh/$(EXTENSION)--$(EXTVERSION).sql: src/common.sql $(MASTER) $(REPLICA)
	cat $^ > $@

updates: $(wildcard src/updates/*.sql)
	cp $^ $(BUILD)/pgwrh

$(BUILD)/pgwrh/pgwrh--0.2.1--0.2.2.sql: $(UPGRADE_022) | prepare
	sed -E -e '/^CREATE TYPE .*rel_id AS/d' -e '/^\\echo /d' \
		-e 's/^CREATE (FUNCTION|VIEW|TRIGGER) /CREATE OR REPLACE \1 /' $^ > $@

all: prepare $(EXTENSION).control $(BUILD)/pgwrh/$(EXTENSION)--$(EXTVERSION).sql updates $(BUILD)/pgwrh/pgwrh--0.2.1--0.2.2.sql

testgres-ext: all
	mkdir -p $(TESTGRES_EXT_DIR)
	cp ./pgwrh.control $(TESTGRES_EXT_DIR)
	cp $(wildcard $(BUILD)/pgwrh/*.sql) $(TESTGRES_EXT_DIR)

prepare:
	mkdir -p ${BUILD}/pgwrh

PHONY: all prepare testgres-ext
