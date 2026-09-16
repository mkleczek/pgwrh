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
BUILD = ../.build/pgwrh
DATA_built = $(BUILD)/pgwrh--$(EXTVERSION).sql \
	$(BUILD)/pgwrh--0.2.1--0.2.2.sql
DATA = $(wildcard src/updates/*.sql)
EXTRA_CLEAN = $(BUILD)

MASTER = $(shell tsort src/master/deps.txt | sed -e 's/^/src\/master\//' -e 's/$$/\.sql/'  | xargs echo)
REPLICA = $(shell tsort src/replica/deps.txt | sed -e 's/^/src\/replica\//' -e 's/$$/\.sql/'  | xargs echo)

# Reuse the current definitions instead of maintaining a second copy of the SQL.
UPGRADE_022 = src/updates/pgwrh--0.2.1--0.2.2.sql.in \
	src/common.sql \
	src/master/api-management.sql src/master/implementation-views.sql src/master/api-replica.sql \
	src/master/triggers.sql src/replica/helpers.sql src/replica/aggregation.sql \
	src/replica/status.sql src/replica/sync.sql src/replica/daemon.sql \
	src/replica/api-management.sql

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

$(BUILD)/pgwrh--$(EXTVERSION).sql: src/common.sql $(MASTER) $(REPLICA) | prepare
	cat $^ > $@

updates: $(wildcard src/updates/*.sql) | prepare
	cp $^ $(BUILD)

$(BUILD)/pgwrh--0.2.1--0.2.2.sql: $(UPGRADE_022) | prepare
	sed -E -e '/^CREATE TYPE .*rel_id AS/d' -e '/^\\echo /d' \
		-e 's/^CREATE (FUNCTION|VIEW|TRIGGER) /CREATE OR REPLACE \1 /' $^ > $@

all: pgwrh.control $(DATA_built) updates

prepare:
	mkdir -p $(BUILD)

STAGE_DIR ?= $(abspath ../.build/testgres-ext)
stage: all
	mkdir -p "$(STAGE_DIR)/extension"
	cp pgwrh.control $(DATA_built) $(DATA) "$(STAGE_DIR)/extension/"

.PHONY: all prepare updates stage
