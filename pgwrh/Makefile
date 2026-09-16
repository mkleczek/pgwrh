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
DATA_built = $(BUILD)/pgwrh--$(EXTVERSION).sql
EXTRA_CLEAN = $(BUILD)

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
	install -c -m 644 ./pgwrh.control $(DATA_built) "$(DESTDIR)$(EXTDIR)"

uninstall:
	rm -f $(addprefix "$(DESTDIR)$(EXTDIR)/",$(notdir pgwrh.control $(DATA_built)))

else # NO_PGXS
# Standard pgxs makefile
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

endif # NO_PGXS

$(BUILD)/pgwrh--$(EXTVERSION).sql: src/common.sql $(MASTER) $(REPLICA) | prepare
	cat $^ > $@

all: pgwrh.control $(DATA_built)

prepare:
	mkdir -p $(BUILD)

STAGE_DIR ?= $(abspath ../.build/testgres-ext)
stage: all
	mkdir -p "$(STAGE_DIR)/extension"
	cp pgwrh.control $(DATA_built) "$(STAGE_DIR)/extension/"

.PHONY: all prepare stage
