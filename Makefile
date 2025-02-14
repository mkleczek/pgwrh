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
DATA = $(BUILD)/pgwrh/$(EXTENSION)--$(EXTVERSION).sql
EXTRA_CLEAN = $(BUILD)

MASTER = $(shell tsort src/master/deps.txt | sed -e 's/^/src\/master\//' -e 's/$$/\.sql/'  | xargs echo)
REPLICA = $(shell tsort src/replica/deps.txt | sed -e 's/^/src\/replica\//' -e 's/$$/\.sql/'  | xargs echo)

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

$(BUILD)/pgwrh/$(EXTENSION)--$(EXTVERSION).sql: src/common.sql $(MASTER) $(REPLICA)
	cat $^ > $@

all: prepare $(EXTENSION).control $(BUILD)/pgwrh/$(EXTENSION)--$(EXTVERSION).sql

PHONY: prepare
prepare:
	mkdir -p ${BUILD}/pgwrh
