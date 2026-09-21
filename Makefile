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

# Aggregate build: each extension owns its PGXS definitions and sources.
.DEFAULT_GOAL := all
PG_CONFIG ?= pg_config
PYTHON ?= python3
BUILD = .build
PG_MAJOR = $(shell $(PG_CONFIG) --version | sed -E 's/^PostgreSQL ([0-9]+).*/\1/')
FDW_DIR = pgwrh_fdw/$(PG_MAJOR)
TESTGRES_EXT_ROOT = $(abspath $(BUILD)/testgres-ext)
TEST_STAGE_ROOT = $(abspath $(BUILD)/test-stage)

ifdef NO_PGXS
WITH_LSN_WAIT ?= 0
WITH_FDW ?= 0
WITH_GIST_EXTRA ?= 0
else
WITH_LSN_WAIT ?= 1
WITH_FDW ?= 1
WITH_GIST_EXTRA ?= 1
endif

EXTENSIONS = pgwrh pgwrh_ui
ifeq ($(WITH_LSN_WAIT),1)
ifdef NO_PGXS
$(error WITH_LSN_WAIT=1 requires PGXS; omit NO_PGXS)
endif
EXTENSIONS += pgwrh_wait
endif
ifeq ($(WITH_FDW),1)
ifdef NO_PGXS
$(error WITH_FDW=1 requires PGXS; omit NO_PGXS)
endif
EXTENSIONS += pgwrh_fdw
endif
ifeq ($(WITH_GIST_EXTRA),1)
ifdef NO_PGXS
$(error WITH_GIST_EXTRA=1 requires PGXS; omit NO_PGXS)
endif
EXTENSIONS += pgwrh_gist_extra
endif

ALL_TARGETS = $(addsuffix -all,$(EXTENSIONS))
INSTALL_TARGETS = $(addsuffix -install,$(EXTENSIONS))
CLEAN_TARGETS = $(addsuffix -clean,$(EXTENSIONS))
UNINSTALL_TARGETS = $(addsuffix -uninstall,$(EXTENSIONS))

all: $(ALL_TARGETS)
install: $(INSTALL_TARGETS)
uninstall: $(UNINSTALL_TARGETS)
clean: $(CLEAN_TARGETS)
	rm -rf $(BUILD)

$(ALL_TARGETS):
	$(MAKE) -C $(@:-all=) all PG_CONFIG="$(PG_CONFIG)"

$(INSTALL_TARGETS):
	$(MAKE) -C $(@:-install=) install PG_CONFIG="$(PG_CONFIG)" DESTDIR="$(DESTDIR)"

$(CLEAN_TARGETS):
	$(MAKE) -C $(@:-clean=) clean PG_CONFIG="$(PG_CONFIG)"

$(UNINSTALL_TARGETS):
	$(MAKE) -C $(@:-uninstall=) uninstall PG_CONFIG="$(PG_CONFIG)" DESTDIR="$(DESTDIR)"

prepare:
	$(MAKE) -C pgwrh $@ PG_CONFIG="$(PG_CONFIG)"

# PostgreSQL 18 can load extension files from a writable staging directory.
testgres-ext: pgwrh-all pgwrh_ui-all $(if $(filter 1,$(WITH_FDW)),pgwrh_fdw-all)
	$(MAKE) -C pgwrh stage PG_CONFIG="$(PG_CONFIG)" STAGE_DIR="$(TESTGRES_EXT_ROOT)"
	$(MAKE) -C pgwrh_ui stage PG_CONFIG="$(PG_CONFIG)" STAGE_DIR="$(TESTGRES_EXT_ROOT)"
ifeq ($(WITH_FDW),1)
	$(MAKE) -C pgwrh_fdw stage PG_CONFIG="$(PG_CONFIG)" STAGE_DIR="$(TESTGRES_EXT_ROOT)"
endif

ifeq ($(WITH_LSN_WAIT),1)
test-stage: all
	$(MAKE) -C pgwrh stage PG_CONFIG="$(PG_CONFIG)" STAGE_DIR="$(TEST_STAGE_ROOT)"
	$(MAKE) -C pgwrh_ui stage PG_CONFIG="$(PG_CONFIG)" STAGE_DIR="$(TEST_STAGE_ROOT)"
	$(MAKE) -C pgwrh_wait stage PG_CONFIG="$(PG_CONFIG)" STAGE_DIR="$(TEST_STAGE_ROOT)"
ifeq ($(WITH_FDW),1)
	$(MAKE) -C pgwrh_fdw stage PG_CONFIG="$(PG_CONFIG)" STAGE_DIR="$(TEST_STAGE_ROOT)"
endif
ifeq ($(WITH_GIST_EXTRA),1)
	$(MAKE) -C pgwrh_gist_extra stage PG_CONFIG="$(PG_CONFIG)" STAGE_DIR="$(TEST_STAGE_ROOT)"
endif

test-wait: test-stage
	$(PYTHON) -m pytest test/pgwrh_wait -v
else
test-stage test-wait:
	$(error test-wait requires WITH_LSN_WAIT=1)
endif

test-pgwrh: testgres-ext
	$(PYTHON) -m pytest test/pgwrh -v

test-ui: testgres-ext
	$(PYTHON) -m pytest test/pgwrh_ui -v

ifeq ($(WITH_FDW),1)
test-fdw: pgwrh_fdw-all
	PG_CONFIG="$(PG_CONFIG)" $(PYTHON) $(FDW_DIR)/test_context.py
	PG_CONFIG="$(PG_CONFIG)" $(PYTHON) $(FDW_DIR)/run-upstream.py
	$(PYTHON) $(FDW_DIR)/check-symbols.py

test-fdw-tap: pgwrh_fdw-all
	PG_CONFIG="$(PG_CONFIG)" $(PYTHON) $(FDW_DIR)/run-tap.py
else
test-fdw test-fdw-tap:
	$(error test-fdw requires WITH_FDW=1)
endif

test-packaging:
	PG_CONFIG="$(PG_CONFIG)" $(PYTHON) test/check-install.py

ifeq ($(WITH_GIST_EXTRA),1)
test-gist: pgwrh_gist_extra-all
	$(MAKE) -C pgwrh_gist_extra stage PG_CONFIG="$(PG_CONFIG)" STAGE_DIR="$(TEST_STAGE_ROOT)"
	$(PYTHON) -m pytest test/pgwrh_gist_extra -v
ifeq ($(WITH_FDW),1)
test-gist-integration: test-gist pgwrh_fdw-all
	$(MAKE) -C pgwrh_fdw stage PG_CONFIG="$(PG_CONFIG)" STAGE_DIR="$(TEST_STAGE_ROOT)"
	$(PYTHON) -m pytest test/pgwrh_gist_extra_integration -v
else
test-gist-integration:
	$(error test-gist-integration requires WITH_FDW=1)
endif
else
test-gist test-gist-integration:
	$(error test-gist requires WITH_GIST_EXTRA=1)
endif

.PHONY: all install uninstall clean prepare testgres-ext test-stage \
	test-pgwrh test-ui test-wait test-fdw test-fdw-tap test-gist test-gist-integration test-packaging \
	$(ALL_TARGETS) $(INSTALL_TARGETS) $(CLEAN_TARGETS) $(UNINSTALL_TARGETS)
