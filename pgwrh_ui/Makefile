EXTENSION = pgwrh_ui
.DEFAULT_GOAL := all
.DELETE_ON_ERROR:
EXTVERSION = 1.0.0-alpha1
BUILD = ../.build/pgwrh_ui
DATA_built = $(BUILD)/$(EXTENSION)--$(EXTVERSION).sql
EXTRA_CLEAN = $(BUILD)
SOURCES = src/common.sql src/data.sql src/html.sql src/actions.sql $(BUILD)/assets.sql src/privileges.sql
PG_CONFIG ?= pg_config
SUPPORT_DIR = $(shell $(PG_CONFIG) --sharedir)/pgwrh_ui
SUPPORT_FILES = README.md readonly.sql operator.sql postgrest.conf vendor/HTMX-LICENSE

ifdef NO_PGXS
EXTDIR := $(shell $(PG_CONFIG) --sharedir)/extension
install: all
	install -d "$(DESTDIR)$(EXTDIR)"
	install -m 644 $(EXTENSION).control $(DATA_built) "$(DESTDIR)$(EXTDIR)"
uninstall:
	rm -f "$(DESTDIR)$(EXTDIR)/$(EXTENSION).control" "$(DESTDIR)$(EXTDIR)/$(notdir $(DATA_built))"
clean:
	rm -rf $(BUILD)
else
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
endif

$(DATA_built): $(SOURCES) Makefile
	mkdir -p $(BUILD)
	cat $(SOURCES) > $@

$(BUILD)/assets.sql: build-assets.sh assets/app.css assets/app.js vendor/htmx-2.0.10.min.js
	mkdir -p $(BUILD)
	$(SHELL) build-assets.sh > $@

all: $(DATA_built)
install: install-support
uninstall: uninstall-support

install-support:
	install -d "$(DESTDIR)$(SUPPORT_DIR)"
	install -m 644 $(SUPPORT_FILES) "$(DESTDIR)$(SUPPORT_DIR)"

uninstall-support:
	rm -f $(addprefix "$(DESTDIR)$(SUPPORT_DIR)/",$(notdir $(SUPPORT_FILES)))

STAGE_DIR ?= $(abspath ../.build/testgres-ext)
stage: all
	mkdir -p "$(STAGE_DIR)/extension"
	cp $(EXTENSION).control $(DATA_built) "$(STAGE_DIR)/extension/"
.PHONY: all stage install-support uninstall-support
