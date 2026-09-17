EXTENSION = pgwrh_ui
.DEFAULT_GOAL := all
.DELETE_ON_ERROR:
EXTVERSION = 1.0.0
BUILD = ../.build/pgwrh_ui
DATA_built = $(BUILD)/$(EXTENSION)--$(EXTVERSION).sql
EXTRA_CLEAN = $(BUILD)
SOURCES = src/common.sql src/data.sql src/html.sql $(BUILD)/assets.sql src/privileges.sql
PG_CONFIG ?= pg_config

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
STAGE_DIR ?= $(abspath ../.build/testgres-ext)
stage: all
	mkdir -p "$(STAGE_DIR)/extension"
	cp $(EXTENSION).control $(DATA_built) "$(STAGE_DIR)/extension/"
.PHONY: all stage
