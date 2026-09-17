EXTENSION = pgwrh_ui
.DEFAULT_GOAL := all
EXTVERSION = 1.0.0
BUILD = ../.build/pgwrh_ui
DATA_built = $(BUILD)/$(EXTENSION)--$(EXTVERSION).sql
EXTRA_CLEAN = $(BUILD)
SOURCES = src/common.sql src/data.sql src/privileges.sql
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

all: $(DATA_built)
STAGE_DIR ?= $(abspath ../.build/testgres-ext)
stage: all
	mkdir -p "$(STAGE_DIR)/extension"
	cp $(EXTENSION).control $(DATA_built) "$(STAGE_DIR)/extension/"
.PHONY: all stage
