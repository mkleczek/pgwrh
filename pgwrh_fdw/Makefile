# Select the independently maintained FDW for the target server.
.DEFAULT_GOAL := all
PG_CONFIG ?= pg_config
PG_MAJOR := $(shell $(PG_CONFIG) --version | sed -E 's/^PostgreSQL ([0-9]+).*/\1/')
ifeq ($(filter $(PG_MAJOR),18 19),)
$(error pgwrh_fdw requires PostgreSQL 18 or 19; check PG_CONFIG)
endif
STAGE_DIR ?= $(abspath ../.build/testgres-ext)

# context_probe is a standalone regression helper, never a package payload.
all install uninstall stage:
	$(MAKE) -C $(PG_MAJOR) $@ PG_CONFIG="$(PG_CONFIG)" MODULES= STAGE_DIR="$(STAGE_DIR)"

clean:
	$(MAKE) -C $(PG_MAJOR) clean PG_CONFIG="$(PG_CONFIG)"

.PHONY: all install uninstall clean stage
