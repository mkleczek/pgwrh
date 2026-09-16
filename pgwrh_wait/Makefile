EXTENSION = pgwrh_wait
MODULE_big = pgwrh_wait
OBJS = src/monitor.o src/wait.o
DATA = pgwrh_wait--1.0.sql

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

$(OBJS): src/monitor.h

STAGE_DIR ?= $(abspath ../.build/test-stage)
stage: all
	mkdir -p "$(STAGE_DIR)/extension"
	cp pgwrh_wait.control $(DATA) "$(STAGE_DIR)/extension/"
	cp $(shlib) "$(STAGE_DIR)/"

.PHONY: stage
