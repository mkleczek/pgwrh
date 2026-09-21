EXTENSION = pgwrh_gist_extra
MODULE_big = pgwrh_gist_extra
OBJS = pgwrh_gist_extra.o
DATA = pgwrh_gist_extra--1.0.0-alpha1.sql

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

STAGE_DIR ?= $(abspath ../.build/test-stage)
stage: all
	mkdir -p "$(STAGE_DIR)/extension"
	cp pgwrh_gist_extra.control $(DATA) "$(STAGE_DIR)/extension/"
	cp $(shlib) "$(STAGE_DIR)/"

.PHONY: stage
