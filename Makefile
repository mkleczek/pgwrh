EXTENSION = pgwrh
DATA = pgwrh--1.0.2.sql pgwrh--1.0.0--1.0.1.sql pgwrh--1.0.1--1.0.2.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
