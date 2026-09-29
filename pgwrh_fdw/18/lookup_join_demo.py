#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""Reproduce selective lookup EXPLAIN ANALYZE in disposable databases."""
from test_lookup_join import LookupJoinTests

LookupJoinTests.setUpClass()
example = LookupJoinTests('test_inner_occurrences_and_retained_output')
try:
    example.setUp()
    example.c.sql('UPDATE lookup SET enabled=false WHERE k>=10000')
    query = 'SELECT s.id,s.k,l.label FROM items s JOIN lookup l ON s.k=l.k WHERE l.enabled'
    for enabled in ('off', 'on'):
        example.c.sql('SET pgwrh_fdw.enable_lookup_join=' + enabled)
        print('\nenable_lookup_join=' + enabled)
        for row in example.c.sql('EXPLAIN(ANALYZE,VERBOSE,COSTS OFF,TIMING OFF) ' + query):
            print(row[0])
finally:
    example.doCleanups()
    LookupJoinTests.tearDownClass()
