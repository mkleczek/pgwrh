#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""Measure foreign rows returned with and without partial aggregate pushdown."""
import argparse
import json

from support import Cluster, literal


def nodes(plan):
    yield plan
    for child in plan.get('Plans', []):
        yield from nodes(child)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows', type=int, default=1_000_000)
    args = parser.parse_args()
    if args.rows < 100 or args.rows % 20:
        parser.error('--rows must be a multiple of 20 and at least 100')
    cluster = Cluster()
    try:
        cluster.setup()
        with cluster.connect() as c:
            c.sql('CREATE EXTENSION pgwrh_fdw; SET max_parallel_workers_per_gather=0; '
                  'CREATE TABLE sales(id int,country int,amount int) PARTITION BY RANGE(id)')
            for part in range(2):
                lower = part * (args.rows // 2)
                upper = (part+1) * (args.rows // 2)
                c.sql(f'''CREATE TABLE stored{part} AS SELECT i AS id,i%10 AS country,i%100 AS amount
                    FROM generate_series({lower},{upper-1}) i;
                    CREATE SERVER s{part} FOREIGN DATA WRAPPER pgwrh_fdw
                      OPTIONS(host {literal(cluster.path)},port '{cluster.port}',dbname 'postgres');
                    CREATE USER MAPPING FOR CURRENT_USER SERVER s{part};
                    CREATE FOREIGN TABLE f{part} PARTITION OF sales
                      FOR VALUES FROM ({lower}) TO ({upper}) SERVER s{part}
                      OPTIONS(schema_name 'public',table_name 'stored{part}')''')
                c.sql(f'ANALYZE f{part}')
            query = 'SELECT country,count(*),sum(amount) FROM sales GROUP BY country ORDER BY country'
            results = []
            for setting in ['off', 'on']:
                c.sql('SET enable_partitionwise_aggregate='+setting)
                plan = json.loads(c.scalar('EXPLAIN(ANALYZE,VERBOSE,FORMAT JSON) '+query))[0]['Plan']
                scans = [n for n in nodes(plan) if 'Remote SQL' in n]
                transferred = sum(n['Actual Rows']*n['Actual Loops'] for n in scans)
                results.append(c.sql(query))
                print(json.dumps({'enable_partitionwise_aggregate': setting,
                    'source_rows': args.rows, 'foreign_rows_returned': transferred,
                    'result_rows': len(results[-1]),
                    'local_finalize': any(n.get('Partial Mode') == 'Finalize' for n in nodes(plan)),
                    'remote_sql': [n['Remote SQL'] for n in scans]}))
                assert transferred == (args.rows if setting == 'off' else 20)
            assert results[0] == results[1]
    finally:
        cluster.close()


if __name__ == '__main__':
    main()
