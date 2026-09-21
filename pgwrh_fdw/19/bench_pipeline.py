#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""Reproducible cursor-pipeline benchmark using a private cluster and wire relay."""
import argparse
import json
import statistics
import time

from support import Cluster, literal
from test_feature_pipeline import WireProxy


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--shards', type=int, default=8)
    parser.add_argument('--rows', type=int, default=1000, help='rows per shard')
    parser.add_argument('--fetch-size', type=int, default=100)
    parser.add_argument('--delay-ms', type=float, default=5, help='simulated round-trip delay')
    parser.add_argument('--repeats', type=int, default=3)
    args = parser.parse_args()
    if min(args.shards, args.rows, args.fetch_size, args.repeats) < 1 or args.delay_ms < 0:
        parser.error('counts must be positive and delay nonnegative')
    cluster = Cluster()
    proxy = None
    try:
        cluster.setup()
        proxy = WireProxy(cluster, delay=args.delay_ms / 1000)
        with cluster.connect() as c:
            c.sql(f"""CREATE EXTENSION pgwrh_fdw;
                SET statement_timeout='60s'; SET max_parallel_workers_per_gather=0;
                CREATE TABLE items(k int) PARTITION BY RANGE(k);
                CREATE SERVER shared FOREIGN DATA WRAPPER pgwrh_fdw OPTIONS
                    (host {literal(proxy.path)}, port '{cluster.port}', dbname 'postgres',
                     async_capable 'true', pipeline_depth '0', fetch_size '{args.fetch_size}');
                CREATE USER MAPPING FOR CURRENT_USER SERVER shared;""")
            for shard in range(args.shards):
                low, high = shard * args.rows, (shard + 1) * args.rows
                c.sql(f"""CREATE TABLE stored{shard} AS SELECT generate_series({low},{high-1}) k;
                    CREATE FOREIGN TABLE f{shard} PARTITION OF items FOR VALUES FROM ({low}) TO ({high})
                    SERVER shared OPTIONS(schema_name 'public', table_name 'stored{shard}');""")
            plan = c.scalar('EXPLAIN (FORMAT JSON) SELECT * FROM items')
            if '"Async Capable": true' not in plan:
                raise RuntimeError('benchmark requires an async Append')
            for depth in [0, 1, 8]:
                c.sql(f"ALTER SERVER shared OPTIONS(SET pipeline_depth '{depth}')")
                c.sql('SELECT count(*) FROM items')  # Warm the member connection.
                connections_before = proxy.connections
                first_times, full_times = [], []
                for _ in range(args.repeats):
                    c.sql('BEGIN; DECLARE measured CURSOR FOR SELECT * FROM items')
                    start = time.perf_counter()
                    rows = c.sql('FETCH 1 FROM measured')
                    first_times.append(1000 * (time.perf_counter() - start))
                    rows += c.sql('FETCH ALL FROM measured')
                    c.sql('CLOSE measured; COMMIT')
                    full_times.append(1000 * (time.perf_counter() - start))
                    if sorted(int(row[0]) for row in rows) != list(range(args.shards * args.rows)):
                        raise AssertionError('incorrect rows')
                print(json.dumps(dict(pipeline_depth=depth, shards=args.shards,
                    rows=args.shards*args.rows, fetch_size=args.fetch_size,
                    simulated_rtt_ms=args.delay_ms, repeats=args.repeats,
                    first_row_ms=round(statistics.median(first_times), 3),
                    full_scan_ms=round(statistics.median(full_times), 3),
                    member_connections=int(c.scalar("SELECT count(*) FROM pgwrh_fdw_get_connections()")), reconnects=proxy.connections-connections_before)), flush=True)
    finally:
        if proxy:
            proxy.close()
        cluster.close()


if __name__ == '__main__':
    main()
