#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""Compare cursor and streamed scans, including spill and early-stop transfer."""
import argparse
import json
import re
import statistics
import time

from support import Cluster, literal
from test_feature_pipeline import WireProxy


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--shards', type=int, default=4)
    parser.add_argument('--rows', type=int, default=1000, help='rows per shard')
    parser.add_argument('--fetch-size', type=int, default=100)
    parser.add_argument('--width', type=int, default=128, help='payload bytes per row')
    parser.add_argument('--work-mem-kb', type=int, default=64)
    parser.add_argument('--delay-ms', type=float, default=5)
    parser.add_argument('--repeats', type=int, default=3)
    args = parser.parse_args()
    if min(args.shards, args.rows, args.fetch_size, args.width, args.repeats) < 1 or args.delay_ms < 0 or args.work_mem_kb < 64:
        parser.error('counts must be positive, work_mem at least 64 kB, delay nonnegative')
    cluster, proxy = Cluster(), None
    try:
        cluster.setup()
        proxy = WireProxy(cluster, delay=args.delay_ms/1000)
        with cluster.connect() as c:
            c.sql(f"""CREATE EXTENSION pgwrh_fdw;
                SET statement_timeout='120s'; SET max_parallel_workers_per_gather=0;
                SET work_mem='{args.work_mem_kb}kB'; SET log_temp_files=0;
                CREATE TABLE items(k int,v text) PARTITION BY RANGE(k);
                CREATE SERVER shared FOREIGN DATA WRAPPER pgwrh_fdw OPTIONS
                    (host {literal(proxy.path)}, port '{cluster.port}', dbname 'postgres',
                     async_capable 'true', pipeline_depth '0', streaming_fetch 'false', fetch_size '{args.fetch_size}');
                CREATE USER MAPPING FOR CURRENT_USER SERVER shared;""")
            for shard in range(args.shards):
                low,high = shard*args.rows,(shard+1)*args.rows
                c.sql(f"""CREATE TABLE stored{shard} AS SELECT i k, repeat('x',{args.width}) v FROM generate_series({low},{high-1}) i;
                    CREATE FOREIGN TABLE f{shard} PARTITION OF items FOR VALUES FROM ({low}) TO ({high})
                    SERVER shared OPTIONS(schema_name 'public',table_name 'stored{shard}');""")
            if '"Async Capable": true' not in c.scalar('EXPLAIN (FORMAT JSON) SELECT * FROM items'):
                raise RuntimeError('benchmark requires async Append')
            for label,depth,stream in [('cursor',0,'false'),('pipeline_cursor',8,'false'),('pipeline_stream',8,'true')]:
                c.sql(f"ALTER SERVER shared OPTIONS(SET pipeline_depth '{depth}', SET streaming_fetch '{stream}')")
                c.sql('SELECT count(*) FROM items')
                for early in [False,True]:
                    results=[]
                    for _ in range(args.repeats):
                        c.sql('BEGIN; DECLARE measured NO SCROLL CURSOR FOR SELECT * FROM items')
                        before_rows,before_bytes=proxy.backend_rows,proxy.backend_bytes
                        log_start=cluster.log.stat().st_size
                        start=time.perf_counter()
                        rows=c.sql('FETCH 1 FROM measured')
                        first=1000*(time.perf_counter()-start)
                        if not early:
                            while True:
                                batch=c.sql(f'FETCH {args.fetch_size} FROM measured')
                                rows+=batch
                                if len(batch)<args.fetch_size:
                                    break
                        c.sql('CLOSE measured; COMMIT')
                        elapsed=1000*(time.perf_counter()-start)
                        if early:
                            assert len(rows)==1 and 0 <= int(rows[0][0]) < args.shards*args.rows
                        else:
                            assert sorted(int(r[0]) for r in rows)==list(range(args.shards*args.rows))
                        assert all(r[1]=='x'*args.width for r in rows)
                        spills=[int(n) for n in re.findall(r'temporary file:.*size (\d+)',cluster.log.read_text()[log_start:])]
                        results.append(dict(first_row_ms=first,total_ms=elapsed,
                            remote_rows=proxy.backend_rows-before_rows,wire_bytes=proxy.backend_bytes-before_bytes,
                            spill_bytes=sum(spills),returned_rows=len(rows)))
                    metrics={k:round(statistics.median(r[k] for r in results),3) for k in results[0]}
                    print(json.dumps(dict(mode=label,early_stop=early,shards=args.shards,rows_per_shard=args.rows,
                        fetch_size=args.fetch_size,width=args.width,work_mem_kb=args.work_mem_kb,
                        simulated_rtt_ms=args.delay_ms,**metrics,
                        member_connections=int(c.scalar('SELECT count(*) FROM pgwrh_fdw_get_connections()')))),flush=True)
    finally:
        if proxy: proxy.close()
        cluster.close()


if __name__=='__main__':
    main()
