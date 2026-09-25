# Shared-connection cursor pipelining

Async scans can queue cursor creation and bounded FETCH operations on the same
connection. Set the ordinary foreign server option `pipeline_depth` to a value
from 0 to 1024 (default 0). Use 8 to enable a bounded pipeline; zero selects the
legacy single-request path. Only
async SELECT scans without row locks or modifying CTEs participate. Enable
`async_capable` on the server/table; an async Append must be chosen by the planner.

The depth limits FETCH requests still on the wire for each cached connection.
There is at most one unconsumed batch per scan. `fetch_size` controls batch rows;
neither setting is a byte limit. Completed batches may be buffered for different
scans, so memory also depends on row width and the number of participating scans.

Commands on one PostgreSQL connection execute serially. Pipelining removes
client round trips between requests; it does not execute shard queries in
parallel inside the remote backend. Existing connection and user-mapping cache
keys still apply: matching host names alone do not cause connections to merge.
The remote transaction and snapshot are shared exactly as before.

Each DECLARE/FETCH job ends with Sync. Ordered results are kept by a connection
queue until their scan consumes them. Wire processing never calls tuple input
functions or executor callbacks. Synchronous reads, writes, savepoints, cursor
close, and transaction completion drain the queue before using the connection.
Cancellation and rollback discard unfinished operations; a discarded operation
raises an error if an outer scan later attempts to consume it. Failed cleanup
makes the cached connection unusable. Cursor rescan and early termination settle
outstanding operations before reusing or closing cursors.

Virtual pgwrh servers use the depth of the selected member server. Configure the
option on ordinary member servers. Existing routing and transaction-context
setup take place before any command is queued.

This feature retains SQL cursors. Cursor-free result streaming and its remote
parallel-plan benefits are separate work. All additions are AGPL-3.0-only.

## Validation and measurement

From an assembled standalone FDW tree, with `PG_CONFIG` selecting the desired
PostgreSQL installation:

```
python3 -m unittest -v test_feature_pipeline
python3 bench_pipeline.py --shards 8 --rows 1000 --fetch-size 100 --delay-ms 5
```

The tests use an instrumented Unix socket relay and a remote advisory lock to
prove that multiple extended-protocol FETCH commands arrive before the first
one completes, on one connection. They also cover depth 0/1/2/8, empty results,
LIMIT, parameterized rescans, nested Appends, large parameters/results, local
conversion errors, remote errors, cancellation, connection loss, savepoints,
parallel commit/abort, mixed local scans, row-lock fallback, and virtual members.

The benchmark reports median first-row and full-scan times and reconnects, checks
every result, and compares depth 0, 1, and 8 on the same single-connection topology.
Its optional relay delay approximates network RTT; it is not a production network
emulator. Run with `--delay-ms 0` as a local overhead control. Real-world benefits
depend on RTT, fetch size, shard count, row width, and remote execution time.

A smoke measurement on PostgreSQL 18.6 and 19beta3 with eight shards, 1,000 rows
per shard, fetch size 100, two repetitions and a simulated 5 ms RTT reduced
median full-scan time from about 700 ms (depth 0) to 466 ms (depth 8), with one
connection and no reconnects. On the PostgreSQL 18 local-delay control, depth 8
was slower (20.6 ms versus 15.3 ms). These are relay-based development measurements,
not deployment guarantees; the feature is opt-in because local workloads can
pay protocol and scheduling overhead without enough RTT savings.
