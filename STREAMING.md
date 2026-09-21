# Cursor-free row streaming

Set `streaming_fetch 'true'` on a foreign server or foreign table to execute
eligible reads as plain extended-protocol SELECTs, without remote DECLARE,
FETCH or CLOSE commands. The default is false; a table setting overrides its
server. A pushed-down join enables streaming if either input enables it.

```sql
ALTER SERVER member OPTIONS (ADD pipeline_depth '8');
ALTER SERVER virtual_server OPTIONS
    (ADD streaming_fetch 'true', ADD async_capable 'true');
```

For an ordinary server, put all three options on that server. On a virtual
server, `streaming_fetch`, `async_capable` and `fetch_size` describe the scan;
`pipeline_depth` belongs to the selected ordinary member. Existing routing,
user mapping, transaction context and read-after-LSN initialization still run
before query submission. Host names alone do not merge cached connections.

Streaming works for ordinary Foreign Scans, synchronous children such as
Merge Append, and asynchronous Append children. `async_capable` is needed only
for the latter. `pipeline_depth` bounds the combined number of in-flight
streamed SELECTs and cursor FETCHes on a cached connection. Zero permits one
streamed SELECT at a time; it does not disable an explicit `streaming_fetch`.
`fetch_size` controls the libpq chunk size in rows. It no longer adds a network
round trip between chunks. It is neither a byte limit nor server-side demand
control.

The remote planner can choose parallel plans for plain SELECTs. It still decides
whether parallel workers are worthwhile, and PostgreSQL executes commands on
one connection serially. Pipelining overlaps submission, transfer and local
consumption; it does not run several shard queries simultaneously in one remote
backend. Statements that modify data, have modifying CTEs or have row marks use
the existing cursor path. Write operations and ANALYZE retain their existing
implementations.

## Buffering and lifecycle

A scan normally consumes one libpq chunk at a time. If another scan or a
synchronous command needs the same connection, the transport saves the unread
rows of earlier streams in connection-owned tuplestores. Each store can spill
beyond `work_mem`; the setting applies per store, not to the entire connection.
Chunks, libpq buffers, tuple conversion and large individual rows add memory
outside that budget. The saved values are raw remote text: draining never
invokes a user-defined tuple input function or executor callback. Conversion
happens only when the owning scan consumes its rows.

A write, savepoint boundary or transaction completion settles pending wire
operations first. Buffered outer results survive a successful nested query's
rollback. A failed or discarded stream cannot be mistaken for EOF. Errors after
partial results remain errors, and cancellation/connection loss use the shared
pipeline recovery path. Rescans rewind the first local batch when possible;
otherwise they drain and reexecute with the current parameters. Lookup joins
release their stream before replacing lookup parameters.

An early stop drains the unused stream without cancelling the shared remote
transaction. Consequently a LIMIT that is not pushed down can transfer the
entire remote result and can encounter errors in otherwise unconsumed rows.
Prefer cursor mode when demand control and early termination matter more than
remote parallel plans or avoiding FETCH round trips. Pushed-down LIMIT still
limits the SELECT sent to the remote server.

## Validation and measurement

In an assembled standalone FDW tree, select PostgreSQL with `PG_CONFIG`:

```
python3 -m unittest -v test_feature_streaming
python3 bench_streaming.py --shards 4 --rows 1000 --fetch-size 100 --delay-ms 5
python3 bench_streaming.py --shards 4 --rows 1000 --fetch-size 100 --delay-ms 0
```

The 66 tests exercise chunk sizes and depths, NULL/empty results, wire order,
first rows before remote completion, standalone/async/mixed scans, spills,
limits, rescans, lookup joins, transaction context, virtual members, row-lock
fallback, conversion errors, late remote errors, interrupted spills,
cancellation and reconnection. A remote-only auto_explain test verifies actual
parallel workers launched in streaming mode and their absence in cursor mode.

The benchmark compares ordinary cursor scans, pipelined cursor scans, and
pipelined streaming. It reports first-row and total latency, remote DataRows,
wire bytes, spill-file bytes and connection count for full scans and early
stops. It validates returned values. The relay's optional delay approximates
RTT; it is a development measurement, not a production network emulator.
Local FETCH batches avoid materializing the whole result in a client cursor's
utility-result store; very wide batches can still contribute local spills.

A development run on PostgreSQL 18.6 / 19beta3, four shards of 1,000 rows,
128-byte payloads, chunk size 100 and two repetitions measured the following
median full-scan times with a simulated 5 ms RTT:

| Mode | PostgreSQL 18 | PostgreSQL 19 |
| --- | ---: | ---: |
| Cursor | 359 ms | 368 ms |
| Pipelined cursor | 214 ms | 201 ms |
| Pipelined stream | 35 ms | 29 ms |

All modes used one connection and this sequential-consumption workload caused
no spill. Stopping after one row transferred 100 / 400 / 4,000 remote rows,
respectively. The PostgreSQL 18 zero-delay control took 12.6 / 11.5 / 9.3 ms
for full scans, but 0.32 / 0.55 / 1.44 ms for early stops. These small local
measurements are illustrative; benchmark the actual query mix and network.

## Provenance and licensing

The `streaming_fetch` option, cursor-free SELECT approach and chunk-size mapping
follow Rafia Sabih's proposal in [Bypassing cursors in postgres_fdw to enable
parallel plans](https://commitfest.postgresql.org/patch/6233/), including its
[v18 patch](https://www.postgresql.org/message-id/attachment/203836/v18-0002-postgres_fdw-Add-streaming_fetch-option-for-curs.patch).
The original idea is attributed there to Bernd Helmle. This implementation uses
the shared pipeline queue, retains asynchronous execution and buffers raw
connection-owned results rather than retaining executor scan pointers.

These additions and adaptations remain **AGPL-3.0-only**, as described in
[LICENSING.md](LICENSING.md). Inherited PostgreSQL notices remain intact. This
is a local implementation, not a claim that the upstream proposal has merged
or that these changes have been relicensed.
