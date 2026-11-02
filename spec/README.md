# Writer handoff specification

`writer_handoff.qnt` is an executable Quint design model for moving shard writes
off the controller. It describes a proposed protocol, not the current pgwrh
implementation. It deliberately precedes a decision about apply-worker hooks.

The first model has one shard, replicas A/B/C, and one planned writer change
from A to B. C can remain offline throughout promotion. Exactly one active
controller is the sole entry point for application writes. It forwards each
shard's writes to one selected replica, stores no application data, and maintains
no subscriber progress registry. Replicas accept application writes only through
that controller; replication apply is a separate internal path. Publisher slots
and subscriber-local durable cursors supply progress.

This scope avoids a distributed replica-lease or writer-election protocol.
The controller controls admission of writes and serializes route changes. A
single logical endpoint backed by multiple independently active controllers
would not satisfy the assumption. Controller high availability is out of scope
for this first model.

**This is a safety model with explicit assumptions, not a proof of the complete
design.** Scenario tests and randomized simulation are useful evidence; they do
not establish exhaustive safety or liveness.

The model currently bounds the shard history to eight original transactions
(including control transactions), sixteen local WAL allocations per node, and
one reinitialization per node. Slot identities are never reused. Replicas have
independent local LSN spaces, with different bases and strides.

The central quantities are:

| Quantity | Meaning and owner |
| --- | --- |
| `log` / `Entry.w` | Ghost reference history and ordinal, used to state ordering properties. Neither exists as a controller service. |
| `epoch`, `author`, `original` | Original transaction identity; the real protocol also includes shard and publisher incarnation. |
| `Entry.lsn` | Commit-end LSN of this transaction's copy on this particular node. |
| `buffer`, `pending` | Locally committed WAL that has not been flushed. A crash discards it. |
| `cursor`, `rows`, `wal` | Durable applied prefix, row effects, and WAL transactions on a replica. |
| `index` | Durable mapping from original watermarks to this node's commit-end LSNs. |
| `historyPos` | Resume position of the node's pre-existing history slot H. |
| `Slot.confirmed` | Publisher-side logical slot feedback position. |
| `Slot.restart` | Publisher-side WAL retention boundary, allowed to lag confirmation. |
| `Slot.read`, `Slot.ack` | Subscriber-side received position and durable feedback position. These are not publisher knowledge until feedback arrives. |
| `safe` | Latest durably applied safe-through certificate on a replica. |
| `inheritedFloor`, `transfers` | Retention bound inherited by B and the subscribers whose replacement-slot installation B has not yet acknowledged. No per-subscriber apply positions are stored here. |
| `writeTargets`, `oldRouteClosed` | Controller-local write admission and closure of A's old route, not replica-issued leases. |

Ghost ordinals allow the checker to compare prefixes across writers. A real
implementation must use the epoch predecessor chain and original transaction
identities. It must never compare raw LSNs from A and B. The invariant reference
history is not information available to a protocol participant.

The WAL and slot algorithm represented by the actions is:

1. **Initialize.** Establish a consistent snapshot baseline and create H on
   every future donor before accepting the history it must preserve. Create
   ordinary A→B, A→C and B→C slots with `origin=none`.
2. **Write.** The writer writes rows, a transactional logical marker identifying
   shard/epoch/writer, then COMMIT. Its commit-end LSN completes the original
   watermark. `commitOriginal` represents that local WAL transaction;
   `flush` separately makes it durable and eligible for publication.
3. **Apply.** `readStream` represents a locally committed transaction containing
   the changes, an `APPLIED(original_watermark)` marker, and source progress.
   The original watermark survives republishing. This transaction remains
   unacknowledged until `flush` makes all three durable together.
4. **Acknowledge.** `feedback` delivers the durable position to the publisher.
   Delay, disconnect, crash and duplicate delivery can intervene. Receipt or
   local commit without flush is insufficient.
5. **Index.** `indexWal` represents a decoder committing original-watermark to
   local-commit-LSN mappings. The implementation can clone H for this decoder;
   the model abstracts that extra slot under H's retention protection.
6. **Certify retention.** The writer computes the oldest relevant valid slot's
   `confirmed_flush_lsn`, including inactive slots. It translates this through
   its own original transactions into a watermark F, and commits
   `SAFE_THROUGH(epoch, F)` as a transactional control message.
7. **Release history.** After that message and the mapping are durable, a replica
   advances H to `M_local(F)`. It never advances H to the LSN of the later
   announcement. `recycle` conservatively models the most aggressive recycling
   permitted by H and all remaining slots' restart positions.
8. **Pause and drain A.** `pauseWrites` closes the controller's admission gate
   for this shard. Already admitted transactions remain bound to A and must
   finish remotely. In the model, an already committed, unflushed transaction
   may still flush. Only after draining can A commit and flush `END_EPOCH`,
   carrying the terminal watermark and frozen oldest-slot bound.
9. **Promote B.** B must durably apply END and retain the history needed from
   that bound. The controller selects B, B inherits the bound, and B commits
   and flushes `BEGIN_EPOCH` before the controller admits new data writes.
10. **Prepare a subscriber.** Pause and drain its previous stream, obtaining
    durable cursor Q. On B, clone B's existing H. Advance the clone forward to
    `M_B(Q)`. A fresh slot created at the current WAL tip cannot replace this
    historical clone; an existing advanced slot cannot be rewound.
11. **Install the replacement.** Persist the subscriber's attachment to the
    clone, initially with `origin=any`. The reader resumes after Q, rejecting
    gaps and suppressing duplicate original transactions.
12. **Acknowledge installation.** Separately, B receives proof of durable
    installation. Only then does this subscriber leave `transfers`. Old slots
    can subsequently be retired. Crashing between installation and its
    acknowledgement conservatively preserves the inherited bound.
13. **Resume ordinary filtering.** After applying the predecessor through END,
    the same replacement slot can switch to `origin=none`. B's new transactions
    then continue on that slot.
14. **Handle loss.** Losing a required subscription slot marks its subscriber as
    needing a new snapshot, including when it is offline. Reinitialization
    couples that snapshot with a fresh slot and a new local history baseline.

The original writer cannot put its final commit-end LSN inside an earlier WAL
message. The model's `original` field denotes information obtained from decoding
COMMIT, not an implementation instruction to predict that LSN. A replica knows
the original watermark before its own COMMIT, but learns the local mapping only
afterward. Control-only transactions also require durable local application.

The important addition to the earlier algorithm is the **inherited retention
bound**. An ordinary B→C slot may advance while filtering out all A-origin
transactions. That position does not establish that C applied A's prefix. Until
replacement slots cover the inherited obligations, B publishes the minimum of
its current slot bound and the inherited bound. Publisher-side membership and
installation acknowledgements suffice; the controller need not track each
replica's ongoing apply position.

`historyProtectsSubscribers` states that a valid history slot on an initialized
donor must preserve every transaction an initialized lagging subscriber still
needs, even if that subscriber is offline. A donor initialized from a newer
snapshot is not claimed to have history preceding its snapshot. Promotion is
blocked if the required historical mapping or history is unavailable.

Other safety predicates check durable prefix equality, preservation of original
transaction identities, controller admission and epoch barriers, durable feedback, slot
position bounds, and preservation of WAL needed by valid slots. These are
conjoined in `safety`. The ghost `rows` list makes duplicate application and
missing prefix elements observable; it does not model SQL row conflicts.

Three deliberately broken actions are excluded from `step`. Their scenario
tests require a safety predicate to fail:

- `badEarlyFeedback`: acknowledge a transaction before the subscriber flushes.
- `badAdvanceToAnnouncement`: use the announcement's local LSN instead of the
  watermark it certifies.
- `badAnnounceWithoutInheritance`: let B forget C's old-writer catch-up needs
  because B's ordinary origin-filtered slot has advanced.

Additional scenarios exercise replay after crash, duplicate delivery, draining
an in-flight write, a complete handoff with an offline subscriber, and snapshot
reinitialization after slot loss. Guard tests reject sealing before flush and
promotion before B has the terminal prefix. A separate scenario crashes the
subscriber between durable installation and its acknowledgement to B.

Run from the repository root with Quint 0.32.0:

```sh
npx --yes --package=@informalsystems/quint@0.32.0 quint typecheck spec/writer_handoff.qnt
npx --yes --package=@informalsystems/quint@0.32.0 quint test spec/writer_handoff.qnt --backend=typescript --max-samples=1
npx --yes --package=@informalsystems/quint@0.32.0 quint run spec/writer_handoff.qnt --backend=typescript --invariant=safety --max-samples=1000 --max-steps=120 --seed=20260923 --verbosity=1
```

The TypeScript evaluator avoids a separate evaluator download. Quint also has
a Rust evaluator and model-checker integrations; see its
[CLI documentation](https://quint.sh/docs/quint) and
[checking-properties guide](https://quint.sh/docs/checking-properties).
An example bounded-check command, requiring the Java/Apalache toolchain, is:

```sh
npx --yes --package=@informalsystems/quint@0.32.0 quint verify spec/writer_handoff.qnt --invariant=safety --max-steps=10
```

Validation on 2026-09-23 with Quint 0.32.0:

| Check | Result |
| --- | --- |
| Parsing, type and effect checking | Passed |
| Eleven deterministic scenarios, TypeScript evaluator | Passed, including the three expected counterexamples |
| `step`, Rust evaluator, 10,000 traces, at most 150 steps, seed `20260923` | No violation; 13 traces reached the new epoch and one completed transfers |
| `healthyStep`, Rust evaluator, 5,000 traces, at most 200 steps, seed `20260924` | No violation; 4,677 reached the new epoch, 4,150 completed transfers, 1,423 released history |
| Apalache 0.56.1, `step`, `--max-steps=3` | No violation; this shallow check does **not** reach a handoff |

The second randomized campaign excludes crashes and slot loss so those failures
do not dominate exploration before promotion. It uses the same protocol actions
and invariants. The first campaign includes those failures; the deterministic
scenarios ensure that failure windows during handoff are exercised explicitly.
The command above with `--max-steps=10` is a suggested deeper check, not a
reported validation result. No exhaustive or inductive proof has been completed.

Sole-entry routing does not make a lost remote connection a successful drain.
For example, A may still execute a COMMIT sent just before the connection failed.
The controller must establish that old remote transactions have finished before
the terminal marker and route switch. Until it can establish that, writes for
the shard remain paused. A controller restart must also recover the handoff
phase and settle any remote work from its previous process before reopening the
gate. These recovery steps are not yet modeled.

The existing [FDW membership locks](../pgwrh_fdw/docs/virtual-server-internals.md#membership-locks)
provide transaction-bound route serialization on a running controller. They are
a useful implementation primitive, but do not by themselves establish remote
quiescence after a connection failure or controller crash.

Cross-shard commit coordination is intended to be a separate responsibility of
`pgwrh_fdw` on the controller, using a recoverable two-phase commit protocol.
This is a proposed boundary: the current FDW sends ordinary remote COMMITs and
rejects PREPARE, and this Quint model does not yet model prepared participants.
The coordinator needs durable transaction identities, participant records and
decisions, integration with the controller's local transaction outcome, and
recovery of interrupted preparation and completion.

The interface to shard handoff must require:

1. An admitted transaction pins the writer of every shard it touches.
2. PREPARE does not release those pins or make a shard drained.
3. Pins remain until the remote participant is definitively committed or
   aborted, including resolution by recovery after a controller crash.
4. END_EPOCH waits for all such participants touching the shard to resolve.
   An unavailable participant can therefore block handoff.

These are durable coordinator obligations, not just locks held by the original
controller backend. Their implementation and refinement into the handoff model
remain to be specified. With logical two-phase decoding disabled, replication
can consume finalized commits without reproducing prepared transactions on all
replicas; PostgreSQL describes this behaviour in its
[logical decoding two-phase documentation](https://www.postgresql.org/docs/18/logicaldecoding-two-phase-commits.html).
Atomic commit decisions do not provide a common cross-shard read snapshot or
simultaneous visibility on asynchronously updated replicas. Those guarantees
remain outside the present scope.

The abstraction boundaries that require further specification are:

- **Controller admission and recovery:** all application writes pass through
  one active controller. Gate closure and route selection are abstract durable
  actions. The model has no controller crash, stale controller session, or
  ambiguous remote commit outcome. Those need recovery rules; replica leases
  and controller election are not part of this scope.
- **Unplanned failover:** crashes preserve durable disks. A can remain down
  after its END reached B; losing A before a terminal prefix is certified can
  block promotion. There is no assumed algorithm for choosing that prefix after
  permanent loss, and no zero-data-loss claim for asynchronous acknowledgements.
- **Multiple epochs and shards:** repeated handoffs, changing membership/ranks,
  shard-sharing slots, and the coordinator/handoff contract above are not yet modeled.
  A database-wide history slot must obey every shard's retention obligation.
- **Concurrency:** one unflushed transaction per node; no parallel apply,
  streamed transactions, prepared participants, DDL or table synchronization workers.
  A local atomic transaction is assumed to couple data, marker and source cursor.
- **Snapshot/slot initialization:** `init` and `reinitialize` abstract a correct
  consistent-snapshot plus slot protocol. New slot generations must reject stale
  feedback; unique slot IDs avoid that separate problem in this first model.
- **WAL internals:** entries represent transaction commit boundaries, not bytes,
  page records or catalog snapshots. Cloning, decoding consistency, restart LSN
  calculation, slot checkpoint recovery and retention of decoding catalogs must
  be checked against PostgreSQL. Indexed mappings survive history recycling;
  their own garbage collection is not modeled.
- **Crash persistence of metadata:** slot advances and subscription installation
  are modeled as durable actions. A concrete implementation must make this true
  or tolerate replay after PostgreSQL restores earlier slot positions.
- **Progress:** unrestricted crashes, slot loss and stuttering are allowed.
  Liveness needs fairness and eventual communication, storage and writer
  availability assumptions. There is no liveness proof in this draft.

Before implementing the apply path, extend the model with the chosen unplanned
failover and durability policy, repeated ownership changes, and the concrete
crash-recovery behaviour of PostgreSQL slots. Then verify the refined protocol
and use its traces as implementation tests.
