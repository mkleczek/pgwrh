# Selection-independent controller handoff

Status: executable proposed protocol, evaluated 2026-11-02. The scope is one
externally exclusive controller routing writes to shard writers. WRH remains
the runtime policy for desired assignments. Consensus, controller election,
shard leader election and a distributed lease service are outside this model.

[`controller_handoff.qnt`](controller_handoff.qnt) extends the specification set
with durable authority phases, arbitrary candidate requests, topology changes
and controller restart. It complements the original WAL model and the retention
frontier model; there is no refinement/composition proof between the three.

## Selection and authority

The desired holder set and desired writer belong to an immutable placement
revision. The model takes them as inputs; it does not implement or verify WRH.
`request(target)` can choose **any** holder authorized by the current revision,
including a holder other than `desired`. Checking this larger behavior set
prevents a safety argument from depending on hash rank or a fixed writer cycle.

A request freezes `(predecessor epoch, source, target, placement revision)`.
Only one operation can be in progress for the shard. Subsequent revisions may
remove the source or target from desired placement without changing the frozen
operation or authorizing physical removal. Once closure starts, finish that
operation and reconcile newer desired placement afterward. Cancellation is
allowed only in `planned`, before any CLOSE command can have been emitted.

## Durable phases

| Journal phase | Admission and permitted next actions |
| --- | --- |
| `writing` | Route to the durable active epoch; request a handoff. |
| `planned` | Persist the frozen operation; old admission may continue. Cancel or close admission. |
| `closing` | Admission stays closed across restart. Emit CLOSE; already admitted remote transactions commit or abort. Source drains and persists its terminal certificate. |
| `sealed` | Persist observation of the terminal certificate. Successor catches up, obtains replay history and persists acceptance plus a retention promise. |
| `opening` | The new assignment is durable in the controller journal. Emit OPEN; successor durably opens the new epoch. Observe completion before publishing the new route. |

Source sealing, observing that seal, successor acceptance, controller activation,
successor opening and route publication are separate transitions. CLOSE,
ACCEPT and OPEN commands can be delayed across a controller crash and replayed
after later handoffs. Source and successor commands do not require the
controller to remain online after issuance.

Each node preserves durable closed-epoch tombstones. An old OPEN cannot reopen
a closed epoch, including after A→B→A or after removing and re-adding a logical
copy. Copy removal does not erase authority metadata for that node incarnation.

Controller crash clears its volatile route and recovery status. Durable intent,
phase, assignment, commands, node evidence and remote transactions survive.
Restart initially admits nothing. Recovery consults the journal: it can restore
old routing in `writing`/`planned`, but must resume the existing operation in
`closing`/`sealed`/`opening`. The current desired writer never determines a
recovered write route. A remote transaction may commit while the controller is
down; loss of its connection or reply is not evidence of abort or completion.

## Topology, data and retention

Desired membership, physical copy existence, stream edges and write authority
are separate state. New copies can be installed; edges can be added/removed;
old copies can be retired. Removing the only useful stream may block catch-up,
but cannot authorize an incomplete successor. The source may become unavailable
after a durable terminal certificate without preventing a qualified successor
from opening. An unavailable source without such a certificate blocks handoff.

The model conservatively keeps one inherited consumer obligation at the initial
baseline. A promised donor retains all transactions in its own data prefix;
the active writer must hold the entire committed prefix as replayable history.
Successor acceptance installs its promise before predecessor protection can be
released. Snapshot data supplies no pre-snapshot replay history. A returning
writer whose history was collected must repair it before becoming eligible.
Consumer positions, generations, compressed frontiers and shared-slot collection
remain covered separately by `obligation_frontiers.qnt`.

Edges abstract usable ordered replication or historical repair, including
transitive sources that actually possess the required replay transaction.
`replicate` applies the next missing original transaction, atomically with its
durable progress; repair can restore replay for already-present data. This is
an abstract contract, **not** verification of PostgreSQL origin filtering,
stream attachment, slot cloning or original-to-local LSN mapping. In particular,
the check does not establish liveness of the proposed ranked `origin=none`
topology; the original model exercises some of those mechanisms separately.

## Checked properties

`safety` conjoins:

- At most one open grant, belonging to the controller's durable active epoch.
- Closed grants never reopen; no admitted transaction survives its epoch seal.
- Every published route agrees with recovered journal authority and an open grant.
- Every copy contains a prefix of original committed history, with replay a
  subset of that copy's data.
- The active writer covers the full committed prefix and holds its retention pin.
- Pinned donors retain their prefixes; acknowledged transactions remain durable
  at the active writer through every handoff.
- At most one activation operation per predecessor epoch; each begun successor
  has predecessor closure, successor acceptance and controller authorization.
- Every issued authority operation names a target authorized by its captured
  immutable placement revision.
- Terminal certificates bound all transactions of their closed epoch; transaction
  identities are unique and pending work belongs to the active epoch.

The reference transaction IDs, ordered history and certificate `end` indices
are ghost values used to check these properties. Runtime watermarks must remain
scoped by shard/epoch/writer incarnation and original commit-end LSN, with
certified predecessor links. This model introduces no global sequence service
or cross-writer numeric LSN comparison.

The central safety argument does not mention rank. Only `openWriter` adds a
grant: it needs an issued authorization, a certified closed predecessor and
successor acceptance, and rejects a grant already closed locally. The journal
serializes operations, so a later grant cannot open until the preceding grant
has closed. Replaying an older OPEN cannot restore a closed grant. Only `admit`
creates outstanding work; durable gate closure precedes CLOSE issuance, and
sealing waits for that work to settle. Only `activate` changes the active owner,
after full prefix/history coverage and a successor pin are established. Topology
updates and crashes change none of those durable facts. This explains the
invariants being checked; it is not a separate machine-checked unbounded proof.

## Verification and reproducibility

Run the complete pinned gate from the repository root:

```sh
QUINT=/path/to/quint python3 spec/check.py --verify \
  --output .build/controller-handoff
```

Quint 0.32.0 and Java 21 are required; tool versions and historical setup are
described in [the earlier evaluation](frontier-evaluation.md). The acceptance
record is [`evidence/2026-11-02-controller-handoff/summary.json`](evidence/2026-11-02-controller-handoff/summary.json),
with source/runner hashes, exact commands, raw-log hashes, scenario counts,
seeded witness counts and completed TLC state counts. The gate requires every
advertised controller witness to occur and requires zero unexplored TLC states.

The default simulation instance has four nodes, four epochs, four transaction
admissions, three topology revisions after the initial revision and three
controller crashes. Node crash/restart toggles can recur. The directed scenarios
include A→B→C→A with actual writes, topology changes and controller restarts in
the same run; crash with outstanding remote work; lost activation replies;
stale commands after writer reuse and copy removal; snapshot/history repair;
and donor retirement. Six deliberate mutation scenarios demonstrate violations
from rank-based recovery, forgotten in-flight work, stale OPEN, unfenced
activation, snapshot-only donor qualification and premature removal.

Two parameter instances use the **full asynchronous `step` relation**, rather
than a prescribed phase or writer schedule:

| TLC instance | Bounds |
| --- | --- |
| `handoff_exhaustive` | Two nodes, three epochs, two transaction admissions, one topology revision and one controller crash. Includes repeated A→B→A with data commits and all permitted interleavings. |
| `handoff_three_node_exhaustive` | Three nodes, four epochs, zero application transactions, one topology revision and one controller crash. Exercises arbitrary eligible targets, three successive transfers and writer reuse with all permitted topology/stream/node interleavings. This instance checks authority mechanics without data payloads. |

Completed finite checks establish safety within those explicit bounds, subject
to TLC's reported fingerprint-collision probability. They are not an unbounded
proof, a liveness proof or verification of the PostgreSQL implementation.

## Acceptance results

All **74 directed scenarios** pass (11 WAL, 26 frontier, 37 controller), including the deliberate
counterexample/guard scenarios. All **4,100 seeded traces** complete
without a safety violation. The 1,500 controller traces witness two or
more transfers in 123 traces, a return to writer A in 42, topology
changes during handoff in 631, recovery after a crash during handoff
in 432, commits while the controller is down in 265, and a completed
handoff across placement revisions in 214.

| Full-relation TLC instance | Generated states | Distinct states | Search depth |
| --- | --- | --- | --- |
| `handoff_exhaustive` | 47,673,225 | 2,088,648 | 41 |
| `handoff_three_node_exhaustive` | 433,025,457 | 8,500,792 | 50 |

Both searches finish with **zero unexplored states** and no invariant
violation. The existing focused retention checks also pass (126 and
42 distinct states). The bounds and exclusions above remain part of
the result.

## Refinement obligations and exclusions

The [implementation plan](../docs/development/replica-writes-plan.md#bounded-baseline-and-remaining-gates)
tracks the concrete PostgreSQL work. Dedicated proof tasks are
[model composition/refinement #225](https://github.com/mkleczek/pgwrh/issues/225),
[inductive safety #226](https://github.com/mkleczek/pgwrh/issues/226),
[conditional liveness/availability #227](https://github.com/mkleczek/pgwrh/issues/227),
and [authority-metadata compaction #228](https://github.com/mkleczek/pgwrh/issues/228).
The existing implementation issues remain open until their runtime gates pass;
the bounded results above do not discharge these obligations. Consensus and
controller/shard election remain outside every task's contract.

| Model transition / assumption | Required concrete evidence |
| --- | --- |
| Exclusive controller | Existing external single-primary guarantee, including after restart; no overlapping independent controllers. |
| `commit` | One actual PostgreSQL transaction; successful durable flush before a success acknowledgement. The model does not verify FDW participant admission. |
| `closeAdmission` | Durable phase update coordinated with all local admission paths; no stale cache can admit an old-epoch transaction. |
| `closeWriter` | Writer-side barrier over every admitted backend/connection, resolution or completion of uncertain remote transactions, and a durable terminal record. The global pending set is specification bookkeeping, not a proposed controller transaction registry. |
| `accept` | Gap-free original-prefix coverage plus durable replay/mapping/slot protection for inherited obligations. Receiving a command or having current rows is insufficient. |
| `activate` → `sendOpen` | Atomic, serialized controller journal persistence before any successor OPEN can escape. |
| `openWriter` | Durable node-local BEGIN/grant and closed-epoch rejection; idempotent operation identity across retry, writer reuse and logical-copy removal. |
| `recover` | Recover journal authority and operation phase, discover durable node evidence, and reconcile ambiguous outcomes before reopening admission. |
| Replication / repair | Ordered, deduplicated apply with original identities, durable cursor and row effects; real slot and stream lifecycle. |
| `releasePin` / `dropCopy` | Discoverable durable replacement protection before releasing old history, while retaining authority tombstones. |

Controller and node crashes preserve durable storage. Permanent storage loss,
new node incarnations, synchronous/quorum durability-policy changes, multi-shard
transaction composition, schema readiness and PITR are not verified here.
The local-durable mode makes no zero-data-loss claim after permanent writer
storage loss. No timeout, observed reachability or CRDT merge promotes a writer.
Authority certificates, command identities and tombstones are retained for the
bounded history; garbage collection or compaction of that authority metadata
is not verified. Application dispatch/server admission and durable certificate
discovery remain abstract interfaces. A concrete implementation must reject
late application requests for closed epochs and obtain valid persisted evidence
before advancing a phase; a lost response is never positive evidence.
Transactions with earlier remote reads must retain their actual participant
pin; they must not become old-epoch writers after closure. That FDW admission
contract is a refinement obligation, not checked by this one-shard model.

Safety permits periods with no open writer and intentionally blocked handoffs.
Eventual completion requires responsive participants, usable history paths,
eventual settlement of admitted work and fair delivery/scheduling. Bounds on
write pauses, queued request behavior and seamless application-level migration
need separate liveness/performance contracts and implementation evidence.
