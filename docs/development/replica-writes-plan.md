# Replica writes and safe shard handoff: change plan

Status: proposed implementation plan, updated 2026-11-02. This document plans changes;
it does not create the jj graph or claim that the protocol has been verified.

GitHub tracking: [Replica writes and safe shard handoff](https://github.com/users/mkleczek/projects/1).
Feature issues F01–F20 group the 122 implementation and formal-specification
sub-issues; native dependency links track the integration prerequisites.

The target is a controller that remains the sole application write entry point
but need not retain application shards. It forwards writes to the highest-ranked
eligible replica for each shard. Replicas preserve enough data and history for
safe writer changes and for disconnected subscribers to resume. Transactions
requiring independently committed database participants are rejected.

## Scope and decisions

- Separate the **desired writer**, selected by rank, from the **active writer**,
  established by the handoff protocol. A rank or reachability change alone never
  grants write authority. Rank has a deterministic tie-breaker and is scoped to
  an immutable placement revision.
- Exactly one active controller admits application writes. Controller election,
  migration between logical replicas, and a distributed lease service are out
  of scope. Restart recovery of that controller is in scope. Existing physical
  controller HA remains subject to its external single-primary guarantee.
- Planned handoff must preserve all completed commits and settle in-flight
  work. Failure after a durable terminal epoch record can be recoverable without
  the old writer. Failure before a terminal prefix can be established blocks
  promotion unless the synchronous recovery protocol can establish an equivalent
  safe boundary. Synchronization alone is not a certificate to promote.
- Support two explicit durability modes: local durable commit with asynchronous
  followers, and local commit plus durable acknowledgment from a selected set of
  lower-ranked holders. Remaining holders stay asynchronous. Recommend one
  explicitly designated synchronous successor first; model configurable required
  sets and quorum policies before enabling them. Neither mode uses 2PC.
- Synchronous success means the required copies durably contain the transaction;
  it costs replication/decode/apply/flush latency. Protecting against loss of the
  writer requires a surviving acknowledged copy and a recovery protocol that
  includes its prefix. Async mode makes no zero-data-loss claim after permanent
  writer storage loss. Do not silently fall back from sync to async on timeout.
- Read-only transactions may query multiple replicas. Writable transactions may
  use only one actual remote transaction and must not also make transactional
  changes to controller-local application data or catalogs. Remember earlier remote reads when a transaction
  first writes. Multiple shards on one actual transaction need no 2PC; different
  connections to the same database still count as different participants.
  Existing transactions confined to controller-local data remain supported.
  Nontransactional sequence allocation has a separate contract under F16;
  consuming a value does not acquire an atomic rollback guarantee.
- Desired metadata is replicated controller-to-replicas. Readiness reports
  initially continue through FDW and are persisted by the controller. Neither
  reverse metadata replication nor a controller registry of every subscriber's
  continuously advancing cursor is required. Replicated metadata is a chosen
  foundation for later mobility, not a prerequisite inherent in remote writes.
- Application replication follows the proposed rank topology: subscribers have
  streams from preceding ranked holders, normally with `origin=none`. This
  delivers changes originally written by each epoch's writer; it does not
  forward another writer's changes transitively. Historical repair uses a
  separately controlled `origin=any` phase with deduplication and ordering.
  F01 must compare this baseline with direct writer-to-followers replication
  before fixing the runtime topology. Changing writer selection alone does not
  remove the offline-subscriber or historical-retention requirements.
- Losing a required subscription/history resource removes the affected resume
  or donor eligibility. A subscriber whose required slot is lost must receive
  a new snapshot. Timeouts do not silently remove offline subscribers from
  retention obligations.
- Explicit controller copy removal and restoration are in scope. Automatic
  restore on a write attempt, distributed 2PC, cluster-wide read snapshots,
  general online DDL, and transparent controller migration are deferred.
- Consistent disaster backup and point-in-time recovery are part of the
  production scope (F18–F20), including when the controller stores no shards.
  Define target precision, consistency, recovery objectives, and retention
  before selecting the protocol. Prefer avoiding global write pauses; evaluate
  online alternatives and justify any necessary blocking. Named restore points,
  a global commit journal, and a physical/logical backup format are design
  options, not prescribed implementations. Synchronous replication is not a
  substitute for independently durable recovery history.

## Evaluated evidence and retention abstraction (2026-11-02)

[The frontier evaluation](coordination-frontiers.md) and
[companion Quint model](../../spec/obligation_frontiers.qnt) assess a common
representation for authority certificates, durable coverage and retention
obligations. The original WAL/slot model remains a separate regression model;
[the evaluation record](../../spec/frontier-evaluation.md) states exact checks,
source hashes, reached scenarios and remaining refinement obligations.

This is an evaluated design candidate for F01/F02/F08/F09/F12/F14/F15, not a
runtime implementation or a change to the transaction/durability contract.
Use immutable scoped facts, componentwise advancement within certified histories,
and conservative frontiers over all outstanding obligations. Transfer durable
destination protection before releasing source protection. Different shards
and unlinked writer histories cannot be collapsed by comparing numeric LSNs.

Preserve explicit writer closure/activation, ordered row apply, local mapping
and slot mechanics, controlled membership, synchronous durability and recovery
lineage. No controller registry of every subscriber's live cursor is required;
local holders can aggregate scoped evidence. CRDT merge laws do not authorize
conflicting writers, prove that a command persisted, or reconstruct lost history.
The follow-up work is to refine these abstractions into the F05/F07/F08 runtime
mechanics and extend their formal models; do not silently replace them with a
library merge call. The antichain library and pg_dbsp remain optional mechanisms.

## jj organization

There are two review levels: **feature grouping changes** and their **small
implementation changes**. Each feature change has the tips of its implementation
stacks as parents. Independent implementation changes may be sibling parents;
dependent ones form short stacks. Ancestors need not also be direct parents.
Within each feature, the numbered components are the default implementation
order; their minimal dependency chain is a valid initial stack. Split independent
model, runtime, and test branches where useful and name their tips explicitly
in the feature description before constructing the merge.

```text
base -> implementation A -> implementation B --+
base -> independent implementation C ----------+-> feature grouping change
```

Use `codex/replica-writes-<feature>` bookmarks for project feature groups and
`codex/fdw-<feature>` for shared FDW feature groups. The IDs below are planning
labels, not jj change IDs. Feature descriptions record purpose, component IDs,
dependencies, and acceptance evidence. Keep functional edits in the component
changes; grouping changes contain only necessary integration resolutions.

Preserve the existing [shared FDW graph](../../pgwrh_fdw/UPSTREAM.md): F04 and
F10 live on the filtered FDW patch graph and feed both existing major aggregates.
Major-specific adaptations stay in those aggregates; refresh their existing
directory moves. Do not make the project tree a parent of a filtered FDW patch,
or copy edits independently into the 18 and 19 directories. Project-side SQL,
formal models, and integration tests live in the other feature groups. Existing
aggregate/move nodes are mechanical integration infrastructure, not a third
review hierarchy. A specification prerequisite across those trees is a review
gate, not an invalid cross-tree ancestry edge.

Each runtime component includes focused tests for its behavior. Dedicated test
changes below cover shared harnesses, protocol traces, and cross-feature failure
scenarios. Every intermediate change must build in its intended integration
context; unfinished behavior remains disabled. Do not create empty placeholder
changes for the entire plan before implementing it.

| Feature | Goal | Prerequisites |
| --- | --- | --- |
| F01 | Protocol contract and formal foundations | None |
| F02 | Replicated control metadata and scoped feedback | F01 |
| F03 | Stable shard identities and ranked writer selection | F02 |
| F04 | FDW single-participant transaction guard | F01 |
| F05 | PostgreSQL replication integration and feasibility | F01 |
| F06 | Writer admission and original WAL records | F03, F04, F05 |
| F07 | Durable, ordered apply with original identities | F05, F06 |
| F08 | Historical slots, watermarks, and retention | F07 |
| F09 | Replica-to-replica initialization and reinitialization | F03, F07, F08 |
| F10 | FDW routing to a pinned shard writer | F03, F04, F06 |
| F11 | Selected synchronous followers and commit durability | F03, F07, F10 |
| F12 | Planned writer handoff and subscriber transfer | F08, F09, F10, F11 |
| F13 | Crash recovery and uncertain outcomes | F11, F12 |
| F14 | Optional controller copies and archive/restore | F09, F12, F13 |
| F15 | pgwrh_wait barriers across shards, writers, and epochs | F07, F10, F12, F13 |
| F16 | Supported SQL, generated identifiers, and schema rules | F10, F12, F14 |
| F17 | Integrated verification, operations, and release | F02–F16, F18–F20 |
| F18 | Consistent point-in-time recovery contract and protocol | F02, F06, F12, F15 |
| F19 | Durable backup coverage and recovery-history retention | F08, F09, F18 |
| F20 | Disaster restore orchestration and recovery validation | F13, F14, F16, F19 |

Prerequisites describe implementation integration. Formal refinements and
feasibility experiments for later features should run early, before their
dependent runtime implementations are committed to an unsuitable design.

## F01 — Protocol contract and formal foundations

Goal: replace the current single-handoff draft with an explicit contract and
reusable executable models. Start from [the existing specification](../../spec/README.md),
preserving its useful scenarios and recording which assumptions change.

| Change | Goal and scope |
| --- | --- |
| F01.1 — Fix the scope | Write the transaction, durability, single-controller, and failure contracts above. Remove the draft's proposed 2PC implementation boundary from the active design. Define supported operations and deliberate rejection behavior. |
| F01.2 — Pin the specification toolchain | Add reproducible Quint tooling, type checks, deterministic scenarios, bounded checks, seeded simulation, and saved counterexamples. Report explored bounds and handoff coverage separately; a passing shallow check is not a proof. |
| F01.3 — Model identities and repeated epochs | Replace operational dependence on ghost transaction ordinals with implementable source-stream ordering and epoch predecessor certificates. Model shard/node incarnations, repeated A→B→C→A changes, and changing ranks. Keep ghost history only for assertions. |
| F01.4 — Specify transaction admission | Add a separate formal model of actual participants, reads before writes, local durable effects, savepoints, route pins, and abort. Prove/check that no accepted writable transaction can commit effects through independent participants. |
| F01.5 — Specify crash boundaries | Distinguish received, locally committed, flushed, replication-acknowledged, client-acknowledged, and checkpointed state. Add permanent storage loss, synchronous follower sets, controller restart, delayed commands, unknown remote COMMIT, and old backends. State exactly when promotion must block. |
| F01.6 — Add the refinement ledger | Map every model action and invariant to its future WAL record, SQL transaction, lock, worker, and test. Record unproved assumptions, required fairness for progress, and capability decisions from F05. |

Gate: executable scenarios reach repeated handoff and the critical crash windows.
Expected-bad variants fail for early acknowledgment, forgotten retention,
overlapping writers, and a second commit participant. All later formal changes
extend this ledger; none is allowed to silently weaken the safety contract.

## F02 — Replicated control metadata and scoped feedback

Goal: replicas can reconcile desired state from local metadata while the
controller remains its sole author. Scope: `pgwrh/src/master`, replica feeds,
reporting, configuration dumps, and focused integration tests.

| Change | Goal and scope |
| --- | --- |
| F02.1 — Separate state ownership | Split desired configuration, observed replica state, durable protocol decisions, and node-local runtime identifiers. Add immutable revisions, operation IDs, node incarnations, and report sequences; FLIP/FLOP alone cannot identify a generation. |
| F02.2 — Persist logical shard definitions | Store schema/partition/index descriptions independently of controller heap tables and OIDs. Preserve stable logical identities across detach/attach and reject incompatible schema changes during movement. |
| F02.3 — Specify metadata reconciliation | Model incomplete initial copy, delayed/reordered reports, revision reuse, and interrupted cleanup. Require a complete revision plus correctly scoped readiness before activation or destructive release. |
| F02.4 — Publish metadata | Add metadata tables/publications and replica bootstrap subscriptions. Gate reconciliation until every required table is synchronized and the revision is complete; do not depend on publisher-side triggers firing on apply. |
| F02.5 — Read local metadata | Replace desired-state FDW reads with local views used by the existing reconciliation code. Keep node-local OIDs, connections, and worker state reconstructible and outside the replicated catalog. |
| F02.6 — Make feedback retryable | Retain FDW reporting, accept reports idempotently by incarnation/revision/sequence, and reject stale reports. Report only completed work. Persist accepted observations separately from durable handoff/cleanup decisions. |
| F02.7 — Preserve credential scoping | Replicate only the secrets/verifiers each recipient needs, or retain the existing scoped credential transport initially. Add dedicated controller-to-writer authority; ordinary replica read credentials must not become write credentials. |

Gate: a replica reconciles during temporary controller disconnection using a
complete local revision; stale or partial reports cannot authorize release.
The metadata format remains usable when a controller leaf is foreign. Native
logical replication does not replicate views or DDL, so publishing the current
FDW-facing views is insufficient. [PostgreSQL restrictions](https://www.postgresql.org/docs/18/logical-replication-restrictions.html)

## F03 — Stable shard identities and ranked writer selection

Goal: represent placement, write authority, and history eligibility separately.

| Change | Goal and scope |
| --- | --- |
| F03.1 — Persist rank order | Expose deterministic per-shard ranks over assigned holders, including tie-breaking and placement revision. Keep existing AZ/copy-count selection separate from ordering the selected holders. |
| F03.2 — Represent active ownership | Add shard incarnation, writer epoch, desired writer, active writer, transition ID, and phase. Changing a weight or desired assignment creates a transition request instead of immediately changing the write route. |
| F03.3 — Track eligibility | Distinguish data-ready, schema-ready, writable, donor-ready, and history baseline. A reachable high-ranked copy may still be ineligible; an eligible lower-ranked holder is used only after an authorized handoff. |
| F03.4 — Reconcile membership safely | Plan predecessor streams across current/target revisions. Serialize overlapping changes for a shard and retain outgoing sources/resources until readers and history consumers can leave them. |

Gate: rank changes, ties, offline members, and new replicas cannot create two
active writers or prematurely remove a required donor.

## F04 — FDW single-participant transaction guard

Goal: reject transactions that would require distributed commit. Shared FDW
feature; its formal prerequisite is F01.4, while managed-cluster tests belong
to F10/F17. This guard must be enabled before exposing remote writes.

| Change | Goal and scope |
| --- | --- |
| F04.1 — Track actual participants | Register executed remote work by its real connection/transaction identity, including user mapping and connection generation. Coalesced virtual routes may share one participant; two connections to one host do not. Planning-only estimates must not enroll execution participants. |
| F04.2 — Prove local-write detection | Audit and prototype executor, utility, transaction, and preload integration for local transactional persistent effects, including SPI/triggers and writes preceding the first FDW use. Select enforceable instrumentation; FDW callbacks or `XactReadOnly` alone are insufficient evidence. |
| F04.3 — Enforce the admission rule | Permit read-only fanout; reject first write after incompatible earlier reads, and second-participant access after a write. Reject mixed local durable writes and remote writes in either order, before any remote COMMIT can occur. |
| F04.4 — Cover nested execution | Cover prepared statements, data-modifying CTEs, functions/triggers, direct/batched modification, COPY where supported, row movement, and savepoints. Retain conservative participant history and writer pins for the top-level transaction. |
| F04.5 — Close commit and error paths | Keep managed `PREPARE TRANSACTION` unsupported. Verify cancellation, pipeline errors, rollback, deferred local work, and connection loss cannot bypass the guard or commit a second participant. Return actionable transaction errors. |
| F04.6 — Package and verify the guard | Make the protection mandatory for managed writes, with startup checks for required hooks/preload support. Add standalone FDW regression/isolation tests and integrate shared changes into both supported major trees. |

Gate: an adversarial transaction matrix cannot produce a partial distributed
commit. If hook coverage is incomplete, reject the uncovered operation or supply
the necessary backend integration before enabling it. A foreign-server OID count
is not an acceptable substitute for this contract.

## F05 — PostgreSQL replication integration and feasibility

Goal: establish the concrete decoding/apply implementation before building the
protocol around assumed hooks. The custom apply path is a likely substantial
component; extension-only feasibility must be demonstrated, not assumed.

| Change | Goal and scope |
| --- | --- |
| F05.1 — Probe decoding and slot primitives | Build disposable experiments for transactional messages, COMMIT end LSNs, logical slot cloning/advancement, exported snapshots, origin filtering, and checkpoint/restart behavior on each supported major. Include logical subscriber sync acknowledgments, filtered publications, and canceled commit waits. Save reproducible evidence. Include F18 feasibility probes for marker commit-end to local recovery-boundary mapping, explicit SET LOCAL utility-hook waits after DML, and visible-versus-flushed apply progress. Evaluate safe wait/COMMIT sequencing and marker application under application-held locks on both supported majors. |
| F05.2 — Select the apply integration | Demonstrate message interception, atomic row/marker/cursor apply, original-identity preservation, and durable acknowledgment. Record the exact hook/worker boundary. If upstream hooks are inadequate, choose a maintained custom worker or explicit backend patch set and account for its packaging. |
| F05.3 — Add the runtime shell | Introduce worker registration, per-database lifecycle, a versioned wire envelope, startup capability checks, and compatibility adapters. Fail closed on unknown protocol versions or missing capabilities. |
| F05.4 — Add deterministic failure controls | Provide test-only barriers around decode, local commit, flush, feedback, slot mutation, and worker restart. Initially disable parallel apply, streamed apply, and two-phase apply unless separately modeled and implemented. |

Gate: the proposed history/apply sequence works on actual PostgreSQL, including
restart. Copying a logical slot starts at the source slot's position; advancing
cannot rewind, and its persistence must account for checkpoints.
[PostgreSQL replication management functions](https://www.postgresql.org/docs/18/functions-admin.html#FUNCTIONS-REPLICATION)

## F06 — Writer admission and original WAL records

Goal: every managed commit has a durable, unambiguous origin and belongs to an
admitted writer epoch. Scope: writer runtime, controller connection context, and
writer-side authorization. Ordinary replica clients retain read access only.

| Change | Goal and scope |
| --- | --- |
| F06.1 — Install writer state | Persist shard incarnation, active/closed epoch, operation IDs, and writer incarnation. Validate controller authority and epoch when a transaction enrolls a shard; replication apply uses a distinct internal path. |
| F06.2 — Add the remote closure barrier | Hold a writer-side shared admission lock until transaction completion; epoch closure takes the conflicting lock and durably closes admission. Surviving old sessions cannot start new writes in a closed epoch. Define canonical lock order for co-located shards. |
| F06.3 — Emit original transaction markers | Write a transactional shard/epoch marker in the same transaction as row changes, once per affected shard. Decode COMMIT to obtain the original commit-end LSN; never predict it inside the earlier marker. Bind a request/transaction identity to that boundary for post-commit durability checks. Aborted transactions publish no committed watermark. |
| F06.4 — Encode control transactions | Define versioned `SAFE_THROUGH`, `END_EPOCH`, and `BEGIN_EPOCH` records, predecessor references, and idempotency rules. Flush required control commits before acknowledging protocol completion. |
| F06.5 — Enforce epoch invariants | Reject ordinary writes on followers, stale epochs, and uninitialized shards. Test concurrent writers through the sole controller, control-only records, rollback, and multiple co-located shard markers in one source transaction. |

Gate: each published transaction can be identified by shard, epoch, writer
incarnation, and original COMMIT end LSN. The earlier logical message's LSN is
not that identity's commit boundary.
[PostgreSQL logical replication message formats](https://www.postgresql.org/docs/18/protocol-logicalrep-message-formats.html)

## F07 — Durable, ordered apply with original identities

Goal: replicas apply each shard history once and retain the identity needed to
republish it after a writer change.

| Change | Goal and scope |
| --- | --- |
| F07.1 — Refine apply semantics formally | Model overlapping streams, duplicate delivery, epoch transitions, local commit-before-flush, and crash replay. Specify ordering evidence from source streams and epoch links without assuming consecutive numeric LSNs. |
| F07.2 — Decode and demultiplex | Validate envelopes and relation identities, associate markers with their source transaction, and route shard/control records. Specify publication filtering of control messages explicitly; unrelated shards must not advance each other's cursors. |
| F07.3 — Commit data and progress together | Atomically apply rows, original-watermark metadata, an `APPLIED` WAL marker, and durable source progress. Preserve the original identity when a transaction is republished from a follower. |
| F07.4 — Enforce one ordered apply path | Serialize overlapping source streams per shard; suppress duplicate identities, reject inconsistent histories, and hold new-epoch data until the predecessor boundary is complete. Do not infer missing transactions from LSN arithmetic. |
| F07.5 — Acknowledge only durable progress | Send publisher feedback only after the corresponding local transaction is flushed. Keep apply commits locally durable without recursively waiting on this node's downstream followers. Recover cursors after crashes and tolerate duplicate transmission when a slot restarts earlier than its last observed position. |

Gate: deterministic crashes between every data/marker/cursor/flush/feedback
boundary cannot create duplicate row effects, missing prefix elements, or an
acknowledgment ahead of durable state. Logical decoding can resend changes after
a crash; replay handling is required. [Logical decoding concepts](https://www.postgresql.org/docs/18/logicaldecoding-explanation.html)

## F08 — Historical slots, watermarks, and retention

Goal: an eligible future writer can serve history needed by disconnected
subscribers. Ordinary publisher slots remain the owners of subscriber progress.

| Change | Goal and scope |
| --- | --- |
| F08.1 — Refine retention formally | Model multiple shards sharing database WAL, inactive subscribers, slot generations, checkpoint rollback, mapping loss, and slot invalidation. Prove/check that every valid resume obligation retains both decodable WAL and its required mappings. |
| F08.2 — Create history anchors | Create a durable history slot before a node starts receiving history it promises to preserve. Register its baseline, decoding/catalog retention requirements, and resource generation. Do not invent an old anchor after WAL has been recycled. |
| F08.3 — Build local watermark mappings | Decode retained WAL through a separately managed slot/clone and persist original watermark → local COMMIT end LSN mappings. Catch the index up after crashes before advancing anchors or serving transfers. |
| F08.4 — Publish conservative safe-through bounds | Compute the oldest relevant valid subscriber position, including disconnected subscribers, and translate it to the writer's original history. Commit a scoped `SAFE_THROUGH` certificate; filtered-out traffic is not proof of applying another writer's prefix. |
| F08.5 — Advance and collect safely | Advance history only to the mapped certified boundary, never to the later certificate's LSN. For shared slots retain the oldest obligation across all covered shards, decoder work, and transfers. Garbage-collect mappings only after the same proof. Coordinate with F19 so releasing live replication retention cannot remove material still needed by the advertised disaster-recovery window; the two retention contracts remain distinct. |
| F08.6 — Carry inherited obligations | Persist predecessor retention floors and pending replacement-slot installations across epochs. A new writer cannot release the old floor merely because its ordinary `origin=none` slots have advanced. |
| F08.7 — Handle invalidation explicitly | Detect missing/invalid slots, WAL limits, idle invalidation, and catalog-history loss. Mark affected consumers for snapshot initialization and donors ineligible as appropriate. Never treat slot disappearance as a successful acknowledgment. |

Gate: repeated handoffs with an offline subscriber retain all required history.
Per-database slot sharing and per-shard progress must be reconciled explicitly;
a shard-local minimum alone cannot authorize advancing a shared anchor.

## F09 — Replica-to-replica initialization and reinitialization

Goal: add or repair replicas without requiring a controller data copy.

| Change | Goal and scope |
| --- | --- |
| F09.1 — Specify snapshot boundaries formally | Model donor choice, snapshot baseline, concurrent writes, initialization crash, changed writer, and a subscriber older than the donor's snapshot. A fresh snapshot does not supply pre-snapshot transaction history. |
| F09.2 — Select and reserve a donor | Choose a compatible initialized holder, verify its epoch/prefix, and reserve the source resources and schema needed for the copy. Carry the snapshot's original-history baseline explicitly; OIDs and local LSNs are not portable identities. |
| F09.3 — Copy and catch up | Couple a consistent snapshot to a source slot, create the target history anchor before catch-up, and import data into detached local tables. Use a mode that includes required origins when copying from a follower. |
| F09.4 — Activate completed copies | Finish ordered catch-up, build required indexes/constraints, publish readiness, and attach only when the current revision's conditions hold. Install ordinary higher-ranked streams without a gap between snapshot and live history. |
| F09.5 — Restart or reinitialize | Resume valid interrupted copies idempotently; discard unusable staging state when a required slot is lost. Increment resource generations, reject stale feedback, and retain existing serving copies until replacements are ready. |

Gate: a new replica initializes with no controller heap, while writes and a
leader change occur. A newly initialized donor cannot promote past an unmet
offline subscriber history obligation. Ordinary `origin=none` filtering is not
a general initial-copy correctness guarantee.
[CREATE SUBSCRIPTION](https://www.postgresql.org/docs/18/sql-createsubscription.html)

## F10 — FDW routing to a pinned shard writer

Goal: execute controller-originated writes at the active writer while retaining
existing read routing where safe. Shared FDW components follow the existing
patch graph; project integration uses F03/F06 metadata and authority.

| Change | Goal and scope |
| --- | --- |
| F10.1 — Represent write routes | Add an explicit single-target write route and shard/epoch context, independent of load-balanced read target lists. Authorize the controller identity at the target without granting replica clients write access. |
| F10.2 — Pin writer transactions | Bind each touched shard's active epoch for the transaction lifetime, propagate admission context before remote work, and acquire the writer-side protection from F06. Preserve pins across savepoint rollback. |
| F10.3 — Integrate reads before writes | Make potentially writable transactions use a compatible authoritative route, or reject an attempted upgrade after reading from another replica. Do not silently retarget an existing transaction; keep explicit read-only fanout available. |
| F10.4 — Cover modification paths | Route INSERT/UPDATE/DELETE and supported direct/batched plans through the guard and writer binding. Handle partition pruning and prepared plans; reject cross-participant row movement and unsupported command paths before committing effects. |
| F10.5 — Handle remote completion | Commit exactly one remote participant, preserve abort behavior, and report uncertain outcomes honestly. Never retry a possibly committed write automatically on another replica. Keep metadata/reporting transactions separate from application transactions. Define the integration point for F18's candidate pre-commit restore-point wait: obtain the current coordinator marker outside an old application snapshot, establish the requirement on the same remote transaction, and preserve publication/dispatch ordering before sending COMMIT. Wait failure must prevent COMMIT; connection reuse, target changes, and batching must not bypass that guarantee. Keep implementation conditional on F18 specification rather than making the entire F10 feature wait for later recovery integration. |
| F10.6 — Integrate both majors | Add feature-owned FDW tests, major compatibility resolutions, and managed-cluster tests for target selection, authorization, transaction pinning, and changed placement. Preserve read-only load balancing. |
| F10.7 — Propagate read barriers | Add the FDW interface used by F15 to send the relevant shard-position set to each selected read target before its snapshot is established. Preserve transaction route bindings and reject an incompatible pre-existing snapshot. Implement against the F15 contract once specified. |

Gate: remote writes succeed on one owner and disallowed combinations fail with
no partial cross-node commit. Multiple virtual servers resolving to one actual
transaction and multiple user mappings resolving to separate transactions are
both exercised. A remote commit may still have an uncertain client outcome;
the guard does not make network failures disappear.

## F11 — Selected synchronous followers and commit durability

Goal: protect successful writes against writer loss using some lower-ranked
replicas, while leaving the others asynchronous. These replicas hold additional
copies of one participant's transaction; they do not become 2PC participants.

Start with a **designated next-ranked eligible successor** whose durable
acknowledgment is mandatory. With A writing, B required, and C asynchronous,
A+B durability protects successful writes if A alone is lost. If B is unavailable,
writes wait or return an uncertain outcome after local commit; they must not
return protected success. Promotion to C requires proving/copying the needed
prefix from a surviving protected copy. Replacing B is a protocol transition,
not an automatic selection of whichever subscriber is connected.

| Change | Goal and scope |
| --- | --- |
| F11.1 — Specify synchronous durability formally | Model required follower sets, threshold acknowledgments, local and client commit outcomes, permanent failures, and membership changes. Assert that every protected successful transaction survives the declared failure budget and belongs to every subsequently activated history. Separate preservation from failover availability. |
| F11.2 — Persist per-shard durability policy | Store sync policy revision, required successor(s) or eligible quorum set, threshold, and failure-domain constraints. Count distinct node/storage incarnations, not duplicate streams. Exclude unsynchronized copies and validate that the selected holders actually contain each affected shard. |
| F11.3 — Choose the commit-wait mechanism | Evaluate native synchronous replication only for uniform groups whose qualifying streams cover every protected transaction. Otherwise implement a shard-aware durability wait before controller success, using F06's original commit identity and F07's durable acknowledgments. Do not count a stream that filtered the shard out. |
| F11.4 — Enforce protected commit outcomes | Apply the configured policy to the writer commit path, including controller-local leaders. Prevent session settings from weakening managed durability. Handle wait cancellation, timeout, disconnect, and crash after local commit as potentially committed outcomes; success requires explicit satisfaction of the policy. |
| F11.5 — Change synchronous membership safely | Catch new required copies up through a protected boundary before activating the new policy. Preserve the old coverage until the transition is durable; model joint coverage/draining and avoid deadlocking already committed transactions waiting for a failed follower. No automatic downgrade to async. |
| F11.6 — Integrate handoff and failure recovery | Protect END/BEGIN and policy transitions at the needed durability level. Before reopening writes, install a viable synchronous set for the new writer. Recover the protected prefix from eligible survivors; preserve old retention obligations and reject a higher-ranked but insufficiently caught-up candidate. |
| F11.7 — Test durability and quorum edge cases | Lose the writer immediately before/after client success; lose required followers; crash while changing policies; cancel sync waits; and attempt promotion from stale candidates. Add an expected counterexample for ANY 1 of B/C followed by promoting B when only C has the latest commit. |

Gate: every success in synchronous mode has durable copies satisfying its policy,
and recovery cannot omit one of those successful transactions. With a quorum
policy, the set consulted during recovery must intersect every possible write
acknowledgment quorum, and the chosen prefix must include the recovered evidence.
An unavailable required survivor may force recovery to wait. Returning old writers
must not resume their abandoned epoch or merge a divergent suffix automatically.

PostgreSQL can include logical subscribers in synchronous replication and select
priority or quorum acknowledgments. However, `synchronous_standby_names` is a
server-wide setting, not a per-shard selector. Its feedback counts are unsafe for
this design if a counted stream does not publish the transaction's shard. This
is a concrete feasibility gate for using the native mechanism, not just a
configuration task. [PostgreSQL replication settings](https://www.postgresql.org/docs/18/runtime-config-replication.html#GUC-SYNCHRONOUS-STANDBY-NAMES)

Use durable flush semantics (`synchronous_commit=on` for a suitable native
configuration), not `remote_write`, for protection against OS/storage restart.
`remote_apply` additionally waits for query visibility; it is not required merely
to preserve a durable copy. In the proposed apply implementation, durable feedback
already follows row application and local WAL flush, so logical decoding/apply
cost remains on the synchronous path. A separate durable receive spool would
be a later optimization with its own replay and recovery model.
[PostgreSQL commit durability settings](https://www.postgresql.org/docs/18/runtime-config-wal.html#GUC-SYNCHRONOUS-COMMIT)

Native cancellation of a synchronous wait can leave the transaction committed
locally without confirmation of remote durability. Audit the completion status
and FDW handling explicitly; the managed success contract must not rely solely
on the configured GUC. [PostgreSQL synchronous wait implementation](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/replication/syncrep.c)

## F12 — Planned writer handoff and subscriber transfer

Goal: safely move write authority, including when another replica is offline.

| Change | Goal and scope |
| --- | --- |
| F12.1 — Refine the complete handoff model | Specify the durable phases, retry/rollback boundaries, repeated handoffs, concurrent placement changes, and inherited retention. Model the exact transition from rank streams to historical catch-up and back. |
| F12.2 — Persist transition intent | Record operation ID, old/new writers, expected epoch, placement revision, and phase before issuing remote commands. Make commands and reports idempotent and reconcile observed state instead of assuming command success. |
| F12.3 — Pause, drain, and seal | Close controller admission, wait for controller pins and writer-side transactions, then commit and flush `END_EPOCH` with terminal watermark and frozen retention obligations. A disconnected FDW connection is not proof of remote drain. |
| F12.4 — Qualify and activate the successor | Require the terminal prefix, compatible schema, retained history, and the new durability policy's required followers. Persist successor acceptance, commit/protect `BEGIN_EPOCH` linked to END, publish the active route, and only then reopen admission. |
| F12.5 — Install replacement subscriber slots | Pause/drain the old stream and read its durable cursor Q. Clone the successor's existing history anchor, advance to the mapped Q boundary, and durably install a uniquely identified replacement stream with `origin=any`. |
| F12.6 — Finish catch-up and transfer obligations | Apply through the predecessor END, then switch the same stream to ordinary origin filtering at a verified transaction boundary. Acknowledge durable installation separately; retain the inherited floor until its obligations are covered. |
| F12.7 — Retire old resources | Drop old subscriptions/slots and release copies only after their data, routing, and retention obligations have replacements. Define cancellation before sealing and completion/recovery after sealing; never undo a committed epoch by reverting metadata alone. Preserve F19 recovery coverage when retiring a former source; live subscriber catch-up does not discharge backup-history obligations. |

Gate: A→B→C→A succeeds with one subscriber disconnected through several
epochs and later catching up. No new write is admitted before the successor is
durable and eligible. Offline consumers can delay cleanup without necessarily
delaying new writes when their retention obligations are safely carried.

## F13 — Crash recovery and uncertain outcomes

Goal: make F12 restartable at every remote/local durability boundary. This is
required before production enablement, not optional hardening after handoff.

| Change | Goal and scope |
| --- | --- |
| F13.1 — Refine recovery formally | Add crash/restart at each handoff phase, independent command and reply loss, stale worker generations, slot rollback after checkpoint, and old remote backends surviving controller restart. Check safety and conditional progress separately. |
| F13.2 — Recover controller admission | Reconstruct transition state and keep affected shards closed until remote state is reconciled. Finish or terminate-and-verify surviving remote transactions through writer-side barriers before sealing; expired sessions/timeouts alone are insufficient. |
| F13.3 — Recover writer and apply workers | Restore durable epochs/cursors, resume indexing and slot installation, and reject obsolete commands. A recovering former writer must remain closed after its epoch was sealed. |
| F13.4 — Classify failed-writer cases | Continue from a valid durable END, or a formally specified synchronous-recovery certificate covering all protected successes and closing old admission. Reconcile possible in-flight commits and survivor evidence. Otherwise wait for authoritative recovery; never select the next ranked replica just on connection failure. |
| F13.5 — Recover partial resource operations | Reconcile crashes during slot creation, cloning, advancement, attachment, acknowledgment, and drop. Identify objects by generation and operation ID; safely replay or clean up orphans without deleting a live obligation. |
| F13.6 — Exercise real failure schedules | Translate model counterexamples into integration tests covering kill/restart, delayed packets/feedback, storage restart, ambiguous COMMIT, and missing slots. Verify previously returned commits under the stated durability policy. |

Gate: every persisted phase has a recovery action or an explicit safe blocked
state with an operator-visible reason. No crash path relies only on controller
backend locks that disappeared during the crash.

## F14 — Optional controller copies and archive/restore

Goal: operate a shard entirely on replicas and later bring it back using the
same initialization and handoff protocol.

Here archive/restore changes shard placement; it is not disaster backup/recovery. Experimental placement work can precede F18–F20, but production enablement of remote-only storage is gated on their verified recovery capability through F17.

| Change | Goal and scope |
| --- | --- |
| F14.1 — Specify copy lifecycle formally | Model controller-local → remote-only → controller-local transitions, old reader drain, offline followers, rollback, and last-copy protection. Separate controller role from whether it stores or leads a shard. |
| F14.2 — Decouple catalog from storage | Make assignment, schema feeds, index templates, publication maintenance, and monitoring work when controller leaves are foreign or absent as heaps. Preserve logical root identities and validate unsupported parent constraints. |
| F14.3 — Make publications node-local | Publish only actual local source relations, manage publications on replicas, and preserve protocol/control records independently. Remove assumptions that all shard publications originate at the controller. |
| F14.4 — Archive explicitly | Initialize required replica holders and transfer leadership before replacing the controller leaf with its remote route. Drain existing users and release the controller heap only after all serving and historical obligations are satisfied. Integrate F19 backup-coverage checks before production copy removal, with release gated by F17 on F20 disaster recovery. |
| F14.5 — Restore explicitly | Copy from a qualified replica into a detached controller table, catch up, attach safely, and optionally hand leadership back. If restoration gives the controller top priority, that priority still cannot bypass eligibility or inherited history. |
| F14.6 — Support controller-local transaction groups | Preserve atomic transactions when every accessed write participant is local to the controller; reject mixed local/remote updates. Supply explicit restore operations for co-used shards without attempting on-demand restore inside an application transaction. |
| F14.7 — Enroll existing controller-origin shards | Establish an initial epoch, history baseline, and protected copy set before switching existing publications/subscriptions to the new protocol. Retain the old donor path for laggards or explicitly reinitialize them; pre-marker history cannot be reconstructed from a fresh slot. |

Gate: a controller with no heap for a shard continues to serve routed reads and
writes, adds a new replica, performs writer handoff, and restores the shard.
For the concrete offline-follower case (follower at 80, restoration snapshot at
100), promotion must preserve a donor of 81–100, wait, or require an explicit
reinitialization policy. The simplest first implementation waits; it must not
pretend the fresh controller snapshot contains that history.

## F15 — pgwrh_wait barriers across shards, writers, and epochs

Goal: replace the assumption of one publisher/subscription LSN with an explicit
set of required shard positions. Scope includes `pgwrh_wait`, its apply-worker
interface, controller write receipts, FDW read context, and public SQL APIs.
The existing single-publisher API remains available only where its original
contract is meaningful.

A barrier is conceptually a map from shard incarnation to required original
transaction position: `(shard, epoch, writer incarnation, original commit end)`.
It can contain positions from several transactions and writers. Combine receipts
using proven history ordering for each shard; never take `max(lsn)` across
writers or reduce the entire map to one publisher position. A transaction on one
writer can still affect several shards, and successive client transactions can
use different writers. Snapshot-baseline certificates may prove coverage of
older positions even after the corresponding source streams have been removed.

| Change | Goal and scope |
| --- | --- |
| F15.1 — Specify visibility barriers formally | Model multi-shard tokens, receipt merging, writer changes, route changes, snapshot baselines, and apply commit visibility. Assert that successful waiting plus the prescribed snapshot acquisition observes every requested write. Do not assume a common cluster snapshot. |
| F15.2 — Produce complete write receipts | Expose all shard positions from a completed transaction using F06's request identity/commit mapping. Define retrieval after COMMIT and preservation of receipts across connection reuse. Remote write receipts must identify their original writers, not the controller's current WAL position. |
| F15.3 — Define versioned token APIs | Add encoding/decoding, validation, receipt-set merge, and wait-set SQL interfaces. Scope entries to shard/node incarnations; normalize positions only when epoch ancestry establishes dominance. Support retaining several requirements when ordering has not yet been established. |
| F15.4 — Track visible shard frontiers | Extend the shared-memory monitor and writer/apply interfaces to publish all affected shard positions only after commit visibility is established. Keep received, durable, and query-visible progress distinct. Rebuild conservative progress on restart and bound memory under shard/epoch churn. Offer reusable progress/wakeup infrastructure for F18's candidate commit wait while retaining separate contracts: read barriers precede snapshot acquisition; restore-point waits may follow DML. Current visible progress is not a marker-flush certificate. Specify whether the new wait requires an explicit flush or whether the ensuing durable application COMMIT covers prior marker WAL; neither alone certifies archived recoverability. |
| F15.5 — Wait for the complete set | Implement cancellation-safe waiting until every required entry is covered, with one total timeout budget and diagnostics for unsatisfied shards. Progress on one shard or subscription cannot release an unrelated wait. Preserve the existing fresh-snapshot contract. |
| F15.6 — Wait at the actual read targets | Send each target the relevant subset through F10.7 and wait before that target acquires its read snapshot. A remote-only controller cannot satisfy the condition with its own apply state. Pin/validate routes and reject or restart transactions whose existing snapshot is too old. |
| F15.7 — Preserve coverage across lifecycle changes | Carry visibility coverage across handoff, slot replacement, reinitialization, and restored snapshots. Retain sufficient epoch/baseline certificates when mappings are collected. Dropping/recreating a subscription or shard name must not inherit the old wait identity. Coordinate disaster-recovery lineage and pre-recovery token handling with F20.5; a visibility lower bound does not define an exact recovery target. |
| F15.8 — Migrate callers and documentation | Update receipt helpers, API examples, configuration, and diagnostics. Keep the old scalar-LSN path explicitly tied to its original publisher; reject ambiguous use for distributed writes. Explain that sync durability does not make all asynchronous read targets current. |
| F15.9 — Test multi-position visibility | Test two publishers with overlapping LSN values, one delayed shard, mixed old/new epochs, merged receipts, controller-remote reads, slot churn, restore baselines, snapshot races, restart, cancellation, and timeout. Run on both supported majors and verify no early success. |

Gate: a read waiting on several writes observes all of them at its actual
execution targets after handoff or restoration. The guarantee is a lower bound
on visible writes, not atomic visibility across replicas or a shared snapshot.
Acknowledgment by a selected synchronous follower does not satisfy a read routed
to an asynchronous follower. Original-position metadata must connect durable
replication and visibility monitoring without treating them as the same event.

## F16 — Supported SQL, generated identifiers, and schema rules

Goal: keep supported SQL behavior explicit when controller leaves are foreign
and a shard's writer can change.

| Change | Goal and scope |
| --- | --- |
| F16.1 — Validate DML behavior | Document and test RETURNING, defaults, generated values, triggers, supported conflict handling, and partition-key updates through foreign leaves. Reject unsupported cases with deliberate errors; do not silently change execution location. |
| F16.2 — Preserve generated identifiers | Decide how sequences/identity defaults remain safe across writer changes, including cached ranges and controller-side evaluation. Initially retain explicitly centralized allocation or reject unsupported leader-local sequence layouts. Document nontransactional consumption and gaps; do not assume table replication transfers sequence state. |
| F16.3 — Define DDL boundaries | Gate schema incompatibility and DDL during initialization/handoff. Define table creation/drop and shard incarnation changes; retain compatible schema for historical decoding and catch-up. General online DDL remains deferred. |
| F16.4 — Enforce supported constraints | Preserve replica identity and leaf constraints; reject unsupported unique-parent/foreign-partition combinations, cross-owner foreign keys, and unsafe truncate paths. Verify behavior before controller copy removal. |

Gate: generated identifiers and supported constraints remain valid through
archive/restore and writer changes. Sequence state and DDL require explicit
handling outside ordinary table logical replication.
[PostgreSQL restrictions](https://www.postgresql.org/docs/18/logical-replication-restrictions.html)

## F17 — Integrated verification, operations, and release

Goal: demonstrate the whole feature and make blocked states diagnosable.

| Change | Goal and scope |
| --- | --- |
| F17.1 — Integrate the formal verification gates | Compose or state assume/guarantee contracts between admission, apply, retention, bootstrap, synchronous durability, handoff, and multi-shard visibility models. Run pinned checks in CI; retain counterexamples, tested bounds, fairness assumptions, and an invariant-to-integration-test matrix. Include F18–F20 consistency, archival retention, and restore-admission models without assuming that ordinary visibility barriers prove recovery consistency. |
| F17.2 — Test complete workloads | Exercise INSERT/UPDATE/DELETE, permitted co-located transactions, rejected distributed transactions, repeated rank changes, offline/rejoining replicas, archive/restore, and controller restart on both supported PostgreSQL majors. Verify protected successful commits after writer loss under each supported durability policy. Include F20 disaster drills and verify the declared recovery target after loss of original nodes. |
| F17.3 — Test resource bounds and latency | Measure local versus remote single-participant writes under identical durability settings, sync successor/quorum latency, marker/indexing overhead, slot/worker growth, retained WAL under long disconnections, initialization cost, and handoff write-pause duration. Document the rank topology's quadratic stream count. Include backup coordination overhead and measured write blocking, if any; global write pauses are not a prescribed recovery mechanism. Compare all-writer barriers with the publication-gate/per-writer-wait candidate: separate coordinator gate duration, individual writer waits, and statement blocking. Report autocommit tail latency, fast-versus-lagging writer throughput, marker backlog, outstanding-commit drain, and SET/COMMIT network overhead. Measure rather than assume that publication remains brief under failures. |
| F17.4 — Expose protocol diagnostics | Add SQL/console views for desired/active writer, epoch, transition phase, eligibility, original apply cursor, oldest retained obligation, missing slots, durability policy, required followers, sync-wait lag, and blocked reasons. Expose safe retry/cancel/reinitialize actions; no timeout-based unsafe promotion or durability downgrade. |
| F17.5 — Update backup and recovery procedures | Document the implemented F18–F20 recovery contract and validated operator procedures: recoverable target selection, backup coverage, retention, controller loss, isolated restore, replica rebuild, and reopening admission. State achieved recovery-point/recovery-time objectives, supported timestamp precision, failure assumptions, and any measured blocking. This is the documentation integration change; protocol and runtime implementation belong to F18–F20. If per-writer commit waits are selected, explain the coordinator publication gate, continued statements, writer-specific delays, SET LOCAL/pre-commit behavior, timeouts, and the difference between locally established boundaries and complete archived recovery targets. Keep this contract distinct from the read-snapshot barrier. |
| F17.6 — Package dependencies | Package the selected worker/backend dependencies and formal tooling; validate runtime versions, preload requirements, installation, and compatibility on both supported majors. |
| F17.7 — Enable and document | Add opt-in activation after protocol/durability readiness checks, and publish operations/SQL limitations. After remote writes begin, rollback is a reverse handoff, not a configuration toggle. Production enablement of remote-only storage additionally requires F18–F20 recovery gates and verified backup coverage under the chosen operational policy. |

Gate: every safety invariant has model evidence and a concrete implementation
argument/test where applicable. Release claims distinguish planned handoff from
unplanned failure, visibility from durability, and simulation from proof.
Production remote-only storage requires verified consistent disaster recovery and documented achievable recovery objectives; synchronous replication alone is insufficient.

## F18 — Consistent point-in-time recovery contract and protocol

Goal: define and implement how a recovery target identifies a mutually consistent controller state and set of shard histories. Prefer operation without global write pauses. The consistency contract is mandatory; the coordination mechanism remains a design decision. Evaluate online approaches before accepting a cluster-wide pause, and quantify any unavoidable blocking. Do not prescribe named restore points, a global transaction order, a backup format, or an additional synchronous round trip on every write. Keep distributed 2PC outside the application-write scope.

First candidate to specify and benchmark: a coordinator marker-publication barrier followed by per-writer commit waits. Briefly gate new COMMIT dispatch, settle commits already dispatched, durably publish the new marker and its current requirement, then reopen dispatch without collecting every writer acknowledgment. Each remote transaction must establish its own writer's corresponding local boundary before committing; `SET LOCAL pgwrh.restore_point = <coordinator marker commit-end LSN>` is the proposed interface. Ordinary statement execution can continue and open transactions need not all be drained. The coordinator gate still delays commit dispatch during publication and can be prolonged by outstanding commits; subsequent waiting is per writer. Compare this refinement with the all-writer barrier and less-blocking alternatives before selecting an implementation. Removing even the publication gate needs a separate consistency argument.

| Change | Goal and scope |
| --- | --- |
| F18.1 — Define recovery semantics and service objectives | Specify requested time/logical-target semantics, target granularity, transaction inclusion, causal consistency, metadata compatibility, and uncertain commit outcomes. Distinguish disaster recovery objectives and archive freshness from synchronous-replication durability. For the per-writer-wait candidate, define the committed state after settling pre-publication commits and before new commits cross their writer-local marker. Distinguish the coordinator publication gate, per-writer commit delay, continued statement execution, and delayed availability of a complete recovery target. Periodic boundaries must not be described as arbitrary-time PITR. |
| F18.2 — Specify consistent recovery formally | Model independent shard histories, colocated transactions, causal dependencies, metadata, ownership changes, and exact recovery targets. Refine the per-writer-wait candidate with publication/dispatch serialization, already-dispatched commits, still-open transactions, marker application and durability, and writer-local waits. Check that each boundary includes settled prior commits and excludes later commits, without requiring an all-writer acknowledgment before dispatch resumes. Model a marker advancing between target lookup, SET LOCAL, and actual COMMIT dispatch; define tracked commit attempts and target revalidation. Include deferred triggers, independent apply/control paths, stale or reset GUC state, lost replies, restart, and obsolete acknowledgments. Expected-bad variants use an old application snapshot for the target, wait only for receipt, delay only client replies, or remove the publication gate without replacing its ordering guarantee. Record safety, conditional progress, and checked bounds. |
| F18.3 — Evaluate recovery without global write pauses | Evaluate publishing a periodic coordinator marker, reopening dispatch after the publication barrier, and making each remote COMMIT wait only for that writer to establish the marker locally. Prototype SET LOCAL pgwrh.restore_point with the marker transaction's commit-end LSN and required source identity. Compare against collecting all writer boundaries before release, generation propagation, and other online schemes. Investigate publication/dispatch races, current-marker lookup outside old transaction snapshots, coordinator-local commits, and safe handling of SET/COMMIT sequencing. Distinguish a wait not yet followed by COMMIT dispatch from an irrevocably dispatched commit; assess batching only with correct error and race handling. Measure fast and lagging writers, marker backlog, publication-gate duration, per-writer wait and tail latency, lock retention, and network exchanges. Do not assume the publication gate is always short or introduce 2PC or a requirement to drain every active transaction. |
| F18.4 — Define durable recovery evidence and identities | Define versioned recovery evidence linking coordinator revisions, marker rounds, shard/node incarnations, ownership, source lineage, schema/identifier state, and exact transaction boundaries. Scope the coordinator marker commit-end LSN to its source/recovery identity; do not compare raw LSNs from different histories. Publish the current requirement consistently with marker creation and COMMIT dispatch and reconstruct it conservatively after restart. Preserve coordinator-marker to exact writer-local recovery-position mappings even when boundary acknowledgments arrive after normal commits resume. Distinguish receipt, committed application, flush durability, and archive coverage. Pin or record the round's membership/metadata revision and reconcile handoff rather than using a later mutable writer list. |
| F18.5 — Implement the selected consistency protocol | Implement the reviewed consistency protocol; keep the mechanism conditional on F18.2/F18.3 evidence. For the per-writer-wait candidate, integrate publication/dispatch coordination across controller backends and all application commit paths. After settling prior dispatched commits and durably publishing the marker/current requirement, reopen dispatch without awaiting every writer. In the FDW pre-commit path, obtain a fresh requirement and send SET LOCAL pgwrh.restore_point on the same remote transaction connection before actual COMMIT; handle target revalidation if publication advances during the wait. Prefer an explicit utility-command hook, as used by pgwrh_wait, with nonblocking GUC assignment/restoration. The new barrier permits prior DML and an existing snapshot; preserve the original read barrier's before-snapshot contract. Wait for the required marker's established local boundary, enforce the selected durability semantics, and prevent COMMIT after timeout, cancellation, or wait failure. Keep marker processing independent of locks held by waiting application transactions. Reconstruct requirements after crashes without reusing discarded identities or treating incomplete recovery rounds as usable. |
| F18.6 — Validate recovery-boundary capture under failures | Translate the formal model into tests for publication racing target lookup/SET/COMMIT, markers advancing during waits, already-dispatched or ambiguous commits, old Repeatable Read snapshots, autocommit, savepoint/GUC restoration, and pooled connection reuse. Include long-running transactions, deferred triggers, marker apply lag, stale acknowledgments, coordinator/writer restart, and timeout/cancellation before COMMIT. Check marker-processing lock independence and distinguish visible application from durable marker completion. Demonstrate that a fast writer can commit after its own boundary while another required writer lags, subject to existing durability/routing obligations; the overall recovery target remains unavailable until complete. Measure publication-gate duration, per-writer wait, p95/p99 latency, and any batching overhead; full disaster drills belong to F20. |

Gate: the chosen protocol has an explicit recovery-target contract, executable formal evidence, and implementation tests covering concurrent writes, ownership changes, and partial failure. Targets preserve supported transaction atomicity and the agreed causal consistency contract, with matching metadata. Document supported timestamp precision, any target adjustment, availability assumptions, and coordination costs. Named recovery points alone must not be described as arbitrary-timestamp recovery. No global write pause is assumed as the baseline; any required blocking needs a documented design justification and measured scope.

## F19 — Durable backup coverage and recovery-history retention

Goal: retain enough independently durable data, history, and recovery evidence to reconstruct every advertised recovery target when the controller or original writers are lost. Integrate suitable backup tooling instead of assuming a new backup engine. Physical, logical, or mixed storage strategies remain open, subject to their actual replay and consistency constraints. Live replicas, logical slots, and synchronous commit acknowledgments do not by themselves constitute disaster backups.

| Change | Goal and scope |
| --- | --- |
| F19.1 — Define backup integration and storage contracts | Choose the boundary between pgwrh recovery coordination and existing backup/archive tooling. Specify identity, integrity, discovery, access, and durability requirements for backup material and recovery evidence, including recovery without the original controller. Compare physical, logical, and mixed approaches without predetermining the provider; account for node-level restore granularity, schemas, identifier state, and necessary configuration. |
| F19.2 — Capture and archive required data and history | Integrate the selected backup adapters for controller state and authoritative shard data/history. Establish usable baselines and continuous history coverage with explicit start/stop and retry semantics. Namespace material by stable source identity and lineage. Avoid requiring every ordinary replica to be a backup source; respect the chosen protocol's consistency and online-operation requirements. |
| F19.3 — Verify and publish recoverable coverage | Combine F18 consistency evidence with verified backup/archive coverage to determine usable recovery targets. Persist the complete boundary map and supporting evidence independently of the live controller; distinguish captured, applied, locally durable, archived, verified, and expired states. For the per-writer-wait candidate, boundary collection and archive verification can finish after commit dispatch has resumed. A fast writer's successful commit or one marker-flush acknowledgment cannot certify a complete cluster target. Require all selected authoritative sources, matching metadata, and sufficient backup/history coverage before advertising recoverability. Handle gaps, partial uploads, and lost replies conservatively; no application-wide commit gate is held for full-map collection or bulk backup/archive upload in this candidate. |
| F19.4 — Preserve backup coverage across shard movement | Connect handoff, bootstrap, controller copy removal/restoration, and node retirement to backup coverage obligations. Preserve the historical sources needed for earlier targets even after live ownership changes. Define when replacement coverage permits retiring an old source; a newly copied current shard does not automatically replace its earlier recovery history. |
| F19.5 — Specify and enforce recovery retention | Model archive retention, in-progress capture/restore, source replacement, and deletion races. Implement eligibility checks that preserve every advertised recovery target until its explicit expiry, independently of live subscription-slot retention. Make policy changes and loss of coverage deliberate and diagnosable; record formal evidence and focused deletion/retry tests. |
| F19.6 — Test archive failures and coverage accounting | Exercise unavailable backup storage, missing/corrupt history, incomplete baselines, interrupted uploads, duplicate retries, storage exhaustion, and loss of retired writers. Verify conservative coverage reporting and safe retention under concurrent handoff and restore. Measure storage growth and archive lag without claiming that archive acceptance alone proves an end-to-end restore. |

Gate: a target is advertised as recoverable only when all required data, metadata, and history are verifiably available in the configured disaster-surviving backup domain. Handoff, shard removal, node retirement, and garbage collection preserve the advertised recovery window. Gaps and degraded freshness are visible; neither current replica readiness nor a fresh snapshot substitutes for missing historical coverage.

## F20 — Disaster restore orchestration and recovery validation

Goal: restore a usable, internally consistent pgwrh deployment from independently stored recovery material, including when the controller holds no application shards. Reconstruct only the authoritative state required by the selected target and rebuild other replicas as appropriate. Keep restore orchestration, source selection, and tooling extensible; do not assume all original nodes survive or that every replica must be restored in place.

| Change | Goal and scope |
| --- | --- |
| F20.1 — Specify disaster restore safety formally | Extend the F18 model to restore planning, interrupted reconstruction, stale-node reappearance, recovery lineage changes, and admission reopening. Check target preservation, transaction atomicity, discarded-history rejection, and retry safety before runtime implementation. Include per-writer marker targets whose WAL contains uncommitted changes at the boundary, and later commits made while other writers were still waiting. Restore must exclude effects committed after the selected local boundaries; incomplete or abandoned rounds remain unselectable. Retain counterexamples and checked bounds. |
| F20.2 — Resolve recovery targets and construct restore plans | Resolve a requested time or logical target to verified F19 coverage under the F18 contract. Select compatible data/history sources, controller metadata, and schema/identifier state; respect colocated transaction boundaries and the chosen backup format's restore granularity. Produce an inspectable plan that reports the achieved target, prerequisites, gaps, and necessary replica rebuilds rather than silently mixing independent node recovery times. For marker-based targets, use the recorded exact node-local recovery boundaries and matching controller state, with the selected format's correct stop semantics. A later sampled flush position or the publisher LSN in a feedback message is not an interchangeable recovery target. |
| F20.3 — Reconstruct an isolated deployment | Execute the restore plan with serving, write admission, and ordinary reconciliation held closed. Reconstruct data and control state using the selected backup adapters, with retryable stages and integrity checks. Support replacement hosts and interrupted execution without requiring original writers to return. Define which failures are recoverable in place and which require restarting a reconstruction stage. |
| F20.4 — Reconcile restored state before reopening writes | Validate shard histories against restored ownership, schema, generated identifiers, and the target's transaction boundaries. Reconcile historical in-progress handoffs and resource operations instead of blindly resuming stale decisions. Establish the new recovery lineage and safe admission state, then rebuild node-local runtime resources as required by the reviewed restore contract. |
| F20.5 — Handle replica rejoin and pre-recovery tokens | Specify and implement how old replicas, slots, subscriptions, reports, write receipts, and pgwrh_wait tokens behave after disaster recovery. Reject or quarantine discarded-history identities and define when a baseline proves legitimate coverage. Reinitialize replicas where continuity cannot be established; do not accept a numeric LSN match as evidence that a pre-disaster operation survived. |
| F20.6 — Run end-to-end disaster recovery drills | Translate formal traces into end-to-end restores across repeated handoffs, remote-only storage, colocated transactions, missing archives, restore interruption, and stale-node reappearance. Use an independent workload oracle and expected-blocked cases on supported PostgreSQL versions. For per-writer commit waits, test one writer continuing after its marker while another delays, a transaction spanning publication, dependent commits, and coordinator failure during publication, waiting, or boundary collection. Demonstrate recovery after loss of the original controller/writers to the selected completed target. Verify committed histories rather than requiring pre-marker uncommitted WAL to be absent. |
| F20.7 — Expose recovery operations and measure recovery objectives | Provide supported operator interfaces and diagnostics for recoverable coverage, restore planning/progress, blockers, and completion evidence. Benchmark achieved recovery point and recovery time under representative volume, archive lag, and failure scenarios. Feed validated procedures and limitations into F17.5. Use local diagnostics and external collection conventions where applicable rather than adding an observability-only reporting hub. |

Gate: repeatable disaster drills recover the declared target with matching data, metadata, transaction boundaries, and recovery lineage after loss of the original controller and required writers. Incomplete evidence blocks admission with an actionable reason. Restored deployments cannot accept stale streams, reports, or authority from the discarded history. Production enablement of remote-only storage requires this recovery capability and documented achievable recovery objectives.

## Delivery order and acceptance

1. **Validate the design:** F01, early F05 probes, and the later feature models.
   Resolve apply integration, local-write guard coverage, slot persistence, and
   failed-writer policy, and synchronous commit-wait mechanism before committing
   to the runtime architecture. Run F18 consistency/online-protocol design and
   the F19/F20 retention/restore models early; recovery is an architectural input.
2. **Build a guarded remote writer:** F02–F07 and F10. Validate remote writes on
   a statically selected writer; keep movement and controller copy removal off.
3. **Preserve and transfer history:** F08–F09 and F11–F13. Demonstrate synchronous
   durability, repeated planned handoff, disconnected subscriber recovery,
   protected-prefix recovery after writer loss, and all crash boundaries.
4. **Remove the controller data dependency:** F14–F16. Archive, add a replica,
   move its writer, and restore while maintaining transaction/visibility rules.
   Verify multi-shard waits at the actual read targets.
5. **Make disaster recovery complete:** F18–F20. Demonstrate independent
   backup coverage and consistent restore across writer changes and original-node
   loss. Establish supported target precision and measure runtime impact; avoid
   global write pauses where feasible.
6. **Enable for operators:** F17 after all required feature gates pass, including
   recovery readiness before production remote-only storage is enabled.

The completion demonstration is one cluster in which the controller holds no
application heap for the selected shards, accepts application writes and routes
them to their active highest-ranked eligible replicas, safely changes writers,
recovers an offline subscriber without data loss while its slots remain valid,
reinitializes after required slot loss, and rejects transactions requiring
independent commit participants. In synchronous mode, every successful write
survives the declared writer/follower failures; if the required evidence or
survivors are unavailable, recovery blocks. The same demonstration must survive
the modeled crashes or stop in a documented safe state. A disaster drill must
restore the declared consistent target from independently stored recovery
material without relying on the original controller or writers. Report the
achieved target and recovery time; never silently substitute independent node
timestamps or incomplete history.

The current draft model is a starting point only: it uses operational ghost
ordinals, has one planned A→B transition, omits controller crash recovery, and
has no exhaustive handoff proof. The formal changes above are implementation
prerequisites, not a final documentation step.
