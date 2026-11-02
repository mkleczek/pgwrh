# Evidence and retention frontiers for replica writes

Status: evaluated design candidate, 2026-11-02. This is a proposed abstraction
for the replica-write plan, not implemented PostgreSQL behavior. It adds no
runtime dependency on the `antichain` crate, CRDT framework, or pg_dbsp.

The existing [replica-write plan](replica-writes-plan.md) retains one active
controller and one active writer per shard. The [Quint specifications](../../spec/README.md)
now separate physical WAL/slot mechanics from repeated handoff and evidence
composition. The recorded checks and their limits are in
[the evaluation record](../../spec/frontier-evaluation.md).

The later [controller handoff specification](../../spec/controller-handoff.md)
extends authority modeling independently of writer selection, including
controller restart and topology changes. It retains WRH for desired assignment
and adds no election/consensus protocol. Its verification record is separate
from the fixed-rotation frontier evaluation below.

## Assessment

The candidate is feasible at the bounded abstract-model level. A common model
of durable evidence and retention obligations can organize progress across F02,
F08, F09, F12, F14 and F15. It does not eliminate writer closure, ordered apply,
original-to-local WAL mappings, slot management, durability waits, or the
single-participant transaction restriction.

The original design's `SAFE_THROUGH`, inherited floor and pending transfer set
already form a specialized retention algorithm. The new abstraction makes the
underlying obligations explicit; it does not claim that the original plan
required every offline subscriber to catch up before new writes could start.

## Three kinds of evidence

1. **Authority evidence:** controller operation/revision, shard incarnation,
   predecessor epoch, durable END, successor acceptance and activation. This
   establishes who may write. Decisions remain serialized and fenced. A maximum
   reported epoch or last-writer-wins register cannot establish exclusivity.
2. **Coverage evidence:** a specific consumer/resource incarnation durably
   covers a prefix of a specific original shard history. A snapshot can establish
   a data baseline without providing the earlier replay transactions.
3. **Retention obligations:** an identified consumer or transfer still needs
   history after a certified boundary. A particular donor/resource incarnation
   promises to preserve replayable history and its required decoding/mapping
   state until the obligation is discharged or transferred.

Immutable evidence can be merged with set union. Within a certified comparable
history, newer prefix evidence can subsume older evidence. Repeated delivery is
idempotent; missing messages delay progress. A generic current-readiness or
resource-existence observation is not automatically a permanent promise.

Actual row changes remain ordinary PostgreSQL transaction effects. Merging
metadata does not turn SQL updates into commutative operations or make repeated
application harmless. F07 must still atomically preserve row effects, original
identities and durable progress, and suppress duplicate application.

## Ordering and frontiers

The runtime identity remains `(shard incarnation, epoch, writer incarnation,
original COMMIT-end LSN)`, with deployment/recovery lineage where required.
Never compare raw LSNs across writers or use the numeric shard identifier as a
time dimension. Cross-epoch ordering requires the predecessor END/BEGIN chain
and proof of coverage; a higher epoch alone cannot conceal an incomplete prefix.

The bounded companion model uses distinct named shards and original source
positions. Different shards are incomparable. Missing epoch links conservatively
prevent advancement. A frontier keeps the minimal non-dominated obligation
bounds; one point per shard is often sufficient. Multiple incomparable points
remain explicit instead of being folded into one unsafe global scalar.

For each obligation, merge valid coverage evidence first, then take the
conservative bound over every outstanding obligation. Folding all historical
reports directly with `min` would stall progress. Advancing a shared database
history slot must account for *all* covered shards, decoder work, snapshots and
transfers, translate through the original-to-local mapping, and honor native
slot retention requirements. The companion's node-wide set collection checks
the aggregation obligation, not the physical correctness of advancing a real
shared PostgreSQL slot.

## Handoff and obligation transfer

1. Persist the transition identity and close/drain predecessor admission through
   the existing writer-side barrier. Commit/protect its terminal epoch record.
2. Qualify successor data coverage, schema, durability policy and replay-history
   coverage. Data copied from a current snapshot is insufficient to serve an
   offline subscriber whose cursor predates that snapshot.
3. Establish durable successor retention promises before predecessor protection
   can be released. A command sent or a reply received before persistence is
   not an installed replacement promise.
4. Activate the successor only after the predecessor and successor certificates
   establish the required chain. The writer may accept new transactions while
   an offline follower's older history still pins reclamation.
5. Complete subscriber slot installation/catch-up with the existing origin
   rules. Release old copies, mappings, slots and subscriptions only when every
   relevant obligation has an installed replacement or an authorized discharge.

The two-node transfer is not one atomic distributed transaction in a runtime
implementation. Persist destination protection first, make its completion
discoverable, then authorize source release. Lost replies and retries retain
extra protection. The controller need not maintain every subscriber's advancing
cursor: donor-local progress and scoped certificates can implement this model.

## Membership, crashes and retirement

Scope evidence to resource generations. Ordinary crash/restart preserves durable
evidence; re-enrollment creates a new generation and prevents old receipts or
pins from certifying it. A disappeared slot or offline node is not a completed
obligation. Snapshot reinitialization is an explicit alternative to resume, not
an implicit timeout policy.

Register a new obligation before allowing collection that could invalidate it.
A consumer cannot request history older than a released frontier without a
retained donor or explicit snapshot initialization. Generation changes and
admission need their own durable protocol; the companion models their registration
and active-donor pin atomically as an explicit refinement boundary.

Backup/PITR retention remains a separate obligation class. Releasing live
subscriber history does not authorize deletion of recovery history, and a
collection of per-writer progress values does not itself establish a causally
consistent disaster-recovery cut. F18's publication/commit ordering and F19/F20's
archive and restore evidence remain unchanged.

## Implementation and verification direction

Start with normalized evidence and obligation tables plus idempotent operation
identities. Keep the authority protocol and local PostgreSQL effects explicit.
Implement a conservative per-shard progress map before considering a general
antichain library. Authenticate issuers and validate identities/lineage; CRDT
merge laws do not validate the truth of input facts.

pg_dbsp is an optional future execution engine for the derived blockers,
eligibility and release-candidate relations. It supplies incremental query
maintenance, not distributed completeness or authorization for irreversible
effects. DBSP signed deltas are not idempotent network facts: deduplicate facts
before they enter maintained relations. Preserve a transactional final decision
or a fully specified monotonic certificate contract for destructive actions.

Before adopting the abstraction in runtime code:

- Refine the abstract pins into actual slots, retained WAL, catalog decoding
  state, indexes and original-to-local mappings. Preserve every F08 bound.
- Refine atomic closure, durable apply, acceptance and re-enrollment into the
  original WAL model and PostgreSQL transactions, locks, workers and recovery.
- Extend membership and failure models beyond the companion's fixed nodes,
  single controlled re-enrollment and durable-storage crash assumption.
- Preserve multi-shard transaction grouping, synchronous-policy transitions and
  disaster-recovery lineage; none is proved by this companion model.
- Keep expected-bad variants for early feedback, wrong certificate/local LSN,
  lost inherited obligations, missing protection, stale incarnations, missing
  shard dimensions, snapshot-only promotion and unclosed predecessor epochs.

Issue mapping: [F01](https://github.com/mkleczek/pgwrh/issues/26),
[F02](https://github.com/mkleczek/pgwrh/issues/27),
[F08](https://github.com/mkleczek/pgwrh/issues/33),
[F12](https://github.com/mkleczek/pgwrh/issues/37), and
[F15](https://github.com/mkleczek/pgwrh/issues/40). This evaluation refines their
design work; it does not close implementation issues or establish production
readiness.
