# Obligation/frontier evaluation

Evaluated 2026-11-02 with Quint **0.32.0**, its native Rust evaluator **0.6.0**,
Apalache **0.56.1** for TLA+ translation, TLC **2.19**, and Java **21.0.11**.
This records bounded model evidence, not a PostgreSQL implementation proof.

## Sources and reproducibility

`writer_handoff.qnt` is restored byte-for-byte from
`f400101554339da722a5abf5776915b1725900f2`, the earlier design snapshot. It retains
its original one-shard/three-node/one-handoff scope. Its SHA-256 is
`9feffc07bc226e239178fdb03260cf49100dcabf7f6bbf54209bcaa880249185`.

The new `obligation_frontiers.qnt` is a companion abstraction. Source hashes for
both models, the runner and TLC settings, every CLI argument, log hashes and
machine-readable results are preserved in
[`evidence/2026-11-02/summary.json`](evidence/2026-11-02/summary.json). Raw output
is retained alongside it. Run from the repository root:

```sh
QUINT=/path/to/quint python3 spec/check.py --verify --output .build/spec
```

Install the pinned Quint package in an explicit tool environment; the repository
does not require a global npm installation. Java must be on PATH for `--verify`.
Quint obtains its pinned native evaluator/checker on first use. The two focused
TLC schedules exhaust their finite graphs; they do not use a shallow depth limit
as a substitute for reaching a handoff.

## Model boundaries

The companion bounds two shards, four nodes, four writer epochs per shard and
two data transactions per epoch. A/B/C rotate as writers; D can stay offline
through A→B→C→A. Each subscriber resource can be re-enrolled once, creating a
second generation. Histories and finite transaction-label sets are ghost
reference data used to express prefix correctness; they are not a controller
registry or a global sequence-number service required by the runtime.

Concrete evidence uses original source positions and certified epoch links.
Shard names are incomparable. Raw LSNs from different writers are never ordered
as one clock. The model preserves these distinctions:

- Data-prefix coverage versus replayable transaction history.
- Durable retention promises versus replies or current readiness observations.
- Authority decisions versus mergeable evidence.
- Consumer and donor resource generations versus reused node names.
- Latest valid coverage per obligation versus the frontier across obligations.

Initial prospective donors already have history anchors, as in the original
model. Collection preserves every live pin across both shards. A donor can
release a pin only when the active writer holds a durable replacement promise.
Snapshot re-enrollment supplies current data but no pre-snapshot replay history.

Durable write/apply, predecessor closure, successor acceptance and controlled
re-enrollment are atomic abstractions here. `applyNext` uses the reference
history to abstract the ordered apply contract, not to propose a runtime
ordinal-based algorithm. `repairHistory` abstracts transfer of both replayable
transactions and the required local decoding/mapping support; it does not
implement physical slot cloning or rewind. The original model separately
exercises local commit/flush/feedback and WAL mapping boundaries. No refinement
proof composes the two models yet.

## Directed scenarios

Both models parse and typecheck. The original **11** scenarios and companion
**26** scenarios pass with the native evaluator. The companion's TypeScript
reference runs also passed before the final focused schedules were added;
the committed acceptance manifest records the final native run.

Positive/guard scenarios include:

- A→B→C→A with D offline, including real writes in each epoch.
- Releasing predecessor pins, collecting history, repairing the returning
  writer and reacquiring its obligations before activation.
- New writes continuing while another subscriber remains offline.
- Duplicate/reordered receipt delivery and gossip convergence.
- Missing epoch links holding back apparent progress.
- Crash/restart after durable evidence or before predecessor release.
- Ignoring receipts from a previous resource generation.
- Refusal to activate a behind or snapshot-only historical donor.
- Collection after every affected consumer has acknowledged a prefix.
- Antichain compression preserving each pin's retention requirement.

Eight companion counterexample scenarios reject raw cross-writer LSN selection,
forgetting an offline obligation, releasing active protection prematurely,
collection that ignores another shard, reuse of old-generation evidence,
snapshot-only promotion, activation without predecessor closure, and applying
new-epoch data without the predecessor prefix. These deliberately incorrect
actions are excluded from every exploration schedule. The three original
counterexamples remain unchanged and passing.

## Seeded exploration

Every campaign checks `safety` after every transition. Each command and actual
witness count is in the manifest; counts describe traces reaching a witness,
not exhaustive state coverage.

| Model / schedule | Traces × maximum steps | Seed argument | Results / witnesses |
| --- | --- | --- | --- |
| Original WAL `step` | 1,000 × 150 | 20261102 | No violation; new epoch 1, completed transfers 0, released history 14, reinitialization 2. |
| Original WAL `healthyStep` | 1,000 × 200 | 20261103 | No violation; new epoch 940, completed transfers 824, released history 292. |
| Companion `step` | 300 × 120 | 20261102 | No violation; repeated handoff 0, offline progress 0, collection 104, re-enrollment 248. |
| Companion `offlineInit` / `progressStep` | 300 × 160 | 20261103 | No violation; repeated handoff 149, offline progress 268, collection 0. |

The broad failure campaign often becomes safely blocked before repeated
handoff. It does **not** independently validate repeated movement. The progress
campaign deliberately holds anchors and membership stable, keeps D offline,
and requires two data writes before each seal. It complements rather than
replaces failure exploration. Collection remains pinned by D in that campaign;
directed scenarios exercise eventual acknowledgement and release separately.

## Focused exhaustive exploration

| Schedule | States generated | Distinct states | Search depth | Scope |
| --- | --- | --- | --- | --- |
| `boundedStep` | 931 | 126 | 14 | One data-bearing handoff, delayed durable receipt, old-pin release, collection and an independently crashing D. |
| `boundedRepeatedStep` | 261 | 42 | 20 | Three successive handoffs, one data transaction per epoch, fixed anchors/generations and D crash/restart. |

Both TLC searches completed with **zero remaining states** and no invariant
violation. These results exhaust the selected finite schedules, subject to
TLC's reported fingerprint-collision probability. They do not exhaust the full
asynchronous `step` relation, prove unbounded safety, or establish liveness.
Neither focused graph models arbitrary generation changes, all message
interleavings, permanent storage loss, or physical PostgreSQL WAL mechanics.

## Decision and remaining obligations

Proceed with the evidence/retention abstraction as a **design candidate** and
update the associated issue scopes. Do not close runtime implementation issues
or replace the authority protocol with a CRDT merge.

Next refinement work must cover actual durable pin acquisition/transfer,
PostgreSQL slots and catalog retention, original-to-local mappings, complete
revision admission, repeated resource identities, permanent storage loss,
single-participant transactions and synchronous-policy transitions. Separate
PITR/backup obligations remain intact. A frontier cannot recreate discarded
history or establish a causally consistent disaster-recovery boundary by itself.

No runtime source, extension API or package dependency changes are made by this
evaluation. PostgreSQL integration tests are therefore not evidence claimed for
this change.
