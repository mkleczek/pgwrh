# Proposal: shard-aware connection reuse in pgwrh_fdw

Status: historical architecture proposal, based on the working tree on
2026-09-14. It is retained as design context, not as current configuration guidance.

Connection reuse, virtual-server routing and join pushdown were subsequently
implemented with interfaces that differ from this proposal. Use the
[virtual-server guide](virtual-servers.md) and
[replica routing implementation](../../docs/development/replica-routing.md) for
current behavior. References below to the "current implementation" mean the
2026-09-14 baseline; proposed options are not an API reference.

## Recommendation

Separate three identities that the current implementation combines:

1. A **logical route** identifies a shard or a complete serving subtree and its
   eligible replicas.
2. A **connection profile** identifies the database, authentication policy,
   credentials, transport options, and remote transaction initialization policy.
3. A **physical participant** owns one connection and its remote transaction on
   one replica, within one local PostgreSQL backend.

Multiple logical routes can reference one physical participant. For routes
X = {a,b,c} and Y = {b,c,d}, an existing compatible participant on b is usable by
both. Reusing it must mean sharing its entire connection/transaction state, not
putting the same PGconn pointer into two existing cache entries.

Deliver this in two main increments: first support reuse behind the existing
replica-set servers; then introduce a logical cluster server with table-level
route identities to remove destination-dependent foreign-table naming. Treat
broader join pushdown and query-wide routing optimization as subsequent work.

## Evidence from the current implementation

| Area | Current behavior | Consequence |
| --- | --- | --- |
| `pgwrh_fdw/connection.c`, `ConnCacheKey`, `GetConnection()` | Cache key is `user->umid`; one entry owns PGconn, transaction depth, pending async request, invalidation and cleanup flags. | Different server mappings always create independent participants, even on the same endpoint. |
| `pgwrh/src/replica/sync.sql`, remote server creation | Creates `postgres_fdw` servers with multi-host lists, `load_balance_hosts 'random'`, async scans enabled, writes disabled. | Installing pgwrh_fdw alone does not change pgwrh routing. Integration must explicitly change generated objects. |
| `pgwrh/src/replica/helpers.sql`, `shard_assignment_r` | Remote schema name includes `shard_server_name`. | Placement identity becomes relation identity; connection pooling alone does not remove these schemas. |
| `pgwrh/src/master/implementation-views.sql` | Controller selects effective current/target routes and credentials; repeats same-zone endpoints in host lists. | The FDW must preserve readiness, credential generations, and weighting. |
| `pgwrh/src/replica/aggregation.sql`, `remote_node_assignment` | Parent aggregation requires identical effective destinations and complete locally serving subtrees on every eligible member. | Mere endpoint overlap is insufficient to replace leaf routes with a parent scan. |
| `pgwrh_fdw/connection.c`, `begin_remote_xact()` | Starts remote transaction, propagates frozen settings, then creates mirrored savepoints. | Initialization belongs to the physical participant and must precede its first snapshot. |
| `pgwrh_fdw/pgwrh_fdw.c`, `postgresBeginForeignScan()` | Acquires the connection at scan initialization; planner estimates and ANALYZE also acquire connections. | An executor-only patch would miss several routing and consistency paths. |

Libpq randomizes connection attempts and then keeps the selected endpoint for
that connection. It does not coordinate separate callers or know which shards a
replica serves. See [PostgreSQL 18 libpq connection parameters](https://www.postgresql.org/docs/18/libpq-connect.html#LIBPQ-CONNECT-LOAD-BALANCE-HOSTS).

## Alternatives

| Option | Advantages | Limitations | Assessment |
| --- | --- | --- | --- |
| Reorder libpq host lists using affinity | Small change; can improve endpoint choices. | Still opens one PGconn per mapping. Even perfect endpoint selection can create two connections to b. | Insufficient. |
| External connection pooler/proxy | Can reduce idle backend connections and centralize admission. | Ordinary pooling does not combine two concurrent remote transactions into one snapshot/transaction; a shard-aware proxy would duplicate routing responsibilities. | Complementary, not the solution. |
| One foreign server per physical replica | Canonical mapping naturally yields connection reuse; ordinary same-server FDW behavior applies. | Assigning a foreign table to a replica makes placement static again. Dynamic alternatives require new table routing/planner logic anyway. | Useful representation for endpoints, insufficient architecture alone. |
| Existing replica-set servers with a shared physical pool | Directly fixes duplicate participants; can preserve current table generation and handoff. | Needs explicit profile compatibility and lifecycle refactoring; retains schema proliferation and existing cross-server planner restrictions. | Recommended first increment. |
| One logical cluster server plus table routes | Stable table identity; shared routing policy; clean future colocation planning. | Requires route metadata, explicit credential profiles, planner safety checks, and pgwrh rollout integration. | Recommended target. |

Do not implement automatic cross-server sharing based only on host, database,
and remote username. Equal usernames do not establish equal credentials,
authentication requirements, session settings, or authorization to use another
mapping. Route sharing should be explicitly configured.

## Data model and ownership

Use a local, FDW-owned route registry, populated by pgwrh's existing sync pass.
pgwrh_fdw remains usable without pgwrh: pgwrh publishes eligible routes; the FDW
does not calculate WRH assignments, contact the controller during user queries,
or decide whether a subscription is ready.

The registry needs these concepts:

| Object | Essential fields |
| --- | --- |
| Routing domain | Logical cluster server OID; membership identity namespace. |
| Member | Stable member ID; endpoint generation; host, optional hostaddr, port; availability zone and selection weight. |
| Connection profile | Canonical foreign server OID with ordinary user mappings; database, transport/auth options, credential generation, ordered transaction-parameter list, connection lifecycle settings. |
| Route revision | Domain and logical shard/subtree ID; revision; profile reference; eligible member IDs; remote relation identity; preparation/activation state. |
| Transaction binding | Logical route and effective access identity mapped to an immutable route revision and physical participant for the local top-level transaction. |
| Physical participant | Member/profile connection key; PGconn; upstream transaction state; single PgFdwConnState; invalidation dependencies; diagnostics. |

Represent profiles using dedicated foreign server objects and normal user
mappings, rather than introducing a second password store. They are referenced
by registered OID dependencies, not an unchecked server-name string. Only the
route-management owner can publish or change these references. Resolve mappings
using the same effective access identity as the current FDW, including
`checkAsUser` and relation-owner access for maintenance operations. Validate the
profile reference and applicable access/authentication requirements on every
acquisition; profile indirection must not become privilege escalation.

For the first increment, existing logical servers keep their host lists and
gain an explicit canonical profile reference. Pgwrh creates one profile per
replication group and credential generation, shared by its replica-set servers.
Conflicting connection or transaction options on a logical server are rejected
in this opt-in mode; the profile supplies the authoritative values. Unconfigured
servers retain today's isolated behavior.

In the target model, all ordinary foreign tables for a replication group can
use one logical cluster server and a stable `route_id` table option. The active
route revision supplies its profile. This indirection is necessary: during a
rollout, some shards can use the old credential generation while others use the
new one. A single cluster-server user mapping cannot express that transition.

Conceptually, the pool key is:

```text
(routing domain, member ID, endpoint generation,
 canonical profile user-mapping OID, profile configuration generation)
```

The profile generation covers connection-affecting options, authentication
policy and initialization policy. Retired active generations can temporarily
coexist with a new generation. That is intentional, not duplicate pooling of an
equivalent identity. Different effective users can share a PUBLIC mapping only
under the normal mapping rules and after checking each caller's access.

Use explicit member IDs from the controller, not inferred identity from an IP
address. Preserve host/hostaddr pairing, TLS hostname verification, ports, Unix
sockets and database identity. In the initial managed mode, require explicit,
unambiguous endpoint configuration; keep opaque service-based configurations in
legacy mode until their expansion and invalidation are defined. Pgwrh-managed
member endpoints must address that member, rather than a load balancer that can
silently choose another member. Endpoint changes advance the generation.

Host-list repetition currently implements weighting. Normalize duplicate
members to one candidate and preserve their multiplicity as weight; otherwise
normalization would silently remove the same-zone preference.

## Acquisition and selection

Introduce a route-aware acquisition API alongside the unchanged legacy entry
point. In the target model, a UserMapping alone is insufficient to select a
connection because one logical server can contain many routes.

```text
AcquireRoute(route_request, effective_user, operation_kind):
    validate the route and resolve its canonical connection profile
    if this logical route is already bound in the transaction:
        validate caller access and return its existing participant

    read and protect the active route revision
    candidates = its eligible members after required policy restrictions
    prefer a compatible participant already active in this transaction
    otherwise prefer a compatible healthy idle cached connection
    otherwise select a weighted candidate and connect to that member

    drain any pending request on the selected physical participant
    initialize its transaction/context if it has not started one
    establish the required mirrored savepoint depth
    publish the transaction binding and return the shared participant
```

Initialize and register failure state before exposing a usable binding. Failed
initialization must leave the same poisoned/incomplete state as today, not make
the route look unused and eligible for silent retry.

Candidate selection happens in the FDW. Supply libpq with the selected member's
endpoint, rather than a list that allows it to choose a different member behind
the pool's back. Libpq still performs protocol negotiation and address handling.

For the example:

| Starting state | Result under reuse preference |
| --- | --- |
| Compatible b participant already exists | X and Y both use b, with different cursors in one remote transaction. |
| X is already bound to a | Y selects b/c/d; do not move X after it has participated. |
| Neither route is bound and both requests are known | A later query-wide policy can select b or c to cover both with one connection. |
| No route overlap | Separate participants are necessary. |
| Same b endpoint but different credential/context profile | Separate participants are necessary. |

A lazy acquisition policy is order-dependent. It fixes redundant connections
to the same eligible member, but cannot guarantee the minimum number of
participants for a cold query. Selecting a covering set for all query routes is
a separate optimization. A reasonable later heuristic is to prefer a candidate
that covers the most remaining route requests, then use weights and randomized
ties. Do not connect to or begin transactions on all candidates to evaluate
that heuristic. Respect partition pruning, including execution-time pruning.

Keep selection policy separate from placement eligibility. Readiness and any
strict locality constraints filter candidates first. Existing same-zone
weighting is a soft preference; reuse can outrank it by default. Offer an
explicit locality-first policy if cross-zone traffic costs require the opposite.

Reconsider route bindings at each top-level transaction, while retaining useful
idle sockets. Weighted randomness on new selections avoids a global preference
for the first member. Optional idle expiry and age-based rotation can reduce
long-lived affinity; do not claim backend-local statistics measure global load.

## Transaction, async and failure invariants

**One physical participant has one lifecycle.** The physical pool, not the
logical binding table, is enumerated by commit, abort, subtransaction and
disconnect callbacks. There must be one START, one savepoint stack, and one
commit/abort sequence for b even if ten routes use it. All scans sharing b must
receive the same `PgFdwConnState *`, including `pendingAreq`.

Keep cursor IDs distinct and aggregate prepared-statement/error state on that
participant. Existing module-wide cursor and prepared-statement counters can
remain. A binding or scan ending does not close its remote transaction; it may
still own the snapshot used by other scans or later statements. Retain bindings
and failure state across local savepoint rollback, consistent with the existing
remote snapshot and frozen-context behavior.

**The initialization policy is part of compatibility.** The canonical profile
owns the ordered `transaction_parameters` list. All aliases in a pool use that
same list; do not union lists on demand. For example, a connection first used
without an LSN barrier cannot later satisfy a route that requires one after a
snapshot has been acquired. Even list order matters when a subscription setting
must precede a watermark setting. Resolve values from the existing frozen local
context and check GUC visibility for every requesting access identity, including
when the participant is already active.

Apply the context once per physical remote transaction, before EXPLAIN, cursor
creation, metadata queries that take snapshots, and savepoints. Preserve the
existing incomplete-initialization rejection. The receiving pgwrh_wait component
must already be deployed correctly; the FDW does not interpret LSNs. Reuse gives
the routes on b the same remote snapshot; it does not establish a snapshot
shared with c or guarantee identical data across logical replicas. See the
local [LSN barrier contract](../../docs/lsn-wait.md).

**One connection serializes its work.** Separate cursors can interleave fetches,
but one PGconn does not execute two queries concurrently. Existing async
coordination can be reused only when the state object is physically shared.
Different member connections remain eligible for async Append. PostgreSQL
documents this serialization behavior in its [async FDW options](https://www.postgresql.org/docs/18/postgres-fdw.html#POSTGRES-FDW-OPTIONS-ASYNC-EXECUTION).

Default to `prefer_reuse`. A later `spread` policy can select different eligible
members for independent, unbound routes when scan concurrency outweighs socket
cost. Do not introduce multiple active connections per equivalent member/profile
as the default: that reintroduces separate snapshots and defeats the stated
connection bound. Use idle LRU/expiry to bound accumulated cache entries; never
evict an active remote transaction merely because no scan currently holds it.

**Failure does not permit migration of an active transaction.** Alternate-member
attempts are allowed for initial transport establishment or a dead idle socket,
before remote transaction initialization has begun. Preserve bounded retries
and cancellation. Authentication, certificate validation, SQL errors, missing
relations, and failed context/LSN initialization are surfaced rather than
interpreted as a reason to try another shard destination. Once a participant
has started its remote transaction, connection loss fails the transaction;
resuming a cursor or another alias on c would silently change snapshots.

The first routing implementation should handle read scans and maintenance
reads, matching pgwrh's current read-only foreign routes. Reject INSERT, UPDATE,
DELETE, COPY FROM, TRUNCATE and locking reads through managed replica-set routes
at all relevant FDW entry points. Keep legacy fixed-server modification support
unchanged. Read-only user mappings should enforce the intended access remotely.

## Planner boundaries

Connection sharing does not itself ship a join. PostgreSQL normally offers the
FDW a join path only when both sides have the same foreign-server identity and
compatible access identity. The current fork also assumes matching servers in
`merge_fdw_options()`. This is described in [PostgreSQL 18 remote query optimization](https://www.postgresql.org/docs/18/postgres-fdw.html#POSTGRES-FDW-REMOTE-QUERY-OPTIMIZATION)
and visible in upstream [`set_foreign_rel_properties()`](https://doxygen.postgresql.org/relnode_8c_source.html).
The Doxygen source tracks upstream development; use the pinned PostgreSQL 18
source when implementing against the maintained fork.

In the first increment, keep existing join behavior. In the logical-cluster
increment, explicitly reject multi-route join pushdown until routing constraints
are implemented: the same logical server no longer proves physical colocation.
Single-route filters, projection and supported upper operations can retain their
normal pushdown rules. Planner remote estimates and ANALYZE must pass the route
request through the same acquisition gate. IMPORT needs an explicit member or
metadata-source contract; it must not arbitrarily import a cluster member's
possibly incomplete schema.

A later route-aware planner can assign a candidate set to every remotely
executable fragment:

```text
eligible(fragment) = intersection of eligible(member relation)
```

Candidates must also share the profile, remote object definitions, extension
and collation assumptions, access identity, and serving/readiness guarantees.
For X and Y the intersection is {b,c}; if empty, retain local join/Append work.
When building larger joins, intersect over all constituent routes, not just
pairwise overlap. Candidate information belongs to paths/fragments and must be
carried through `fdw_private`; a join's first base relation cannot stand in for
the placement requirements of its other relations.

Transaction pinning further constrains this: a previously scanned X pinned to
a cannot subsequently be shipped with Y to b in the same transaction. Generic
plans can be built before such bindings exist. A bare intersection check at
planning time is therefore insufficient. Initially restrict pushdown to route
groups with guaranteed common transaction bindings (or leave it disabled).
Broader pushdown needs explicit validation against current bindings and a
designed replan/fallback path before execution. The ordinary ForeignScan does
not automatically retain a runnable local alternative. If the selected plan
cannot be satisfied, report an error before returning rows; never silently
change an existing route's snapshot.

Plans store logical route IDs and dependency information, never backend-local
connection pointers or an indefinitely frozen endpoint list. Route publication
must invalidate dependent plans when placement-dependent SQL or pushdown
assumptions change. Membership can be re-resolved at execution for ordinary
single-route scans. Prefer local estimates initially if planning-time remote
access would pin destinations before query-wide selection can see the work.

## Pgwrh naming, aggregation and rollout integration

Use one stable foreign relation per logical remote node, for example
`orders_remote.orders_17`, pointing to the existing remote shield relation and
carrying a route ID. Placement revisions live in metadata. Keep separate local,
slot, template and shield objects where the partition/handoff design requires
them; removing destination suffixes does not eliminate their roles.

Maintain prepared and active route revisions separately. Preparing a new route
must not make it visible to user scans. A verification/ANALYZE operation should
explicitly acquire the prepared revision; statistics/readiness publication and
activation must be coordinated. If a temporary staging relation is useful for
this verification, its lifecycle need not be tied to every replica-set name.

Metadata activation must use the equivalent of today's query-root locking and
serialized sync/reporting. Also protect direct access to affected foreign
relations. Readers must retain relation/route protection from resolution through
transaction completion, including the gap between connection acquisition and
cursor creation. An update API should acquire the necessary relation locks,
publish atomically, and issue invalidations. Do not permit unrestricted catalog
DML or unlocked metadata updates that bypass this protocol. Preserve this
blocking design first; a future nonblocking revision scheme would need explicit
generation leases and retirement acknowledgements.

Feedback must report logical leaf coverage, the active/prepared route revision,
eligible membership identity and credential profile/generation. A stable cluster
server name alone proves nothing about the effective destination. Keep runtime
diagnostics of which member a particular backend selected separate from rollout
acknowledgements; a readiness acknowledgement cannot depend on one lucky probe
to b if the published route can also select an unready c.

Retain old route revisions, profiles and serving copies until existing rollout
conditions and reader protection allow their release. Changing route eligibility
does not automatically make the member's pooled socket unusable for other
routes. Endpoint/profile/mapping invalidation does invalidate that physical
participant: close it if idle, otherwise retire it at transaction end and refuse
new incompatible bindings. Track all relevant dependencies, rather than the
single server and mapping hashes stored in today's entry.

Initially preserve the current aggregation criteria. A later extension may
aggregate leaves with different eligible sets using their intersection, but
only when every member in the selected intersection advertises the complete
native serving subtree. The remote parent shield must cover exactly the intended
logical descendants and must not recursively route missing descendants elsewhere.
Empty intersections or incomplete serving metadata retain leaf routes. Socket
sharing alone does not establish any of these facts.

## Delivery and validation

| Increment | Main changes | Completion evidence |
| --- | --- | --- |
| 1. Physical participant pool | Refactor `connection.c`; add route/binding module; canonical profile option and dependencies; legacy-server adapter; shared transaction/async state; diagnostics. | Two replica-set servers using b have one remote backend PID and one transaction lifecycle. Legacy configuration keeps its prior behavior. |
| 2. Pgwrh reuse integration | Generate pgwrh_fdw routes and canonical profiles per credential generation; publish explicit member identity/weights; preserve current foreign tables and rollout protocol. | Existing handoff, rollback, endpoint failure and credential-rotation integration tests pass with pooling enabled. |
| 3. Stable logical routes | Local route catalogs/API; cluster server/table route IDs; prepared versus active revisions; stable foreign-table naming; route locks/invalidation and feedback changes. | Placement changes preserve stable relation OIDs where the logical node remains; generic plans refresh correctly; stale acknowledgements cannot release old copies. |
| 4. Optional optimization | Query-wide candidate coverage, route-aware join paths, intersection-based subtree aggregation, concurrency policy. | Measured benefit with pruning, pre-existing transaction bindings, rollout and async behavior covered. |

Keep `connection.c` responsible for physical libpq/upstream lifecycle work and a
new routing module responsible for candidate resolution, transaction bindings
and policy. Extend `PgFdwRelationInfo` and scan private data only where route
requests cross planning/execution. Update `transaction_context.c` to resolve a
profile policy and revalidate requesting roles independently of whether a new
physical transaction starts. Extend `option.c`, the SQL installation file,
namespace/symbol checks, and tests with each introduced API.

Add a physical connection diagnostic returning connection ID, domain, member,
profile, remote PID, transaction depth, pending-work state and retirement state;
add a separate binding diagnostic returning route/revision to connection ID.
Provide reuse/open/failure counters. Preserve existing inspection signatures
with documented alias semantics rather than silently redefining a logical-server
row as a unique connection. Logical-server disconnect should document that
closing an idle shared connection also affects its aliases; an active participant
remains protected. A dedicated connection-ID disconnect can remove ambiguity.

Required tests include overlapping/disjoint routes; incompatible credentials
and ordered context policies; PUBLIC mappings and changed access roles;
same host with different ports/databases; weighted duplicate endpoints;
savepoint rollback and initialization failure; async Append and cursor rescans;
dead idle versus active connections; manual disconnect and every dependency
invalidation; planner estimates, ANALYZE and generic prepared plans; runtime
partition pruning; credential generations during rollout; prepared but inactive
routes; stale feedback; parent serving-tree readiness; and the existing upstream
SQL/isolation, context-propagation and coexistence suites.

Benchmark cold and warm queries, narrow and broad scans, skewed shard costs,
different overlap patterns and multiple client backends. Measure distinct remote
PIDs, transaction participants, peak/idle connections, query latency, cross-zone
traffic and replica utilization. Under reuse mode the intended per-backend bound
is one live participant per compatible member/profile generation, apart from
explicitly retiring generations. Cluster-wide connections still multiply by the
number of client backends. A lower connection count is a concrete benefit; lower
latency must be measured because reuse reduces available concurrency.
