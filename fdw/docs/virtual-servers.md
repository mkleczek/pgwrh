# Virtual foreign servers

A virtual pgwrh_fdw server selects an ordinary pgwrh_fdw server when first used
in a local transaction. All subsequent accesses through that virtual server by
the same effective local user use the selected target and its user mapping.

```sql
CREATE SERVER replica_a FOREIGN DATA WRAPPER pgwrh_fdw
    OPTIONS (host 'a', dbname 'app');
CREATE SERVER replica_b FOREIGN DATA WRAPPER pgwrh_fdw
    OPTIONS (host 'b', dbname 'app');
CREATE USER MAPPING FOR CURRENT_USER SERVER replica_a
    OPTIONS (user 'reader', password 'replace-me');
CREATE USER MAPPING FOR CURRENT_USER SERVER replica_b
    OPTIONS (user 'reader', password 'replace-me');

CREATE SERVER replicas_ab FOREIGN DATA WRAPPER pgwrh_fdw
    OPTIONS (members 'replica_a,replica_b',
             updatable 'false', truncatable 'false');
CREATE USER MAPPING FOR PUBLIC SERVER replicas_ab;

CREATE FOREIGN TABLE items(id bigint, value text)
    SERVER replicas_ab OPTIONS (schema_name 'public', table_name 'items');
```

`members` is a server-only option containing a nonempty comma-separated list of
SQL identifiers. Whitespace is allowed; quote case-sensitive names and names
containing punctuation with double quotes. Duplicate names are rejected. Members
must be ordinary servers using the same foreign-data wrapper; nesting and
self-reference are rejected when resolving the list. An invalid member name is
a configuration error, even if another member is available.

Member names are resolved on first access in each transaction. These are name
references, not new catalog dependencies: rename/drop does not rewrite or cascade
through a `members` list. Update that list when renaming a member. A selected
target is retained by OID, so reusing a dropped target's name cannot redirect an
existing transaction. Membership changes take effect for new bindings in later
transactions, even if the current transaction changes or removes `members`.

## User mappings and options

The virtual server requires an **empty user mapping** because the existing FDW
callers look up that mapping before acquiring a connection. It supplies no
credentials. Routing uses the effective local user preserved in that mapping,
including when it is a PUBLIC mapping or access occurs through a view owner.

A member is eligible only if the virtual server's owner has USAGE on the actual
server and the effective query user has a user-specific or PUBLIC mapping there.
The querying user needs the normal table permissions, but no server USAGE.
The owner's credentials are never substituted for the query user's mapping.
Owner privileges are checked when resolving routes, including previously selected
targets, so ownership changes and revoked grants affect subsequent accesses.
Role-specific mappings take precedence;
role membership does not make another role's mapping applicable. If no member
is accessible, the query errors. An empty PUBLIC mapping on the virtual server
does not grant access to its members.

Ordinary targets own all connection and transaction options: host, database,
authentication, `transaction_parameters`, `keep_connections`, `parallel_commit`,
and `parallel_abort`. These options are rejected on virtual servers. Nonempty
virtual user-mapping options are rejected at connection acquisition, since the
option validator is not given the mapping's server identity.

Virtual servers own their planner/executor options: `use_remote_estimate`, cost
settings, `extensions`, `fetch_size`, `batch_size`, `async_capable`,
`analyze_sampling`, `updatable`, and `truncatable`. Normal foreign-table overrides
still apply. Their values are not inherited from targets. All listed targets
must provide the referenced remote relations and satisfy the virtual server's
data, type, collation, extension and privilege assumptions. For read replicas,
disable writes on the virtual server and use read-only remote credentials.

## Selection and transactions

The initial selection prefers an accessible member with an existing connection
for the exact applicable user mapping:

1. An active connection already participating in this local transaction.
2. An idle cached connection.
3. A member that needs a new connection.

Ties are chosen uniformly. Cache inspection does not open connections, start
transactions, or drain pending async requests on unselected members. Incomplete
connections and invalidated/broken active connections cannot accept new virtual
bindings. Dead idle connections are handled by the existing reconnect path when
selected; inspecting local libpq status is not a network health probe.

Selection is pinned for the local top-level transaction, separately for each
virtual server and effective local user. It is not repeated for another scan,
statement or savepoint. A direct reference to the selected actual server uses
the same physical connection when it resolves to the same actual mapping.

For `members 'a,b,c'` and `members 'b,c,d'`, an existing applicable connection to
`b` can serve both virtual servers. If the first route has already selected `a`,
it stays on `a`; routing does not anticipate later scans or minimize the cold
query's total connection count. Reuse is backend-local and preserves one physical
connection per actual mapping. Independent connections still permit concurrent
work; scans sharing one connection serialize their remote requests. Idle affinity
can persist across transactions until disconnect, invalidation or session end.

The physical connection remains managed entirely by the existing FDW cache:
one transaction, savepoint stack, pending async request, and prepared-statement
state per actual user-mapping OID. Context propagation uses the actual server's
policy and existing frozen local values. It runs before the first remote snapshot
and mirrored savepoints. Independent actual servers still have independent
snapshots; this does not add distributed snapshot or commit guarantees.

A failed acquisition poisons that virtual binding until top-level rollback.
This includes initial connection and context errors caught inside a savepoint.
An active connection cannot be replaced by routing to another member after its
snapshot has been established. Dead idle connections retain upstream reconnect
behavior. There is no automatic retry on another member after an acquisition
error; retry the local transaction. Successful savepoint rollback preserves the
original selection and permits normal continued use of the surviving connection.

Bound targets must remain accessible. Replacing the selected mapping with a
different mapping OID errors instead of changing authentication mid-transaction.
Ordinary target option invalidation retains upstream behavior: existing remote
transactions finish on their original connection, which is then retired.

Scans, remote-estimate planning, ANALYZE, IMPORT and modification operations all
go through the same resolver without changing their call sites. Remote estimates open ordinary target sessions and freeze transaction context,
but do not bind virtual servers: join planning must first find a common target.
Estimation can therefore open sessions that execution does not ultimately use.
Prepared plans acquire
the current transaction's connection when executed. Joins within one virtual server retain normal pushdown behavior. SELECT joins
across different virtual servers (or a virtual server and one of its actual
members) can also be pushed down when all inputs share an accessible target.
The planner intersects all inputs, including existing transaction bindings;
pairwise overlap alone is insufficient. It retains the normal join-safety and
cost checks. Execution chooses one common member and binds every virtual input
before opening the shared connection. Cached plans store the input server OIDs,
not a selected replica; PostgreSQL invalidates plans on ALTER SERVER.

The additional paths cover the existing INNER, LEFT, RIGHT, FULL and SEMI join
support, as well as eligible upper operations and partitionwise joins. Inputs
must have the same effective local user and matching shippable-extension lists.
Cross-server write queries and row-locking queries keep local joins; their EPQ
and direct-modification routing is not extended.

Eligibility is checked again at execution. A prepared remote join can become
incompatible with a binding established after it was planned, even without any
catalog change. Execution reports `no common target` rather than moving an
established snapshot. Replanning the query in that transaction permits a local
join. A membership change invalidates the cached plan automatically.

Existing connection inspection and disconnect functions operate on **actual
servers**. They do not show alias rows or expand virtual-server names. In
particular, disconnect the actual server (or use disconnect-all) to close an idle
connection; a virtual server itself owns no connection. Connections in use remain
protected by the existing transaction checks.

## Implementation and tests

Ordinary execution still enters through `GetConnection()` in
`connection.c`. It resolves the incoming mapping before looking up the physical
cache, checks a previously bound connection, and marks successful acquisition.
`virtual.c` owns routing and validation. Its binding hash and reset callback live
in `TopTransactionContext`; no existing transaction callback is modified, and
bindings never own or free libpq connections. A read-only ranking callback in
`connection.c` lets routing inspect the existing private cache without exposing
its structure or changing its ownership. The validator additionally registers
the option and checks virtual-server option combinations.

`python3 test/test_virtual.py` runs the routing tests against private PostgreSQL
clusters. The existing `python3 test/test_context.py` entry point also runs them,
so the parent repository's test command and CI include them unchanged. Tests
cover mapping and privilege resolution, view owners, context propagation,
savepoint affinity, failed acquisition, connection loss, catalog changes,
planning, generic plans, ANALYZE, IMPORT, joins and modifications. Reuse tests
cover overlapping/disjoint memberships, mapping isolation, active versus idle
preference, invalidated connections, shared async state and runtime pruning.

The coordinated connection helper intersects all requested servers, respecting
existing transaction bindings and target mapping privileges. Execution reserves
every virtual input on the chosen member before acquiring their shared physical
connection. A caught acquisition failure poisons every participating binding.
Estimation uses the same eligibility checks without reserving new bindings.

`join.c` chains PostgreSQL's existing `set_join_pathlist_hook` to offer paths for
joins skipped by the core's server-OID check. It only handles this FDW's input
relations and preserves effective-user checks. `pgwrh_fdw.c` carries input server
OIDs through join/upper planning into the selected ForeignScan and uses the
coordinated connection helper at scan initialization. Server catalog identities
and PostgreSQL core are unchanged.
