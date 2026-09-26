# Virtual foreign servers

A foreign server describes a remote PostgreSQL database. An ordinary `pgwrh_fdw`
server supplies its endpoint and connection settings. A **virtual server** names
ordinary servers as eligible **members** and chooses one for a transaction. See
the [FDW guide](../README.md) for installation and transaction settings.

Virtual servers with identical member sets share a selection for the same
effective local user. This combination of members and user is a **routing
group**. Once a group selects a target, it retains that target until the local
transaction ends, including across statements and savepoint rollback.

## Create a virtual server

The example assumes both remote databases contain `public.items`, and that the
remote `reader` role can read it. Replace endpoints and credentials before use.
Enable `pgwrh_fdw` in the local database first:

```sql
CREATE EXTENSION pgwrh_fdw;

CREATE SERVER replica_a FOREIGN DATA WRAPPER pgwrh_fdw
    OPTIONS (host 'a', dbname 'app_a');
CREATE SERVER replica_b FOREIGN DATA WRAPPER pgwrh_fdw
    OPTIONS (host 'b', dbname 'app_b');
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

Each ordinary member has its own `dbname`; virtual-server load balancing does
not require matching database names. Both databases must expose the relation
named by the foreign table with compatible columns.

`members` is a server-only option containing a nonempty comma-separated list of
SQL identifiers. Whitespace is allowed; double-quote case-sensitive names and
names containing punctuation. Duplicate names, nested virtual servers and
self-reference are rejected. Every named member must be an ordinary server using
the same FDW. An invalid member name is an error even if another member is
available.

Member names are resolved on first access in each transaction. Renaming or
dropping a member does not update the list; update it yourself. Reusing a
dropped target's name cannot redirect an existing transaction.

## User mappings and options

A **user mapping** specifies how a local user authenticates to a foreign server.
The virtual server requires an empty mapping; credentials belong to its members.
The virtual server's owner must have USAGE on the selected member. The querying
user needs normal table permissions and an applicable user-specific or PUBLIC
mapping, but no server USAGE. Owner privileges are checked when resolving routes,
including previously selected targets; ownership changes and revoked grants affect
subsequent accesses. Each virtual server's owner is checked even when servers
share a transaction's selected connection.

Credentials always come from the effective query user's mapping, never from the
virtual server owner's mapping. Role-specific mappings take precedence; role
membership does not make another role's mapping applicable. Access through a view
uses the effective user for that view. If no member is authorized for the server
owner and mapped for the query user, the query fails.

Configure options at the level that owns them:

| Server | Options |
| --- | --- |
| Ordinary member | Endpoint, database, authentication, `transaction_parameters`, `keep_connections`, `parallel_commit`, `parallel_abort`, `load_balance_weight` |
| Virtual server | `members`, `use_remote_estimate`, cost settings, `extensions`, `fetch_size`, `batch_size`, `async_capable`, `streaming_fetch`, `analyze_sampling`, `updatable`, `truncatable` |

Connection and transaction options are rejected on virtual servers. Virtual user
mappings with options are rejected when accessed. Query options belong to the
virtual server and are not inherited from members; normal foreign-table
overrides still apply.

All members must provide the referenced relations and compatible data, types,
collations, extensions and privileges. For read replicas, disable writes on the
virtual server and use remote credentials restricted to reading.

## How a target is selected

Selection prefers an accessible member in this order:

1. A connection already participating in the local transaction.
2. An idle connection retained by the local session.
3. A member requiring a new connection.

Within each tier, selection is random in proportion to `load_balance_weight`.
This member-server option accepts integers from 1 through 2147483647 and
defaults to 1. For example:

```sql
ALTER SERVER replica_a OPTIONS (ADD load_balance_weight '4');
```

This gives `replica_a` four times the selection probability of a default-weight
member in the same tier. It does not override connection reuse or a target
already selected for the transaction. Omit or drop the option to restore weight
1. Weight changes affect subsequent selections, including joins and remote
estimates. Existing remote transactions finish on their original connections.
Weights are preferences, not capacity limits; connection reuse can outweigh
them across transactions.

Member order and whitespace do not affect routing-group identity. Different
configured sets remain separate groups even if their accessible members happen
to be the same. Once a group selects a target, other virtual servers in that
group inherit it. A virtual server already used in a transaction retains its
original group even if its `members` option changes.

Overlapping groups can reuse a connection. For example, servers with members
`a,b,c` and `b,c,d` can both use an existing connection to `b`. If the first has
already selected `a`, it stays there. Selection does not anticipate later
queries or guarantee the fewest possible connections.

Reuse occurs within a local database session and requires the same member user
mapping. Direct access to the selected member can use that connection too. Scans
sharing a connection serialize their requests and share one remote transaction
and its savepoints. Different connections still have independent snapshots;
there is no cluster-wide snapshot or distributed atomic commit.

## Connection failures and retries

Connection reuse is not a network health check. If an initial connection fails
with SQLSTATE `08001`, routing can try another eligible member, provided no
participating route has acquired a remote transaction. Each candidate is tried
at most once. This also applies to failed reconnection of an idle connection and
to remote estimates. A pushed join retries only among members eligible for all
its inputs.

Transaction-start, transaction-setting, query and cancellation errors do not
trigger target failover. Even an initialization error reporting `08001` cannot
switch targets after a remote transaction has started. An established selection
never fails over; retry the local transaction after a connection failure.

If every connection candidate fails, or transaction initialization fails, the
routing group remains unusable until top-level rollback. Catching such an error
inside a savepoint does not restore the group. A successful savepoint rollback
on an otherwise usable connection preserves the original selection.

Selected targets must remain accessible. Replacing the selected user mapping
with a different mapping causes an error instead of changing authentication
mid-transaction. Endpoint or option changes allow an existing remote transaction
to finish on its original connection before that connection is retired.

## Change membership safely

The server owner or a superuser can update a virtual server's members with:

```sql
BEGIN;
SELECT pgwrh_fdw_set_members('replicas_ab', ARRAY['replica_b']);
COMMIT;
```

The function preserves other server options and applies normal ownership checks
and DDL event triggers. The array must be nonempty, one-dimensional, and contain
distinct, non-null ordinary server names. Array elements are literal names; do
not put SQL identifier quoting inside them. Read-only transactions cannot call
the function.

The update waits for transactions using or planning through this virtual server,
including remote estimates, ANALYZE and IMPORT. Planning a join can delay an
update even if the plan is never executed. Rolling a reader back to a savepoint
does not release its use of the old membership. An unused virtual server does
not block an update merely because it belongs to the same routing group.

New readers wait for the updating transaction to finish. Treat the change as
effective only after commit. Cancellation, `lock_timeout` and deadlock handling
apply normally. Run updates separately from queries using the affected server:
an update after reading or planning through it is rejected, and using it after
an update is rejected until top-level transaction end, even if the update was
rolled back to a savepoint. Other virtual servers remain usable.

Plain `ALTER SERVER ... OPTIONS (SET members ...)` bypasses this waiting step.
Use `pgwrh_fdw_set_members` before relying on a membership change to retire a
destination. It does not wait for direct queries through ordinary members or
coordinate changes to their endpoints, credentials or names. Reconnect older
sessions after replacing the FDW library so all readers use the supported
version.

## Joins and prepared queries

**Join pushdown** means executing a join on a remote database. SELECT joins
between virtual servers, or between a virtual server and an ordinary member, can
be pushed when every input shares an accessible target. Pairwise overlap is
insufficient: the target must satisfy all inputs and any selections already made
in the transaction. Normal cost and query-safety checks still apply.

Supported join types include INNER, LEFT, RIGHT, FULL and SEMI, along with
eligible upper operations and partitionwise joins. Inputs need the same
effective local user and matching `extensions` option lists. Cross-server write
queries and row-locking queries keep local joins.

A join involving different routing groups must contain every reference to each
participating group in the statement, including references through other virtual
servers with the same members. Otherwise those references might select
incompatible targets. Such partial joins stay local; a larger join covering all
references can still be pushed. Joins entirely within one group do not have this
restriction, including repeated references, UNION branches and separate scans.

Remote estimates can open member connections and freeze transaction settings
without selecting a transaction target for the virtual server. Planning can
therefore open connections that execution does not use. Prepared plans select a
target when executed; membership changes invalidate affected plans.

A prepared remote join can conflict with a target selected later in the same
transaction. Execution then reports `no common target`; it does not change an
established snapshot. Replanning in that transaction allows a local join.

## Inspect or close connections

Connection inspection and disconnect functions operate on **ordinary member
servers**. They do not expand virtual-server names or show separate rows for
virtual servers sharing a connection. Disconnect the ordinary member, or use
disconnect-all, to close an idle connection. Connections still used by a
transaction cannot be closed this way.

For implementation details and routing tests, see the [contributor
guide](virtual-server-internals.md).
