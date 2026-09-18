# Virtual-server implementation

For option syntax and transaction behavior, see the [user
guide](virtual-servers.md). A virtual server names eligible ordinary servers;
routing binds it to a selected member for the local transaction.

Ordinary execution still enters through `GetConnection()` in `connection.c`. It
resolves the incoming mapping before looking up the physical cache, checks a
previously bound connection, and marks successful acquisition. `virtual.c` owns
routing and validation. Its alias/group hashes and reset callback live in
`TopTransactionContext`; no existing transaction callback is modified, and
bindings never own or free libpq connections. A read-only ranking callback in
`connection.c` lets routing inspect the existing private cache without exposing
its structure or changing its ownership. The validator additionally registers
the option and checks virtual-server option combinations.

From the repository root, `python3 test/pgwrh_fdw/test_virtual.py` runs the
routing tests against private PostgreSQL clusters. The existing `python3
test/pgwrh_fdw/test_context.py` entry point also runs them, so the parent
repository's test command and CI include them unchanged. Tests cover mapping and
privilege resolution, view owners, context propagation, savepoint affinity,
failed acquisition, connection loss, catalog changes, planning, generic plans,
ANALYZE, IMPORT, joins and modifications. Reuse tests cover overlapping/disjoint
memberships, mapping isolation, active versus idle preference, invalidated
connections, shared async state and runtime pruning.

The coordinated connection helper intersects all requested servers, respecting
existing transaction bindings, each virtual server owner's target USAGE, and
the effective query user's target mappings. Execution reserves
every virtual input on the chosen member before acquiring their shared physical
connection. A caught acquisition failure poisons every participating binding.
Estimation uses the same eligibility checks without reserving new bindings.

`join.c` chains PostgreSQL's existing `set_join_pathlist_hook` to offer paths
for joins skipped by the core's server-OID check. It only handles this FDW's
input relations and preserves effective-user checks. `pgwrh_fdw.c` carries input
server OIDs through join/upper planning into the selected ForeignScan and uses
the coordinated connection helper at scan initialization. Server catalog
identities and PostgreSQL core are unchanged.

A join involving different routing groups must contain every reference to each
participating virtual group in the statement, including references through
sibling shard servers with the same members. Otherwise a separate scan or pushed
join could pin that group to an incompatible replica. Such partial joins stay
local; a larger join containing all those references can still be pushed. The
check also covers sibling subqueries and partitioned inputs. It is conservative:
it can decline a partial pushdown even when a statement-wide routing optimizer
could coordinate the independent scans. This avoids changing scan initialization
or opening connections to pruned branches.

Joins entirely within one virtual routing group skip that restriction and the
statement-wide reference walk. Group comparisons use the original membership of
already-acquired aliases. A topology edit cannot make independently pinned
groups appear interchangeable merely by giving their servers the same current
`members` option. The existing cross-server write, row-lock and shippability
restrictions still apply.

Statement references are collected once in planner memory, avoiding repeated
partition-tree walks for each candidate join. The cache is released with the
planner context on success or error.

## Membership locks

The FDW acquires an `AccessShareLock` on the virtual server's database object
before inspecting its membership for routing or join planning. This lock is
owned by the top-level transaction, matching routing bindings even when a
savepoint or PL/pgSQL exception block is rolled back. It covers all tables and
effective users of the server. Pushed joins lock every virtual input; remote
estimation, ANALYZE and IMPORT also participate. Merely planning a virtual join
can therefore delay an update, even if that plan is never executed. An unused
alias is not locked just because another alias shares its routing group.

The update function acquires the conflicting `AccessExclusiveLock`. The lock
remains held after the function returns and is released by transaction commit or
rollback; rolling back an updating subtransaction releases its lock and undoes
its catalog change together. Waiting readers refresh membership after acquiring
their lock. Reconciliation can report the updated configuration **after
commit**, when old bindings have drained and new readers can proceed. Normal
PostgreSQL cancellation, `lock_timeout` and deadlock handling apply.
