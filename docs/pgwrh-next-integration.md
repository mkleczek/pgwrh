# pgwrh-next integration audit

Compared `pgwrh-next` (`tkuolkry`, commit `96c3a3d6`) with the working change
`vowyykwr` (snapshot `7d2c852e`) on 2026-09-15. The comparison covered every
ancestor of that bookmark outside the working change's ancestry: eight changes
and the empty merge at the bookmark. Fixes and missing regression coverage are
transplanted into focused changes above `vowyykwr`: the test environment, worker
migration and upgrade, index-readiness regressions, metadata-deduplication
regression, and this audit. The original working tree is preserved in `vowyykwr`.

## Disposition of each ancestor

| Source change | Finding and resolution |
| --- | --- |
| `yknstuyu` / `bc4d9feb` — Next version: 0.2.2 | Already covered by the current 0.2.2 control file, generated installation script, and 0.2.1 upgrade. |
| `vystvkrw` / `d3d2a99e` — deleted flake.nix | Build workflow choice, with no runtime fix. Keep the current flake, which explicitly builds the SQL-only package with the older locked PostgreSQL; use the maintained PostgreSQL 18 test shell for integration tests. |
| `tqnvvyws` / `ff05ea73` — Test suite | The test harness, seed correction, multi-replica fixtures, non-partitioned workaround, and offline scale-out regression are present. Current readiness and convergence checks account for retained local copies. The skipped skeleton tests from the old change provide no additional assertions. Corrected the test launcher's obsolete `pg_config` path. |
| `rvurwluo` / `36adf7a2` — Gate remote route changes on target index readiness | The controller already requires subscription, online status, credentials, and indexes before exposing a changed target route, while allowing unchanged routes during index builds. Restored all three missing regression tests: unchanged route, hidden unindexed target, and indexed target handoff. |
| `utnkwkzl` / `78c9483f` — Upgrade pg_background to 1.9.2 | Missing. Ported cookie-protected handles, replica-role grants, dependency capability checks, detached submission, sync-plan acquisition, boolean results, and controller subscription creation. Included daemon and controller-configuration definitions in the generated 0.2.1 upgrade. Preserved the existing `launch_in_background` entry point as a v2 wrapper. |
| `wurpuqyp` / `306406eb` — Deduplicate shard_structure across versions | Already implemented with `SELECT DISTINCT` on replication group and relation before expanding the partition tree. Restored the regression that adds a table during a rollout and checks the replica's metadata for duplicates. |
| `okosnmns` / `b055c1aa` — Use structured shard metadata for replica bootstrap | Already present, including column/constraint metadata and slot-first bootstrap. Current code extends it for retained local copies and remote subtree aggregation; keep those extensions. Both original structured-bootstrap tests are present. |
| `uxmvsopk` / `ef6d9b4f` — Expose root relation in shard_structure | Already present in the controller view, replica foreign table, helper view, upgrade, and metadata assertions. |
| `tkuolkry` / `96c3a3d6` — Pgwrh next | Empty merge. Its constituent changes are covered above; no merge-only patch to transplant. |

## Adaptations to the background-worker port

The source change used `pg_background_wait_v2` and then detached for synchronous
commands. In the [1.9.2 implementation](https://github.com/vibhorkum/pg_background/blob/v1.9.2/pg_background.c),
waiting only waits for worker shutdown: it neither consumes queued output nor
raises worker SQL errors. The port instead consumes `pg_background_result_v2`,
preserving command failure reporting and progress when output exceeds the queue.
The existing single-text-column results for transactional sync scripts and state
reports are retained. Tests exercise a large result, a failed SQL statement,
nontransactional command ordering, and subsequent session reuse.

The capability check requires only the four APIs actually used: launch, submit,
result, and detach. These signatures work on 1.9.2 and on the compatibility aliases
in 2.0.2; the changed `wait_v2` signature in 2.x is irrelevant to the port.
The integration shell pins 1.9.2 because the released pgwrh 0.2.1 still needs the
legacy API to initialize the upgrade test. Production upgrade order is documented
in the README and remote aggregation guide.

## Validation

- PostgreSQL 18.6 / pinned pg_background 1.9.2: the complete SQL integration
  suite passed, **51 tests**, including retained-local handoff, both rollback
  variants, canonical target-set aggregation, failover, credential rotation,
  offline scale-out, upgrades, and virtual-server membership synchronization.
  Command: `nix-shell --run 'pgwrh-test test --ignore=test/native -q --durations=10'`.
- PostgreSQL 18.6 / pg_background 2.0.2: all 10 worker, multi-replica,
  index-readiness, metadata-deduplication, and structured-bootstrap tests passed.
- PostgreSQL 18.3 / pg_background 1.9.2: both worker tests and the real 0.2.1
  upgrade passed. The upgrade preserves existing rows and root relation OIDs and
  exercises the v2 helpers using the replica role after upgrade.
- The base change `vowyykwr` matches the original `7d2c852e` snapshot exactly, and
  `git diff --check` passes. The `pgwrh-next` bookmark remains at `96c3a3d6`.
