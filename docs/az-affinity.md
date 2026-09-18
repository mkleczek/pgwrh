# Availability-zone affinity

An **availability zone (AZ)** groups replicas that share a failure domain. For
replication groups, shards and pending configurations, see [cluster
concepts](overview.md).

A sharded table can prefer an AZ by assigning it a larger relative weight.
Preferences apply to its leaf shards and can be overridden on subpartitions. An
AZ with no inherited or explicit preference has weight `1`. Equal weights spread
copies evenly, subject to host capacity and copy requirements.

Preferences are soft. Host availability for placement, the minimum number of
copies per AZ, and the required number of surviving copies take precedence.
Here, an eligible host is one in the configuration's `shard_host_weight` table;
transient connection failures do not change the placement calculation.

## Configuration

| Configuration | Field | Meaning |
| --- | --- | --- |
| `replication_group_config` | `min_replica_count_after_az_failure` | Minimum copies that must remain after loss of any one AZ; default `0`. |
| `sharded_table` | `min_replica_count_after_az_failure` | Nullable override; inherit from the nearest ancestor with a value, then the group. |
| `sharded_table_az_affinity` | `availability_zone`, `weight` | Positive integer relative weight for a table and AZ, identified by group, configuration version, schema, and table name. |

`replication_group_config.min_replica_count_per_availability_zone` sets a hard
minimum for **every eligible AZ**. Its default is `1`.

Affinity inherits independently for each AZ. For example, a parent with weights
`a=4, b=2` and a child with `a=1` gives the child `a=1, b=2`; unmentioned zones
have weight `1`. Deleting the child's row restores inheritance. An explicit
weight of `1` cancels the inherited preference for that AZ. An explicit survivor
requirement of `0` similarly overrides an inherited requirement.

These settings belong to a [configuration
version](overview.md#configuration-and-rollouts). A new pending configuration
inherits current group and table settings. Editing a pending policy does not
change current assignments; apply it through a rollout.

## Three copies across three AZs

Assume AZ A has at least two eligible hosts, and B and C have at least one each.
The result column lists copy counts in A/B/C order.

| Minimum per AZ | Required survivors | Affinity | Result |
| --- | --- | --- | --- |
| `1` | `0`, `1`, or `2` | Any | `1/1/1` |
| `0` | `2` | Any | `1/1/1` |
| `0` | `1` | Equal weights | `1/1/1` |
| `0` | `1` | A=4, B=1, C=1 | `2/1/0` or `2/0/1` |

The survivor requirement means `copies_in_any_AZ <= total_copies - survivors`.
Thus `1` prevents all three copies from being placed in one AZ; `2` requires one
in each AZ. It describes data-copy survival, not a consensus quorum. The default
`0` permits configurations with no single-zone failure guarantee, including
single-AZ groups. When reducing the per-AZ minimum to zero, set the survivor
requirement explicitly to retain the desired AZ-failure guarantee.

An infeasible policy is rejected by preview and `start_rollout`, with the
configured constraints in the error. The failed rollout leaves the current
configuration and assignments unchanged. An AZ with fewer eligible hosts than
the per-AZ minimum causes rejection.

## Example: prefer AZ A

This example assumes group `g1` and configured table `data.root` already exist.
It requests three copies, with at least one surviving any single AZ failure. Use
a replication factor of zero when exactly the group minimum is wanted; a
positive factor can raise the copy count as the group grows.

```sql
BEGIN;

-- Omitting version creates/clones the next pending configuration when necessary.
INSERT INTO pgwrh.sharded_table_az_affinity
    (replication_group_id, sharded_table_schema, sharded_table_name,
     availability_zone, weight)
VALUES ('g1', 'data', 'root', 'a', 4);

UPDATE pgwrh.replication_group_config
SET min_replica_count = 3,
    min_replica_count_per_availability_zone = 0,
    min_replica_count_after_az_failure = 1
WHERE replication_group_id = 'g1'
  AND version = pgwrh.next_pending_version('g1');

UPDATE pgwrh.sharded_table
SET replication_factor = 0
WHERE replication_group_id = 'g1'
  AND version = pgwrh.next_pending_version('g1')
  AND sharded_table_schema = 'data'
  AND sharded_table_name = 'root';

SELECT * FROM pgwrh.preview_shard_placement(
    'g1', pgwrh.next_pending_version('g1'));

COMMIT;
```

If that affinity row already exists in the pending configuration, update it
instead. To override the survivor requirement for one subtree, set
`sharded_table.min_replica_count_after_az_failure` on its configured table.
Other table and leaf overrides continue to apply.

After reviewing the preview, follow the [rollout
procedure](overview.md#configuration-and-rollouts): start, check readiness, and
commit. Rollback restores the current configuration and waits for replica
acknowledgements before releasing unneeded copies. Retrying an unchanged pending
configuration produces the same placement.

Preview reports the effective survivor requirement, inherited affinity map, and
`unavailable_preferred_zones`. A preferred AZ with no eligible hosts is ignored
when dividing copies; it does not exclude the other AZs. Preview uses the
**current source partition tree**. For an already started or committed rollout,
read `shard_assigned_host` for its saved assignments.

## What weights guarantee

The same shard and configuration produce the same placement. Multiplying all
eligible AZ weights by the same factor does not change the result. Copy counts,
minimums and host capacity can limit how closely placement follows the weights.

Changing the zone inventory, weights or requested copy count can move copies
between zones. Preferences do not impose host load quotas, and a small number of
shards may produce uneven host utilization.
