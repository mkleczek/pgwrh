# Availability-zone affinity

A sharded table can prefer an availability zone (AZ) by assigning it a larger
relative weight. Preferences apply to its leaf shards and can be overridden on
subpartitions. There is no placement mode: every unspecified AZ has weight `1`,
and equal weights retain the existing even spread and host assignments.

Preferences are soft. Host availability for placement, the minimum number of
copies per AZ, and the required number of surviving copies take precedence.
Here, an eligible host is one in the configuration's `shard_host_weight` table;
transient connection failures do not change the placement calculation.

## Metadata

| Configuration | Field | Meaning |
| --- | --- | --- |
| `replication_group_config` | `min_replica_count_after_az_failure` | Minimum copies that must remain after loss of any one AZ; default `0`. |
| `sharded_table` | `min_replica_count_after_az_failure` | Nullable override; inherit from the nearest ancestor with a value, then the group. |
| `sharded_table_az_affinity` | `availability_zone`, `weight` | Positive integer relative weight for a table and AZ, identified by group, configuration version, schema, and table name. |

The existing `min_replica_count_per_availability_zone` remains a hard minimum
for **every eligible AZ**. Its default is `1`. Neither new columns nor resolved
policy copies are added to `shard`.

Affinity inherits independently for each AZ. For example, a parent with weights
`a=4, b=2` and a child with `a=1` gives the child `a=1, b=2`; unmentioned zones
have weight `1`. Deleting the child's row restores inheritance. An explicit
weight of `1` cancels the inherited preference for that AZ. An explicit survivor
requirement of `0` similarly overrides an inherited requirement.

All these settings are versioned with the existing FLIP/FLOP configuration.
Affinity rows are cloned, locked, and deleted with their configuration. A new
pending configuration inherits the current group settings as well as table
settings. Changing a pending policy does not change current assignments.

## Three copies across three AZs

Assume AZ A has at least two eligible hosts, and B and C have at least one each.

| Minimum per AZ | Required survivors | Affinity | Result |
| --- | --- | --- | --- |
| `1` | `0`, `1`, or `2` | Any | `1/1/1` |
| `0` | `2` | Any | `1/1/1` |
| `0` | `1` | Equal weights | `1/1/1` |
| `0` | `1` | A=4, B=1, C=1 | `2/1/0` or `2/0/1` |

The survivor requirement means `copies_in_any_AZ <= total_copies - survivors`.
Thus `1` prevents all three copies from being placed in one AZ; `2` requires
one in each AZ. It describes data-copy survival, not a consensus quorum.
The default `0` preserves existing configurations, including single-AZ groups.
When reducing the per-AZ minimum to zero, set the survivor requirement explicitly
to retain the desired AZ-failure guarantee.

An infeasible policy is rejected by preview and `start_rollout`, with the
configured constraints in the error. The failed rollout leaves the current
configuration, locks, and snapshots unchanged. In particular, the per-AZ minimum
is now checked explicitly: an AZ with fewer eligible hosts than that minimum
causes rejection rather than silently receiving fewer copies.

## Example: prefer AZ A

This example assumes group `g1` and configured table `data.root` already exist.
It requests three copies, with at least one surviving any single AZ failure.
Use a replication factor of zero when exactly the group minimum is wanted;
a positive factor can raise the copy count as the group grows.

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

After reviewing the preview, use the usual `start_rollout`, readiness checks,
and `commit_rollout` flow. `rollback_rollout` restores the current configuration
through the usual replica acknowledgement protocol. Retrying an unchanged
pending configuration produces the same placement.

Preview reports the effective survivor requirement, inherited affinity map, and
`unavailable_preferred_zones`. A preferred AZ with no eligible hosts is ignored
when dividing copies; it does not exclude the other AZs. Preview uses the
**current source partition tree**. For an already started or committed rollout,
read `shard_assigned_host` for its saved assignments.

## Selection rule

1. Calculate the copy count using the existing replication factor and group
   minimum rules.
2. Limit every AZ to `min(eligible_hosts, copies - required_survivors)` and
   validate that the minima and total copy count are feasible.
3. Reserve the per-AZ minimum. Divide the remaining copies in proportion to AZ
   weights. When an AZ reaches its limit, redistribute its excess share among
   the remaining AZs.
4. Assign each AZ the whole part of its extra share. Use weighted rendezvous
   hashing over the fractional parts to award the remaining slots, at most one
   additional slot per AZ.
5. Within each AZ, select the corresponding prefix of the existing host WRH
   ranking, using host weights independently of AZ weights.

The same shard key and configuration always produce the same result. Hashes
vary the fractional choices across shard keys; there is no runtime randomness.
With one unconstrained copy, AZ weights give the usual WRH selection
probabilities. With multiple copies, rounding, per-AZ floors, and capacity or HA
limits constrain the attainable proportions. Integer shares need no random
choice. Multiplying all eligible AZ weights by the same factor changes nothing.

For equal weights this reduces to the old round-by-round AZ spread. A fixed
copy count and unchanged AZ shares retain WRH's host movement behavior within
each AZ. Changing the AZ inventory, capacity limits, weights, or copy count can
change those shares and move copies between AZs. This feature does not impose
host load quotas or solve uneven host utilization when there are few shards.
