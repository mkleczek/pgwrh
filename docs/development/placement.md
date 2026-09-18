# Shard placement algorithm

This contributor guide describes how weighted rendezvous hashing (WRH) applies
availability-zone (AZ) preferences. For configuration and examples, see [AZ
affinity](../az-affinity.md).

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

The same shard key and configuration always produce the same result. Hashes vary
the fractional choices across shard keys; there is no runtime randomness. With
one unconstrained copy, AZ weights give the usual WRH selection probabilities.
With multiple copies, rounding, per-AZ floors, and capacity or HA limits
constrain the attainable proportions. Integer shares need no random choice.
Multiplying all eligible AZ weights by the same factor changes nothing.

For equal weights this reduces to the old round-by-round AZ spread. A fixed copy
count and unchanged AZ shares retain WRH's host movement behavior within each
AZ. Changing the AZ inventory, capacity limits, weights, or copy count can
change those shares and move copies between AZs. This feature does not impose
host load quotas or solve uneven host utilization when there are few shards.
