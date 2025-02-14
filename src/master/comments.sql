-- requires: core

COMMENT ON TABLE replication_group IS
'Represents a specific cluster (replica group) configuration.
A single sever may be a source of data for multiple groups of replicas.
Each group may have different configuration, in particular:
* what tables should be sharded
* number of desired copies per shard
* member servers and shard hosts topology

Username and password are credentials shared between cluster members and used
to access remote shards (ie. they are used in USER MAPPINGs created on cluster members).
';
COMMENT ON COLUMN replication_group.replication_group_id IS
'Unique identifier of a replication group.';

COMMENT ON TABLE replication_group_config IS
'Represents a version of configuration of a replication group.

Each cluster (replication group) configuration is versioned to make sure
changes in cluster topology and shards configuration does not cause any downtime.

There may be two versions of configuration present at the same time.
A configuration version might be "pending" or "ready".

Version marked as "ready" (pending = false) is a configuration version that all
replicas installed and configured successfully. The shards assigned to replicas in that version are copied, indexed and available to use.

Version marked as "pending" (pending = true) is a configuration version that is under installaction/configuration by the replicas.

A replica keeps all shards from "ready" configuration even if a shard might be no longer assigned to it in "pending" configuration version.';

COMMENT ON TABLE replication_group_member IS
'Represents a node in a cluster (replication group).

A cluster consists of two types of nodes:

* shard hosts - nodes that replicate and serve data
* non replicating members - nodes that act only as proxies (ie. not hosting any shards)';

COMMENT ON TABLE shard_host IS
'Represents a data replicating node in a cluster (replication group).';
COMMENT ON COLUMN shard_host.online IS
'Shard host marked as offline is not going to receive any requests for data from other nodes.
It is still replicating shards assigned to it.

This flag is supposed to be used in situation when a particular node must be
temporarily disconnected from a cluster for maintenance purposes.';

COMMENT ON TABLE shard_host_weight IS
'Weight of a shard host in a specific configuration version';


COMMENT ON FUNCTION next_pending_version(group_id text) IS
'Inserts next pending version into replication_group_config and returns it.';

COMMENT ON FUNCTION clone_config(group_id text, target_version config_version) IS
'Copies configuration from one version to another. Ignores already existing items.';

COMMENT ON FUNCTION mark_pending_version_ready(group_id text) IS
'Swaps pending and ready configuration versions for a group.
Does not do anything if there is no pending version present';
