-- Run as administrator on an isolated logical restore before reconnecting replicas.
-- Saved reports describe the old controller, not the restored database.
BEGIN;
UPDATE pgwrh.shard_host SET online = false;
UPDATE pgwrh.replication_group_member SET
    subscribed_local_shards = '[]',
    indexes = '[]',
    connected_local_shards = '[]',
    connected_remote_shards = '[]',
    prepared_remote_shards = '[]',
    serving_subtrees = '[]',
    users = '[]',
    credential_generation = NULL;
COMMIT;
