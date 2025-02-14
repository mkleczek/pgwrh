-- name: ext-config-dump
-- requires: tables

SELECT pg_catalog.pg_extension_config_dump('replication_group_config_lock', '');
SELECT pg_catalog.pg_extension_config_dump('replication_group_config_clone', '');
SELECT pg_catalog.pg_extension_config_dump('replication_group_config', '');
SELECT pg_catalog.pg_extension_config_dump('replication_group', '');
SELECT pg_catalog.pg_extension_config_dump('replication_group_member', '');
SELECT pg_catalog.pg_extension_config_dump('shard_host', '');
SELECT pg_catalog.pg_extension_config_dump('shard_host_weight', '');
SELECT pg_catalog.pg_extension_config_dump('sharded_table', '');
SELECT pg_catalog.pg_extension_config_dump('shard_index_template', '');
SELECT pg_catalog.pg_extension_config_dump('shard', '');
SELECT pg_catalog.pg_extension_config_dump('shard_assigned_host', '');
SELECT pg_catalog.pg_extension_config_dump('shard_assigned_index', '');
