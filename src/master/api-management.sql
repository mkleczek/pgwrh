-- name: master-api-management
-- requires: master-tables

-- pgwrh
-- Copyright (C) 2024  Michal Kleczek

-- This program is free software: you can redistribute it and/or modify
-- it under the terms of the GNU Affero General Public License as published by
-- the Free Software Foundation, either version 3 of the License, or
-- (at your option) any later version.

-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU Affero General Public License for more details.

-- You should have received a copy of the GNU Affero General Public License
-- along with this program.  If not, see <http://www.gnu.org/licenses/>.

CREATE FUNCTION start_rollout(
        _replication_group_id text)
    RETURNS void
    SET SEARCH_PATH FROM CURRENT
    LANGUAGE sql
    AS
$$
    WITH lock AS (
        INSERT INTO replication_group_config_lock (replication_group_id, version)
        SELECT
            replication_group_id, version
        FROM
            replication_group_config
               JOIN replication_group USING (replication_group_id)
        WHERE
                replication_group_id = $1
            AND current_version = target_version AND version <> current_version
        ON CONFLICT DO NOTHING
        RETURNING *
    )
    UPDATE replication_group g
        SET target_version = l.version
        FROM lock l
        WHERE
            g.replication_group_id = l.replication_group_id;
$$;
COMMENT ON FUNCTION start_rollout(_replication_group_id text) IS
$$
Starts rollout of group's next configuration version.

The new version is locked and marked as target version in replication_group record.
If there is no new configuration version the function is a noop.

# Parameters
## _replication_group_id
Identifier of the replication group to start rollout.
$$;

CREATE FUNCTION create_replica_cluster(
        _replication_group_id text)
    RETURNS void
    SET SEARCH_PATH FROM CURRENT
    LANGUAGE sql
    AS
$$
    INSERT INTO replication_group (replication_group_id)
    VALUES ($1);
$$;
COMMENT ON FUNCTION create_replica_cluster(_replication_group_id text) IS
$$
Creates new replica cluster.
$$;

CREATE FUNCTION add_shard_host(
        _replication_group_id text,
        _host_id text,
        _host_name text,
        _port int,
        _member_role regrole DEFAULT NULL,
        _availability_zone text DEFAULT 'default',
        _weight int DEFAULT 100)
    RETURNS void
    SET SEARCH_PATH FROM CURRENT
    LANGUAGE sql
    AS
$$
    WITH m AS (
        INSERT INTO replication_group_member (replication_group_id, host_id, member_role, availability_zone)
        VALUES (_replication_group_id, _host_id, coalesce(_member_role::text, _host_id::regrole::text), _availability_zone)
    ),
    h AS (
        INSERT INTO shard_host (replication_group_id, availability_zone, host_id, host_name, port)
        VALUES (_replication_group_id, _availability_zone, _host_id, _host_name, _port)
    )
    INSERT INTO shard_host_weight (replication_group_id, availability_zone, host_id, weight)
    VALUES (_replication_group_id, _availability_zone, _host_id, _weight)
$$;
COMMENT ON FUNCTION add_shard_host(_replication_group_id text, _host_id text, _host_name text, _port int, _member_role regrole, _availability_zone text, _weight int) IS
$$
Adds new replica to a cluster.
$$;

CREATE OR REPLACE FUNCTION commit_rollout(
        group_id text, keep_old_config boolean DEFAULT false)
    RETURNS void
    SET SEARCH_PATH FROM CURRENT
    LANGUAGE plpgsql
    AS
$$
BEGIN
    UPDATE replication_group g
        SET current_version = target_version
    WHERE
            replication_group_id = group_id;
    DELETE FROM replication_group_config cfg
    USING replication_group g
    WHERE
            g.replication_group_id = cfg.replication_group_id
        AND g.replication_group_id = group_id
        AND cfg.version <> g.current_version
        AND NOT keep_old_config;
END
$$;
COMMENT ON FUNCTION commit_rollout(group_id text, keep_old_config boolean) IS
$$
Marks the version being rolled out as current.
If any of the replicas did not report all remote and local shards as ready error is raised.

# WARNING
This is destructive operation. During rollout replicas maintain shards from both versions.
After marking new version as current they will delete no longer needed shards.
$$;

CREATE FUNCTION rollback_rollout(_replication_group_id text, unlock boolean DEFAULT TRUE)
    RETURNS void
    SET SEARCH_PATH FROM CURRENT
    LANGUAGE sql
    AS
$$
    UPDATE replication_group
        SET target_version = current_version
    WHERE replication_group_id = _replication_group_id;
    DELETE FROM replication_group_config_lock l
        USING replication_group g
        WHERE
                g.replication_group_id = _replication_group_id
            AND l.replication_group_id = g.replication_group_id
            AND l.version <> g.current_version
            AND unlock;
$$;
COMMENT ON FUNCTION rollback_rollout(_replication_group_id text, unlock boolean) IS
$$
Rolls back any changes that are effects of roll out of new configuration version.
Unlocks configuration version being rolled out.
$$;