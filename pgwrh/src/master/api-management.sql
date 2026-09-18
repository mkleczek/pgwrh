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
    LANGUAGE plpgsql
    AS
$$
BEGIN
    IF EXISTS (SELECT 1 FROM replication_group_config_lock
               WHERE replication_group_id = _replication_group_id AND rollback_unlock IS NOT NULL) THEN
        RAISE EXCEPTION 'Rollback has not finished on all replicas';
    END IF;
    INSERT INTO replication_group_config_lock (replication_group_id, version)
    SELECT
        replication_group_id, version
    FROM
        replication_group_config
           JOIN replication_group USING (replication_group_id)
    WHERE
            replication_group_id = $1
        AND current_version = target_version AND version <> current_version
    ON CONFLICT DO NOTHING;
    UPDATE replication_group g
        SET target_version = l.version
        FROM replication_group_config_lock l
        WHERE
                g.replication_group_id = l.replication_group_id
            AND l.version = next_version(current_version)
            AND g.replication_group_id = $1;
END
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

CREATE FUNCTION add_replica(
        _replication_group_id text,
        _replica_id text,
        _host_name text,
        _port int,
        _member_role regrole DEFAULT NULL,
        _availability_zone text DEFAULT 'default',
        _weight int DEFAULT 100,
        _dbname text DEFAULT current_database())
    RETURNS void
    SET SEARCH_PATH FROM CURRENT
    LANGUAGE sql
    AS
$$
    WITH m AS (
        INSERT INTO replication_group_member (replication_group_id, host_id, member_role, availability_zone)
        VALUES (_replication_group_id, _replica_id, coalesce(_member_role::text, _replica_id::regrole::text), _availability_zone)
    ),
    h AS (
        INSERT INTO shard_host (replication_group_id, availability_zone, host_id, host_name, port, dbname)
        VALUES (_replication_group_id, _availability_zone, _replica_id, _host_name, _port, _dbname)
    )
    INSERT INTO shard_host_weight (replication_group_id, availability_zone, host_id, weight)
    VALUES (_replication_group_id, _availability_zone, _replica_id, _weight)
$$;
COMMENT ON FUNCTION add_replica(_replication_group_id text, _host_id text, _host_name text, _port int, _member_role regrole, _availability_zone text, _weight int, _dbname text) IS
$$
Adds new replica to a cluster.
_dbname identifies the replica database and defaults to the controller database name.
$$;

CREATE FUNCTION set_replica_weight(
    _replication_group_id text,
    _availability_zone text,
    _replica_id text,
    _weight int)
    RETURNS void
    SET SEARCH_PATH FROM CURRENT
    LANGUAGE sql
    AS
$$
    INSERT INTO shard_host_weight (replication_group_id, availability_zone, host_id, weight)
    VALUES (_replication_group_id, _availability_zone, _replica_id, _weight)
    ON CONFLICT (replication_group_id, availability_zone, host_id, version)
    DO UPDATE SET weight = EXCLUDED.weight;
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
Requires connected target local shards and ready target remote routes. A prepared
foreign replacement qualifies only while its leaf is still served locally.

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
    -- Keep target copies, indexes and credentials alive while readers restore
    -- current routes. Clearing acknowledgements also covers in-flight sync plans.
    UPDATE replication_group_config_lock l SET rollback_unlock = unlock
    FROM replication_group g
    WHERE g.replication_group_id = _replication_group_id
        AND (l.replication_group_id, l.version) = (g.replication_group_id, g.target_version)
        AND g.current_version <> g.target_version;
    UPDATE replication_group_member m
        SET connected_local_shards = '[]', connected_remote_shards = '[]', prepared_remote_shards = '[]'
    FROM replication_group g
    WHERE m.replication_group_id = g.replication_group_id
        AND g.replication_group_id = _replication_group_id
        AND g.current_version <> g.target_version;
    UPDATE replication_group SET target_version = current_version
    WHERE replication_group_id = _replication_group_id;
$$;
COMMENT ON FUNCTION rollback_rollout(_replication_group_id text, unlock boolean) IS
$$
Rolls back any changes that are effects of roll out of new configuration version.
Retains target copies until all replicas report current routes again, then unlocks
the abandoned configuration if requested. A new rollout waits for that acknowledgement.
$$;
