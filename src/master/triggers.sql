-- name: master-triggers
-- requires: core
-- requires: publication-sync
-- requires: master-snapshot

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


CREATE OR REPLACE FUNCTION replication_group_prepare() RETURNS trigger LANGUAGE plpgsql AS
$$
BEGIN
    INSERT INTO "@extschema@".replication_group_config VALUES (NEW.replication_group_id, NEW.current_version)
    ON CONFLICT DO NOTHING;
    INSERT INTO "@extschema@".replication_group_config_lock VALUES (NEW.replication_group_id, NEW.current_version)
    ON CONFLICT DO NOTHING;
    INSERT INTO "@extschema@".replication_group_lock VALUES (NEW.replication_group_id);
    RETURN NEW;
END
$$;
CREATE OR REPLACE TRIGGER replication_group_prepare
AFTER INSERT ON replication_group
FOR EACH ROW EXECUTE FUNCTION replication_group_prepare();
COMMENT ON TRIGGER replication_group_prepare ON replication_group IS
$$
* Creates a default, empty configuration that is locked and marked as current.
* Inserts a replication_group_lock to prevent accidental deletes of newly created replication_group.
$$;

CREATE FUNCTION version_lifecycle_check() RETURNS trigger LANGUAGE plpgsql AS
$$
BEGIN
    IF OLD.current_version = OLD.target_version THEN
        IF NEW.current_version <> OLD.current_version THEN
            RAISE 'Cannot switch current version directly. Please update target version first.';
        END IF;
    ELSE
        IF NEW.target_version <> NEW.current_version AND NEW.current_version <> OLD.current_version THEN
            RAISE 'Cannot swap version. Please rollback target version first.';
        END IF;
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER version_lifecycle_check
    BEFORE UPDATE ON replication_group
    FOR EACH ROW EXECUTE FUNCTION version_lifecycle_check();

CREATE FUNCTION check_replication_group_rollout_done() RETURNS trigger LANGUAGE plpgsql AS
$$
BEGIN
    IF EXISTS (SELECT 1 FROM
        "@extschema@".missing_connected_local_shard
               WHERE
                   version = NEW.current_version
    ) THEN
        RAISE 'Not all hosts confirmed configuration of required local shards'
            USING HINT = 'Check missing_connected_local_shard view for details';
    END IF;
    IF EXISTS (SELECT 1 FROM
        "@extschema@".missing_connected_remote_shard
               WHERE
                   version = NEW.current_version
    ) THEN
        RAISE 'Not all hosts confirmed configuration of required remote shards'
            USING HINT = 'Check missing_connected_remote_shard view for details';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER check_replication_group_rollout_done
    BEFORE UPDATE ON replication_group
    FOR EACH ROW
    WHEN ( NEW.current_version <> OLD.current_version )
EXECUTE FUNCTION check_replication_group_rollout_done();


CREATE OR REPLACE FUNCTION next_pending_version_trigger() RETURNS TRIGGER
LANGUAGE plpgsql AS
$$BEGIN
    NEW.version := "@extschema@".next_pending_version(NEW.replication_group_id);
    RETURN NEW;
END$$;

CREATE OR REPLACE FUNCTION forbid_locked_version_modifications() RETURNS TRIGGER
LANGUAGE plpgsql AS
$$
BEGIN
    RAISE 'This config version is locked. Modifications in % are forbidden.', TG_RELID::regclass;
    RETURN NULL;
END
$$;

CREATE OR REPLACE FUNCTION clone_config_trigger() RETURNS TRIGGER
LANGUAGE plpgsql AS
$$BEGIN
    PERFORM "@extschema@".clone_config(NEW.replication_group_id, NEW.version);
    RETURN NEW;
END$$;

CREATE OR REPLACE TRIGGER "00_next_pending_version" BEFORE INSERT ON shard_host_weight
FOR EACH ROW EXECUTE FUNCTION next_pending_version_trigger();

CREATE OR REPLACE TRIGGER forbid_not_pending_version_insert BEFORE INSERT ON shard_host_weight
FOR EACH ROW
WHEN (is_locked(NEW.replication_group_id, NEW.version))
EXECUTE FUNCTION forbid_locked_version_modifications();
CREATE OR REPLACE TRIGGER forbid_not_pending_version_update BEFORE UPDATE ON shard_host_weight
FOR EACH ROW
WHEN (is_locked(OLD.replication_group_id, OLD.version) OR is_locked(NEW.replication_group_id, NEW.version))
EXECUTE FUNCTION forbid_locked_version_modifications();
CREATE OR REPLACE TRIGGER forbid_not_pending_version_delete BEFORE DELETE ON shard_host_weight
FOR EACH ROW
WHEN (is_locked(OLD.replication_group_id, OLD.version))
EXECUTE FUNCTION forbid_locked_version_modifications();

CREATE OR REPLACE TRIGGER clone_config AFTER INSERT ON shard_host_weight
FOR EACH ROW EXECUTE FUNCTION clone_config_trigger();

CREATE OR REPLACE TRIGGER "00_next_pending_version" BEFORE INSERT ON sharded_table
FOR EACH ROW EXECUTE FUNCTION next_pending_version_trigger();

CREATE OR REPLACE TRIGGER forbid_not_pending_version_insert BEFORE INSERT ON sharded_table
FOR EACH ROW
WHEN (is_locked(NEW.replication_group_id, NEW.version))
EXECUTE FUNCTION forbid_locked_version_modifications();
CREATE OR REPLACE TRIGGER forbid_not_pending_version_update BEFORE UPDATE ON sharded_table
FOR EACH ROW
WHEN (is_locked(OLD.replication_group_id, OLD.version) OR is_locked(NEW.replication_group_id, NEW.version))
EXECUTE FUNCTION forbid_locked_version_modifications();
CREATE OR REPLACE TRIGGER forbid_not_pending_version_delete BEFORE DELETE ON sharded_table
FOR EACH ROW
WHEN (is_locked(OLD.replication_group_id, OLD.version))
EXECUTE FUNCTION forbid_locked_version_modifications();

CREATE OR REPLACE TRIGGER clone_config AFTER INSERT ON sharded_table
FOR EACH ROW EXECUTE FUNCTION clone_config_trigger();

CREATE OR REPLACE TRIGGER "00_next_pending_version" BEFORE INSERT ON shard_index_template
FOR EACH ROW EXECUTE FUNCTION next_pending_version_trigger();

CREATE OR REPLACE TRIGGER forbid_not_pending_version_insert BEFORE INSERT ON shard_index_template
FOR EACH ROW
WHEN (is_locked(NEW.replication_group_id, NEW.version))
EXECUTE FUNCTION forbid_locked_version_modifications();
CREATE OR REPLACE TRIGGER forbid_not_pending_version_update BEFORE UPDATE ON shard_index_template
FOR EACH ROW
WHEN (is_locked(OLD.replication_group_id, OLD.version) OR is_locked(NEW.replication_group_id, NEW.version))
EXECUTE FUNCTION forbid_locked_version_modifications();
CREATE OR REPLACE TRIGGER forbid_not_pending_version_delete BEFORE DELETE ON shard_index_template
FOR EACH ROW
WHEN (is_locked(OLD.replication_group_id, OLD.version))
EXECUTE FUNCTION forbid_locked_version_modifications();

CREATE OR REPLACE TRIGGER clone_config AFTER INSERT ON shard_index_template
FOR EACH ROW EXECUTE FUNCTION clone_config_trigger();

CREATE OR REPLACE FUNCTION replication_group_config_snapshot_trigger() RETURNS trigger LANGUAGE plpgsql AS
$$
BEGIN
    PERFORM "@extschema@".replication_group_config_snapshot(NEW.replication_group_id, NEW.version);
    RETURN NEW;
END
$$;
CREATE OR REPLACE TRIGGER replication_group_config_snapshot AFTER INSERT ON replication_group_config_lock
FOR EACH ROW EXECUTE FUNCTION replication_group_config_snapshot_trigger();
