-- Managed authentication is independent of topology configuration versions.
-- Only source_credential contains reusable passwords. Each PostgreSQL target
-- receives a separately salted verifier, cached for the generation's lifetime.
CREATE FUNCTION provision_credentials(group_id text) RETURNS void
LANGUAGE plpgsql SET search_path FROM CURRENT AS
$$
BEGIN
    PERFORM pg_advisory_xact_lock(1735289201, hashtext(group_id));
    INSERT INTO credential_generation (replication_group_id, state)
    SELECT group_id, 'active'
    WHERE NOT EXISTS (SELECT 1 FROM credential_generation WHERE replication_group_id = group_id);

    INSERT INTO source_credential (replication_group_id, generation, source_role, username)
    SELECT g.replication_group_id, g.generation, m.member_role,
           'pgwrh_' || md5(g.generation::text || ':' || m.member_role)
    FROM credential_generation g JOIN replication_group_member m USING (replication_group_id)
    WHERE g.replication_group_id = group_id
    ON CONFLICT DO NOTHING;

    INSERT INTO target_credential_verifier
        (replication_group_id, generation, source_role, host_name, port, verifier)
    SELECT c.replication_group_id, c.generation, c.source_role, h.host_name, h.port,
           "@extschema:pgwrh_fdw@".pgwrh_fdw_scram_verifier(c.password)
    FROM source_credential c
        JOIN (SELECT DISTINCT replication_group_id, host_name, port FROM shard_host) h
            USING (replication_group_id)
    WHERE c.replication_group_id = group_id AND NOT EXISTS (
        SELECT 1 FROM target_credential_verifier v
        WHERE (v.replication_group_id, v.generation, v.source_role, v.host_name, v.port)
            = (c.replication_group_id, c.generation, c.source_role, h.host_name, h.port)
    );
END
$$;
REVOKE ALL ON FUNCTION provision_credentials(text) FROM PUBLIC;

CREATE FUNCTION provision_credentials_trigger() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS
$$
BEGIN
    PERFORM "@extschema@".provision_credentials(NEW.replication_group_id);
    RETURN NULL;
END
$$;
CREATE TRIGGER provision_credentials AFTER INSERT ON replication_group
    FOR EACH ROW EXECUTE FUNCTION provision_credentials_trigger();
CREATE TRIGGER provision_credentials AFTER INSERT OR UPDATE OF member_role, replication_group_id
    ON replication_group_member FOR EACH ROW EXECUTE FUNCTION provision_credentials_trigger();
CREATE TRIGGER provision_credentials AFTER INSERT OR UPDATE OF host_name, port, replication_group_id
    ON shard_host FOR EACH ROW EXECUTE FUNCTION provision_credentials_trigger();

CREATE VIEW replica_credentials AS
SELECT c.replication_group_id, c.generation, g.state, c.source_role,
       m.member_role, c.username, c.password, v.verifier
FROM source_credential c JOIN credential_generation g USING (replication_group_id, generation)
    JOIN target_credential_verifier v USING (replication_group_id, generation, source_role)
    JOIN shard_host h USING (replication_group_id, host_name, port)
    JOIN replication_group_member m USING (replication_group_id, availability_zone, host_id)
WHERE c.source_role <> m.member_role;

-- These views contain no secrets and explain which normal sync reports are
-- outstanding. No timeout evicts an offline reader or discards its old login.
CREATE VIEW missing_credential_installation AS
SELECT c.replication_group_id, c.generation, c.source_role, c.member_role, c.username
FROM replica_credentials c JOIN replication_group_member m USING (replication_group_id, member_role)
WHERE NOT EXISTS (SELECT 1 FROM json_array_elements_text(m.users) u(username)
                  WHERE u.username = c.username);

CREATE VIEW credential_rotation AS
SELECT g.replication_group_id, g.generation, g.state, g.created_at,
       (SELECT count(*) FROM missing_credential_installation i
        WHERE (i.replication_group_id, i.generation) = (g.replication_group_id, g.generation))
            AS missing_installations,
       (SELECT count(*) FROM replication_group_member m
        WHERE m.replication_group_id = g.replication_group_id
            AND m.credential_generation IS DISTINCT FROM g.generation) AS unconfirmed_sources
FROM credential_generation g;

CREATE FUNCTION advance_credential_rotation(group_id text) RETURNS void
LANGUAGE plpgsql SET search_path FROM CURRENT AS
$$
DECLARE
    pending uuid;
    active uuid;
BEGIN
    -- Reports already hold a member-row lock. Never wait for a concurrent
    -- credential operation here; the next periodic report retries progress.
    IF NOT pg_try_advisory_xact_lock(1735289201, hashtext(group_id)) THEN RETURN; END IF;
    SELECT generation INTO pending FROM credential_generation
    WHERE replication_group_id = group_id AND state = 'preparing';
    IF pending IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM missing_credential_installation
        WHERE replication_group_id = group_id AND generation = pending
    ) THEN
        UPDATE credential_generation SET state = 'retiring'
        WHERE replication_group_id = group_id AND state = 'active';
        UPDATE credential_generation SET state = 'active'
        WHERE replication_group_id = group_id AND generation = pending;
    END IF;

    SELECT generation INTO active FROM credential_generation
    WHERE replication_group_id = group_id AND state = 'active';
    IF NOT EXISTS (SELECT 1 FROM replication_group_member
                   WHERE replication_group_id = group_id
                     AND credential_generation IS DISTINCT FROM active) THEN
        DELETE FROM credential_generation WHERE replication_group_id = group_id AND state = 'retiring';
    END IF;
END
$$;
REVOKE ALL ON FUNCTION advance_credential_rotation(text) FROM PUBLIC;

CREATE FUNCTION advance_credential_rotation_trigger() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS
$$
BEGIN
    PERFORM "@extschema@".advance_credential_rotation(NEW.replication_group_id);
    RETURN NULL;
END
$$;
CREATE TRIGGER advance_credential_rotation AFTER UPDATE OF users, credential_generation
    ON replication_group_member FOR EACH ROW EXECUTE FUNCTION advance_credential_rotation_trigger();

CREATE FUNCTION rotate_credentials(group_id text) RETURNS uuid
LANGUAGE plpgsql SET search_path FROM CURRENT AS
$$
DECLARE
    result uuid;
BEGIN
    PERFORM pg_advisory_xact_lock(1735289201, hashtext(group_id));
    IF NOT EXISTS (SELECT 1 FROM replication_group WHERE replication_group_id = group_id) THEN
        RAISE EXCEPTION 'Unknown replication group: %', group_id;
    END IF;
    IF EXISTS (SELECT 1 FROM credential_generation
               WHERE replication_group_id = group_id AND state <> 'active') THEN
        RAISE EXCEPTION 'Credential rotation is already in progress for group %', group_id
            USING HINT = 'Check credential_rotation and missing_credential_installation.';
    END IF;
    INSERT INTO credential_generation (replication_group_id, state)
    VALUES (group_id, 'preparing') RETURNING generation INTO result;
    PERFORM provision_credentials(group_id);
    PERFORM advance_credential_rotation(group_id);
    RETURN result;
END
$$;
COMMENT ON FUNCTION rotate_credentials(text) IS
'Starts an independent group-wide credential rotation. Destinations install new
verifiers before sources switch; old credentials retire after every source reports
the new generation on its actual routes. Topology commits and rollbacks do not
change credentials. An offline member delays completion.';
