-- name: publication-sync
-- requires: core
-- requires: master-helpers

CREATE PUBLICATION pgwrh_controller_ping FOR TABLE ping WITH (PUBLISH = 'insert');
SELECT add_ext_dependency('pg_publication', (SELECT oid FROM pg_publication WHERE pubname = 'pgwrh_controller_ping'));

CREATE OR REPLACE FUNCTION sync_publications() RETURNS void
SET search_path = pg_catalog, "@extschema@", pg_temp
LANGUAGE plpgsql AS
$$DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT format('CREATE PUBLICATION %I FOR TABLE %s WITH ( publish = %L )',
                        pubname,
                        c.oid::regclass,
                        'insert,update,delete') stmt,
                pubname
        FROM
            pg_class c
                JOIN pg_namespace n ON c.relnamespace = n.oid,
                pubname(nspname, relname) AS pubname
        WHERE
                EXISTS (SELECT 1 FROM
                    shard
                        JOIN replication_group USING (replication_group_id)
                        JOIN replication_group_config_lock USING (replication_group_id, version)
                    WHERE
                            (schema_name, table_name) = (nspname, relname)
                        AND
                            (version IN (current_version, target_version) OR rollback_unlock IS NOT NULL)
                )
            AND
                NOT EXISTS (SELECT 1 FROM
                    pg_publication_rel
                    WHERE
                            prrelid = c.oid
                        AND
                            is_dependent_object('pg_publication', prpubid)
                )
    LOOP
        EXECUTE r.stmt;
        PERFORM add_ext_dependency('pg_publication', (SELECT oid FROM pg_publication WHERE pubname = r.pubname::text));
    END LOOP;
    FOR r IN
        SELECT format('DROP PUBLICATION IF EXISTS %I CASCADE',
                        pubname) stmt
        FROM
            pg_publication p
        WHERE
                is_dependent_object('pg_publication', oid)
            AND
                pubname NOT IN ('pgwrh_controller_ping')
            -- Keep publishing until subscribers acknowledge removal, including
            -- copies still in initial sync. An immediate rerollout can otherwise
            -- reuse a ready pg_subscription_rel row that missed intervening WAL.
            AND NOT EXISTS (
                SELECT 1 FROM replication_group_member m
                WHERE m.subscribed_publications ? p.pubname::text
            )
            AND
                NOT EXISTS (SELECT 1 FROM
                    shard s
                        JOIN replication_group USING (replication_group_id)
                        JOIN replication_group_config_lock USING (replication_group_id, version)
                    WHERE
                        (version IN (current_version, target_version) OR rollback_unlock IS NOT NULL)
                        AND pubname(schema_name, table_name) = p.pubname
                )
    LOOP
        EXECUTE r.stmt;
    END LOOP;
    RETURN;
END
$$;

CREATE OR REPLACE FUNCTION sync_publications_trigger() RETURNS TRIGGER
SECURITY DEFINER
SET search_path = pg_catalog
LANGUAGE plpgsql AS
$$BEGIN
    PERFORM "@extschema@".sync_publications();
    RETURN NULL;
END$$;

CREATE OR REPLACE TRIGGER sync_publications AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON replication_group
FOR EACH STATEMENT EXECUTE FUNCTION sync_publications_trigger();

CREATE TRIGGER sync_publications_on_release AFTER UPDATE OF subscribed_publications ON replication_group_member
-- Recheck even unchanged reports: concurrent releases can each have observed
-- the other's preceding report, conservatively retaining the publication.
FOR EACH ROW
EXECUTE FUNCTION sync_publications_trigger();
