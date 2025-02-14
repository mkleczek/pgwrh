-- name: publication-sync
-- requires: core
-- requires: master-helpers

CREATE PUBLICATION pgwrh_controller_publication;
SELECT add_ext_dependency('pg_publication', (SELECT oid FROM pg_publication WHERE pubname = 'pgwrh_controller_publication'));

CREATE OR REPLACE FUNCTION sync_publications() RETURNS void
SET SEARCH_PATH FROM CURRENT
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
                    WHERE
                            (schema_name, table_name) = (nspname, relname)
                        AND
                            version IN (current_version, target_version)
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
        SELECT format('DROP PUBLICATION %I CASCADE',
                        pubname) stmt
        FROM
            pg_publication p
        WHERE
                is_dependent_object('pg_publication', oid)
            AND
                pubname <> 'pgwrh_controller_publication'
            AND
                NOT EXISTS (SELECT 1 FROM
                    shard s
                        JOIN replication_group USING (replication_group_id)
                    WHERE
                        version IN (current_version, target_version)
                        AND pubname(schema_name, table_name) = p.pubname
                )
    LOOP
        EXECUTE r.stmt;
    END LOOP;
    RETURN;
END
$$;

CREATE OR REPLACE FUNCTION sync_publications_trigger() RETURNS TRIGGER
SET SEARCH_PATH FROM CURRENT
LANGUAGE plpgsql AS
$$BEGIN
    PERFORM sync_publications();
    RETURN NULL;
END$$;

CREATE OR REPLACE TRIGGER sync_publications AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON replication_group
FOR EACH STATEMENT EXECUTE FUNCTION sync_publications_trigger();
