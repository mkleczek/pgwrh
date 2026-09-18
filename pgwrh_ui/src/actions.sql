-- SPDX-License-Identifier: AGPL-3.0-or-later
-- Stateless revision checks avoid introducing controller columns or UI tables.
-- Replica reports deliberately do not invalidate an operator's open form.
CREATE FUNCTION revision(group_id text) RETURNS text
LANGUAGE sql STABLE SET search_path = pg_catalog, pgwrh_ui, pg_temp AS $$
    SELECT md5(jsonb_build_array(
        (SELECT to_jsonb(g) FROM pgwrh.replication_group g WHERE g.replication_group_id=group_id),
        (SELECT jsonb_agg(to_jsonb(c) ORDER BY version) FROM pgwrh.replication_group_config c WHERE c.replication_group_id=group_id),
        (SELECT jsonb_agg(to_jsonb(l) ORDER BY version) FROM pgwrh.replication_group_config_lock l WHERE l.replication_group_id=group_id),
        (SELECT jsonb_agg(to_jsonb(h) ORDER BY availability_zone,host_id) FROM pgwrh.shard_host h WHERE h.replication_group_id=group_id),
        (SELECT jsonb_agg(jsonb_build_array(m.availability_zone,m.host_id,m.member_role,m.same_zone_multiplier)
                         ORDER BY availability_zone,host_id) FROM pgwrh.replication_group_member m WHERE m.replication_group_id=group_id),
        (SELECT jsonb_agg(to_jsonb(w) ORDER BY version,availability_zone,host_id) FROM pgwrh.shard_host_weight w WHERE w.replication_group_id=group_id),
        (SELECT jsonb_agg(to_jsonb(t) ORDER BY version,sharded_table_schema,sharded_table_name) FROM pgwrh.sharded_table t WHERE t.replication_group_id=group_id),
        (SELECT jsonb_agg(to_jsonb(a) ORDER BY version,sharded_table_schema,sharded_table_name,availability_zone) FROM pgwrh.sharded_table_az_affinity a WHERE a.replication_group_id=group_id),
        (SELECT jsonb_agg(to_jsonb(i) ORDER BY version,index_template_schema,index_template_table_name,index_template_name) FROM pgwrh.shard_index_template i WHERE i.replication_group_id=group_id),
        (SELECT jsonb_agg(to_jsonb(tree) ORDER BY oid,parentrelid) FROM (
            SELECT DISTINCT c.oid,c.relname,c.relnamespace,c.relkind,c.relpartbound::text,
                p.parentrelid,pg_get_partkeydef(c.oid) AS partkey
            FROM pgwrh.sharded_table t
            JOIN pg_namespace n ON n.nspname=t.sharded_table_schema
            JOIN pg_class root ON root.relnamespace=n.oid AND root.relname=t.sharded_table_name
            CROSS JOIN LATERAL pg_partition_tree(root.oid) p JOIN pg_class c ON c.oid=p.relid
            WHERE t.replication_group_id=group_id
        ) tree)
    )::text)
$$;

CREATE FUNCTION hidden(name text, value text) RETURNS text
LANGUAGE sql IMMUTABLE SET search_path = pg_catalog, pgwrh_ui, pg_temp AS $$
    SELECT '<input type="hidden" name="' || escape(name) || '" value="' || escape(value) || '">'
$$;

CREATE FUNCTION form_start(group_id text, operation text, expected text, replica_id text DEFAULT NULL, zone text DEFAULT NULL)
RETURNS text LANGUAGE sql IMMUTABLE SET search_path = pg_catalog, pgwrh_ui, pg_temp AS $$
    SELECT '<form hx-post="mutate" hx-target="#action-result" hx-select="unset" hx-swap="innerHTML" hx-disabled-elt="find button">' ||
        hidden('group_id',group_id) || hidden('operation',operation) || hidden('expected',expected) ||
        CASE WHEN replica_id IS NULL THEN '' ELSE hidden('replica_id',replica_id) || hidden('availability_zone',zone) END
$$;

CREATE FUNCTION mutate(
    group_id text, operation text, expected text,
    replica_id text DEFAULT '', availability_zone text DEFAULT '',
    host_name text DEFAULT '', port int DEFAULT 5432, member_role text DEFAULT '',
    weight int DEFAULT 100, online boolean DEFAULT true, confirm boolean DEFAULT false,
    dbname text DEFAULT current_database()
) RETURNS "text/html"
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, pgwrh_ui, pg_temp SET lock_timeout = '3s' AS $$
DECLARE
    state record; role_id regrole; draft pgwrh.config_version;
    headers jsonb; method text; result text; message text; hint text;
    destination text := 'replicas'; code text;
BEGIN
    PERFORM check_session();
    method := nullif(current_setting('request.method',true),'');
    headers := coalesce(nullif(current_setting('request.headers',true),''),'{}')::jsonb;
    -- Direct SQL uses database authentication. HTTP mutations must come from
    -- the console's origin, using the Host preserved by a reverse proxy.
    IF method IS NOT NULL AND (
        method <> 'POST' OR headers->>'x-pgwrh-ui' IS DISTINCT FROM '1'
        OR headers->>'sec-fetch-site' = 'cross-site'
        OR headers->>'origin' IS NULL OR headers->>'host' IS NULL
        OR regexp_replace(headers->>'origin', '^https?://', '') <> headers->>'host'
        OR headers->>'origin' !~ '^https?://'
    ) THEN RAISE sqlstate 'PT403' USING MESSAGE = 'Management requests must originate from this console'; END IF;
    IF operation IS NULL OR operation NOT IN ('add','weight','routing','exclude','start','commit','rollback') THEN
        RAISE sqlstate 'PT400' USING MESSAGE = 'Unknown operation';
    END IF;
    -- Serialize UI mutations, then re-read the revision under the row lock.
    PERFORM 1 FROM pgwrh.replication_group g WHERE g.replication_group_id=group_id FOR UPDATE;
    IF NOT FOUND THEN RAISE sqlstate 'PT404' USING MESSAGE = 'Replica group not found'; END IF;
    IF expected IS NULL OR expected IS DISTINCT FROM revision(group_id) THEN
        RAISE sqlstate 'PT409' USING MESSAGE = 'Configuration changed since this form was loaded. Refresh and review it again.';
    END IF;
    SELECT * INTO STRICT state FROM group_state(group_id);
    IF operation IN ('add','weight','exclude') AND state.phase NOT IN ('stable','draft') THEN
        RAISE sqlstate 'PT409' USING MESSAGE = 'Wait for the rollout or rollback to finish before editing replica placement.';
    END IF;
    IF operation IN ('weight','routing','exclude') AND NOT EXISTS (
        SELECT FROM pgwrh.shard_host h WHERE h.replication_group_id=group_id
            AND h.host_id=replica_id AND h.availability_zone=mutate.availability_zone
    ) THEN RAISE sqlstate 'PT404' USING MESSAGE = 'Shard host not found in this group and zone'; END IF;
    CASE operation
    WHEN 'add' THEN
        IF replica_id IS NULL OR btrim(replica_id)='' OR length(replica_id)>200
            OR availability_zone IS NULL OR btrim(availability_zone)='' OR length(availability_zone)>200
            OR host_name IS NULL OR btrim(host_name)='' OR length(host_name)>253
            OR dbname IS NULL OR dbname=''
            OR port IS NULL OR port NOT BETWEEN 1 AND 65535 OR weight IS NULL OR weight<=0 THEN
            RAISE sqlstate 'PT422' USING MESSAGE = 'Provide a replica ID, zone, hostname, database, port (1–65535), and positive weight.';
        END IF;
        SELECT oid::regrole INTO role_id FROM pg_roles
        WHERE rolname=member_role AND rolcanlogin AND rolreplication AND NOT rolsuper;
        IF role_id IS NULL THEN RAISE sqlstate 'PT422' USING MESSAGE = 'Choose an existing non-superuser role with LOGIN and REPLICATION. Create it and grant shard access separately.'; END IF;
        PERFORM pgwrh.add_replica(group_id,replica_id,host_name,port,role_id,availability_zone,weight,dbname);
        message := 'Replica registered in the pending configuration. Configure its controller connection separately, then review placement and start rollout.';
    WHEN 'weight' THEN
        IF weight IS NULL OR weight<=0 THEN RAISE sqlstate 'PT422' USING MESSAGE = 'Weight must be positive. Use exclusion to remove a host from the next placement.'; END IF;
        PERFORM pgwrh.set_replica_weight(group_id,availability_zone,replica_id,weight);
        message := 'Pending weight saved. Review placement before starting rollout.';
    WHEN 'exclude' THEN
        draft := pgwrh.next_pending_version(group_id);
        INSERT INTO pgwrh.replication_group_config_clone(replication_group_id,source_version,target_version)
        VALUES (group_id,state.current_version,draft) ON CONFLICT DO NOTHING;
        DELETE FROM pgwrh.shard_host_weight w WHERE w.replication_group_id=group_id
            AND w.version=draft AND w.availability_zone=mutate.availability_zone AND w.host_id=replica_id;
        message := 'Host excluded from pending placement. Existing copies and routing remain until rollout. Set a positive weight to include it again.';
    WHEN 'routing' THEN
        IF online IS NULL THEN RAISE sqlstate 'PT422' USING MESSAGE = 'Choose whether routing is enabled'; END IF;
        UPDATE pgwrh.shard_host h SET online=mutate.online WHERE h.replication_group_id=group_id
            AND h.availability_zone=mutate.availability_zone AND h.host_id=replica_id;
        message := CASE WHEN online THEN 'Routing enabled. Replicas will pick up this setting during synchronization.'
                       ELSE 'Maintenance requested. Replicas will stop selecting this host during synchronization; existing queries may finish. Assigned shards continue replicating.' END;
    WHEN 'start' THEN
        IF state.phase <> 'draft' THEN RAISE sqlstate 'PT409' USING MESSAGE = 'There is no editable draft to roll out'; END IF;
        PERFORM pgwrh.start_rollout(group_id);
        message := 'Rollout started. Follow the readiness checks below.';
        destination := 'rollout';
    WHEN 'commit' THEN
        IF state.phase <> 'rolling_out' THEN RAISE sqlstate 'PT409' USING MESSAGE = 'No rollout is in progress'; END IF;
        IF confirm IS DISTINCT FROM true THEN RAISE sqlstate 'PT422' USING MESSAGE = 'Confirm that committing permits replicas to retire old copies'; END IF;
        PERFORM pgwrh.commit_rollout(group_id);
        message := 'Rollout committed. Replicas may now complete handoff and retire unneeded copies.';
        destination := 'rollout';
    WHEN 'rollback' THEN
        IF state.phase <> 'rolling_out' THEN RAISE sqlstate 'PT409' USING MESSAGE = 'No rollout is in progress'; END IF;
        IF confirm IS DISTINCT FROM true THEN RAISE sqlstate 'PT422' USING MESSAGE = 'Confirm rollback to the current configuration'; END IF;
        PERFORM pgwrh.rollback_rollout(group_id);
        message := 'Rollback requested. Waiting for replicas to acknowledge current routes.';
        destination := 'rollout';
    END CASE;
    result := replace(index(group_id,destination),'<div id="action-result" aria-live="polite"></div>',
        '<div id="action-result" aria-live="polite">' || notice(message) || '</div>');
    PERFORM set_config('response.headers', jsonb_build_array(
        jsonb_build_object('Cache-Control','no-store'), jsonb_build_object('HX-Retarget','#workspace'),
        jsonb_build_object('HX-Reselect','#workspace'), jsonb_build_object('HX-Reswap','outerHTML'),
        jsonb_build_object('HX-Push-Url',page_url(group_id,destination)))::text,true);
    RETURN result;
EXCEPTION
    WHEN sqlstate 'PT400' OR sqlstate 'PT409' OR sqlstate 'PT422'
         OR integrity_constraint_violation OR invalid_parameter_value OR raise_exception OR lock_not_available THEN
        GET STACKED DIAGNOSTICS hint = PG_EXCEPTION_HINT;
        code := CASE WHEN SQLSTATE='PT400' THEN '400' WHEN SQLSTATE IN ('PT409','55P03') THEN '409' ELSE '422' END;
        PERFORM set_config('response.status',code,true);
        PERFORM set_config('response.headers','[{"Cache-Control":"no-store"}]',true);
        -- The exception subtransaction undoes the entire mutation first.
        RETURN notice(SQLERRM || CASE WHEN SQLSTATE='23505' THEN ' A replica, endpoint, or role is already registered.' ELSE '' END ||
            ' ' || coalesce(hint,''),'error');
END
$$;

CREATE OR REPLACE FUNCTION controls(group_id text, page text) RETURNS text
LANGUAGE plpgsql STABLE SET search_path = pg_catalog, pgwrh_ui, pg_temp AS $$
DECLARE
    actor name := coalesce(nullif(current_setting('role',true),'none'),session_user::text)::name;
    state record; r record; token text; result text := ''; options text := ''; missing bigint;
BEGIN
    IF group_id IS NULL OR NOT has_function_privilege(actor,
        'pgwrh_ui.mutate(text,text,text,text,text,text,integer,text,integer,boolean,boolean,text)','EXECUTE') THEN RETURN ''; END IF;
    SELECT * INTO STRICT state FROM group_state(group_id);
    token := revision(group_id);
    IF page='replicas' THEN
        IF state.phase IN ('stable','draft') THEN
            SELECT coalesce(string_agg('<option value="' || escape(rolname) || '">' || escape(rolname) || '</option>','' ORDER BY rolname),'')
            INTO options FROM pg_roles WHERE rolcanlogin AND rolreplication AND NOT rolsuper
                AND NOT EXISTS (SELECT FROM pgwrh.replication_group_member m WHERE m.member_role=rolname);
            result := '<details class="panel"><summary>Add replica</summary><p class="muted">Register an existing replica role. Replica installation and its controller connection are configured separately.</p>' ||
                form_start(group_id,'add',token) || '<div class="form-grid">' ||
                '<label>Replica ID<input name="replica_id" required maxlength="200"></label>' ||
                '<label>Availability zone<input name="availability_zone" value="default" required maxlength="200"></label>' ||
                '<label>Hostname<input name="host_name" required maxlength="253"></label>' ||
                '<label>Port<input name="port" type="number" value="5432" min="1" max="65535" required></label>' ||
                '<label>Database<input name="dbname" value="' || escape(current_database()) || '" required></label>' ||
                '<label>Replication role<select name="member_role" required><option value="">Choose a role</option>' || options || '</select></label>' ||
                '<label>Weight<input name="weight" type="number" value="100" min="1" max="2147483647" required></label>' ||
                '</div><button type="submit">Add to pending configuration</button></form></details>';
        ELSE result := notice('Placement editing is locked until rollout or rollback finishes. Routing controls remain available.'); END IF;
        result := result || '<details class="panel"><summary>Manage existing replicas</summary>';
        FOR r IN SELECT * FROM replica_state(group_id) WHERE host_name IS NOT NULL ORDER BY availability_zone,host_id LOOP
            result := result || '<div class="panel"><h3>' || escape(r.host_id) || ' <small>' || escape(r.availability_zone) || '</small></h3><div class="split">';
            IF state.phase IN ('stable','draft') THEN
                result := result || '<div>' || form_start(group_id,'weight',token,r.host_id,r.availability_zone) ||
                    '<label>Pending weight<input name="weight" type="number" min="1" max="2147483647" value="' || coalesce(r.pending_weight,r.current_weight,100) || '" required></label>' ||
                    '<p><small>Saving includes this host in the next placement.</small></p><button type="submit" class="secondary">Save weight</button></form><br>' ||
                    form_start(group_id,'exclude',token,r.host_id,r.availability_zone) ||
                    '<button type="submit" class="secondary">Exclude from next placement</button></form></div>';
            END IF;
            result := result || '<div>' || form_start(group_id,'routing',token,r.host_id,r.availability_zone) ||
                hidden('online',(NOT r.online)::text) || '<p>Routing: ' || badge(CASE WHEN r.online THEN 'Enabled' ELSE 'Maintenance' END) ||
                '</p><p class="muted">Applies during replica synchronization. Assigned shards keep replicating.</p>' ||
                '<button type="submit" class="secondary">' || CASE WHEN r.online THEN 'Request maintenance' ELSE 'Enable routing' END || '</button></form></div></div></div>';
        END LOOP;
        RETURN result || '</details>';
    ELSIF page='placement' AND state.phase='draft' THEN
        RETURN '<div class="panel">' || form_start(group_id,'start',token) ||
            '<p>Review the proposed placement below. Starting locks this configuration and begins replica preparation.</p>' ||
            '<button type="submit">Start rollout</button></form></div>';
    ELSIF page='rollout' AND state.phase='rolling_out' THEN
        SELECT count(*) INTO missing FROM rollout_blockers(group_id);
        RETURN '<div class="panel split"><div>' || form_start(group_id,'commit',token) ||
            '<label><input class="check" type="checkbox" name="confirm" value="true" required> Allow replicas to retire old copies after handoff.</label><br>' ||
            '<button id="commit-rollout" type="submit"' || CASE WHEN missing>0 THEN ' disabled' ELSE '' END || '>Commit rollout</button>' ||
            '<p><small>Readiness is rechecked by the controller when you commit.</small></p></form></div><div>' ||
            form_start(group_id,'rollback',token) || '<label><input class="check" type="checkbox" name="confirm" value="true" required> Restore the current configuration.</label><br>' ||
            '<button type="submit" class="danger">Roll back rollout</button></form></div></div>';
    ELSIF page='rollout' AND state.phase='draft' THEN
        RETURN '<p><a href="' || escape(page_url(group_id,'placement')) || '">Review placement to start rollout →</a></p>';
    END IF;
    RETURN '';
END
$$;
