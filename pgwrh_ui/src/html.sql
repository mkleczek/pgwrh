-- SPDX-License-Identifier: AGPL-3.0-or-later
-- Escape text and quoted attribute values separately from SQL quoting.
CREATE FUNCTION escape(value text) RETURNS text
LANGUAGE sql IMMUTABLE SET search_path = pg_catalog AS $$
    SELECT replace(replace(replace(replace(replace(coalesce(value, ''),
        '&', '&amp;'), '<', '&lt;'), '>', '&gt;'), '"', '&quot;'), '''', '&#39;')
$$;

CREATE FUNCTION urlencode(value text) RETURNS text
LANGUAGE sql IMMUTABLE SET search_path = pg_catalog AS $$
    SELECT coalesce(string_agg(CASE
        WHEN b BETWEEN 65 AND 90 OR b BETWEEN 97 AND 122 OR b BETWEEN 48 AND 57 OR b IN (45,46,95,126)
        THEN chr(b) ELSE '%' || lpad(upper(to_hex(b)), 2, '0') END, '' ORDER BY i), '')
    FROM (SELECT i, get_byte(convert_to(value, 'UTF8'), i) b
          FROM generate_series(0, octet_length(value) - 1) i) bytes
$$;

CREATE FUNCTION page_url(group_id text, page text, q text DEFAULT '', page_no int DEFAULT 1)
RETURNS text LANGUAGE sql IMMUTABLE SET search_path = pg_catalog, pgwrh_ui, pg_temp AS $$
    SELECT 'index?page=' || urlencode(page) || CASE WHEN group_id IS NULL THEN ''
        ELSE '&group_id=' || urlencode(group_id) END || '&q=' || urlencode(q) || '&page_no=' || page_no
$$;

CREATE FUNCTION badge(label text, kind text DEFAULT '') RETURNS text
LANGUAGE sql IMMUTABLE SET search_path = pg_catalog, pgwrh_ui, pg_temp AS $$
    SELECT '<span class="badge ' || escape(kind) || '">' || escape(label) || '</span>'
$$;

CREATE FUNCTION notice(message text, kind text DEFAULT '') RETURNS text
LANGUAGE sql IMMUTABLE SET search_path = pg_catalog, pgwrh_ui, pg_temp AS $$
    SELECT '<div class="notice ' || escape(kind) || '" role="status">' || escape(message) || '</div>'
$$;

CREATE FUNCTION pager(group_id text, page text, q text, page_no int, total bigint) RETURNS text
LANGUAGE sql IMMUTABLE SET search_path = pg_catalog, pgwrh_ui, pg_temp AS $$
    SELECT '<div class="pager">' || CASE WHEN page_no > 1 THEN
        '<a href="' || escape(page_url(group_id,page,q,page_no-1)) || '">← Previous</a>' ELSE '' END ||
        '<span>Page ' || page_no || ' · ' || total || ' rows</span>' ||
        CASE WHEN page_no::bigint * 100 < total THEN '<a href="' || escape(page_url(group_id,page,q,page_no+1)) ||
            '">Next →</a>' ELSE '' END || '</div>'
$$;

CREATE FUNCTION controls(group_id text, page text) RETURNS text
LANGUAGE sql STABLE AS $$ SELECT ''::text $$;

CREATE FUNCTION overview_html(group_id text) RETURNS text
LANGUAGE plpgsql STABLE SET search_path = pg_catalog, pgwrh_ui, pg_temp AS $$
DECLARE r record; result text := ''; blockers bigint;
BEGIN
    FOR r IN SELECT * FROM group_state(group_id) ORDER BY replication_group_id LOOP
        SELECT count(*) INTO blockers FROM rollout_blockers(r.replication_group_id);
        result := result || '<article class="panel"><div class="panel-header"><h2><a href="' ||
            escape(page_url(r.replication_group_id,'overview')) || '">' || escape(r.replication_group_id) ||
            '</a></h2>' || badge(replace(r.phase,'_',' '), CASE WHEN r.phase = 'stable' THEN '' ELSE 'warning' END) ||
            '</div><div class="cards"><div class="card"><div class="metric-label">Replicas</div><div class="metric">' ||
            r.replica_count || '</div></div><div class="card"><div class="metric-label">Current shards</div><div class="metric">' ||
            r.shard_count || '</div></div><div class="card"><div class="metric-label">Readiness blockers</div><div class="metric"><a href="' ||
            escape(page_url(r.replication_group_id,'rollout')) || '">' || blockers || '</a></div></div></div>' ||
            '<p class="muted">Current: <code>' || r.current_version || '</code> · Target: <code>' || r.target_version ||
            '</code></p><div class="actions"><a href="' || escape(page_url(r.replication_group_id,'replicas')) ||
            '">Inspect replicas →</a><a href="' || escape(page_url(r.replication_group_id,'placement')) || '">Review placement →</a></div></article>';
    END LOOP;
    RETURN CASE WHEN result = '' THEN '<div class="panel empty">No replica groups are configured on this controller.</div>' ELSE result END;
END
$$;

CREATE FUNCTION replicas_html(group_id text) RETURNS text
LANGUAGE plpgsql STABLE SET search_path = pg_catalog, pgwrh_ui, pg_temp AS $$
DECLARE r record; rows text := '';
BEGIN
    FOR r IN SELECT * FROM replica_state(group_id) ORDER BY availability_zone, host_id LOOP
        rows := rows || '<tr><td><strong>' || escape(r.host_id) || '</strong><br><small>' || escape(r.member_role) ||
            '</small></td><td>' || escape(r.availability_zone) || '</td><td>' ||
            coalesce(escape(r.host_name) || ':' || r.port || '<br><small>' || escape(r.dbname) || '</small>', 'Proxy member') || '</td><td>' ||
            badge(CASE WHEN r.online THEN 'Enabled' WHEN r.online IS FALSE THEN 'Maintenance' ELSE 'Proxy' END,
                CASE WHEN r.online IS FALSE THEN 'warning' ELSE '' END) || '</td><td>' ||
            coalesce(r.current_weight::text,'—') || ' / ' || coalesce(r.pending_weight::text,'—') || '</td><td>' ||
            r.current_copies || ' / ' || r.target_copies || '</td><td>' || r.reported_local_copies || ' local<br><small>' ||
            r.reported_remote_routes || ' remote · ' || r.prepared_remote_routes || ' prepared</small></td><td>' ||
            CASE WHEN r.confirmed_lag_bytes IS NULL THEN 'Unavailable' ELSE escape(pg_size_pretty(r.confirmed_lag_bytes)) END ||
            '<br><small>' || r.active_slots || '/' || r.slot_count || ' active slots · ' || r.sessions || ' sessions</small></td></tr>';
    END LOOP;
    RETURN '<div class="panel"><h2>Replicas by availability zone</h2><div class="table-wrap"><table><thead><tr>' ||
        '<th>Replica / role</th><th>Zone</th><th>Endpoint</th><th>Routing</th><th>Weight<br>current / next</th>' ||
        '<th>Copies<br>current / target</th><th>Reported state</th><th>Confirmed WAL lag</th></tr></thead><tbody>' ||
        rows || '</tbody></table></div>' || CASE WHEN rows = '' THEN '<p class="empty">No replicas registered.</p>' ELSE '' END ||
        '</div>' || notice('Routing is an administrative setting. Session counts and WAL acknowledgements do not certify query readiness. Replica report age is unavailable.');
END
$$;

CREATE FUNCTION placement_html(group_id text, q text, page_no int) RETURNS text
LANGUAGE plpgsql STABLE SET search_path = pg_catalog, pgwrh_ui, pg_temp AS $$
DECLARE r record; rows text := ''; total bigint; added bigint; removed bigint; kept bigint; state record; detail text; hint text;
BEGIN
    SELECT * INTO STRICT state FROM group_state(group_id);
    -- Materialize one evaluation: preview may be expensive and must be coherent
    -- with the summary. Filter and paginate before sending HTML to the browser.
    WITH diff AS MATERIALIZED (SELECT * FROM placement_diff(group_id)),
    filtered AS (SELECT * FROM diff WHERE q = '' OR strpos(schema_name || '.' || table_name || ' ' || availability_zone || ' ' || host_id, q) > 0),
    numbered AS (SELECT *, row_number() OVER (ORDER BY schema_name, table_name, availability_zone, host_id) n FROM filtered)
    SELECT (SELECT count(*) FROM filtered),
        (SELECT count(*) FROM diff WHERE change='add'),
        (SELECT count(*) FROM diff WHERE change='remove'),
        (SELECT count(*) FROM diff WHERE change='keep'),
        coalesce(string_agg('<tr><td><code>' || escape(schema_name || '.' || table_name) || '</code></td><td>' ||
            escape(availability_zone) || '</td><td>' || escape(host_id) || '</td><td>' || badge(change,change) ||
            '</td><td>' || badge(CASE WHEN online THEN 'Enabled' ELSE 'Maintenance' END) || '</td><td>' ||
            CASE WHEN reported_local THEN 'Local copy reported' ELSE 'No local copy reported' END || '</td></tr>', '' ORDER BY n), '')
    INTO total, added, removed, kept, rows FROM numbered WHERE n BETWEEN (page_no::bigint-1)*100+1 AND page_no::bigint*100;
    RETURN '<div class="cards"><div class="card"><div class="metric-label">Add copies</div><div class="metric">' || added ||
        '</div></div><div class="card"><div class="metric-label">Retain copies</div><div class="metric">' || kept ||
        '</div></div><div class="card"><div class="metric-label">Retire copies</div><div class="metric">' || removed ||
        '</div></div></div>' || notice(CASE state.phase WHEN 'draft' THEN
            'Draft preview uses the current source partition tree. No changes have been deployed.' WHEN 'rolling_out' THEN
            'Showing the saved target assignments. Retiring copies remain available until commit and replica handoff.' ELSE
            'Showing saved current assignments.' END) ||
        '<div class="panel"><h2>Shard placement</h2><div class="table-wrap"><table><thead><tr><th>Shard</th><th>Zone</th><th>Replica</th>' ||
        '<th>Change</th><th>Routing</th><th>Reported state</th></tr></thead><tbody>' || rows || '</tbody></table></div>' ||
        CASE WHEN total=0 THEN '<p class="empty">No shard copies match this view.</p>' ELSE '' END || pager(group_id,'placement',q,page_no,total) || '</div>';
EXCEPTION WHEN check_violation OR raise_exception THEN
    GET STACKED DIAGNOSTICS detail = PG_EXCEPTION_DETAIL, hint = PG_EXCEPTION_HINT;
    RETURN notice('Placement cannot be calculated: ' || SQLERRM || ' ' || detail || ' ' || hint, 'error');
END
$$;

CREATE FUNCTION rollout_html(group_id text, q text, page_no int) RETURNS text
LANGUAGE plpgsql STABLE SET search_path = pg_catalog, pgwrh_ui, pg_temp AS $$
DECLARE state record; total bigint; rows text; counts text;
BEGIN
    SELECT * INTO STRICT state FROM group_state(group_id);
    WITH blockers AS MATERIALIZED (SELECT * FROM rollout_blockers(group_id)),
    filtered AS (SELECT * FROM blockers WHERE q = '' OR strpos(schema_name || '.' || table_name || ' ' || host_id || ' ' || availability_zone || ' ' || kind,q)>0),
    numbered AS (SELECT *, row_number() OVER (ORDER BY availability_zone,host_id,schema_name,table_name,kind) n FROM filtered)
    SELECT (SELECT count(*) FROM filtered),
        (SELECT 'Local readiness: ' || count(*) FILTER (WHERE kind <> 'remote') ||
            ' outstanding · Remote readiness: ' || count(*) FILTER (WHERE kind = 'remote') || ' outstanding' FROM blockers),
        coalesce(string_agg('<tr><td><strong>' || escape(host_id) || '</strong><br><small>' || escape(availability_zone) ||
            '</small></td><td><code>' || escape(schema_name || '.' || table_name) || '</code></td><td>' ||
            badge(kind,'warning') || '</td><td>' || escape(detail) || '</td></tr>', '' ORDER BY n),'')
    INTO total,counts,rows FROM numbered WHERE n BETWEEN (page_no::bigint-1)*100+1 AND page_no::bigint*100;
    RETURN '<div class="panel"><div class="panel-header"><h2>Rollout readiness</h2>' || badge(replace(state.phase,'_',' ')) ||
        '</div><p>' || counts || '</p><p class="muted">Current ' || state.current_version || ' → target ' || state.target_version ||
        '</p>' || CASE state.phase WHEN 'draft' THEN notice('The draft has not started. Readiness below describes the current configuration.')
        WHEN 'rolling_back' THEN notice('Rollback is waiting for replicas to acknowledge current routes. Target copies are retained until acknowledgement.','warning')
        ELSE '' END || '<div class="table-wrap"><table><thead><tr><th>Replica / zone</th><th>Shard</th><th>Waiting for</th><th>Explanation</th>' ||
        '</tr></thead><tbody>' || rows || '</tbody></table></div>' || CASE WHEN total=0 THEN
        '<p class="empty">No readiness blockers match this view.</p>' ELSE '' END || pager(group_id,'rollout',q,page_no,total) || '</div>' ||
        notice('Readiness follows the controller’s existing commit checks. Reports have no timestamps; this page cannot certify that a replica is currently reachable.');
END
$$;

CREATE FUNCTION status(group_id text DEFAULT NULL, page text DEFAULT 'overview', q text DEFAULT '', page_no int DEFAULT 1)
RETURNS "text/html" LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = pg_catalog, pgwrh_ui, pg_temp AS $$
DECLARE content text;
BEGIN
    PERFORM check_session();
    IF page NOT IN ('overview','replicas','placement','rollout') OR page IS NULL OR
       page_no IS NULL OR page_no < 1 OR page_no > 1000000 OR q IS NULL OR length(q)>500 THEN
        RAISE sqlstate 'PT400' USING MESSAGE = 'Invalid page or filter';
    END IF;
    IF group_id IS NOT NULL AND NOT EXISTS (SELECT FROM group_state(group_id)) THEN
        RAISE sqlstate 'PT404' USING MESSAGE = 'Replica group not found';
    END IF;
    IF page <> 'overview' AND group_id IS NULL THEN RAISE sqlstate 'PT400' USING MESSAGE = 'Choose a replica group'; END IF;
    content := CASE page WHEN 'overview' THEN overview_html(group_id) WHEN 'replicas' THEN replicas_html(group_id)
        WHEN 'placement' THEN placement_html(group_id,q,page_no) WHEN 'rollout' THEN rollout_html(group_id,q,page_no) END;
    PERFORM set_config('response.headers', '[{"Cache-Control":"no-store"}]', true);
    RETURN '<section id="live-status" data-poll data-commit-ready="' ||
        (page='rollout' AND EXISTS (SELECT FROM group_state(group_id) WHERE phase='rolling_out')
            AND NOT EXISTS (SELECT FROM rollout_blockers(group_id)))::text || '" hx-get="' || escape(replace(page_url(group_id,page,q,page_no),'index?','status?')) ||
        '" hx-trigger="every 10s" hx-target="this" hx-select="unset" hx-swap="outerHTML">' || content ||
        '<div class="status-footer"><span>Controller queried ' || escape(to_char(statement_timestamp(),'YYYY-MM-DD HH24:MI:SS TZ')) ||
        ' · refreshes every 10s</span><span>Replica report age unavailable <span class="loading">· Refreshing…</span></span></div></section>';
END
$$;

CREATE FUNCTION index(group_id text DEFAULT NULL, page text DEFAULT 'overview', q text DEFAULT '', page_no int DEFAULT 1)
RETURNS "text/html" LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = pg_catalog, pgwrh_ui, pg_temp AS $$
DECLARE content text; navigation text := ''; p text; headers jsonb;
BEGIN
    -- Validate all arguments before emitting links or forms.
    content := status(group_id,page,q,page_no);
    FOREACH p IN ARRAY ARRAY['overview','replicas','placement','rollout'] LOOP
        IF group_id IS NOT NULL OR p = 'overview' THEN
            navigation := navigation || '<a href="' || escape(page_url(group_id,p)) || '"' ||
                CASE WHEN p=page THEN ' aria-current="page"' ELSE '' END || '>' || initcap(p) || '</a>';
        END IF;
    END LOOP;
    content := '<main id="workspace" hx-boost="true" hx-target="#workspace" hx-select="#workspace" hx-swap="outerHTML">' ||
        '<div class="heading"><div><p class="eyebrow">Controller console</p><h1>' ||
        CASE WHEN group_id IS NULL THEN 'Replica groups' ELSE escape(group_id) END ||
        '</h1><p class="muted">Placement, replication and configuration readiness.</p></div>' ||
        CASE WHEN group_id IS NOT NULL THEN '<a href="index">All groups</a>' ELSE '' END || '</div><nav aria-label="Group">' ||
        navigation || '</nav>' || CASE WHEN page IN ('placement','rollout') THEN
        '<form class="toolbar" hx-get="index" hx-target="#workspace" hx-select="#workspace" hx-swap="outerHTML" hx-push-url="true">' ||
        '<input type="hidden" name="group_id" value="' || escape(group_id) || '"><input type="hidden" name="page" value="' || page ||
        '"><label>Filter shards, replicas or zones<input name="q" value="' || escape(q) || '" maxlength="500"></label>' ||
        '<button type="submit" class="secondary">Filter</button></form>' ELSE '' END ||
        controls(group_id,page) || '<div id="action-result" aria-live="polite"></div>' || content || '</main>';
    PERFORM set_config('response.headers', '[{"Cache-Control":"no-store"},{"X-Content-Type-Options":"nosniff"},{"Content-Security-Policy":"default-src ''self''; script-src ''self''; style-src ''self''; object-src ''none''; base-uri ''none''; frame-ancestors ''none''"}]', true);
    headers := coalesce(nullif(current_setting('request.headers',true),''),'{}')::jsonb;
    IF headers->>'hx-request' = 'true' AND headers->>'hx-history-restore-request' IS DISTINCT FROM 'true' THEN RETURN content; END IF;
    RETURN '<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">' ||
        '<meta name="htmx-config" content='' {"allowEval":false,"allowScriptTags":false,"includeIndicatorStyles":false,"historyCacheSize":0,"historyRestoreAsHxRequest":false} ''>' ||
        '<title>pgwrh · Controller console</title><link rel="stylesheet" href="style"><script src="htmx" defer></script><script src="script" defer></script>' ||
        '</head><body><header><a href="index">pgwrh<span> / console</span></a><span>POSTGRESQL · CONTROLLER ONLY</span></header>' ||
        '<div id="connection-error" role="alert" hidden></div>' || content ||
        '<footer>pgwrh_ui · Replica reports are read from this controller. No direct connections to replicas.</footer></body></html>';
END
$$;
