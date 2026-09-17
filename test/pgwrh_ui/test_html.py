from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def scalar(node, sql):
    return node.execute(sql)[0][0]


def test_all_pages_and_assets_render(configured):
    node = configured
    for page in ('overview', 'replicas', 'placement', 'rollout'):
        html = scalar(node, f"SELECT pgwrh_ui.index('g1','{page}')")
        assert html.startswith('<!doctype html>')
        assert 'id="workspace"' in html
        assert 'Replica report age unavailable' in html
        assert 'hx-trigger="every 10s"' in html
    assert 'Draft preview' in scalar(node, "SELECT pgwrh_ui.index('g1','placement')")
    assert 'r1.invalid' in scalar(node, "SELECT pgwrh_ui.index('g1','replicas')")
    assert len(scalar(node, 'SELECT pgwrh_ui.htmx()')) > 50000
    assert b':root' in scalar(node, 'SELECT pgwrh_ui.style()')


def test_fragments_and_history_restore(configured):
    with configured.connect() as conn:
        conn.execute('BEGIN READ ONLY')
        conn.execute("SELECT set_config('request.headers', '{\"hx-request\":\"true\"}', true)")
        assert conn.execute("SELECT pgwrh_ui.index('g1')")[0][0].startswith('<main ')
        conn.execute("SELECT set_config('request.headers', '{\"hx-request\":\"true\",\"hx-history-restore-request\":\"true\"}', true)")
        assert conn.execute("SELECT pgwrh_ui.index('g1')")[0][0].startswith('<!doctype html>')


def test_identifiers_are_escaped_and_urls_are_encoded(controller):
    with controller.connect() as conn:
        conn.execute("SELECT pgwrh.create_replica_cluster(%s)", '<img src=x onerror=alert(1)>&"/é')
        html = conn.execute('SELECT pgwrh_ui.index()')[0][0]
    assert '<img src=x' not in html
    assert '&lt;img src=x' in html
    assert '%3Cimg%20src%3Dx' in html
    assert '%C3%A9' in html


def test_viewer_can_render_but_not_read_controller_tables(configured):
    configured.psql(filename=str(ROOT / 'pgwrh_ui/readonly.sql'))
    with configured.connect() as conn:
        conn.execute('SET ROLE pgwrh_ui_viewer')
        assert 'g1' in conn.execute('SELECT pgwrh_ui.index()')[0][0]
        assert conn.execute("SELECT has_table_privilege(current_user, 'pgwrh.replication_group', 'SELECT')") == [(False,)]
        assert conn.execute("SELECT has_function_privilege(current_user, 'pgwrh_ui.group_state(text)', 'EXECUTE')") == [(False,)]


def test_invalid_preview_is_an_explained_page(configured):
    configured.execute("UPDATE pgwrh.replication_group_config SET min_replica_count_after_az_failure = 2 WHERE version='FLOP'")
    html = scalar(configured, "SELECT pgwrh_ui.index('g1','placement')")
    assert 'Placement cannot be calculated' in html
    assert 'Cannot place' in html
