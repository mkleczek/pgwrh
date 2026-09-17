"""Real PostgREST contract tests, including media negotiation and role grants.

Set POSTGREST_BIN or install postgrest on PATH. These tests start only disposable
local servers and do not need a replica or external service.
"""
from contextlib import contextmanager
import os
from pathlib import Path
import shutil
import socket
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request

import pytest

ROOT = Path(__file__).resolve().parents[2]


@contextmanager
def serve(node, tmp_path, role='pgwrh_ui_viewer'):
    binary = os.environ.get('POSTGREST_BIN') or shutil.which('postgrest')
    if not binary:
        pytest.skip('Set POSTGREST_BIN to run the PostgREST integration tests')
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        port = listener.getsockname()[1]
    node.execute(f'CREATE ROLE ui_authenticator NOINHERIT LOGIN; GRANT {role} TO ui_authenticator')
    env = os.environ | {
        'PGRST_DB_URI': f'postgresql://ui_authenticator@127.0.0.1:{node.port}/postgres',
        'PGRST_DB_SCHEMAS': 'pgwrh_ui',
        'PGRST_DB_ANON_ROLE': role,
        'PGRST_SERVER_HOST': '127.0.0.1',
        'PGRST_SERVER_PORT': str(port),
        'PGRST_DB_POOL': '3',
        'PGRST_LOG_LEVEL': 'warn',
    }
    url = f'http://127.0.0.1:{port}/rpc/'
    with (tmp_path / 'postgrest.log').open('w+') as log:
        process = subprocess.Popen([binary], env=env, stdout=log, stderr=log)
        try:
            for _ in range(100):
                try:
                    with urllib.request.urlopen(url + 'index', timeout=1) as response:
                        if response.status == 200:
                            break
                except (OSError, urllib.error.HTTPError):
                    time.sleep(.1)
            else:
                log.seek(0)
                raise AssertionError('PostgREST did not become ready:\n' + log.read())
            yield url
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def request(url, fields=None, headers=None):
    payload = urllib.parse.urlencode(fields).encode() if fields is not None else None
    req = urllib.request.Request(url, data=payload, headers=headers or {})
    try:
        response = urllib.request.urlopen(req, timeout=10)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        return response.status, dict(response.headers), response.read().decode()


def test_real_postgrest_html_assets_and_readonly_contract(configured, tmp_path):
    configured.psql(filename=str(ROOT / 'pgwrh_ui/readonly.sql'))
    with serve(configured, tmp_path) as url:
        status, headers, html = request(url + 'index?group_id=g1', headers={'Accept': 'text/html'})
        assert status == 200, html
        assert headers['Content-Type'].startswith('text/html')
        assert headers['Cache-Control'] == 'no-store'
        assert html.startswith('<!doctype html>')
        status, _, html = request(url + 'index?group_id=g1&page=replicas', headers={'HX-Request': 'true', 'Accept': 'text/html'})
        assert status == 200 and html.startswith('<main ')
        status, _, html = request(url + 'status?group_id=g1&page=placement', headers={'Accept': 'text/html'})
        assert status == 200 and html.startswith('<section ')
        for endpoint, media in [('style','text/css'), ('htmx','application/javascript'), ('script','application/javascript')]:
            status, headers, content = request(url + endpoint, headers={'Accept': media})
            assert status == 200, content
            assert headers['Content-Type'].startswith(media)
        status, _, _ = request(url + 'group_state?group_id=g1')
        assert status in (401, 404)
        status, _, _ = request(url + 'index?page=missing')
        assert status == 400
        status, _, _ = request(url + 'index?group_id=missing')
        assert status == 404
