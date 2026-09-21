"""Exercise the independently installed extension in disposable databases."""
import os
from pathlib import Path

import pytest
from testgres import get_new_node

ROOT = Path(__file__).resolve().parents[2]
STAGE = ROOT / '.build/test-stage'


@pytest.fixture
def node():
    with get_new_node('gist-extra', bin_dir=os.environ.get('PGWRH_TEST_BIN_DIR')) as server:
        server.init()
        server.append_conf('postgresql.conf', f"""
listen_addresses = '127.0.0.1'
unix_socket_directories = '{server.base_dir}'
dynamic_library_path = '{STAGE}:$libdir'
extension_control_path = '{STAGE}:$system'
statement_timeout = '15s'
""")
        server.start()
        with server.connect(autocommit=True) as connection:
            connection.execute('CREATE EXTENSION pgwrh_gist_extra CASCADE')
            try:
                yield connection
            finally:
                print(Path(server.pg_log_file).read_text())
