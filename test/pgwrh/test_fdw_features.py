"""Collect managed-topology cases supplied by the selected FDW feature graph.

Keeping feature-specific cases in the shared FDW changes makes adding/removing a
feature parent add/remove its tests together with its implementation.
"""
import os
from pathlib import Path
import re
import runpy
import subprocess


pg_config = os.environ.get('PG_CONFIG', 'pg_config')
version = subprocess.check_output([pg_config, '--version'], text=True)
major = re.search(r'PostgreSQL (\d+)', version)[1]
fdw = Path(__file__).resolve().parents[2] / 'pgwrh_fdw' / major
for suite in sorted(fdw.glob('managed_test_*.py')):
    for name, value in runpy.run_path(str(suite)).items():
        if name.startswith('test_') and callable(value):
            if name in globals():
                raise RuntimeError(f'duplicate managed FDW test: {name}')
            globals()[name] = value
