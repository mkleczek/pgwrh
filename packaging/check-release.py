#!/usr/bin/env python3
"""Reject an inconsistent release before building or publishing any artifacts."""
import argparse
from pathlib import Path
import re

root = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--tag', default='')
args = parser.parse_args()
version = (root / 'VERSION').read_text().strip()
assert re.fullmatch(r'\d+\.\d+\.\d+', version), 'Invalid VERSION'
assert not args.tag or args.tag == f'v{version}', 'Release tag differs from VERSION'
dependencies = {
    'pgwrh': {'pg_background', 'pgwrh_fdw'},
    'pgwrh_ui': {'pgwrh'},
    'pgwrh_fdw': set(),
    'pgwrh_wait': set(),
}
for name, expected_dependencies in dependencies.items():
    control = (root / name / f'{name}.control').read_text()
    assert re.search(r"^default_version\s*=\s*'" + re.escape(version) + "'", control, re.M), name
    requires = re.search(r"^requires\s*=\s*'([^']*)'", control, re.M)
    actual_dependencies = (
        {item.strip() for item in requires[1].split(',') if item.strip()}
        if requires else set()
    )
    assert actual_dependencies == expected_dependencies, f'Unexpected extension dependencies: {name}'
    scripts = sorted(p.name for p in (root / name).glob(f'{name}--*.sql'))
    expected = [] if name in ('pgwrh', 'pgwrh_ui') else [f'{name}--{version}.sql']
    assert scripts == expected, f'Unexpected install/upgrade scripts: {scripts}'
checks = {
    'nix/pgwrh.nix': f'version = "{version}";',
    'packaging/rpm/pgwrh.spec': f'Version:        {version}',
    'packaging/deb/debian/changelog': f'pgwrh ({version}-1)',
    'packaging/container/Dockerfile': f'org.opencontainers.image.version="{version}"',
    'examples/compose/compose.yaml': f'pgwrh:{version}-pg18',
    'test/packaging/installed.sql': f"extversion = '{version}'",
    'pgwrh_fdw/pgwrh_fdw.c': f'.version = "{version}"',
    'pgwrh_ui/Makefile': f'EXTVERSION = {version}',
}
for path, expected in checks.items():
    assert expected in (root / path).read_text(), f'{path} differs from VERSION'
for name in ('pgwrh', 'pgwrh_ui', 'pgwrh_fdw', 'pgwrh_wait'):
    assert f'{name}--{version}.sql' in (root / 'packaging/deb/debian/postgresql-18-pgwrh.install').read_text()
for path, expected in {
    'packaging/deb/debian/postgresql-18-pgwrh.install': 'usr/share/postgresql/18/pgwrh_ui',
    'packaging/rpm/pgwrh.spec': '%{pginstdir}/share/pgwrh_ui/',
    'nix/pgwrh.nix': '../pgwrh_ui',
    '.github/workflows/release.yml': "'pgwrh_ui/**'",
}.items():
    assert expected in (root / path).read_text(), f'{path} omits the controller UI'
for name in ('README.md', 'readonly.sql', 'operator.sql', 'postgrest.conf', 'vendor/HTMX-LICENSE'):
    assert (root / 'pgwrh_ui' / name).is_file(), f'Missing UI deployment file: {name}'
print(version)
