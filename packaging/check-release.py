#!/usr/bin/env python3
"""Reject an inconsistent release before building or publishing any artifacts."""
import argparse
from pathlib import Path
import re

from release_version import release_metadata, validate_release

root = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--tag', default='')
parser.add_argument('--github-prerelease', choices=('true', 'false'))
parser.add_argument('--github-output', type=Path)
args = parser.parse_args()
version = (root / 'VERSION').read_text().strip()
metadata = release_metadata(version)
package_version = metadata['package_version']
validate_release(metadata, args.tag,
                 None if args.github_prerelease is None else args.github_prerelease == 'true')
dependencies = {
    'pgwrh': {'pg_background', 'pgwrh_fdw'},
    'pgwrh_ui': {'pgwrh'},
    'pgwrh_fdw/18': set(),
    'pgwrh_fdw/19': set(),
    'pgwrh_wait': set(),
    'pgwrh_gist_extra': {'btree_gist'},
}
for directory, expected_dependencies in dependencies.items():
    name = directory.split("/")[0]
    control = (root / directory / f'{name}.control').read_text()
    assert re.search(r"^default_version\s*=\s*'" + re.escape(version) + "'", control, re.M), name
    requires = re.search(r"^requires\s*=\s*'([^']*)'", control, re.M)
    actual_dependencies = (
        {item.strip() for item in requires[1].split(',') if item.strip()}
        if requires else set()
    )
    assert actual_dependencies == expected_dependencies, f'Unexpected extension dependencies: {name}'
    scripts = sorted(p.name for p in (root / directory).glob(f'{name}--*.sql'))
    expected = [] if name in ('pgwrh', 'pgwrh_ui') else [f'{name}--{version}.sql']
    assert scripts == expected, f'Unexpected install/upgrade scripts: {scripts}'
checks = {
    'nix/pgwrh.nix': f'version = "{version}";',
    'packaging/rpm/pgwrh.spec': f'Version:        {package_version}',
    'packaging/deb/debian/changelog': f'pgwrh ({package_version}-1)',
    'packaging/container/Dockerfile': f'org.opencontainers.image.version="{version}"',
    'examples/compose/compose.yaml': f'pgwrh:{version}-pg18',
    'test/packaging/installed.sql': f"extversion = '{version}'",
    'pgwrh_ui/Makefile': f'EXTVERSION = {version}',
    'pgwrh_wait/Makefile': f'DATA = pgwrh_wait--{version}.sql',
    'pgwrh_gist_extra/Makefile': f'DATA = pgwrh_gist_extra--{version}.sql',
}
for major in ('18', '19'):
    checks[f'pgwrh_fdw/{major}/postgres_fdw.c'] = f'.version = "{version}"'
    checks[f'pgwrh_fdw/{major}/Makefile'] = f'DATA = pgwrh_fdw--{version}.sql'
for path, expected in checks.items():
    assert expected in (root / path).read_text(), f'{path} differs from VERSION'
assert f'%global upstream_version {version}' in (root / 'packaging/rpm/pgwrh.spec').read_text()
assert (root / f'docs/releases/{version}.md').is_file(), 'Missing release notes'
for name in ('pgwrh', 'pgwrh_ui', 'pgwrh_fdw', 'pgwrh_wait', 'pgwrh_gist_extra'):
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
if args.github_output:
    with args.github_output.open('a') as output:
        for key, value in metadata.items():
            output.write(f'{key}={str(value).lower() if isinstance(value, bool) else value}\n')
print(version)
