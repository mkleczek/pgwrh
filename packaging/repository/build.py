#!/usr/bin/env python3
"""Build signed APT/YUM repositories from tested release artifacts (never uploads)."""
import argparse
import gzip
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from release_version import release_metadata


def run(*args, cwd=None):
    return subprocess.check_output(args, cwd=cwd, text=True)


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('artifacts', type=Path)
parser.add_argument('output', type=Path)
parser.add_argument('--version', required=True, help='Upstream version of every package in the repository')
parser.add_argument('--key', required=True, help='Full GPG signing-key fingerprint; key must already be imported')
args = parser.parse_args()
metadata = release_metadata(args.version)
package_version = metadata['package_version']
if not re.fullmatch(r'[A-Fa-f0-9]{40,64}', args.key):
    parser.error('--key must be a full GPG fingerprint')
if args.output.exists():
    parser.error('output must not already exist')
source = args.artifacts.resolve()
output = args.output.resolve()
output.mkdir(parents=True)


def sign(path, *, clear=False):
    destination = path.with_name('InRelease') if clear else Path(str(path) + '.asc')
    subprocess.run(['gpg', '--batch', '--yes', '--local-user', args.key, '--armor',
                    '--output', str(destination), '--clearsign' if clear else '--detach-sign',
                    str(path)], check=True)


apt_targets = set()
rpm_targets = set()
for package in sorted(source.rglob('*')):
    if package.suffix == '.deb':
        name, version, arch = run('dpkg-deb', '-f', str(package), 'Package', 'Version', 'Architecture').splitlines()
        # Multiple requested fields are returned with their field-name prefixes.
        name, version, arch = [value.split(': ', 1)[-1] for value in (name, version, arch)]
        if name != 'postgresql-18-pgwrh':
            continue
        if not version.startswith(package_version + '-1+'):
            raise ValueError(f'Package version does not match {args.version}: {package.name}')
        distro = version.rsplit('+', 1)[-1]
        if distro not in ('noble', 'resolute', 'trixie') or arch not in ('amd64', 'arm64'):
            raise ValueError(f'Unsupported DEB target: {package.name}')
        target = output / 'apt' / 'pool' / distro / arch
        target.mkdir(parents=True, exist_ok=True)
        shutil.copy2(package, target)
        apt_targets.add((distro, arch))
    elif package.suffix == '.rpm' and not package.name.endswith('.src.rpm'):
        name, version, release, arch = run('rpm', '-qp', '--qf', '%{NAME}\n%{VERSION}\n%{RELEASE}\n%{ARCH}', str(package)).splitlines()
        if name not in ('pgwrh_18', 'pgwrh_18-llvmjit'):
            continue
        if version != package_version:
            raise ValueError(f'Package version does not match {args.version}: {package.name}')
        if not release.endswith('.el9') or arch not in ('x86_64', 'aarch64'):
            raise ValueError(f'Unsupported RPM target: {package.name}')
        target = output / 'rpm' / 'el9' / arch
        target.mkdir(parents=True, exist_ok=True)
        copied = target / package.name
        shutil.copy2(package, copied)
        subprocess.run(['rpmsign', '--define', f'_gpg_name {args.key}',
                        '--define', f'_gpg_path {os.environ["GNUPGHOME"]}',
                        '--define', '__gpg /usr/bin/gpg', '--addsign', str(copied)], check=True)
        rpm_targets.add(target)

if not apt_targets and not rpm_targets:
    raise ValueError('No supported binary packages were found')
for distro, arch in sorted(apt_targets):
    apt = output / 'apt'
    index = apt / 'dists' / distro / 'main' / f'binary-{arch}'
    index.mkdir(parents=True)
    packages = run('apt-ftparchive', 'packages', f'pool/{distro}/{arch}', cwd=apt).encode()
    (index / 'Packages').write_bytes(packages)
    (index / 'Packages.gz').write_bytes(gzip.compress(packages, mtime=0))
for distro in sorted({distro for distro, _ in apt_targets}):
    apt = output / 'apt'
    release = apt / 'dists' / distro / 'Release'
    architectures = ' '.join(sorted(arch for dist, arch in apt_targets if dist == distro))
    release.write_text(run('apt-ftparchive',
        '-o', 'APT::FTPArchive::Release::Origin=pgwrh',
        '-o', 'APT::FTPArchive::Release::Label=pgwrh' + (' prerelease' if metadata['prerelease'] else ''),
        '-o', f'APT::FTPArchive::Release::Suite={distro}',
        '-o', f'APT::FTPArchive::Release::Codename={distro}',
        '-o', f'APT::FTPArchive::Release::Architectures={architectures}',
        '-o', 'APT::FTPArchive::Release::Components=main',
        'release', f'dists/{distro}', cwd=apt))
    sign(release, clear=True)
    sign(release)
    # APT's detached signature convention is Release.gpg.
    Path(str(release) + '.asc').rename(release.with_name('Release.gpg'))
for target in sorted(rpm_targets):
    subprocess.run(['createrepo_c', str(target)], check=True)
    sign(target / 'repodata' / 'repomd.xml')
(output / 'pgwrh.asc').write_text(run('gpg', '--armor', '--export', args.key))
(output / '.nojekyll').touch()
(output / 'index.html').write_text('<!doctype html><title>pgwrh packages</title>'
    '<h1>pgwrh PostgreSQL 18 packages</h1><p>Signed APT and YUM repositories.</p>'
    '<p><a href="pgwrh.asc">Signing key</a> · '
    '<a href="https://github.com/mkleczek/pgwrh/blob/main/docs/packages.md">Installation instructions</a></p>')
print(output)
